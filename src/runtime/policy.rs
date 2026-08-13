//! Owner-only inbound access and safe workspace validation.
//!
//! A validated workspace is only a point-in-time filesystem observation.
//! Callers must validate it again immediately before every Codex RPC that
//! consumes a cwd, then compare the returned canonical path before reuse.

use std::env;
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::codex::types::{ApprovalPolicy, SandboxMode};
use crate::config::{BridgeConfig, ConfigError};
use crate::lark::api::ChatMode;
use crate::lark::normalize::InboundEvent;

/// Static access result that is safe to log.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccessDecision {
    Allow,
    DenyNotOwner,
    DenyMissingMention,
    DenyWorkspace { reason: &'static str },
}

/// A workspace validation failure without the untrusted requested path.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum WorkspaceRejection {
    #[error("workspace path must be absolute")]
    Relative,
    #[error("workspace path is inaccessible")]
    Inaccessible,
    #[error("workspace path is not a directory")]
    NotDirectory,
    #[error("workspace path is a filesystem root")]
    FilesystemRoot,
    #[error("workspace path is the home root")]
    HomeRoot,
    #[error("workspace path is under a protected system tree")]
    SystemTree,
    #[error("workspace path is under the protected temporary tree")]
    TempTree,
    #[error("workspace path is under Desktop or Downloads")]
    DesktopOrDownloads,
    #[error("workspace path is outside configured allow roots")]
    OutsideAllowRoots,
}

/// Canonical platform paths used for pure workspace classification.
#[derive(Clone)]
pub struct PlatformRoots {
    pub home: PathBuf,
    pub temp: PathBuf,
    pub system_trees: Vec<PathBuf>,
    pub desktop: Option<PathBuf>,
    pub downloads: Option<PathBuf>,
}

impl fmt::Debug for PlatformRoots {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PlatformRoots")
            .field("system_tree_count", &self.system_trees.len())
            .field("desktop_present", &self.desktop.is_some())
            .field("downloads_present", &self.downloads.is_some())
            .finish_non_exhaustive()
    }
}

impl PlatformRoots {
    /// Builds roots for deterministic policy tests or platform adapters.
    ///
    /// # Errors
    ///
    /// Returns a static rejection when a required supplied root is absent or
    /// not a directory.
    pub fn new(
        home: &Path,
        temp: &Path,
        system_trees: Vec<PathBuf>,
        desktop: Option<PathBuf>,
        downloads: Option<PathBuf>,
    ) -> Result<Self, WorkspaceRejection> {
        let home = canonical_directory(home)?;
        let temp = canonical_directory(temp)?;
        let mut canonical_system_trees = Vec::with_capacity(system_trees.len());
        for root in system_trees {
            canonical_system_trees.push(canonical_directory(&root)?);
        }
        canonical_system_trees.sort();
        canonical_system_trees.dedup();
        let desktop = desktop.map(|path| canonical_directory(&path)).transpose()?;
        let downloads = downloads
            .map(|path| canonical_directory(&path))
            .transpose()?;
        Ok(Self {
            home,
            temp,
            system_trees: canonical_system_trees,
            desktop,
            downloads,
        })
    }

    pub(crate) fn discover() -> Result<Self, WorkspaceRejection> {
        let home = home_directory().ok_or(WorkspaceRejection::Inaccessible)?;
        let temp = env::temp_dir();
        let desktop = home.join("Desktop");
        let downloads = home.join("Downloads");
        let system_trees = discovered_system_trees();
        Self::new(
            &home,
            &temp,
            system_trees,
            directory_if_present(desktop),
            directory_if_present(downloads),
        )
    }
}

/// Stable opaque policy identity used to prevent unsafe thread reuse.
#[derive(Clone, Eq, PartialEq)]
pub struct PolicyFingerprint(String);

impl PolicyFingerprint {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for PolicyFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("PolicyFingerprint")
            .field(&self.0)
            .finish()
    }
}

/// Immutable effective access policy.
#[derive(Clone)]
pub struct AccessPolicy {
    owners: Vec<String>,
    allow_roots: Vec<PathBuf>,
    roots: PlatformRoots,
    sandbox: SandboxMode,
    approval_policy: ApprovalPolicy,
    network_access: bool,
}

impl fmt::Debug for AccessPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let approval_policy_kind = match &self.approval_policy {
            ApprovalPolicy::Named(_) => "named",
            ApprovalPolicy::Granular { .. } => "granular",
        };
        formatter
            .debug_struct("AccessPolicy")
            .field("owner_count", &self.owners.len())
            .field("allow_root_count", &self.allow_roots.len())
            .field("platform_roots", &self.roots)
            .field("sandbox", &self.sandbox)
            .field("approval_policy_kind", &approval_policy_kind)
            .field("network_access", &self.network_access)
            .finish()
    }
}

impl AccessPolicy {
    /// Constructs a production policy after applying all config validation.
    ///
    /// # Errors
    ///
    /// Returns a static configuration error when policy roots cannot be
    /// discovered or configuration validation fails.
    pub fn from_config(config: &BridgeConfig) -> Result<Self, ConfigError> {
        let roots = PlatformRoots::discover().map_err(|_| ConfigError::PlatformRoots)?;
        Self::with_platform_roots(config, &roots)
    }

    /// Constructs a policy with explicitly discovered roots. This keeps path
    /// classification deterministic in tests without weakening production
    /// root discovery.
    ///
    /// # Errors
    ///
    /// Returns a static configuration error when the supplied policy fails
    /// validation against the supplied platform roots.
    pub fn with_platform_roots(
        config: &BridgeConfig,
        platform_roots: &PlatformRoots,
    ) -> Result<Self, ConfigError> {
        let mut config = config.clone();
        config.validate_with_platform_roots(platform_roots)?;
        Ok(Self::from_prepared_config(&config, platform_roots))
    }

    pub(crate) fn prepare_config(
        config: &mut BridgeConfig,
        roots: &PlatformRoots,
    ) -> Result<(), ConfigError> {
        let mut canonical_roots = Vec::with_capacity(config.workspace.allow_roots.len());
        for allow_root in &config.workspace.allow_roots {
            if !allow_root.is_absolute() {
                return Err(ConfigError::InvalidAllowRoot);
            }
            let allow_root =
                canonical_directory(allow_root).map_err(|_| ConfigError::InvalidAllowRoot)?;
            if classify_hard_deny(&allow_root, roots).is_some() {
                return Err(ConfigError::InvalidAllowRoot);
            }
            canonical_roots.push(allow_root);
        }
        canonical_roots.sort();
        canonical_roots.dedup();
        config.workspace.allow_roots = canonical_roots;
        Ok(())
    }

    pub(crate) fn from_prepared_config(config: &BridgeConfig, roots: &PlatformRoots) -> Self {
        Self {
            owners: config.owners.clone(),
            allow_roots: config.workspace.allow_roots.clone(),
            roots: roots.clone(),
            sandbox: config.codex.sandbox,
            approval_policy: config.codex.approval_policy.clone(),
            network_access: config.workspace.network_access,
        }
    }

    /// Gates inbound events: owners only, with a direct bot mention required
    /// in group and topic chats. P2P messages are mention-exempt.
    #[must_use]
    pub fn decide(&self, event: &InboundEvent) -> AccessDecision {
        if !self.owners.iter().any(|owner| owner == &event.sender_id) {
            return AccessDecision::DenyNotOwner;
        }
        if event.chat_type != ChatMode::P2p && !event.mentions_bot {
            return AccessDecision::DenyMissingMention;
        }
        AccessDecision::Allow
    }

    /// Canonicalizes and validates one requested workspace without creating
    /// filesystem entries. A successful result is only a point-in-time check;
    /// validate again immediately before a Codex RPC consumes the cwd.
    ///
    /// # Errors
    ///
    /// Returns a path-free rejection classification when the candidate is not
    /// an allowed, existing directory.
    pub fn validate_workspace(&self, path: &Path) -> Result<PathBuf, WorkspaceRejection> {
        if !path.is_absolute() {
            return Err(WorkspaceRejection::Relative);
        }
        let canonical = fs::canonicalize(path).map_err(|_| WorkspaceRejection::Inaccessible)?;
        if !fs::metadata(&canonical)
            .map_err(|_| WorkspaceRejection::Inaccessible)?
            .is_dir()
        {
            return Err(WorkspaceRejection::NotDirectory);
        }
        if let Some(rejection) = classify_hard_deny(&canonical, &self.roots) {
            return Err(rejection);
        }
        if self
            .allow_roots
            .iter()
            .any(|allow_root| canonical.starts_with(allow_root))
        {
            return Ok(canonical);
        }
        Err(WorkspaceRejection::OutsideAllowRoots)
    }

    /// Fingerprints all policy dimensions used to authorize a Codex thread.
    /// The cwd is revalidated first, so aliases hash identically and invalid
    /// workspaces cannot acquire a reusable identity.
    ///
    /// # Errors
    ///
    /// Returns the same path-free rejection as workspace validation when the
    /// cwd cannot safely be fingerprinted.
    pub fn fingerprint(&self, cwd: &Path) -> Result<PolicyFingerprint, WorkspaceRejection> {
        let cwd = self.validate_workspace(cwd)?;
        let mut hash = Sha256::new();
        hash.update(b"lark-codex-policy\0v1");
        write_part(&mut hash, b"os", env::consts::OS.as_bytes());
        write_part(&mut hash, b"cwd", cwd.as_os_str().as_encoded_bytes());
        hash.update([b's', sandbox_tag(self.sandbox)]);
        match &self.approval_policy {
            ApprovalPolicy::Named(name) => {
                write_part(&mut hash, b"approval:named", name.as_bytes());
            }
            ApprovalPolicy::Granular { granular } => {
                hash.update(b"approval:granular");
                hash.update([
                    u8::from(granular.mcp_elicitations),
                    u8::from(granular.rules),
                    u8::from(granular.sandbox_approval),
                    u8::from(granular.request_permissions),
                    u8::from(granular.skill_approval),
                ]);
            }
        }
        hash.update([b'n', u8::from(self.network_access)]);
        let digest = hash.finalize();
        let mut encoded = String::with_capacity(32);
        for byte in &digest[..16] {
            use fmt::Write as _;
            write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
        }
        Ok(PolicyFingerprint(encoded))
    }
}

fn canonical_directory(path: &Path) -> Result<PathBuf, WorkspaceRejection> {
    let canonical = fs::canonicalize(path).map_err(|_| WorkspaceRejection::Inaccessible)?;
    if fs::metadata(&canonical)
        .map_err(|_| WorkspaceRejection::Inaccessible)?
        .is_dir()
    {
        Ok(canonical)
    } else {
        Err(WorkspaceRejection::NotDirectory)
    }
}

fn classify_hard_deny(path: &Path, roots: &PlatformRoots) -> Option<WorkspaceRejection> {
    if path.parent().is_none() {
        return Some(WorkspaceRejection::FilesystemRoot);
    }
    if path == roots.home {
        return Some(WorkspaceRejection::HomeRoot);
    }
    if roots.system_trees.iter().any(|root| path.starts_with(root)) {
        return Some(WorkspaceRejection::SystemTree);
    }
    if path.starts_with(&roots.temp) {
        return Some(WorkspaceRejection::TempTree);
    }
    if roots
        .desktop
        .as_ref()
        .is_some_and(|desktop| path.starts_with(desktop))
        || roots
            .downloads
            .as_ref()
            .is_some_and(|downloads| path.starts_with(downloads))
    {
        return Some(WorkspaceRejection::DesktopOrDownloads);
    }
    None
}

fn write_part(hash: &mut Sha256, tag: &[u8], value: &[u8]) {
    hash.update((tag.len() as u64).to_be_bytes());
    hash.update(tag);
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}

const fn sandbox_tag(sandbox: SandboxMode) -> u8 {
    match sandbox {
        SandboxMode::ReadOnly => 1,
        SandboxMode::WorkspaceWrite => 2,
        SandboxMode::DangerFullAccess => 3,
    }
}

fn home_directory() -> Option<PathBuf> {
    #[cfg(windows)]
    {
        env::var_os("USERPROFILE")
            .filter(|value| !value.is_empty())
            .or_else(|| env::var_os("HOME").filter(|value| !value.is_empty()))
            .map(PathBuf::from)
    }
    #[cfg(not(windows))]
    {
        env::var_os("HOME")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
    }
}

fn directory_if_present(path: PathBuf) -> Option<PathBuf> {
    match fs::metadata(&path) {
        Ok(metadata) if metadata.is_dir() => Some(path),
        _ => None,
    }
}

fn discovered_system_trees() -> Vec<PathBuf> {
    #[cfg(windows)]
    let candidates = [
        env::var_os("SystemRoot").map(PathBuf::from),
        env::var_os("ProgramFiles").map(PathBuf::from),
        env::var_os("ProgramFiles(x86)").map(PathBuf::from),
        env::var_os("ProgramData").map(PathBuf::from),
    ];
    #[cfg(target_os = "macos")]
    let candidates = [
        Some(PathBuf::from("/Applications")),
        Some(PathBuf::from("/Library")),
        Some(PathBuf::from("/System")),
        Some(PathBuf::from("/private")),
        Some(PathBuf::from("/Volumes")),
        Some(PathBuf::from("/bin")),
        Some(PathBuf::from("/boot")),
        Some(PathBuf::from("/dev")),
        Some(PathBuf::from("/etc")),
        Some(PathBuf::from("/lib")),
        Some(PathBuf::from("/lib64")),
        Some(PathBuf::from("/proc")),
        Some(PathBuf::from("/run")),
        Some(PathBuf::from("/sbin")),
        Some(PathBuf::from("/sys")),
        Some(PathBuf::from("/usr")),
        Some(PathBuf::from("/var")),
    ];
    #[cfg(all(not(windows), not(target_os = "macos")))]
    let candidates = [
        Some(PathBuf::from("/bin")),
        Some(PathBuf::from("/boot")),
        Some(PathBuf::from("/dev")),
        Some(PathBuf::from("/etc")),
        Some(PathBuf::from("/lib")),
        Some(PathBuf::from("/lib64")),
        Some(PathBuf::from("/proc")),
        Some(PathBuf::from("/run")),
        Some(PathBuf::from("/sbin")),
        Some(PathBuf::from("/sys")),
        Some(PathBuf::from("/usr")),
        Some(PathBuf::from("/var")),
    ];
    candidates
        .into_iter()
        .flatten()
        .filter_map(|path| canonical_directory(&path).ok())
        .collect()
}
