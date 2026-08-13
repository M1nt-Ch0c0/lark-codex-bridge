//! Owner-only inbound access and safe workspace validation.
//!
//! A validated workspace is only a point-in-time filesystem observation.
//! Callers must validate it again immediately before every Codex RPC that
//! consumes a cwd, then compare the returned canonical path before reuse.

use std::env;
#[cfg(any(windows, test))]
use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::codex::types::{ApprovalPolicy, SandboxMode};
use crate::config::{BridgeConfig, ConfigError};
use crate::lark::api::ChatMode;
use crate::lark::normalize::InboundEvent;
use crate::limits::{
    MAX_CONFIG_ALLOW_ROOT_BYTES, MAX_CONFIG_ALLOW_ROOTS, MAX_PLATFORM_PROTECTED_ROOT_BYTES,
    MAX_PLATFORM_PROTECTED_ROOTS,
};

/// Static access result that is safe to log.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum AccessDecision {
    Allow,
    DenyNotOwner,
    DenyMissingMention,
    DenyWorkspace { reason: &'static str },
}

impl fmt::Debug for AccessDecision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Allow => "Allow",
            Self::DenyNotOwner => "DenyNotOwner",
            Self::DenyMissingMention => "DenyMissingMention",
            Self::DenyWorkspace { .. } => "DenyWorkspace",
        })
    }
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
pub(crate) struct PlatformRoots {
    home: PathBuf,
    temp_trees: Vec<PathBuf>,
    system_trees: Vec<PathBuf>,
    desktop_download_trees: Vec<PathBuf>,
}

impl fmt::Debug for PlatformRoots {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PlatformRoots")
            .field("system_tree_count", &self.system_trees.len())
            .field("temp_tree_count", &self.temp_trees.len())
            .field(
                "desktop_download_tree_count",
                &self.desktop_download_trees.len(),
            )
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
    pub(crate) fn new(
        home: &Path,
        temp_trees: Vec<PathBuf>,
        system_trees: Vec<PathBuf>,
        desktop_download_trees: Vec<PathBuf>,
    ) -> Result<Self, WorkspaceRejection> {
        if !home.is_absolute() {
            return Err(WorkspaceRejection::Inaccessible);
        }
        let home = canonical_directory(home)?;
        let temp_trees = canonicalize_bounded_roots(temp_trees)?;
        let system_trees = canonicalize_bounded_roots(system_trees)?;
        let mut desktop_download_trees = canonicalize_bounded_roots(desktop_download_trees)?;
        desktop_download_trees.retain(|root| root != &home);
        Ok(Self {
            home,
            temp_trees,
            system_trees,
            desktop_download_trees,
        })
    }

    pub(crate) fn discover() -> Result<Self, WorkspaceRejection> {
        let home = home_directory().ok_or(WorkspaceRejection::Inaccessible)?;
        let temp_trees = discovered_temp_trees()?;
        let desktop_download_trees = discovered_desktop_download_trees(&home)?;
        let system_trees = discovered_system_trees()?;
        Self::new(&home, temp_trees, system_trees, desktop_download_trees)
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
    pub(crate) fn with_platform_roots(
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
        validate_path_collection_bounds(&canonical_roots)
            .map_err(|()| ConfigError::AllowRootsTooLarge)?;
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
        let cwd = stable_path_bytes(&cwd);
        Ok(fingerprint_v1(
            env::consts::OS.as_bytes(),
            &cwd,
            self.sandbox,
            &self.approval_policy,
            self.network_access,
        ))
    }
}

const POLICY_FINGERPRINT_VERSION: &[u8] = b"lark-codex-policy-v1";

fn fingerprint_v1(
    platform: &[u8],
    cwd: &[u8],
    sandbox: SandboxMode,
    approval_policy: &ApprovalPolicy,
    network_access: bool,
) -> PolicyFingerprint {
    let mut hash = Sha256::new();
    write_part(&mut hash, b"version", POLICY_FINGERPRINT_VERSION);
    write_part(&mut hash, b"platform", platform);
    write_part(&mut hash, b"cwd", cwd);
    write_part(&mut hash, b"sandbox", &[sandbox_tag(sandbox)]);
    match approval_policy {
        ApprovalPolicy::Named(name) => {
            write_part(&mut hash, b"approval-kind", b"named");
            write_part(&mut hash, b"approval-value", name.as_bytes());
        }
        ApprovalPolicy::Granular { granular } => {
            write_part(&mut hash, b"approval-kind", b"granular");
            write_part(
                &mut hash,
                b"approval-value",
                &[
                    u8::from(granular.mcp_elicitations),
                    u8::from(granular.rules),
                    u8::from(granular.sandbox_approval),
                    u8::from(granular.request_permissions),
                    u8::from(granular.skill_approval),
                ],
            );
        }
    }
    write_part(&mut hash, b"network", &[u8::from(network_access)]);
    let digest = hash.finalize();
    let mut encoded = String::with_capacity(32);
    for byte in &digest[..16] {
        use fmt::Write as _;
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    PolicyFingerprint(encoded)
}

#[cfg(unix)]
fn stable_path_bytes(path: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt as _;

    path.as_os_str().as_bytes().to_vec()
}

#[cfg(windows)]
fn stable_path_bytes(path: &Path) -> Vec<u8> {
    use std::os::windows::ffi::OsStrExt as _;

    path.as_os_str()
        .encode_wide()
        .flat_map(u16::to_le_bytes)
        .collect()
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

fn canonicalize_bounded_roots(roots: Vec<PathBuf>) -> Result<Vec<PathBuf>, WorkspaceRejection> {
    validate_platform_root_input_bounds(&roots)?;
    if roots.iter().any(|root| !root.is_absolute()) {
        return Err(WorkspaceRejection::Inaccessible);
    }
    let mut canonical = Vec::with_capacity(roots.len());
    for root in roots {
        canonical.push(canonical_directory(&root)?);
    }
    canonical.sort();
    canonical.dedup();
    validate_platform_root_input_bounds(&canonical)?;
    Ok(canonical)
}

fn validate_platform_root_input_bounds(roots: &[PathBuf]) -> Result<(), WorkspaceRejection> {
    if roots.len() > MAX_PLATFORM_PROTECTED_ROOTS
        || encoded_path_bytes(roots) > MAX_PLATFORM_PROTECTED_ROOT_BYTES
    {
        return Err(WorkspaceRejection::Inaccessible);
    }
    Ok(())
}

fn validate_path_collection_bounds(roots: &[PathBuf]) -> Result<(), ()> {
    if roots.len() > MAX_CONFIG_ALLOW_ROOTS
        || encoded_path_bytes(roots) > MAX_CONFIG_ALLOW_ROOT_BYTES
    {
        return Err(());
    }
    Ok(())
}

fn encoded_path_bytes(paths: &[PathBuf]) -> usize {
    paths.iter().fold(0_usize, |total, path| {
        total.saturating_add(path.as_os_str().as_encoded_bytes().len())
    })
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
    if roots.temp_trees.iter().any(|root| path.starts_with(root)) {
        return Some(WorkspaceRejection::TempTree);
    }
    if roots
        .desktop_download_trees
        .iter()
        .any(|root| path.starts_with(root))
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
            .map(PathBuf::from)
            .or_else(|| {
                let drive = env::var_os("HOMEDRIVE").filter(|value| !value.is_empty())?;
                let path = env::var_os("HOMEPATH").filter(|value| !value.is_empty())?;
                Some(PathBuf::from(drive).join(path))
            })
    }
    #[cfg(not(windows))]
    {
        env::var_os("HOME")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
    }
}

fn discovered_temp_trees() -> Result<Vec<PathBuf>, WorkspaceRejection> {
    let environment_temp = env::temp_dir();
    if !environment_temp.is_absolute()
        || !matches!(fs::metadata(&environment_temp), Ok(metadata) if metadata.is_dir())
    {
        return Err(WorkspaceRejection::Inaccessible);
    }
    let mut candidates = vec![environment_temp];
    #[cfg(unix)]
    {
        candidates.push(PathBuf::from("/tmp"));
        candidates.push(PathBuf::from("/var/tmp"));
    }
    existing_directories(candidates)
}

fn discovered_desktop_download_trees(home: &Path) -> Result<Vec<PathBuf>, WorkspaceRejection> {
    #[cfg(windows)]
    {
        // Without a Known Folder API dependency (and with unsafe forbidden),
        // redirected Windows folders cannot be discovered reliably. Rejecting
        // profile Desktop/Downloads when present is a conservative baseline;
        // the explicit workspace allow-list remains the primary boundary.
        existing_directories([home.join("Desktop"), home.join("Downloads")])
    }
    #[cfg(not(windows))]
    {
        let source = read_xdg_user_directory_source(home)?;
        desktop_download_trees_from_xdg_source(home, source.as_deref())
    }
}

fn existing_directories(
    paths: impl IntoIterator<Item = PathBuf>,
) -> Result<Vec<PathBuf>, WorkspaceRejection> {
    let mut directories = Vec::new();
    for path in paths {
        match fs::metadata(&path) {
            Ok(metadata) if metadata.is_dir() => directories.push(path),
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(WorkspaceRejection::Inaccessible),
        }
    }
    Ok(directories)
}

#[cfg(not(windows))]
fn read_xdg_user_directory_source(home: &Path) -> Result<Option<String>, WorkspaceRejection> {
    let config_home = env::var_os("XDG_CONFIG_HOME")
        .filter(|value| !value.is_empty())
        .map_or_else(|| home.join(".config"), PathBuf::from);
    if !config_home.is_absolute() {
        return Err(WorkspaceRejection::Inaccessible);
    }
    let source = match fs::read_to_string(config_home.join("user-dirs.dirs")) {
        Ok(source) => source,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(WorkspaceRejection::Inaccessible),
    };
    if source.len() > crate::limits::MAX_XDG_USER_DIRS_BYTES {
        return Err(WorkspaceRejection::Inaccessible);
    }
    Ok(Some(source))
}

#[cfg(not(windows))]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
enum XdgUserDirectory {
    #[default]
    Unspecified,
    Disabled,
    Path(PathBuf),
}

#[cfg(not(windows))]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct XdgUserDirectoryConfig {
    desktop: XdgUserDirectory,
    downloads: XdgUserDirectory,
}

#[cfg(not(windows))]
fn parse_xdg_user_directory_config(
    home: &Path,
    source: &str,
) -> Result<XdgUserDirectoryConfig, WorkspaceRejection> {
    let mut config = XdgUserDirectoryConfig::default();
    for line in source.lines() {
        let Some((key, raw)) = line.split_once('=') else {
            continue;
        };
        let destination = match key.trim() {
            "XDG_DESKTOP_DIR" => &mut config.desktop,
            "XDG_DOWNLOAD_DIR" => &mut config.downloads,
            _ => continue,
        };
        let value = raw.trim();
        let Some(value) = value
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
        else {
            return Err(WorkspaceRejection::Inaccessible);
        };
        if value == "$HOME" {
            // The XDG user-dirs specification uses $HOME as the disabled
            // sentinel. It is not a request to protect the entire home tree.
            *destination = XdgUserDirectory::Disabled;
            continue;
        }
        if value.contains(['`', '$']) && !value.starts_with("$HOME/") {
            return Err(WorkspaceRejection::Inaccessible);
        }
        let path = if let Some(relative) = value.strip_prefix("$HOME/") {
            if relative.split('/').any(|component| component == "..") {
                return Err(WorkspaceRejection::Inaccessible);
            }
            home.join(relative)
        } else {
            let path = PathBuf::from(value);
            if !path.is_absolute() {
                return Err(WorkspaceRejection::Inaccessible);
            }
            path
        };
        *destination = XdgUserDirectory::Path(path);
    }
    Ok(config)
}

#[cfg(not(windows))]
fn desktop_download_trees_from_xdg_source(
    home: &Path,
    source: Option<&str>,
) -> Result<Vec<PathBuf>, WorkspaceRejection> {
    let config = source.map_or_else(
        || Ok(XdgUserDirectoryConfig::default()),
        |source| parse_xdg_user_directory_config(home, source),
    )?;
    let mut directories = Vec::with_capacity(2);
    for (directory, fallback) in [
        (config.desktop, home.join("Desktop")),
        (config.downloads, home.join("Downloads")),
    ] {
        match directory {
            XdgUserDirectory::Unspecified => {
                directories.extend(existing_directories([fallback])?);
            }
            XdgUserDirectory::Disabled => {}
            XdgUserDirectory::Path(path) => directories.push(canonical_directory(&path)?),
        }
    }
    Ok(directories)
}

#[cfg(any(windows, test))]
#[derive(Clone)]
struct WindowsSystemEnvironment {
    system_root: Option<OsString>,
    windir: Option<OsString>,
    program_files: Option<OsString>,
    program_files_x86: Option<OsString>,
    program_data: Option<OsString>,
}

#[cfg(windows)]
impl WindowsSystemEnvironment {
    fn read_production() -> Self {
        Self {
            system_root: env::var_os("SystemRoot"),
            windir: env::var_os("WINDIR"),
            program_files: env::var_os("ProgramFiles"),
            program_files_x86: env::var_os("ProgramFiles(x86)"),
            program_data: env::var_os("ProgramData"),
        }
    }
}

#[cfg(any(windows, test))]
fn windows_system_trees_from_environment(
    environment: &WindowsSystemEnvironment,
    is_64_bit: bool,
) -> Result<Vec<PathBuf>, WorkspaceRejection> {
    let windows = if environment.system_root.is_some() {
        required_environment_directory(environment.system_root.as_deref())?
    } else {
        required_environment_directory(environment.windir.as_deref())?
    };
    let program_files = required_environment_directory(environment.program_files.as_deref())?;
    let program_data = required_environment_directory(environment.program_data.as_deref())?;
    let mut directories = vec![windows, program_files];
    if is_64_bit {
        directories.push(required_environment_directory(
            environment.program_files_x86.as_deref(),
        )?);
    }
    directories.push(program_data);
    Ok(directories)
}

#[cfg(any(windows, test))]
fn required_environment_directory(value: Option<&OsStr>) -> Result<PathBuf, WorkspaceRejection> {
    let value = value
        .filter(|value| !value.is_empty())
        .ok_or(WorkspaceRejection::Inaccessible)?;
    let path = PathBuf::from(value);
    if !path.is_absolute() {
        return Err(WorkspaceRejection::Inaccessible);
    }
    canonical_directory(&path).map_err(|_| WorkspaceRejection::Inaccessible)
}

#[cfg(windows)]
fn discovered_system_trees() -> Result<Vec<PathBuf>, WorkspaceRejection> {
    windows_system_trees_from_environment(
        &WindowsSystemEnvironment::read_production(),
        cfg!(target_pointer_width = "64"),
    )
}

#[cfg(not(windows))]
fn discovered_system_trees() -> Result<Vec<PathBuf>, WorkspaceRejection> {
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
    if candidates.iter().flatten().any(|path| !path.is_absolute()) {
        return Err(WorkspaceRejection::Inaccessible);
    }
    existing_directories(candidates.into_iter().flatten())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;
    use crate::codex::types::GranularApprovalPolicy;
    use tempfile::TempDir;

    fn scratch() -> TempDir {
        tempfile::Builder::new()
            .prefix("runtime-policy-unit-")
            .tempdir_in(env!("CARGO_MANIFEST_DIR"))
            .expect("repository scratch directory should be created")
    }

    fn injected_roots(base: &Path) -> PlatformRoots {
        let home = base.join("home");
        let temp = base.join("temp");
        let temp_alias = base.join("temp-alias");
        let system = base.join("system");
        let desktop = home.join("Desktop");
        let downloads = home.join("Downloads");
        for path in [&home, &temp, &temp_alias, &system, &desktop, &downloads] {
            fs::create_dir_all(path).expect("injected platform root should be created");
        }
        PlatformRoots::new(
            &home,
            vec![temp, temp_alias],
            vec![system],
            vec![desktop, downloads],
        )
        .expect("injected platform roots should canonicalize")
    }

    fn config(allow_root: PathBuf) -> BridgeConfig {
        let mut config = BridgeConfig::default();
        config.owners.push("ou_owner_123456".to_owned());
        config.workspace.allow_roots.push(allow_root);
        config
    }

    #[test]
    fn fingerprint_v1_encoding_has_a_stable_framed_golden_vector() {
        let approval = ApprovalPolicy::Granular {
            granular: GranularApprovalPolicy {
                mcp_elicitations: true,
                rules: false,
                sandbox_approval: true,
                request_permissions: false,
                skill_approval: true,
            },
        };

        let fingerprint = fingerprint_v1(
            b"test-os",
            b"/workspace",
            SandboxMode::DangerFullAccess,
            &approval,
            true,
        );

        assert_eq!(fingerprint.as_str(), "9442d343c7246355ce2f616f8f0ef418");
    }

    #[test]
    fn fingerprint_v1_frames_field_boundaries() {
        let approval = ApprovalPolicy::Named("never".to_owned());

        assert_ne!(
            fingerprint_v1(b"a", b"bc", SandboxMode::ReadOnly, &approval, false),
            fingerprint_v1(b"ab", b"c", SandboxMode::ReadOnly, &approval, false)
        );
    }

    #[test]
    fn classifier_hard_denies_protected_descendants_before_allow_roots() {
        let temp = scratch();
        let roots = injected_roots(temp.path());
        let broad = temp.path().to_path_buf();
        let policy = AccessPolicy::with_platform_roots(&config(broad), &roots)
            .expect("broad synthetic allow root should be safe itself");

        for (path, expected) in [
            (
                roots.system_trees[0].join("descendant"),
                WorkspaceRejection::SystemTree,
            ),
            (
                roots.temp_trees[0].join("descendant"),
                WorkspaceRejection::TempTree,
            ),
            (
                roots.temp_trees[1].join("descendant"),
                WorkspaceRejection::TempTree,
            ),
            (
                roots.desktop_download_trees[0].join("descendant"),
                WorkspaceRejection::DesktopOrDownloads,
            ),
            (
                roots.desktop_download_trees[1].join("descendant"),
                WorkspaceRejection::DesktopOrDownloads,
            ),
        ] {
            fs::create_dir_all(&path).expect("protected descendant should be created");
            assert_eq!(policy.validate_workspace(&path), Err(expected));
            assert!(AccessPolicy::with_platform_roots(&config(path), &roots).is_err());
        }

        let safe_home_child = roots.home.join("src/project");
        fs::create_dir_all(&safe_home_child).expect("safe home child should be created");
        let safe_policy =
            AccessPolicy::with_platform_roots(&config(safe_home_child.clone()), &roots)
                .expect("explicit safe home child should be allowed");
        assert_eq!(
            safe_policy.validate_workspace(&safe_home_child).unwrap(),
            fs::canonicalize(safe_home_child).unwrap()
        );
    }

    #[test]
    fn platform_root_collections_enforce_count_limit_before_retention() {
        let temp = scratch();
        let home = temp.path().join("home");
        fs::create_dir(&home).expect("home should be created");
        let too_many = vec![temp.path().to_path_buf(); MAX_PLATFORM_PROTECTED_ROOTS + 1];
        assert!(encoded_path_bytes(&too_many) <= MAX_PLATFORM_PROTECTED_ROOT_BYTES);
        assert!(PlatformRoots::new(&home, too_many, vec![], vec![]).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn platform_root_collections_enforce_raw_aggregate_bytes_before_canonical_dedup() {
        use std::os::unix::fs::symlink;

        let temp = scratch();
        let home = temp.path().join("home");
        let target = temp.path().join("target");
        fs::create_dir(&home).expect("home should be created");
        fs::create_dir(&target).expect("short canonical target should be created");
        let aliases = (0..MAX_PLATFORM_PROTECTED_ROOTS)
            .map(|index| {
                let alias = temp
                    .path()
                    .join(format!("raw-{index:02}-{}", "a".repeat(230)));
                symlink(&target, &alias).expect("long root alias should be created");
                alias
            })
            .collect::<Vec<_>>();
        assert_eq!(aliases.len(), MAX_PLATFORM_PROTECTED_ROOTS);
        assert!(
            aliases
                .iter()
                .all(|path| path.is_absolute() && path.is_dir())
        );
        assert!(aliases.iter().all(|path| {
            path.as_os_str().as_encoded_bytes().len() < MAX_PLATFORM_PROTECTED_ROOT_BYTES
        }));
        assert!(encoded_path_bytes(&aliases) > MAX_PLATFORM_PROTECTED_ROOT_BYTES);
        let mut canonical = aliases
            .iter()
            .map(|path| fs::canonicalize(path).expect("alias should canonicalize"))
            .collect::<Vec<_>>();
        canonical.sort();
        canonical.dedup();
        assert_eq!(canonical, [fs::canonicalize(&target).unwrap()]);
        assert!(encoded_path_bytes(&canonical) <= MAX_PLATFORM_PROTECTED_ROOT_BYTES);

        assert!(PlatformRoots::new(&home, aliases, vec![], vec![]).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn platform_root_collections_recheck_aggregate_bytes_after_canonicalization() {
        use std::os::unix::fs::symlink;

        let temp = scratch();
        let home = temp.path().join("home");
        fs::create_dir(&home).expect("home should be created");
        let deep_parent = temp
            .path()
            .join("canonical-targets")
            .join("x".repeat(200))
            .join("y".repeat(100));
        fs::create_dir_all(&deep_parent).expect("deep canonical parent should be created");
        let aliases = (0..MAX_PLATFORM_PROTECTED_ROOTS)
            .map(|index| {
                let target = deep_parent.join(format!("target-{index:02}"));
                fs::create_dir(&target).expect("canonical target should be created");
                let alias = temp.path().join(format!("a{index:02}"));
                symlink(&target, &alias).expect("short root alias should be created");
                alias
            })
            .collect::<Vec<_>>();
        assert_eq!(aliases.len(), MAX_PLATFORM_PROTECTED_ROOTS);
        assert!(
            aliases
                .iter()
                .all(|path| path.is_absolute() && path.is_dir())
        );
        assert!(encoded_path_bytes(&aliases) <= MAX_PLATFORM_PROTECTED_ROOT_BYTES);
        let canonical = aliases
            .iter()
            .map(|path| fs::canonicalize(path).expect("alias should canonicalize"))
            .collect::<Vec<_>>();
        assert!(encoded_path_bytes(&canonical) > MAX_PLATFORM_PROTECTED_ROOT_BYTES);

        assert!(PlatformRoots::new(&home, aliases, vec![], vec![]).is_err());
    }

    #[test]
    fn windows_system_discovery_rejects_missing_or_invalid_mandatory_roots() {
        let temp = scratch();
        let windows = temp.path().join("Windows");
        let program_files = temp.path().join("Program Files");
        let program_files_x86 = temp.path().join("Program Files (x86)");
        let program_data = temp.path().join("ProgramData");
        for directory in [&windows, &program_files, &program_files_x86, &program_data] {
            fs::create_dir(directory).expect("synthetic Windows root should be created");
        }
        let file = temp.path().join("not-a-directory");
        fs::write(&file, b"file").expect("synthetic file should be created");

        let valid = WindowsSystemEnvironment {
            system_root: Some(windows.clone().into_os_string()),
            windir: None,
            program_files: Some(program_files.clone().into_os_string()),
            program_files_x86: Some(program_files_x86.clone().into_os_string()),
            program_data: Some(program_data.clone().into_os_string()),
        };
        assert_eq!(
            windows_system_trees_from_environment(&valid, true)
                .expect("complete 64-bit Windows roots should validate")
                .len(),
            4
        );

        let missing_values = ["system_root", "windir", "program_files", "program_data"];
        for field in missing_values {
            let mut environment = valid.clone();
            match field {
                "system_root" => environment.system_root = None,
                "windir" => {
                    environment.system_root = None;
                    environment.windir = None;
                }
                "program_files" => environment.program_files = None,
                "program_data" => environment.program_data = None,
                _ => unreachable!(),
            }
            if field == "system_root" {
                environment.windir = Some(windows.clone().into_os_string());
                assert!(windows_system_trees_from_environment(&environment, true).is_ok());
            } else {
                assert_eq!(
                    windows_system_trees_from_environment(&environment, true),
                    Err(WorkspaceRejection::Inaccessible),
                    "missing {field} must fail closed"
                );
            }
        }

        let invalid_values = [
            OsString::new(),
            OsString::from("relative-root"),
            temp.path().join("missing-root").into_os_string(),
            file.into_os_string(),
        ];
        for field in [
            "system_root",
            "windir",
            "program_files",
            "program_files_x86",
            "program_data",
        ] {
            for invalid in &invalid_values {
                let mut environment = valid.clone();
                match field {
                    "system_root" => {
                        environment.system_root = Some(invalid.clone());
                        environment.windir = None;
                    }
                    "windir" => {
                        environment.system_root = None;
                        environment.windir = Some(invalid.clone());
                    }
                    "program_files" => environment.program_files = Some(invalid.clone()),
                    "program_files_x86" => {
                        environment.program_files_x86 = Some(invalid.clone());
                    }
                    "program_data" => environment.program_data = Some(invalid.clone()),
                    _ => unreachable!(),
                }
                assert_eq!(
                    windows_system_trees_from_environment(&environment, true),
                    Err(WorkspaceRejection::Inaccessible),
                    "invalid {field} must fail closed"
                );
            }
        }

        let mut without_x86 = valid;
        without_x86.program_files_x86 = None;
        assert_eq!(
            windows_system_trees_from_environment(&without_x86, true),
            Err(WorkspaceRejection::Inaccessible)
        );
        assert_eq!(
            windows_system_trees_from_environment(&without_x86, false)
                .expect("32-bit Windows may ignore ProgramFiles(x86)")
                .len(),
            3
        );
    }

    #[cfg(unix)]
    #[test]
    fn production_discovery_includes_the_fixed_tmp_alias() {
        let roots = PlatformRoots::discover().expect("production roots should be discoverable");
        let fixed_tmp = fs::canonicalize("/tmp").expect("fixed temp alias should exist");

        assert!(roots.temp_trees.contains(&fixed_tmp));
    }

    #[cfg(not(windows))]
    #[test]
    fn xdg_user_directory_parser_accepts_only_absolute_or_home_anchored_paths() {
        let home = Path::new("/home/tester");
        let parsed = parse_xdg_user_directory_config(
            home,
            "XDG_DESKTOP_DIR=\"$HOME/Work Desk\"\nXDG_DOWNLOAD_DIR=\"/srv/drop\"\n",
        )
        .expect("conservative XDG paths should parse");

        assert_eq!(
            parsed,
            XdgUserDirectoryConfig {
                desktop: XdgUserDirectory::Path(PathBuf::from("/home/tester/Work Desk")),
                downloads: XdgUserDirectory::Path(PathBuf::from("/srv/drop")),
            }
        );
        assert!(parse_xdg_user_directory_config(home, "XDG_DESKTOP_DIR=\"relative\"\n").is_err());
        assert!(
            parse_xdg_user_directory_config(home, "XDG_DOWNLOAD_DIR=\"$HOME/../escape\"\n")
                .is_err()
        );
        assert!(
            parse_xdg_user_directory_config(home, "XDG_DOWNLOAD_DIR=\"$OTHER/drop\"\n").is_err()
        );
    }

    #[cfg(unix)]
    #[test]
    fn xdg_declared_directories_are_mandatory_but_absent_fallbacks_are_optional() {
        use std::os::unix::fs::symlink;

        let temp = scratch();
        let home = temp.path().join("home");
        fs::create_dir(&home).expect("home should be created");

        let missing = temp.path().join("declared-missing");
        let missing_source = format!("XDG_DESKTOP_DIR=\"{}\"\n", missing.display());
        parse_xdg_user_directory_config(&home, &missing_source)
            .expect("the missing absolute declaration should parse");
        assert_eq!(
            desktop_download_trees_from_xdg_source(&home, Some(&missing_source)),
            Err(WorkspaceRejection::Inaccessible)
        );

        let dangling = temp.path().join("declared-dangling");
        symlink(temp.path().join("absent-target"), &dangling)
            .expect("dangling declaration should be created");
        let dangling_source = format!("XDG_DOWNLOAD_DIR=\"{}\"\n", dangling.display());
        parse_xdg_user_directory_config(&home, &dangling_source)
            .expect("the dangling absolute declaration should parse");
        assert_eq!(
            desktop_download_trees_from_xdg_source(&home, Some(&dangling_source)),
            Err(WorkspaceRejection::Inaccessible)
        );

        let file = temp.path().join("declared-file");
        fs::write(&file, b"not a directory").expect("declared file should be created");
        let file_source = format!("XDG_DESKTOP_DIR=\"{}\"\n", file.display());
        parse_xdg_user_directory_config(&home, &file_source)
            .expect("the absolute file declaration should parse");
        assert_eq!(
            desktop_download_trees_from_xdg_source(&home, Some(&file_source)),
            Err(WorkspaceRejection::NotDirectory)
        );

        assert_eq!(
            desktop_download_trees_from_xdg_source(&home, None)
                .expect("absent profile fallbacks may be ignored"),
            Vec::<PathBuf>::new()
        );

        for fallback in [home.join("Desktop"), home.join("Downloads")] {
            fs::create_dir(fallback).expect("profile fallback should be created");
        }
        let disabled_source = "XDG_DESKTOP_DIR=\"$HOME\"\nXDG_DOWNLOAD_DIR=\"$HOME\"\n";
        assert_eq!(
            desktop_download_trees_from_xdg_source(&home, Some(disabled_source))
                .expect("the XDG disabled sentinel should be accepted"),
            Vec::<PathBuf>::new(),
            "$HOME disables the user directory and must not protect the whole home or its fallback"
        );
    }
}
