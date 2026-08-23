//! Strict, fail-closed bridge configuration.

use std::collections::HashSet;
use std::env;
use std::fmt;
use std::fs;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

use crate::codex::process::CodexProcessConfig;
use crate::codex::types::{ApprovalPolicy, SandboxMode};
use crate::limits::{
    ASR_MAX_ARG_BYTES, ASR_MAX_ARGS, ASR_MAX_DURATION_MS, ASR_TRANSCRIPT_MAX_BYTES,
    DEFAULT_ACTIVE_TURN_PERMITS, DEFAULT_MAX_SCOPE_ACTORS, MAX_CONFIG_ALLOW_ROOT_BYTES,
    MAX_CONFIG_ALLOW_ROOTS, MAX_CONFIG_ALLOWED_GROUP_BYTES, MAX_CONFIG_ALLOWED_GROUPS,
    MAX_CONFIG_ALLOWED_SENDER_BYTES, MAX_CONFIG_ALLOWED_SENDERS, MAX_CONFIG_OWNER_BYTES,
    MAX_CONFIG_OWNERS,
};
use crate::runtime::policy::{AccessPolicy, PlatformRoots};

/// All configuration failures intentionally have static messages: configuration
/// paths and values are untrusted operator input and must not reach logs.
#[derive(Error)]
pub enum ConfigError {
    #[error("unable to locate the platform configuration directory")]
    LocateConfigDirectory,
    #[error("unable to read bridge configuration")]
    Read,
    #[error("bridge configuration is malformed or contains unknown fields")]
    Parse,
    #[error("bridge configuration requires at least one owner open ID")]
    EmptyOwners,
    #[error("bridge configuration contains an invalid owner open ID")]
    InvalidOwner,
    #[error("bridge configuration has too many owner open IDs")]
    TooManyOwners,
    #[error("bridge configuration owner IDs exceed the byte limit")]
    OwnersTooLarge,
    #[error("bridge configuration contains an invalid allowed sender open ID")]
    InvalidSender,
    #[error("bridge configuration has too many allowed sender open IDs")]
    TooManySenders,
    #[error("bridge configuration sender IDs exceed the byte limit")]
    SendersTooLarge,
    #[error("bridge configuration contains an invalid allowed group chat ID")]
    InvalidGroup,
    #[error("bridge configuration has too many allowed group chat IDs")]
    TooManyGroups,
    #[error("bridge configuration group chat IDs exceed the byte limit")]
    GroupsTooLarge,
    #[error("bridge configuration has too many workspace allow roots")]
    TooManyAllowRoots,
    #[error("bridge configuration workspace allow roots exceed the byte limit")]
    AllowRootsTooLarge,
    #[error("bridge configuration contains an invalid workspace allow root")]
    InvalidAllowRoot,
    #[error("bridge configuration contains an invalid runtime path")]
    InvalidRuntimePath,
    #[error("bridge configuration default workspace is not permitted")]
    InvalidDefaultWorkspace,
    #[error("unable to determine safe platform filesystem roots")]
    PlatformRoots,
    #[error("bridge configuration contains an invalid ASR sidecar command")]
    InvalidAsrCommand,
    #[error("bridge configuration has too many ASR sidecar arguments")]
    TooManyAsrArgs,
    #[error("bridge configuration ASR sidecar arguments exceed the byte limit")]
    AsrArgsTooLarge,
    #[error("bridge configuration contains an invalid ASR limit")]
    InvalidAsrLimit,
}

impl fmt::Debug for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let category = match self {
            Self::LocateConfigDirectory => "LocateConfigDirectory",
            Self::Read => "Read",
            Self::Parse => "Parse",
            Self::EmptyOwners => "EmptyOwners",
            Self::InvalidOwner => "InvalidOwner",
            Self::TooManyOwners => "TooManyOwners",
            Self::OwnersTooLarge => "OwnersTooLarge",
            Self::InvalidSender => "InvalidSender",
            Self::TooManySenders => "TooManySenders",
            Self::SendersTooLarge => "SendersTooLarge",
            Self::InvalidGroup => "InvalidGroup",
            Self::TooManyGroups => "TooManyGroups",
            Self::GroupsTooLarge => "GroupsTooLarge",
            Self::TooManyAllowRoots => "TooManyAllowRoots",
            Self::AllowRootsTooLarge => "AllowRootsTooLarge",
            Self::InvalidAllowRoot => "InvalidAllowRoot",
            Self::InvalidRuntimePath => "InvalidRuntimePath",
            Self::InvalidDefaultWorkspace => "InvalidDefaultWorkspace",
            Self::PlatformRoots => "PlatformRoots",
            Self::InvalidAsrCommand => "InvalidAsrCommand",
            Self::TooManyAsrArgs => "TooManyAsrArgs",
            Self::AsrArgsTooLarge => "AsrArgsTooLarge",
            Self::InvalidAsrLimit => "InvalidAsrLimit",
        };
        formatter.write_str(category)
    }
}

/// Top-level service configuration.
#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct BridgeConfig {
    pub owners: Vec<String>,
    pub allowed_senders: Vec<String>,
    pub allowed_groups: Vec<String>,
    pub default_workspace: Option<PathBuf>,
    pub workspace: WorkspacePolicy,
    pub concurrency: ConcurrencyConfig,
    pub codex: CodexSection,
    pub paths: PathsSection,
    pub asr: AsrSection,
}

impl fmt::Debug for BridgeConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BridgeConfig")
            .field("owner_count", &self.owners.len())
            .field("allowed_sender_count", &self.allowed_senders.len())
            .field("allowed_group_count", &self.allowed_groups.len())
            .field(
                "default_workspace_configured",
                &self.default_workspace.is_some(),
            )
            .field("workspace", &self.workspace)
            .field("concurrency", &self.concurrency)
            .field("codex", &self.codex)
            .field("paths", &self.paths)
            .field("asr", &self.asr)
            .finish()
    }
}

impl BridgeConfig {
    /// Loads an explicit configuration file, or the platform default when no
    /// explicit path is supplied. This function never creates configuration
    /// files or directories.
    ///
    /// # Errors
    ///
    /// Returns a static classification when the path cannot be read, TOML is
    /// invalid, or the resulting configuration violates policy constraints.
    pub fn load(path: Option<&Path>) -> Result<Self, ConfigError> {
        let path = match path {
            Some(path) => path.to_path_buf(),
            None => default_config_path()?,
        };
        let text = fs::read_to_string(&path).map_err(|_| ConfigError::Read)?;
        let mut config: Self = toml::from_str(&text).map_err(|_| ConfigError::Parse)?;
        config.resolve_runtime_paths(&path)?;
        config.validate()?;
        Ok(config)
    }

    /// Validates and normalizes the policy portion of the configuration.
    ///
    /// # Errors
    ///
    /// Returns a static classification for invalid owners, roots, defaults,
    /// or unavailable platform roots.
    pub fn validate(&mut self) -> Result<(), ConfigError> {
        let roots = PlatformRoots::discover().map_err(|_| ConfigError::PlatformRoots)?;
        self.validate_with_platform_roots(&roots)
    }

    pub(crate) fn validate_with_platform_roots(
        &mut self,
        platform_roots: &PlatformRoots,
    ) -> Result<(), ConfigError> {
        self.validate_static()?;
        AccessPolicy::prepare_config(self, platform_roots)?;
        let policy = AccessPolicy::from_prepared_config(self, platform_roots);
        if let Some(default_workspace) = self.default_workspace.clone() {
            let canonical = policy
                .validate_workspace(&default_workspace)
                .map_err(|_| ConfigError::InvalidDefaultWorkspace)?;
            self.default_workspace = Some(canonical);
        }
        Ok(())
    }

    pub(crate) fn validate_static(&mut self) -> Result<(), ConfigError> {
        normalize_id_collection(
            &mut self.owners,
            MAX_CONFIG_OWNERS,
            MAX_CONFIG_OWNER_BYTES,
            ConfigError::TooManyOwners,
            ConfigError::OwnersTooLarge,
            ConfigError::InvalidOwner,
        )?;
        if self.owners.is_empty() {
            return Err(ConfigError::EmptyOwners);
        }
        normalize_id_collection(
            &mut self.allowed_senders,
            MAX_CONFIG_ALLOWED_SENDERS,
            MAX_CONFIG_ALLOWED_SENDER_BYTES,
            ConfigError::TooManySenders,
            ConfigError::SendersTooLarge,
            ConfigError::InvalidSender,
        )?;
        normalize_id_collection(
            &mut self.allowed_groups,
            MAX_CONFIG_ALLOWED_GROUPS,
            MAX_CONFIG_ALLOWED_GROUP_BYTES,
            ConfigError::TooManyGroups,
            ConfigError::GroupsTooLarge,
            ConfigError::InvalidGroup,
        )?;
        if self.workspace.allow_roots.len() > MAX_CONFIG_ALLOW_ROOTS {
            return Err(ConfigError::TooManyAllowRoots);
        }
        if self
            .workspace
            .allow_roots
            .iter()
            .map(|path| path.as_os_str().as_encoded_bytes().len())
            .sum::<usize>()
            > MAX_CONFIG_ALLOW_ROOT_BYTES
        {
            return Err(ConfigError::AllowRootsTooLarge);
        }
        self.asr.validate()?;
        Ok(())
    }

    fn resolve_runtime_paths(&mut self, config_path: &Path) -> Result<(), ConfigError> {
        let parent = config_path.parent().ok_or(ConfigError::Read)?;
        let parent = if parent.is_absolute() {
            parent.to_path_buf()
        } else {
            env::current_dir()
                .map_err(|_| ConfigError::Read)?
                .join(parent)
        };
        self.paths.database = resolve_relative_path(&parent, &self.paths.database)?;
        self.paths.attachment_cache = resolve_relative_path(&parent, &self.paths.attachment_cache)?;
        if let Some(command) = self.asr.command.take() {
            self.asr.command = Some(resolve_command_path(&parent, &command)?);
        }
        self.asr.ffmpeg = resolve_command_path(&parent, &self.asr.ffmpeg)?;
        Ok(())
    }
}

/// Returns the normal platform-specific configuration path without creating
/// it. Explicit `--config` values are handled by [`BridgeConfig::load`].
///
/// # Errors
///
/// Returns an error when the required platform configuration base is absent.
pub fn default_config_path() -> Result<PathBuf, ConfigError> {
    #[cfg(windows)]
    {
        let base = env::var_os("APPDATA").ok_or(ConfigError::LocateConfigDirectory)?;
        if base.is_empty() {
            return Err(ConfigError::LocateConfigDirectory);
        }
        Ok(PathBuf::from(base)
            .join("lark-codex-bridge")
            .join("config.toml"))
    }
    #[cfg(not(windows))]
    {
        if let Some(base) = env::var_os("XDG_CONFIG_HOME") {
            if !base.is_empty() {
                return Ok(PathBuf::from(base)
                    .join("lark-codex-bridge")
                    .join("config.toml"));
            }
        }
        let home = env::var_os("HOME").ok_or(ConfigError::LocateConfigDirectory)?;
        if home.is_empty() {
            return Err(ConfigError::LocateConfigDirectory);
        }
        Ok(PathBuf::from(home)
            .join(".config")
            .join("lark-codex-bridge")
            .join("config.toml"))
    }
}

/// Workspace access settings.
#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct WorkspacePolicy {
    pub allow_roots: Vec<PathBuf>,
    pub network_access: bool,
}

impl fmt::Debug for WorkspacePolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkspacePolicy")
            .field("allow_root_count", &self.allow_roots.len())
            .field("network_access", &self.network_access)
            .finish()
    }
}

/// Runtime capacity controls.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConcurrencyConfig {
    pub active_turn_permits: usize,
    pub max_scope_actors: usize,
}

impl Default for ConcurrencyConfig {
    fn default() -> Self {
        Self {
            active_turn_permits: DEFAULT_ACTIVE_TURN_PERMITS,
            max_scope_actors: DEFAULT_MAX_SCOPE_ACTORS,
        }
    }
}

/// Codex process and policy settings.
#[derive(Clone, Serialize)]
pub struct CodexSection {
    pub binary: PathBuf,
    pub codex_home: Option<PathBuf>,
    pub model: Option<String>,
    pub sandbox: SandboxMode,
    pub approval_policy: ApprovalPolicy,
}

impl<'de> Deserialize<'de> for CodexSection {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let config = CodexSectionConfig::deserialize(deserializer)?;
        Ok(Self {
            binary: config.binary,
            codex_home: config.codex_home,
            model: config.model,
            sandbox: config.sandbox,
            approval_policy: config.approval_policy.into(),
        })
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct CodexSectionConfig {
    binary: PathBuf,
    codex_home: Option<PathBuf>,
    model: Option<String>,
    sandbox: SandboxMode,
    approval_policy: ConfigApprovalPolicy,
}

impl Default for CodexSectionConfig {
    fn default() -> Self {
        let defaults = CodexSection::default();
        Self {
            binary: defaults.binary,
            codex_home: defaults.codex_home,
            model: defaults.model,
            sandbox: defaults.sandbox,
            approval_policy: ConfigApprovalPolicy::default(),
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ConfigApprovalPolicy {
    Named(String),
    Granular(StrictGranularApprovalWrapper),
}

impl Default for ConfigApprovalPolicy {
    fn default() -> Self {
        Self::Named("never".to_owned())
    }
}

impl From<ConfigApprovalPolicy> for ApprovalPolicy {
    fn from(policy: ConfigApprovalPolicy) -> Self {
        match policy {
            ConfigApprovalPolicy::Named(name) => Self::Named(name),
            ConfigApprovalPolicy::Granular(wrapper) => Self::Granular {
                granular: wrapper.granular.into(),
            },
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictGranularApprovalWrapper {
    granular: StrictGranularApprovalPolicy,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(clippy::struct_excessive_bools)] // Mirrors the five RPC approval switches exactly.
struct StrictGranularApprovalPolicy {
    #[serde(default)]
    mcp_elicitations: bool,
    #[serde(default)]
    rules: bool,
    #[serde(default)]
    sandbox_approval: bool,
    #[serde(default)]
    request_permissions: bool,
    #[serde(default)]
    skill_approval: bool,
}

impl From<StrictGranularApprovalPolicy> for crate::codex::types::GranularApprovalPolicy {
    fn from(policy: StrictGranularApprovalPolicy) -> Self {
        Self {
            mcp_elicitations: policy.mcp_elicitations,
            rules: policy.rules,
            sandbox_approval: policy.sandbox_approval,
            request_permissions: policy.request_permissions,
            skill_approval: policy.skill_approval,
        }
    }
}

impl Default for CodexSection {
    fn default() -> Self {
        Self {
            binary: PathBuf::from("codex"),
            codex_home: None,
            model: None,
            sandbox: SandboxMode::WorkspaceWrite,
            approval_policy: ApprovalPolicy::Named("never".to_owned()),
        }
    }
}

impl CodexSection {
    #[must_use]
    pub fn process_config(&self) -> CodexProcessConfig {
        CodexProcessConfig {
            binary: self.binary.clone(),
            codex_home: self.codex_home.clone(),
        }
    }
}

impl fmt::Debug for CodexSection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let approval_policy_kind = match &self.approval_policy {
            ApprovalPolicy::Named(_) => "named",
            ApprovalPolicy::Granular { .. } => "granular",
        };
        formatter
            .debug_struct("CodexSection")
            .field("binary", &"[configured]")
            .field(
                "codex_home",
                &self.codex_home.as_ref().map(|_| "[configured]"),
            )
            .field("model_configured", &self.model.is_some())
            .field("sandbox", &self.sandbox)
            .field("approval_policy_kind", &approval_policy_kind)
            .finish()
    }
}

/// Fail-closed operator configuration for the local ASR sidecar.
#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct AsrSection {
    /// Optional sidecar executable. Absent means audio cannot be transcribed
    /// unless the inbound payload already carries recognition text.
    pub command: Option<PathBuf>,
    /// Extra arguments inserted before the decoded WAV path.
    pub args: Vec<String>,
    /// ffmpeg executable used to decode inbound audio to 16 kHz PCM WAV.
    pub ffmpeg: PathBuf,
    /// Audio longer than this is refused without invoking the sidecar.
    pub max_duration_ms: u64,
    /// Maximum accepted transcript bytes from inbound text or sidecar stdout.
    pub max_transcript_bytes: usize,
}

impl Default for AsrSection {
    fn default() -> Self {
        Self {
            command: None,
            args: Vec::new(),
            ffmpeg: PathBuf::from("ffmpeg"),
            max_duration_ms: ASR_MAX_DURATION_MS,
            max_transcript_bytes: ASR_TRANSCRIPT_MAX_BYTES,
        }
    }
}

impl AsrSection {
    pub(crate) fn validate(&mut self) -> Result<(), ConfigError> {
        if let Some(command) = &self.command {
            if command.as_os_str().is_empty() {
                return Err(ConfigError::InvalidAsrCommand);
            }
        }
        if self.ffmpeg.as_os_str().is_empty() {
            return Err(ConfigError::InvalidAsrCommand);
        }
        if self.args.len() > ASR_MAX_ARGS {
            return Err(ConfigError::TooManyAsrArgs);
        }
        if self.args.iter().any(|arg| arg.len() > ASR_MAX_ARG_BYTES) {
            return Err(ConfigError::AsrArgsTooLarge);
        }
        if self.max_duration_ms == 0 || self.max_transcript_bytes == 0 {
            return Err(ConfigError::InvalidAsrLimit);
        }
        Ok(())
    }

    /// Returns whether a sidecar executable is configured.
    #[must_use]
    pub fn is_configured(&self) -> bool {
        self.command
            .as_ref()
            .is_some_and(|command| !command.as_os_str().is_empty())
    }
}

impl fmt::Debug for AsrSection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AsrSection")
            .field("command_configured", &self.command.is_some())
            .field("arg_count", &self.args.len())
            .field("ffmpeg_configured", &!self.ffmpeg.as_os_str().is_empty())
            .field("max_duration_ms", &self.max_duration_ms)
            .field("max_transcript_bytes", &self.max_transcript_bytes)
            .finish()
    }
}

/// Local runtime storage locations.
#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct PathsSection {
    pub database: PathBuf,
    pub attachment_cache: PathBuf,
}

impl Default for PathsSection {
    fn default() -> Self {
        Self {
            database: PathBuf::from("bridge.sqlite3"),
            attachment_cache: PathBuf::from("attachments"),
        }
    }
}

impl fmt::Debug for PathsSection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PathsSection")
            .field("database", &"[configured]")
            .field("attachment_cache", &"[configured]")
            .finish()
    }
}

fn resolve_command_path(parent: &Path, path: &Path) -> Result<PathBuf, ConfigError> {
    if path.as_os_str().is_empty() {
        return Err(ConfigError::InvalidAsrCommand);
    }
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    #[cfg(windows)]
    if path.has_root() || matches!(path.components().next(), Some(Component::Prefix(_))) {
        return Err(ConfigError::InvalidAsrCommand);
    }
    if path.components().count() <= 1 {
        return Ok(path.to_path_buf());
    }
    resolve_relative_path(parent, path)
}

fn resolve_relative_path(parent: &Path, path: &Path) -> Result<PathBuf, ConfigError> {
    if !parent.is_absolute() {
        return Err(ConfigError::InvalidRuntimePath);
    }
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    #[cfg(windows)]
    if path.has_root() || matches!(path.components().next(), Some(Component::Prefix(_))) {
        return Err(ConfigError::InvalidRuntimePath);
    }
    Ok(lexical_normalize(&parent.join(path)))
}

/// Validates one identity/chat ID collection against its count and byte caps,
/// rejects malformed IDs, and deduplicates idempotently in place.
fn normalize_id_collection(
    ids: &mut Vec<String>,
    max_count: usize,
    max_bytes: usize,
    too_many: ConfigError,
    too_large: ConfigError,
    invalid: ConfigError,
) -> Result<(), ConfigError> {
    if ids.len() > max_count {
        return Err(too_many);
    }
    if ids.iter().map(String::len).sum::<usize>() > max_bytes {
        return Err(too_large);
    }
    if ids.iter().any(|id| {
        id.is_empty() || id.trim() != id || id.bytes().any(|byte| byte.is_ascii_whitespace())
    }) {
        return Err(invalid);
    }
    let mut known = HashSet::new();
    ids.retain(|id| known.insert(id.clone()));
    Ok(())
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;

    #[test]
    fn drive_relative_and_root_relative_runtime_paths_fail_closed() {
        let parent = Path::new(r"C:\config\lark-codex-bridge");

        assert!(resolve_relative_path(parent, Path::new(r"C:state\bridge.sqlite3")).is_err());
        assert!(resolve_relative_path(parent, Path::new(r"\state\bridge.sqlite3")).is_err());
        assert_eq!(
            resolve_relative_path(parent, Path::new(r"state\bridge.sqlite3")).unwrap(),
            parent.join(r"state\bridge.sqlite3")
        );
        assert!(resolve_command_path(parent, Path::new(r"C:")).is_err());
        assert!(resolve_command_path(parent, Path::new(r"C:ffmpeg.exe")).is_err());
        assert!(resolve_command_path(parent, Path::new(r"\ffmpeg.exe")).is_err());
        assert_eq!(
            resolve_command_path(parent, Path::new("ffmpeg.exe")).unwrap(),
            PathBuf::from("ffmpeg.exe")
        );
    }
}

fn lexical_normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::RootDir | Component::Prefix(_) | Component::Normal(_) => {
                normalized.push(component);
            }
        }
    }
    normalized
}
