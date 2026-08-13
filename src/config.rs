//! Strict, fail-closed bridge configuration.

use std::collections::HashSet;
use std::env;
use std::fmt;
use std::fs;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::codex::process::CodexProcessConfig;
use crate::codex::types::{ApprovalPolicy, SandboxMode};
use crate::limits::{
    DEFAULT_ACTIVE_TURN_PERMITS, DEFAULT_MAX_SCOPE_ACTORS, MAX_CONFIG_ALLOW_ROOT_BYTES,
    MAX_CONFIG_ALLOW_ROOTS, MAX_CONFIG_OWNER_BYTES, MAX_CONFIG_OWNERS,
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
    #[error("bridge configuration has too many workspace allow roots")]
    TooManyAllowRoots,
    #[error("bridge configuration workspace allow roots exceed the byte limit")]
    AllowRootsTooLarge,
    #[error("bridge configuration contains an invalid workspace allow root")]
    InvalidAllowRoot,
    #[error("bridge configuration default workspace is not permitted")]
    InvalidDefaultWorkspace,
    #[error("unable to determine safe platform filesystem roots")]
    PlatformRoots,
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
            Self::TooManyAllowRoots => "TooManyAllowRoots",
            Self::AllowRootsTooLarge => "AllowRootsTooLarge",
            Self::InvalidAllowRoot => "InvalidAllowRoot",
            Self::InvalidDefaultWorkspace => "InvalidDefaultWorkspace",
            Self::PlatformRoots => "PlatformRoots",
        };
        formatter.write_str(category)
    }
}

/// Top-level service configuration.
#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct BridgeConfig {
    pub owners: Vec<String>,
    pub default_workspace: Option<PathBuf>,
    pub workspace: WorkspacePolicy,
    pub concurrency: ConcurrencyConfig,
    pub codex: CodexSection,
    pub paths: PathsSection,
}

impl fmt::Debug for BridgeConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BridgeConfig")
            .field("owner_count", &self.owners.len())
            .field(
                "owner_suffixes",
                &self
                    .owners
                    .iter()
                    .map(|owner| {
                        owner
                            .chars()
                            .rev()
                            .take(6)
                            .collect::<String>()
                            .chars()
                            .rev()
                            .collect::<String>()
                    })
                    .collect::<Vec<_>>(),
            )
            .field(
                "default_workspace",
                &self
                    .default_workspace
                    .as_ref()
                    .and_then(|path| fs::canonicalize(path).ok())
                    .map(|path| path.display().to_string()),
            )
            .field("workspace", &self.workspace)
            .field("concurrency", &self.concurrency)
            .field("codex", &self.codex)
            .field("paths", &self.paths)
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
        if self.owners.len() > MAX_CONFIG_OWNERS {
            return Err(ConfigError::TooManyOwners);
        }
        if self.owners.iter().map(String::len).sum::<usize>() > MAX_CONFIG_OWNER_BYTES {
            return Err(ConfigError::OwnersTooLarge);
        }
        if self.owners.iter().any(|owner| {
            owner.is_empty()
                || owner.trim() != owner
                || owner.bytes().any(|byte| byte.is_ascii_whitespace())
        }) {
            return Err(ConfigError::InvalidOwner);
        }
        let mut known = HashSet::new();
        self.owners.retain(|owner| known.insert(owner.clone()));
        if self.owners.is_empty() {
            return Err(ConfigError::EmptyOwners);
        }
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
        self.paths.database = resolve_relative_path(&parent, &self.paths.database);
        self.paths.attachment_cache = resolve_relative_path(&parent, &self.paths.attachment_cache);
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
            .field(
                "canonical_allow_roots",
                &self
                    .allow_roots
                    .iter()
                    .filter_map(|path| fs::canonicalize(path).ok())
                    .collect::<Vec<_>>(),
            )
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
#[derive(Clone, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CodexSection {
    pub binary: PathBuf,
    pub codex_home: Option<PathBuf>,
    pub model: Option<String>,
    pub sandbox: SandboxMode,
    pub approval_policy: ApprovalPolicy,
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

fn resolve_relative_path(parent: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }
    lexical_normalize(&parent.join(path))
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
