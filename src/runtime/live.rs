//! Process-wide, hot-reloadable access and Codex settings.

use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::codex::types::{ApprovalPolicy, SandboxMode};
use crate::config::BridgeConfig;
use crate::limits::{
    MAX_CONFIG_ALLOWED_GROUP_BYTES, MAX_CONFIG_ALLOWED_GROUPS, MAX_CONFIG_ALLOWED_SENDER_BYTES,
    MAX_CONFIG_ALLOWED_SENDERS,
};
use crate::runtime::policy::AccessPolicy;
use crate::runtime::router::RouterSettings;

const MAX_CONFIG_MODEL_BYTES: usize = 128;

/// One operator-visible change applied by `/config`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigPatch {
    Model(Option<String>),
    Effort(Option<String>),
    Sandbox(SandboxMode),
    Approval(String),
    GroupAdd(String),
    GroupRemove(String),
    SenderAdd(String),
    SenderRemove(String),
    Form {
        model: Option<String>,
        effort: Option<String>,
        sandbox: Option<SandboxMode>,
        approval: Option<String>,
    },
}

/// Snapshot used to render `/config` cards and confirmations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfigView {
    pub model: Option<String>,
    pub effort: Option<String>,
    pub sandbox: SandboxMode,
    pub approval: String,
    pub allowed_groups: Vec<String>,
    pub allowed_senders: Vec<String>,
}

/// Shared runtime configuration that `/config` can mutate without a restart.
pub struct LiveBridgeConfig {
    persist_path: Option<PathBuf>,
    inner: RwLock<LiveInner>,
}

struct LiveInner {
    config: Option<BridgeConfig>,
    policy: AccessPolicy,
    settings: RouterSettings,
}

/// Failures from applying a live `/config` patch.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum LiveConfigError {
    #[error("the live configuration lock is unavailable")]
    Lock,
    #[error("the requested configuration value is invalid")]
    InvalidValue,
    #[error("the configuration file could not be written")]
    Persist,
    #[error("the updated configuration failed validation")]
    Validate,
}

impl LiveBridgeConfig {
    /// Wraps already-validated policy and settings for tests and memory-only runs.
    #[must_use]
    pub fn from_runtime(policy: AccessPolicy, settings: RouterSettings) -> Arc<Self> {
        Arc::new(Self {
            persist_path: None,
            inner: RwLock::new(LiveInner {
                config: None,
                policy,
                settings,
            }),
        })
    }

    /// Wraps the loaded file so later `/config` writes persist and reload in place.
    #[must_use]
    pub fn persistent(
        path: PathBuf,
        config: BridgeConfig,
        policy: AccessPolicy,
        settings: RouterSettings,
    ) -> Arc<Self> {
        Arc::new(Self {
            persist_path: Some(path),
            inner: RwLock::new(LiveInner {
                config: Some(config),
                policy,
                settings,
            }),
        })
    }

    #[must_use]
    pub fn policy(&self) -> AccessPolicy {
        self.read_inner().policy.clone()
    }

    #[must_use]
    pub fn settings(&self) -> RouterSettings {
        self.read_inner().settings.clone()
    }

    #[must_use]
    pub fn view(&self) -> ConfigView {
        let inner = self.read_inner();
        ConfigView::from_runtime(&inner.policy, &inner.settings)
    }

    fn read_inner(&self) -> std::sync::RwLockReadGuard<'_, LiveInner> {
        self.inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Applies one patch to memory and, when a path is configured, to disk.
    ///
    /// # Errors
    ///
    /// Returns a static classification when the patch is invalid, validation
    /// fails, or the configuration file cannot be replaced.
    pub fn apply(&self, patch: &ConfigPatch) -> Result<ConfigView, LiveConfigError> {
        let mut inner = self.inner.write().map_err(|_| LiveConfigError::Lock)?;
        if let Some(config) = inner.config.clone() {
            let mut next = config;
            apply_patch_to_config(&mut next, patch)?;
            let roots = inner.policy.platform_roots().clone();
            next.validate_with_platform_roots(&roots)
                .map_err(|_| LiveConfigError::Validate)?;
            let policy = AccessPolicy::from_prepared_config(&next, &roots);
            let settings = inner.settings.overlay_hot_reload(&next);
            if let Some(path) = &self.persist_path {
                next.write_atomic(path)
                    .map_err(|_| LiveConfigError::Persist)?;
            }
            inner.config = Some(next);
            inner.policy = policy;
            inner.settings = settings;
        } else {
            apply_patch_to_runtime(&mut inner, patch)?;
        }
        Ok(ConfigView::from_runtime(&inner.policy, &inner.settings))
    }
}

impl fmt::Debug for LiveBridgeConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LiveBridgeConfig")
            .field("persist_configured", &self.persist_path.is_some())
            .finish_non_exhaustive()
    }
}

impl ConfigView {
    fn from_runtime(policy: &AccessPolicy, settings: &RouterSettings) -> Self {
        Self {
            model: settings.model.clone(),
            effort: settings.effort.clone(),
            sandbox: settings.sandbox,
            approval: named_approval(&settings.approval_policy),
            allowed_groups: policy.allowed_groups().to_vec(),
            allowed_senders: policy.allowed_senders().to_vec(),
        }
    }
}

fn apply_patch_to_config(
    config: &mut BridgeConfig,
    patch: &ConfigPatch,
) -> Result<(), LiveConfigError> {
    match patch {
        ConfigPatch::Model(model) => config.codex.model = normalize_model(model.as_deref())?,
        ConfigPatch::Effort(effort) => config.codex.effort = normalize_effort(effort.as_deref())?,
        ConfigPatch::Sandbox(sandbox) => config.codex.sandbox = *sandbox,
        ConfigPatch::Approval(name) => {
            config.codex.approval_policy = ApprovalPolicy::Named(normalize_approval(name)?);
        }
        ConfigPatch::GroupAdd(id) => add_unique(&mut config.allowed_groups, id, true)?,
        ConfigPatch::GroupRemove(id) => config.allowed_groups.retain(|item| item != id),
        ConfigPatch::SenderAdd(id) => add_unique(&mut config.allowed_senders, id, false)?,
        ConfigPatch::SenderRemove(id) => config.allowed_senders.retain(|item| item != id),
        ConfigPatch::Form {
            model,
            effort,
            sandbox,
            approval,
        } => {
            if model.is_some() {
                config.codex.model = normalize_model(model.as_deref())?;
            }
            if effort.is_some() {
                config.codex.effort = normalize_effort(effort.as_deref())?;
            }
            if let Some(sandbox) = sandbox {
                config.codex.sandbox = *sandbox;
            }
            if let Some(approval) = approval {
                config.codex.approval_policy = ApprovalPolicy::Named(normalize_approval(approval)?);
            }
        }
    }
    Ok(())
}

fn apply_patch_to_runtime(
    inner: &mut LiveInner,
    patch: &ConfigPatch,
) -> Result<(), LiveConfigError> {
    match patch {
        ConfigPatch::Model(model) => inner.settings.model = normalize_model(model.as_deref())?,
        ConfigPatch::Effort(effort) => inner.settings.effort = normalize_effort(effort.as_deref())?,
        ConfigPatch::Sandbox(sandbox) => {
            inner.settings.sandbox = *sandbox;
            inner
                .policy
                .replace_codex_policy(*sandbox, inner.settings.approval_policy.clone());
        }
        ConfigPatch::Approval(name) => {
            let approval = ApprovalPolicy::Named(normalize_approval(name)?);
            inner.settings.approval_policy = approval.clone();
            inner
                .policy
                .replace_codex_policy(inner.settings.sandbox, approval);
        }
        ConfigPatch::GroupAdd(id) => {
            let mut groups = inner.policy.allowed_groups().to_vec();
            add_unique(&mut groups, id, true)?;
            inner
                .policy
                .replace_allowlists(inner.policy.allowed_senders().to_vec(), groups);
        }
        ConfigPatch::GroupRemove(id) => {
            let groups = inner
                .policy
                .allowed_groups()
                .iter()
                .filter(|item| item.as_str() != id)
                .cloned()
                .collect();
            inner
                .policy
                .replace_allowlists(inner.policy.allowed_senders().to_vec(), groups);
        }
        ConfigPatch::SenderAdd(id) => {
            let mut senders = inner.policy.allowed_senders().to_vec();
            add_unique(&mut senders, id, false)?;
            inner
                .policy
                .replace_allowlists(senders, inner.policy.allowed_groups().to_vec());
        }
        ConfigPatch::SenderRemove(id) => {
            let senders = inner
                .policy
                .allowed_senders()
                .iter()
                .filter(|item| item.as_str() != id)
                .cloned()
                .collect();
            inner
                .policy
                .replace_allowlists(senders, inner.policy.allowed_groups().to_vec());
        }
        ConfigPatch::Form {
            model,
            effort,
            sandbox,
            approval,
        } => {
            if model.is_some() {
                inner.settings.model = normalize_model(model.as_deref())?;
            }
            if effort.is_some() {
                inner.settings.effort = normalize_effort(effort.as_deref())?;
            }
            if let Some(sandbox) = sandbox {
                inner.settings.sandbox = *sandbox;
            }
            if let Some(approval) = approval {
                inner.settings.approval_policy =
                    ApprovalPolicy::Named(normalize_approval(approval)?);
            }
            inner.policy.replace_codex_policy(
                inner.settings.sandbox,
                inner.settings.approval_policy.clone(),
            );
        }
    }
    Ok(())
}

fn add_unique(ids: &mut Vec<String>, id: &str, group: bool) -> Result<(), LiveConfigError> {
    if !valid_allow_id(id) {
        return Err(LiveConfigError::InvalidValue);
    }
    if ids.iter().any(|item| item == id) {
        return Ok(());
    }
    let (max_count, max_bytes) = if group {
        (MAX_CONFIG_ALLOWED_GROUPS, MAX_CONFIG_ALLOWED_GROUP_BYTES)
    } else {
        (MAX_CONFIG_ALLOWED_SENDERS, MAX_CONFIG_ALLOWED_SENDER_BYTES)
    };
    if ids.len() >= max_count {
        return Err(LiveConfigError::InvalidValue);
    }
    let used = ids.iter().map(String::len).sum::<usize>();
    if used.saturating_add(id.len()) > max_bytes {
        return Err(LiveConfigError::InvalidValue);
    }
    ids.push(id.to_owned());
    Ok(())
}

fn valid_allow_id(id: &str) -> bool {
    !id.is_empty() && id.trim() == id && !id.bytes().any(|byte| byte.is_ascii_whitespace())
}

fn normalize_model(value: Option<&str>) -> Result<Option<String>, LiveConfigError> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    if matches!(value, "-" | "default") {
        return Ok(None);
    }
    if value.len() > MAX_CONFIG_MODEL_BYTES || value.bytes().any(|byte| byte.is_ascii_control()) {
        return Err(LiveConfigError::InvalidValue);
    }
    Ok(Some(value.to_owned()))
}

fn normalize_effort(value: Option<&str>) -> Result<Option<String>, LiveConfigError> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    if matches!(value, "-" | "default" | "none") {
        return Ok(None);
    }
    if !matches!(value, "low" | "medium" | "high" | "xhigh") {
        return Err(LiveConfigError::InvalidValue);
    }
    Ok(Some(value.to_owned()))
}

fn normalize_approval(value: &str) -> Result<String, LiveConfigError> {
    let value = value.trim();
    if !matches!(value, "untrusted" | "on-failure" | "on-request" | "never") {
        return Err(LiveConfigError::InvalidValue);
    }
    Ok(value.to_owned())
}

fn named_approval(policy: &ApprovalPolicy) -> String {
    match policy {
        ApprovalPolicy::Named(name) => name.clone(),
        ApprovalPolicy::Granular { .. } => "granular".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WorkspacePolicy;
    use crate::runtime::policy::PlatformRoots;

    fn runtime_live() -> Arc<LiveBridgeConfig> {
        let temporary = tempfile::tempdir().expect("temp");
        let home = temporary.path().join("home");
        let cwd = home.join("workspace");
        std::fs::create_dir_all(&cwd).expect("workspace");
        let roots = PlatformRoots::new(&home, Vec::new(), Vec::new(), Vec::new()).expect("roots");
        let config = BridgeConfig {
            owners: vec!["ou_owner".to_owned()],
            default_workspace: Some(cwd.clone()),
            workspace: WorkspacePolicy {
                allow_roots: vec![cwd],
                ..WorkspacePolicy::default()
            },
            ..BridgeConfig::default()
        };
        let policy = AccessPolicy::with_platform_roots(&config, &roots).expect("policy");
        let settings = RouterSettings::from_config(&config);
        LiveBridgeConfig::from_runtime(policy, settings)
    }

    #[test]
    fn apply_model_is_visible_without_persist() {
        let live = runtime_live();
        let view = live
            .apply(&ConfigPatch::Model(Some("gpt-6-astra".to_owned())))
            .expect("apply");
        assert_eq!(view.model.as_deref(), Some("gpt-6-astra"));
        assert_eq!(live.settings().model.as_deref(), Some("gpt-6-astra"));
    }

    #[test]
    fn apply_group_updates_policy() {
        let live = runtime_live();
        live.apply(&ConfigPatch::GroupAdd("oc_allowed".to_owned()))
            .expect("add");
        assert!(
            live.policy()
                .allowed_groups()
                .iter()
                .any(|group| group == "oc_allowed")
        );
        live.apply(&ConfigPatch::GroupRemove("oc_allowed".to_owned()))
            .expect("remove");
        assert!(live.policy().allowed_groups().is_empty());
    }

    #[test]
    fn apply_writes_toml_and_updates_memory() {
        let temporary = tempfile::tempdir().expect("temp");
        let home = temporary.path().join("home");
        let cwd = home.join("workspace");
        std::fs::create_dir_all(&cwd).expect("workspace");
        let roots = PlatformRoots::new(&home, Vec::new(), Vec::new(), Vec::new()).expect("roots");
        let config = BridgeConfig {
            owners: vec!["ou_owner".to_owned()],
            default_workspace: Some(cwd.clone()),
            workspace: WorkspacePolicy {
                allow_roots: vec![cwd],
                ..WorkspacePolicy::default()
            },
            ..BridgeConfig::default()
        };
        let policy = AccessPolicy::with_platform_roots(&config, &roots).expect("policy");
        let settings = RouterSettings::from_config(&config);
        let path = temporary.path().join("config.toml");
        let live = LiveBridgeConfig::persistent(path.clone(), config, policy, settings);
        live.apply(&ConfigPatch::Model(Some("gpt-6-astra".to_owned())))
            .expect("persist model");
        live.apply(&ConfigPatch::GroupAdd("oc_allowed".to_owned()))
            .expect("persist group");
        assert_eq!(live.settings().model.as_deref(), Some("gpt-6-astra"));
        assert!(
            live.policy()
                .allowed_groups()
                .iter()
                .any(|group| group == "oc_allowed")
        );
        let written = std::fs::read_to_string(&path).expect("written config");
        assert!(written.contains("gpt-6-astra"));
        assert!(written.contains("oc_allowed"));
    }
}
