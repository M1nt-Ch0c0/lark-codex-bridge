use std::fs;
use std::path::{Path, PathBuf};

use lark_codex_bridge::codex::types::{ApprovalPolicy, GranularApprovalPolicy, SandboxMode};
use lark_codex_bridge::config::{
    BridgeConfig, CodexSection, ConcurrencyConfig, PathsSection, WorkspacePolicy,
};
use lark_codex_bridge::lark::api::ChatMode;
use lark_codex_bridge::lark::normalize::{InboundEvent, ScopeKey};
use lark_codex_bridge::runtime::policy::{
    AccessDecision, AccessPolicy, PlatformRoots, WorkspaceRejection,
};
use tempfile::TempDir;

const MINIMAL_CONFIG: &str = include_str!("fixtures/runtime/config_minimal.toml");
const FULL_CONFIG: &str = include_str!("fixtures/runtime/config_full.toml");

fn scratch() -> TempDir {
    tempfile::Builder::new()
        .prefix("runtime-policy-")
        .tempdir_in(env!("CARGO_MANIFEST_DIR"))
        .expect("repository scratch directory should be created")
}

fn event(sender: &str, chat_type: ChatMode, mentions_bot: bool) -> InboundEvent {
    InboundEvent {
        event_id: "evt_test".to_owned(),
        message_id: "om_test".to_owned(),
        chat_id: "oc_test".to_owned(),
        sender_id: sender.to_owned(),
        chat_type,
        thread_id: None,
        root_id: None,
        reply_to_message_id: None,
        text: "untrusted text".to_owned(),
        mentions_bot,
        mention_all: false,
        resources: vec![],
        message_type: "text".to_owned(),
        create_time_ms: 0,
        scope: ScopeKey::Chat("oc_test".to_owned()),
    }
}

fn policy_config(allow_root: PathBuf) -> BridgeConfig {
    BridgeConfig {
        owners: vec!["ou_owner_123456".to_owned()],
        default_workspace: None,
        workspace: WorkspacePolicy {
            allow_roots: vec![allow_root],
            network_access: false,
        },
        concurrency: ConcurrencyConfig::default(),
        codex: CodexSection::default(),
        paths: PathsSection::default(),
    }
}

fn roots(base: &Path) -> PlatformRoots {
    let home = base.join("home");
    let temp = base.join("temp");
    let system = base.join("system");
    let desktop = home.join("Desktop");
    let downloads = home.join("Downloads");
    for path in [&home, &temp, &system, &desktop, &downloads] {
        fs::create_dir_all(path).expect("test root should be created");
    }
    PlatformRoots::new(&home, &temp, vec![system], Some(desktop), Some(downloads))
        .expect("test roots should be canonicalized")
}

fn policy(base: &Path, allow_root: PathBuf) -> AccessPolicy {
    AccessPolicy::with_platform_roots(&policy_config(allow_root), &roots(base))
        .expect("safe test policy should build")
}

#[test]
fn minimal_config_has_safe_defaults_and_resolves_relative_runtime_paths() {
    let temp = scratch();
    let config_path = temp.path().join("config.toml");
    fs::write(&config_path, MINIMAL_CONFIG).expect("fixture should write");

    let config = BridgeConfig::load(Some(&config_path)).expect("minimal config should load");

    assert_eq!(config.owners, ["ou_owner_123456"]);
    assert!(config.workspace.allow_roots.is_empty());
    assert!(!config.workspace.network_access);
    assert_eq!(config.concurrency.active_turn_permits, 4);
    assert_eq!(config.concurrency.max_scope_actors, 256);
    assert_eq!(config.codex.sandbox, SandboxMode::WorkspaceWrite);
    assert_eq!(
        config.codex.approval_policy,
        ApprovalPolicy::Named("never".to_owned())
    );
    assert_eq!(config.paths.database, temp.path().join("bridge.sqlite3"));
    assert_eq!(
        config.paths.attachment_cache,
        temp.path().join("attachments")
    );
}

#[test]
fn full_config_round_trips_and_resolves_only_runtime_relative_paths() {
    let temp = scratch();
    let config_path = temp.path().join("config.toml");
    fs::write(&config_path, FULL_CONFIG).expect("fixture should write");

    let config = BridgeConfig::load(Some(&config_path)).expect("full config should load");

    assert_eq!(config.owners.len(), 2);
    assert!(config.workspace.network_access);
    assert_eq!(config.concurrency.active_turn_permits, 9);
    assert_eq!(config.concurrency.max_scope_actors, 31);
    assert_eq!(config.codex.model.as_deref(), Some("gpt-5.6"));
    assert_eq!(config.codex.sandbox, SandboxMode::ReadOnly);
    assert_eq!(
        config.codex.approval_policy,
        ApprovalPolicy::Named("on-request".to_owned())
    );
    assert_eq!(
        config.paths.database,
        temp.path().join("state/bridge.sqlite3")
    );
    assert_eq!(
        config.paths.attachment_cache,
        temp.path().join("cache/attachments")
    );
    assert_eq!(config.codex.binary, PathBuf::from("/opt/codex/bin/codex"));
}

#[test]
fn config_rejects_unknown_keys_at_every_schema_level() {
    for source in [
        "owners = [\"ou_owner_123456\"]\nunexpected = true",
        "owners = [\"ou_owner_123456\"]\n[workspace]\nunexpected = true",
        "owners = [\"ou_owner_123456\"]\n[concurrency]\nunexpected = true",
        "owners = [\"ou_owner_123456\"]\n[codex]\nunexpected = true",
        "owners = [\"ou_owner_123456\"]\n[paths]\nunexpected = true",
    ] {
        assert!(toml::from_str::<BridgeConfig>(source).is_err());
    }
}

#[test]
fn config_rejects_missing_owners_and_oversized_owner_or_root_collections() {
    assert!(BridgeConfig::default().validate().is_err());

    let temp = scratch();
    let safe = temp.path().join("safe");
    fs::create_dir_all(&safe).expect("safe root should be created");
    let mut config = policy_config(safe);
    config.owners = vec!["o".repeat(lark_codex_bridge::limits::MAX_CONFIG_OWNER_BYTES + 1)];
    assert!(config.validate().is_err());

    let mut config = policy_config(temp.path().join("safe"));
    config.owners = (0..=lark_codex_bridge::limits::MAX_CONFIG_OWNERS)
        .map(|index| format!("ou_owner_{index}"))
        .collect();
    assert!(config.validate().is_err());

    let mut config = policy_config(temp.path().join("safe"));
    config.workspace.allow_roots =
        vec![temp.path().join("safe"); lark_codex_bridge::limits::MAX_CONFIG_ALLOW_ROOTS + 1];
    assert!(config.validate().is_err());

    let mut config = policy_config(temp.path().join("safe"));
    config.workspace.allow_roots = vec![PathBuf::from(
        "x".repeat(lark_codex_bridge::limits::MAX_CONFIG_ALLOW_ROOT_BYTES + 1),
    )];
    assert!(config.validate().is_err());
}

#[test]
fn owner_and_direct_mention_gate_uses_chat_mode_not_scope() {
    let temp = scratch();
    let allowed = temp.path().join("safe");
    fs::create_dir_all(&allowed).expect("safe root should be created");
    let policy = policy(temp.path(), allowed);

    assert_eq!(
        policy.decide(&event("ou_owner_123456", ChatMode::P2p, false)),
        AccessDecision::Allow
    );
    assert_eq!(
        policy.decide(&event("ou_stranger", ChatMode::P2p, true)),
        AccessDecision::DenyNotOwner
    );
    assert_eq!(
        policy.decide(&event("ou_owner_123456", ChatMode::Group, false)),
        AccessDecision::DenyMissingMention
    );
    assert_eq!(
        policy.decide(&event("ou_owner_123456", ChatMode::Topic, false)),
        AccessDecision::DenyMissingMention
    );
    assert_eq!(
        policy.decide(&event("ou_owner_123456", ChatMode::Topic, true)),
        AccessDecision::Allow
    );
}

#[test]
fn workspace_validator_fails_closed_and_canonicalizes_safe_aliases() {
    let temp = scratch();
    let allowed = temp.path().join("safe");
    let project = allowed.join("project");
    let outside = temp.path().join("outside");
    fs::create_dir_all(&project).expect("project should be created");
    fs::create_dir_all(&outside).expect("outside should be created");
    let policy = policy(temp.path(), allowed.clone());

    assert_eq!(
        policy.validate_workspace(Path::new("project")),
        Err(WorkspaceRejection::Relative)
    );
    assert_eq!(
        policy.validate_workspace(&temp.path().join("missing")),
        Err(WorkspaceRejection::Inaccessible)
    );
    let file = temp.path().join("file");
    fs::write(&file, "x").expect("file should be created");
    assert_eq!(
        policy.validate_workspace(&file),
        Err(WorkspaceRejection::NotDirectory)
    );
    assert_eq!(
        policy.validate_workspace(&outside),
        Err(WorkspaceRejection::OutsideAllowRoots)
    );
    let prefix_collision = temp.path().join("safe-prefix/project");
    fs::create_dir_all(&prefix_collision).expect("prefix collision should be created");
    assert_eq!(
        policy.validate_workspace(&prefix_collision),
        Err(WorkspaceRejection::OutsideAllowRoots)
    );
    assert_eq!(
        policy
            .validate_workspace(&allowed.join("project/../project"))
            .unwrap(),
        fs::canonicalize(project).unwrap()
    );
}

#[test]
fn workspace_validator_hard_denies_injected_roots_before_allow_roots() {
    let temp = scratch();
    let all = temp.path().to_path_buf();
    let broad_policy = policy(temp.path(), all);
    let injected = roots(temp.path());

    #[cfg(unix)]
    assert_eq!(
        broad_policy.validate_workspace(Path::new("/")),
        Err(WorkspaceRejection::FilesystemRoot)
    );
    assert_eq!(
        broad_policy.validate_workspace(&injected.home),
        Err(WorkspaceRejection::HomeRoot)
    );
    assert_eq!(
        broad_policy.validate_workspace(&injected.temp),
        Err(WorkspaceRejection::TempTree)
    );
    assert_eq!(
        broad_policy.validate_workspace(&injected.system_trees[0]),
        Err(WorkspaceRejection::SystemTree)
    );
    assert_eq!(
        broad_policy.validate_workspace(injected.desktop.as_ref().unwrap()),
        Err(WorkspaceRejection::DesktopOrDownloads)
    );
    assert_eq!(
        broad_policy.validate_workspace(injected.downloads.as_ref().unwrap()),
        Err(WorkspaceRejection::DesktopOrDownloads)
    );

    let safe_home = injected.home.join("safe-project");
    fs::create_dir_all(&safe_home).expect("safe home child should be created");
    let safe_policy = policy(temp.path(), safe_home.clone());
    assert_eq!(
        safe_policy.validate_workspace(&safe_home).unwrap(),
        fs::canonicalize(safe_home).unwrap()
    );
}

#[cfg(unix)]
#[test]
fn workspace_validator_rejects_symlink_escape() {
    use std::os::unix::fs::symlink;

    let temp = scratch();
    let allowed = temp.path().join("safe");
    let outside = temp.path().join("outside");
    fs::create_dir_all(&allowed).expect("safe root should be created");
    fs::create_dir_all(&outside).expect("outside root should be created");
    symlink(&outside, allowed.join("escape")).expect("symlink should be created");
    let policy = policy(temp.path(), allowed);

    assert_eq!(
        policy.validate_workspace(&temp.path().join("safe/escape")),
        Err(WorkspaceRejection::OutsideAllowRoots)
    );
}

#[test]
fn canonical_allow_roots_are_deduplicated_and_default_workspace_must_be_usable() {
    let temp = scratch();
    let safe = temp.path().join("safe");
    fs::create_dir_all(&safe).expect("safe root should be created");
    let mut config = policy_config(safe.clone());
    config.workspace.allow_roots.push(safe.join("."));
    config.validate().expect("duplicate roots should normalize");
    assert_eq!(
        config.workspace.allow_roots,
        vec![fs::canonicalize(&safe).unwrap()]
    );

    config.default_workspace = Some(safe.join("."));
    config
        .validate()
        .expect("default workspace should normalize");
    assert_eq!(
        config.default_workspace,
        Some(fs::canonicalize(&safe).unwrap())
    );

    let mut unusable = BridgeConfig::default();
    unusable.owners.push("ou_owner_123456".to_owned());
    unusable.default_workspace = Some(safe);
    assert!(unusable.validate().is_err());
}

#[test]
fn fingerprint_is_stable_for_aliases_and_changes_for_every_policy_dimension() {
    let temp = scratch();
    let safe = temp.path().join("safe");
    let one = safe.join("one");
    let two = safe.join("two");
    fs::create_dir_all(&one).expect("workspace should be created");
    fs::create_dir_all(&two).expect("workspace should be created");
    let baseline = policy(temp.path(), safe.clone());
    let first = baseline.fingerprint(&one).unwrap();
    assert_eq!(first, baseline.fingerprint(&safe.join("one/.")).unwrap());
    assert_ne!(first, baseline.fingerprint(&two).unwrap());

    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let alias = temp.path().join("workspace-alias");
        symlink(&one, &alias).expect("workspace alias should be created");
        assert_eq!(first, baseline.fingerprint(&alias).unwrap());
    }

    let mut sandbox_config = policy_config(safe.clone());
    sandbox_config.codex.sandbox = SandboxMode::ReadOnly;
    let sandbox = AccessPolicy::with_platform_roots(&sandbox_config, &roots(temp.path())).unwrap();
    assert_ne!(first, sandbox.fingerprint(&one).unwrap());

    let mut named_config = policy_config(safe.clone());
    named_config.codex.approval_policy = ApprovalPolicy::Named("on-request".to_owned());
    let named = AccessPolicy::with_platform_roots(&named_config, &roots(temp.path())).unwrap();
    assert_ne!(first, named.fingerprint(&one).unwrap());

    for granular in granular_policies() {
        let mut granular_config = policy_config(safe.clone());
        granular_config.codex.approval_policy = ApprovalPolicy::Granular { granular };
        let granular_policy =
            AccessPolicy::with_platform_roots(&granular_config, &roots(temp.path())).unwrap();
        assert_ne!(first, granular_policy.fingerprint(&one).unwrap());
    }

    let mut network_config = policy_config(safe);
    network_config.workspace.network_access = true;
    let network = AccessPolicy::with_platform_roots(&network_config, &roots(temp.path())).unwrap();
    assert_ne!(first, network.fingerprint(&one).unwrap());
}

fn granular_policies() -> [GranularApprovalPolicy; 5] {
    [
        GranularApprovalPolicy {
            mcp_elicitations: true,
            rules: false,
            sandbox_approval: false,
            request_permissions: false,
            skill_approval: false,
        },
        GranularApprovalPolicy {
            mcp_elicitations: false,
            rules: true,
            sandbox_approval: false,
            request_permissions: false,
            skill_approval: false,
        },
        GranularApprovalPolicy {
            mcp_elicitations: false,
            rules: false,
            sandbox_approval: true,
            request_permissions: false,
            skill_approval: false,
        },
        GranularApprovalPolicy {
            mcp_elicitations: false,
            rules: false,
            sandbox_approval: false,
            request_permissions: true,
            skill_approval: false,
        },
        GranularApprovalPolicy {
            mcp_elicitations: false,
            rules: false,
            sandbox_approval: false,
            request_permissions: false,
            skill_approval: true,
        },
    ]
}

#[test]
fn debug_and_error_output_never_echo_sensitive_config_or_requested_paths() {
    let temp = scratch();
    let safe = temp.path().join("safe");
    fs::create_dir_all(&safe).expect("safe root should be created");
    let mut config = policy_config(safe.clone());
    config.owners = vec!["ou_extremely_sensitive_owner_123456".to_owned()];
    config.codex.binary = PathBuf::from("/outside/secret-codex");
    config.codex.codex_home = Some(PathBuf::from("/outside/secret-home"));
    config.paths.database = PathBuf::from("/outside/secret.sqlite");
    config.paths.attachment_cache = PathBuf::from("/outside/secret-cache");
    let debug = format!("{config:?}");
    assert!(!debug.contains("ou_extremely_sensitive_owner_123456"));
    assert!(!debug.contains("/outside/secret"));

    let policy = AccessPolicy::with_platform_roots(&config, &roots(temp.path())).unwrap();
    let rejected = policy
        .validate_workspace(&temp.path().join("private-requested-path"))
        .unwrap_err();
    let error = format!("{rejected:?} {rejected}");
    assert!(!error.contains("private-requested-path"));
    assert!(!format!("{policy:?}").contains("ou_extremely_sensitive_owner_123456"));
    let config_error = BridgeConfig::default().validate().unwrap_err();
    assert!(!format!("{config_error:?} {config_error}").contains("private-requested-path"));
}
