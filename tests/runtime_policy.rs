use std::fs;
use std::path::{Path, PathBuf};

use lark_codex_bridge::codex::{
    external::CodexBackendConfig,
    types::{ApprovalPolicy, GranularApprovalPolicy, SandboxMode},
};
use lark_codex_bridge::config::{
    BridgeConfig, CodexSection, ConcurrencyConfig, PathsSection, WorkspacePolicy,
};
use lark_codex_bridge::lark::api::ChatMode;
use lark_codex_bridge::lark::normalize::{InboundEvent, ScopeKey};
use lark_codex_bridge::runtime::policy::{AccessDecision, AccessPolicy, WorkspaceRejection};
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
        sender_is_human: true,
        mentions: Vec::new(),
        parts: Vec::new(),
        resources: vec![],
        message_type: "text".to_owned(),
        create_time_ms: 0,
        scope: ScopeKey::Chat("oc_test".to_owned()),
    }
}

fn event_with_mentions(
    sender: &str,
    chat_type: ChatMode,
    mentions_bot: bool,
    mention_all: bool,
) -> InboundEvent {
    let mut event = event(sender, chat_type, mentions_bot);
    event.mention_all = mention_all;
    event
}

fn event_in_chat(
    sender: &str,
    chat_type: ChatMode,
    chat_id: &str,
    mentions_bot: bool,
) -> InboundEvent {
    let mut event = event(sender, chat_type, mentions_bot);
    chat_id.clone_into(&mut event.chat_id);
    event.scope = match chat_type {
        ChatMode::P2p | ChatMode::Group => ScopeKey::Chat(chat_id.to_owned()),
        ChatMode::Topic => ScopeKey::Thread(chat_id.to_owned(), "omt_test".to_owned()),
    };
    event
}

fn non_human(sender: &str, chat_type: ChatMode, chat_id: &str, mentions_bot: bool) -> InboundEvent {
    let mut event = event_in_chat(sender, chat_type, chat_id, mentions_bot);
    event.sender_is_human = false;
    event
}

fn policy_config(allow_root: PathBuf) -> BridgeConfig {
    BridgeConfig {
        owners: vec!["ou_owner_123456".to_owned()],
        allowed_senders: vec![],
        allowed_groups: vec![],
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

fn policy(allow_root: PathBuf) -> AccessPolicy {
    AccessPolicy::from_config(&policy_config(allow_root)).expect("safe test policy should build")
}

fn policy_with(
    allow_root: PathBuf,
    allowed_senders: Vec<String>,
    allowed_groups: Vec<String>,
) -> AccessPolicy {
    let mut config = policy_config(allow_root);
    config.allowed_senders = allowed_senders;
    config.allowed_groups = allowed_groups;
    AccessPolicy::from_config(&config).expect("safe test policy should build")
}

fn production_home() -> PathBuf {
    #[cfg(windows)]
    {
        std::env::var_os("USERPROFILE")
            .filter(|value| !value.is_empty())
            .map(PathBuf::from)
            .or_else(|| {
                let drive = std::env::var_os("HOMEDRIVE")?;
                let path = std::env::var_os("HOMEPATH")?;
                Some(PathBuf::from(drive).join(path))
            })
            .expect("test environment should expose its home directory")
    }
    #[cfg(not(windows))]
    {
        std::env::var_os("HOME")
            .map(PathBuf::from)
            .expect("test environment should expose its home directory")
    }
}

#[test]
fn minimal_config_has_safe_defaults_and_resolves_relative_runtime_paths() {
    let temp = scratch();
    let config_path = temp.path().join("config.toml");
    fs::write(&config_path, MINIMAL_CONFIG).expect("fixture should write");

    let config = BridgeConfig::load(Some(&config_path)).expect("minimal config should load");

    assert_eq!(config.owners, ["ou_owner_123456"]);
    assert!(config.allowed_senders.is_empty());
    assert!(config.allowed_groups.is_empty());
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
    assert_eq!(config.allowed_senders, ["ou_sender_111111"]);
    assert_eq!(config.allowed_groups, ["oc_group_222222"]);
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
    assert!(matches!(
        config.codex.backend,
        CodexBackendConfig::SpawnedStdio { ref binary, ref codex_home }
            if binary == &PathBuf::from("/opt/codex/bin/codex")
                && codex_home.as_deref() == Some(Path::new("/opt/codex/home"))
    ));

    let encoded = toml::to_string(&config).expect("full config should serialize");
    let reparsed = toml::from_str::<BridgeConfig>(&encoded).expect("full config should reparse");
    assert_eq!(reparsed.owners, config.owners);
    assert_eq!(reparsed.allowed_senders, config.allowed_senders);
    assert_eq!(reparsed.allowed_groups, config.allowed_groups);
    assert_eq!(reparsed.codex.approval_policy, config.codex.approval_policy);
    assert_eq!(reparsed.paths.database, config.paths.database);
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
fn granular_approval_rejects_unknown_wrapper_and_inner_keys() {
    let wrapper_unknown = r#"
owners = ["ou_owner_123456"]

[codex.approval_policy]
unexpected_wrapper = true

[codex.approval_policy.granular]
mcp_elicitations = false
rules = false
sandbox_approval = false
request_permissions = false
skill_approval = false
"#;
    let inner_unknown = r#"
owners = ["ou_owner_123456"]

[codex.approval_policy.granular]
mcp_elicitations = false
rules = false
sandbox_approval = false
request_permissions = false
skill_approval = false
unexpected_inner = true
"#;

    assert!(toml::from_str::<BridgeConfig>(wrapper_unknown).is_err());
    assert!(toml::from_str::<BridgeConfig>(inner_unknown).is_err());
}

#[test]
fn valid_granular_approval_converts_to_the_rpc_policy_type() {
    let source = r#"
owners = ["ou_owner_123456"]

[codex.approval_policy.granular]
mcp_elicitations = true
rules = false
sandbox_approval = true
request_permissions = false
skill_approval = true
"#;

    let config =
        toml::from_str::<BridgeConfig>(source).expect("strict granular policy should parse");
    assert_eq!(
        config.codex.approval_policy,
        ApprovalPolicy::Granular {
            granular: GranularApprovalPolicy {
                mcp_elicitations: true,
                rules: false,
                sandbox_approval: true,
                request_permissions: false,
                skill_approval: true,
            },
        }
    );
    let encoded = toml::to_string(&config).expect("valid granular config should serialize");
    let reparsed =
        toml::from_str::<BridgeConfig>(&encoded).expect("serialized granular config should parse");
    assert_eq!(reparsed.codex.approval_policy, config.codex.approval_policy);
}

#[test]
fn granular_approval_omissions_default_every_bit_to_false() {
    let empty = r#"
owners = ["ou_owner_123456"]

[codex.approval_policy.granular]
"#;
    let config = toml::from_str::<BridgeConfig>(empty)
        .expect("an empty granular policy should conservatively parse");
    assert_eq!(
        config.codex.approval_policy,
        ApprovalPolicy::Granular {
            granular: granular_all_false(),
        }
    );

    let fields = [
        "mcp_elicitations",
        "rules",
        "sandbox_approval",
        "request_permissions",
        "skill_approval",
    ];
    for (field, expected) in fields.into_iter().zip(granular_single_bit_policies()) {
        let source = format!(
            "owners = [\"ou_owner_123456\"]\n\n[codex.approval_policy.granular]\n{field} = true\n"
        );
        let config = toml::from_str::<BridgeConfig>(&source)
            .expect("a single granular override should conservatively parse");
        assert_eq!(
            config.codex.approval_policy,
            ApprovalPolicy::Granular { granular: expected },
            "unexpected default for {field}"
        );
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

#[cfg(unix)]
#[test]
fn config_rechecks_allow_root_bytes_after_canonicalization() {
    use std::os::unix::fs::symlink;

    let temp = scratch();
    let deep = temp
        .path()
        .join("canonical-targets")
        .join("x".repeat(200))
        .join("y".repeat(100));
    fs::create_dir_all(&deep).expect("deep canonical target should be created");
    let mut aliases = Vec::new();
    for index in 0..lark_codex_bridge::limits::MAX_CONFIG_ALLOW_ROOTS {
        let target = deep.join(format!("target-{index:02}"));
        fs::create_dir(&target).expect("distinct canonical target should be created");
        let alias = temp.path().join(format!("alias-{index:02}"));
        symlink(target, &alias).expect("short allow-root alias should be created");
        aliases.push(alias);
    }
    let raw_bytes = aliases
        .iter()
        .map(|path| path.as_os_str().as_encoded_bytes().len())
        .sum::<usize>();
    let canonical_bytes = aliases
        .iter()
        .map(|path| {
            fs::canonicalize(path)
                .expect("alias should canonicalize")
                .as_os_str()
                .as_encoded_bytes()
                .len()
        })
        .sum::<usize>();
    assert!(raw_bytes <= lark_codex_bridge::limits::MAX_CONFIG_ALLOW_ROOT_BYTES);
    assert!(canonical_bytes > lark_codex_bridge::limits::MAX_CONFIG_ALLOW_ROOT_BYTES);

    let mut config = policy_config(aliases.remove(0));
    config.workspace.allow_roots.extend(aliases);

    assert!(config.validate().is_err());
}

#[test]
fn owner_and_direct_mention_gate_uses_chat_mode_not_scope() {
    let temp = scratch();
    let allowed = temp.path().join("safe");
    fs::create_dir_all(&allowed).expect("safe root should be created");
    let policy = policy(allowed);

    assert_eq!(
        policy.decide(&event("ou_owner_123456", ChatMode::P2p, false)),
        AccessDecision::Allow
    );
    assert_eq!(
        policy.decide(&event("ou_stranger", ChatMode::P2p, true)),
        AccessDecision::DenyNotOwner
    );
    assert_eq!(
        policy.decide(&event("ou_stranger", ChatMode::Group, false)),
        AccessDecision::DenyNotGroup
    );
    assert_eq!(
        policy.decide(&event("ou_owner_123456", ChatMode::Group, false)),
        AccessDecision::DenyMissingMention
    );
    assert_eq!(
        policy.decide(&event_with_mentions(
            "ou_owner_123456",
            ChatMode::Group,
            false,
            true,
        )),
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

fn matrix_policy() -> AccessPolicy {
    let temp = scratch();
    let allowed = temp.path().join("safe");
    fs::create_dir_all(&allowed).expect("safe root should be created");
    policy_with(
        allowed,
        vec!["ou_sender".to_owned()],
        vec!["oc_allowed_group".to_owned()],
    )
}

#[test]
fn ordinary_turn_matrix_owner_and_sender_rows() {
    let policy = matrix_policy();
    // owner, P2P
    assert_eq!(
        policy.decide(&event("ou_owner_123456", ChatMode::P2p, false)),
        AccessDecision::Allow
    );
    // allowed sender, P2P
    assert_eq!(
        policy.decide(&event("ou_sender", ChatMode::P2p, false)),
        AccessDecision::Allow
    );
    // owner in group, direct mention
    assert_eq!(
        policy.decide(&event("ou_owner_123456", ChatMode::Group, true)),
        AccessDecision::Allow
    );
    // owner in group, no mention
    assert_eq!(
        policy.decide(&event("ou_owner_123456", ChatMode::Group, false)),
        AccessDecision::DenyMissingMention
    );
    // allowed sender in group, direct mention
    assert_eq!(
        policy.decide(&event("ou_sender", ChatMode::Group, true)),
        AccessDecision::Allow
    );
    // allowed sender in group, no mention
    assert_eq!(
        policy.decide(&event("ou_sender", ChatMode::Group, false)),
        AccessDecision::DenyMissingMention
    );
    // allowed sender in topic, direct mention
    assert_eq!(
        policy.decide(&event("ou_sender", ChatMode::Topic, true)),
        AccessDecision::Allow
    );
    // allowed sender in topic, no mention
    assert_eq!(
        policy.decide(&event("ou_sender", ChatMode::Topic, false)),
        AccessDecision::DenyMissingMention
    );
}

#[test]
fn ordinary_turn_matrix_allowed_group_rows() {
    let policy = matrix_policy();
    // allowed-group ordinary member, direct mention
    assert_eq!(
        policy.decide(&event_in_chat(
            "ou_member",
            ChatMode::Group,
            "oc_allowed_group",
            true,
        )),
        AccessDecision::Allow
    );
    // allowed-group ordinary member, no mention
    assert_eq!(
        policy.decide(&event_in_chat(
            "ou_member",
            ChatMode::Group,
            "oc_allowed_group",
            false,
        )),
        AccessDecision::DenyMissingMention
    );
    // allowed-group ordinary member, @all only
    let mut at_all = event_in_chat("ou_member", ChatMode::Group, "oc_allowed_group", false);
    at_all.mention_all = true;
    assert_eq!(policy.decide(&at_all), AccessDecision::DenyMissingMention);
    // allowed-group ordinary member in topic, direct mention
    assert_eq!(
        policy.decide(&event_in_chat(
            "ou_member",
            ChatMode::Topic,
            "oc_allowed_group",
            true,
        )),
        AccessDecision::Allow
    );
    // allowed-group ordinary member in topic, no mention
    assert_eq!(
        policy.decide(&event_in_chat(
            "ou_member",
            ChatMode::Topic,
            "oc_allowed_group",
            false,
        )),
        AccessDecision::DenyMissingMention
    );
}

#[test]
fn ordinary_turn_matrix_unauthorized_and_non_human_rows() {
    let policy = matrix_policy();
    // unauthorized group
    assert_eq!(
        policy.decide(&event("ou_stranger", ChatMode::Group, true)),
        AccessDecision::DenyNotGroup
    );
    // unauthorized P2P
    assert_eq!(
        policy.decide(&event("ou_stranger", ChatMode::P2p, false)),
        AccessDecision::DenyNotOwner
    );
    // non-human in an allowed group is never accepted
    assert_eq!(
        policy.decide(&non_human(
            "ou_member",
            ChatMode::Group,
            "oc_allowed_group",
            true,
        )),
        AccessDecision::DenyNotSender
    );
    // non-human P2P is never accepted, even with an owner ID
    assert_eq!(
        policy.decide(&non_human(
            "ou_owner_123456",
            ChatMode::P2p,
            "oc_test",
            false
        )),
        AccessDecision::DenyNotSender
    );
}

#[test]
fn command_path_never_authorizes_via_sender_or_group_allowlists() {
    let temp = scratch();
    let allowed = temp.path().join("safe");
    fs::create_dir_all(&allowed).expect("safe root should be created");
    let policy = policy_with(
        allowed,
        vec!["ou_sender".to_owned()],
        vec!["oc_allowed_group".to_owned()],
    );

    assert_eq!(
        policy.decide_command(&event("ou_owner_123456", ChatMode::P2p, false)),
        AccessDecision::Allow
    );
    assert_eq!(
        policy.decide_command(&event("ou_owner_123456", ChatMode::Group, true)),
        AccessDecision::Allow
    );
    assert_eq!(
        policy.decide_command(&event("ou_owner_123456", ChatMode::Group, false)),
        AccessDecision::DenyMissingMention
    );
    assert_eq!(
        policy.decide_command(&event("ou_sender", ChatMode::P2p, false)),
        AccessDecision::DenyOwnerCommandRequired
    );
    assert_eq!(
        policy.decide_command(&event("ou_sender", ChatMode::Group, true)),
        AccessDecision::DenyOwnerCommandRequired
    );
    assert_eq!(
        policy.decide_command(&event_in_chat(
            "ou_member",
            ChatMode::Group,
            "oc_allowed_group",
            true,
        )),
        AccessDecision::DenyOwnerCommandRequired
    );
    assert_eq!(
        policy.decide_command(&non_human(
            "ou_owner_123456",
            ChatMode::Group,
            "oc_allowed_group",
            true,
        )),
        AccessDecision::DenyNotSender
    );
}

#[test]
fn config_deduplicates_sender_and_group_allowlists_idempotently() {
    let temp = scratch();
    let safe = temp.path().join("safe");
    fs::create_dir_all(&safe).expect("safe root should be created");
    let mut config = policy_config(safe);
    config.allowed_senders = vec![
        "ou_sender".to_owned(),
        "ou_sender".to_owned(),
        "ou_other".to_owned(),
    ];
    config.allowed_groups = vec![
        "oc_group".to_owned(),
        "oc_group".to_owned(),
        "oc_other".to_owned(),
    ];
    config
        .validate()
        .expect("duplicate allowlist IDs should normalize");
    assert_eq!(config.allowed_senders, ["ou_sender", "ou_other"]);
    assert_eq!(config.allowed_groups, ["oc_group", "oc_other"]);
}

#[test]
fn config_rejects_malformed_or_oversized_sender_and_group_allowlists() {
    let temp = scratch();
    let safe = temp.path().join("safe");
    fs::create_dir_all(&safe).expect("safe root should be created");

    for bad_sender in ["", " ", "ou_sender ", "ou sender", "ou\tsender"] {
        let mut config = policy_config(safe.clone());
        config.allowed_senders = vec![bad_sender.to_owned()];
        assert!(config.validate().is_err());
    }
    for bad_group in ["", " ", "oc_group ", "oc group"] {
        let mut config = policy_config(safe.clone());
        config.allowed_groups = vec![bad_group.to_owned()];
        assert!(config.validate().is_err());
    }

    let mut config = policy_config(safe.clone());
    config.allowed_senders = (0..=lark_codex_bridge::limits::MAX_CONFIG_ALLOWED_SENDERS)
        .map(|index| format!("ou_sender_{index}"))
        .collect();
    assert!(config.validate().is_err());

    let mut config = policy_config(safe.clone());
    config.allowed_senders =
        vec!["o".repeat(lark_codex_bridge::limits::MAX_CONFIG_ALLOWED_SENDER_BYTES + 1)];
    assert!(config.validate().is_err());

    let mut config = policy_config(safe.clone());
    config.allowed_groups = (0..=lark_codex_bridge::limits::MAX_CONFIG_ALLOWED_GROUPS)
        .map(|index| format!("oc_group_{index}"))
        .collect();
    assert!(config.validate().is_err());

    let mut config = policy_config(safe.clone());
    config.allowed_groups =
        vec!["g".repeat(lark_codex_bridge::limits::MAX_CONFIG_ALLOWED_GROUP_BYTES + 1)];
    assert!(config.validate().is_err());
}

#[test]
fn group_authorization_revokes_when_the_group_id_is_removed() {
    let temp = scratch();
    let allowed = temp.path().join("safe");
    fs::create_dir_all(&allowed).expect("safe root should be created");

    let granted = policy_with(allowed.clone(), vec![], vec!["oc_allowed_group".to_owned()]);
    let member = event_in_chat("ou_member", ChatMode::Group, "oc_allowed_group", true);
    assert_eq!(granted.decide(&member), AccessDecision::Allow);

    let revoked = policy_with(allowed, vec![], vec![]);
    assert_eq!(revoked.decide(&member), AccessDecision::DenyNotGroup);
}

#[test]
fn workspace_validator_fails_closed_and_canonicalizes_safe_aliases() {
    let temp = scratch();
    let allowed = temp.path().join("safe");
    let project = allowed.join("project");
    let outside = temp.path().join("outside");
    fs::create_dir_all(&project).expect("project should be created");
    fs::create_dir_all(&outside).expect("outside should be created");
    let policy = policy(allowed.clone());

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
fn production_policy_hard_denies_filesystem_and_home_roots() {
    let temp = scratch();
    let safe = temp.path().join("safe");
    fs::create_dir_all(&safe).expect("safe root should be created");
    let policy = policy(safe);

    let home = production_home();
    assert_eq!(
        policy.validate_workspace(&home),
        Err(WorkspaceRejection::HomeRoot)
    );
    for broad_root in [home.join("Desktop"), home.join("Downloads")] {
        if broad_root.is_dir() {
            assert!(AccessPolicy::from_config(&policy_config(broad_root)).is_err());
        }
    }
    #[cfg(unix)]
    assert_eq!(
        policy.validate_workspace(Path::new("/")),
        Err(WorkspaceRejection::FilesystemRoot)
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
    let policy = policy(allowed);

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
fn policy_construction_rejects_missing_file_and_protected_allow_roots() {
    let temp = scratch();
    let missing = temp.path().join("missing");
    assert!(AccessPolicy::from_config(&policy_config(missing)).is_err());

    let file = temp.path().join("file");
    fs::write(&file, "not a directory").expect("allow-root file should be created");
    assert!(AccessPolicy::from_config(&policy_config(file)).is_err());

    let home = production_home();
    assert!(AccessPolicy::from_config(&policy_config(home)).is_err());

    #[cfg(unix)]
    {
        assert!(AccessPolicy::from_config(&policy_config(PathBuf::from("/"))).is_err());
        assert!(AccessPolicy::from_config(&policy_config(PathBuf::from("/tmp"))).is_err());
        assert!(AccessPolicy::from_config(&policy_config(PathBuf::from("/etc"))).is_err());
    }
}

#[cfg(unix)]
#[test]
fn policy_construction_rejects_dangling_allow_root() {
    use std::os::unix::fs::symlink;

    let temp = scratch();
    let dangling = temp.path().join("dangling");
    symlink(temp.path().join("absent-target"), &dangling).expect("dangling symlink should build");

    assert!(AccessPolicy::from_config(&policy_config(dangling)).is_err());
}

#[test]
fn fingerprint_is_stable_for_aliases_and_changes_for_every_policy_dimension() {
    let temp = scratch();
    let safe = temp.path().join("safe");
    let one = safe.join("one");
    let two = safe.join("two");
    let outside = temp.path().join("outside");
    fs::create_dir_all(&one).expect("workspace should be created");
    fs::create_dir_all(&two).expect("workspace should be created");
    fs::create_dir_all(&outside).expect("outside workspace should be created");
    let baseline = policy(safe.clone());
    let first = baseline.fingerprint(&one).unwrap();
    assert_eq!(first.as_str().len(), 32);
    assert!(
        first
            .as_str()
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
    assert_eq!(
        baseline.fingerprint(Path::new("relative-workspace")),
        Err(WorkspaceRejection::Relative)
    );
    assert_eq!(
        baseline.fingerprint(&safe.join("missing-workspace")),
        Err(WorkspaceRejection::Inaccessible)
    );
    assert_eq!(
        baseline.fingerprint(&outside),
        Err(WorkspaceRejection::OutsideAllowRoots)
    );
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
    let sandbox = AccessPolicy::from_config(&sandbox_config).unwrap();
    let read_only_fingerprint = sandbox.fingerprint(&one).unwrap();
    assert_ne!(first, read_only_fingerprint);

    let mut danger_config = policy_config(safe.clone());
    danger_config.codex.sandbox = SandboxMode::DangerFullAccess;
    let danger = AccessPolicy::from_config(&danger_config).unwrap();
    let danger_fingerprint = danger.fingerprint(&one).unwrap();
    assert_ne!(first, danger_fingerprint);
    assert_ne!(read_only_fingerprint, danger_fingerprint);

    let mut named_config = policy_config(safe.clone());
    named_config.codex.approval_policy = ApprovalPolicy::Named("on-request".to_owned());
    let named = AccessPolicy::from_config(&named_config).unwrap();
    assert_ne!(first, named.fingerprint(&one).unwrap());

    let mut granular_config = policy_config(safe.clone());
    granular_config.codex.approval_policy = ApprovalPolicy::Granular {
        granular: granular_all_false(),
    };
    let granular_baseline = AccessPolicy::from_config(&granular_config).unwrap();
    let granular_baseline_fingerprint = granular_baseline.fingerprint(&one).unwrap();
    assert_ne!(first, granular_baseline_fingerprint);

    for granular in granular_single_bit_policies() {
        let mut changed = policy_config(safe.clone());
        changed.codex.approval_policy = ApprovalPolicy::Granular { granular };
        let changed = AccessPolicy::from_config(&changed).unwrap();
        assert_ne!(
            granular_baseline_fingerprint,
            changed.fingerprint(&one).unwrap()
        );
    }

    let mut network_config = policy_config(safe);
    network_config.workspace.network_access = true;
    let network = AccessPolicy::from_config(&network_config).unwrap();
    assert_ne!(first, network.fingerprint(&one).unwrap());
}

fn granular_all_false() -> GranularApprovalPolicy {
    GranularApprovalPolicy {
        mcp_elicitations: false,
        rules: false,
        sandbox_approval: false,
        request_permissions: false,
        skill_approval: false,
    }
}

fn granular_single_bit_policies() -> [GranularApprovalPolicy; 5] {
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
    config.codex.backend = CodexBackendConfig::SpawnedStdio {
        binary: PathBuf::from("/outside/secret-codex"),
        codex_home: Some(PathBuf::from("/outside/secret-home")),
    };
    config.paths.database = PathBuf::from("/outside/secret.sqlite");
    config.paths.attachment_cache = PathBuf::from("/outside/secret-cache");
    let debug = format!("{config:?}");
    assert!(!debug.contains("ou_extremely_sensitive_owner_123456"));
    assert!(!debug.contains("/outside/secret"));

    let policy = AccessPolicy::from_config(&config).unwrap();
    let rejected = policy
        .validate_workspace(&temp.path().join("private-requested-path"))
        .unwrap_err();
    let error = format!("{rejected:?} {rejected}");
    assert!(!error.contains("private-requested-path"));
    assert!(!format!("{policy:?}").contains("ou_extremely_sensitive_owner_123456"));
    let config_error = BridgeConfig::default().validate().unwrap_err();
    assert!(!format!("{config_error:?} {config_error}").contains("private-requested-path"));
    let decision = AccessDecision::DenyWorkspace {
        reason: "STATIC_REASON_SENTINEL",
    };
    assert_eq!(format!("{decision:?}"), "DenyWorkspace");
}

#[test]
fn unvalidated_config_debug_shows_only_counts_presence_and_static_summaries() {
    let temp = scratch();
    let path_sentinel = temp.path().join("debug-path-sentinel");
    fs::create_dir_all(&path_sentinel).expect("debug sentinel directory should be created");
    let mut config = policy_config(path_sentinel.clone());
    config.owners = vec!["ou_sensitive_OWNER_FRAGMENT".to_owned()];
    config.default_workspace = Some(path_sentinel.clone());
    config.codex.backend = CodexBackendConfig::SpawnedStdio {
        binary: path_sentinel.join("binary-sentinel"),
        codex_home: Some(path_sentinel.join("home-sentinel")),
    };
    config.paths.database = path_sentinel.join("database-sentinel");
    config.paths.attachment_cache = path_sentinel.join("cache-sentinel");

    let debug = format!("{config:?}");

    assert!(debug.contains("owner_count: 1"));
    assert!(debug.contains("default_workspace_configured: true"));
    assert!(debug.contains("allow_root_count: 1"));
    assert!(!debug.contains("OWNER_FRAGMENT"));
    assert!(!debug.contains(&path_sentinel.display().to_string()));
    assert!(!debug.contains("binary-sentinel"));
    assert!(!debug.contains("database-sentinel"));
    assert!(!debug.contains("cache-sentinel"));
    assert!(!format!("{:?}", config.workspace).contains(&path_sentinel.display().to_string()));
}
