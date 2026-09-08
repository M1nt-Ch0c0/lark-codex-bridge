use std::path::PathBuf;

use lark_codex_bridge::codex::types::SandboxMode;
use lark_codex_bridge::runtime::commands::{
    BridgeCommand, CommandParseError, command_specs, command_text_from_card_action, parse_command,
    render_help,
};
use lark_codex_bridge::runtime::live::ConfigPatch;

#[test]
fn parses_the_first_stage_commands() {
    assert_eq!(parse_command("/new"), Ok(Some(BridgeCommand::New)));
    assert_eq!(parse_command("/reset"), Ok(Some(BridgeCommand::New)));
    assert_eq!(parse_command(" /stop "), Ok(Some(BridgeCommand::Stop)));
    assert_eq!(parse_command("/status"), Ok(Some(BridgeCommand::Status)));
    assert_eq!(parse_command("/help"), Ok(Some(BridgeCommand::Help)));
    assert_eq!(parse_command("/info"), Ok(Some(BridgeCommand::Info)));
    assert_eq!(
        parse_command("/resume"),
        Ok(Some(BridgeCommand::Resume { selector: None }))
    );
    assert_eq!(
        parse_command("/resume use thread-7"),
        Ok(Some(BridgeCommand::Resume {
            selector: Some("thread-7".to_owned())
        }))
    );
    assert_eq!(
        parse_command("/threads opaque-next-page"),
        Ok(Some(BridgeCommand::Threads {
            cursor: Some("opaque-next-page".to_owned())
        }))
    );
    assert_eq!(
        parse_command("/adopt t-candidate --handoff-complete"),
        Ok(Some(BridgeCommand::Adopt {
            selector: "t-candidate".to_owned()
        }))
    );
    assert_eq!(
        parse_command(r#"/adopt "selector with spaces" --handoff-complete"#),
        Ok(Some(BridgeCommand::Adopt {
            selector: "selector with spaces".to_owned()
        }))
    );
    assert_eq!(parse_command("/release"), Ok(Some(BridgeCommand::Release)));
    assert_eq!(
        parse_command("/cd"),
        Ok(Some(BridgeCommand::Cd { path: None }))
    );
    assert_eq!(
        parse_command("/cd ./workspace with spaces"),
        Ok(Some(BridgeCommand::Cd {
            path: Some(PathBuf::from("./workspace with spaces"))
        }))
    );
    assert_eq!(
        parse_command("/config"),
        Ok(Some(BridgeCommand::Config {
            action: None,
            cancel: false
        }))
    );
    assert_eq!(
        parse_command("/config model gpt-6-astra"),
        Ok(Some(BridgeCommand::Config {
            action: Some(ConfigPatch::Model(Some("gpt-6-astra".to_owned()))),
            cancel: false
        }))
    );
    assert_eq!(
        parse_command("/config apply model=gpt-6-astra sandbox=workspace-write approval=never"),
        Ok(Some(BridgeCommand::Config {
            action: Some(ConfigPatch::Form {
                model: Some("gpt-6-astra".to_owned()),
                effort: None,
                sandbox: Some(SandboxMode::WorkspaceWrite),
                approval: Some("never".to_owned()),
            }),
            cancel: false
        }))
    );
    assert_eq!(
        parse_command("/config cancel"),
        Ok(Some(BridgeCommand::Config {
            action: None,
            cancel: true
        }))
    );
    assert_eq!(
        parse_command("/config group add oc_allowed"),
        Ok(Some(BridgeCommand::Config {
            action: Some(ConfigPatch::GroupAdd("oc_allowed".to_owned())),
            cancel: false
        }))
    );
    assert_eq!(
        parse_command("/config sender rm ou_alice"),
        Ok(Some(BridgeCommand::Config {
            action: Some(ConfigPatch::SenderRemove("ou_alice".to_owned())),
            cancel: false
        }))
    );
    assert_eq!(
        parse_command("/config sandbox not-a-mode"),
        Err(CommandParseError::InvalidConfigValue)
    );
}

#[test]
fn unknown_slash_text_and_plain_text_remain_user_input() {
    assert_eq!(parse_command("/frobnicate"), Ok(None));
    assert_eq!(parse_command("/newish"), Ok(None));
    assert_eq!(parse_command("hello /new"), Ok(None));
    assert_eq!(parse_command(""), Ok(None));
}

#[test]
fn recognized_commands_reject_invalid_arguments() {
    assert_eq!(
        parse_command("/resume use"),
        Err(CommandParseError::MissingArgument { command: "/resume" })
    );
    assert_eq!(
        parse_command("/new unexpected"),
        Err(CommandParseError::UnexpectedArgument { command: "/new" })
    );
    assert_eq!(
        parse_command("/stop now"),
        Err(CommandParseError::UnexpectedArgument { command: "/stop" })
    );
    assert_eq!(
        parse_command("/threads one two"),
        Err(CommandParseError::UnexpectedArgument {
            command: "/threads"
        })
    );
    assert_eq!(
        parse_command("/adopt t-candidate"),
        Err(CommandParseError::HandoffConfirmationRequired)
    );
    assert_eq!(
        parse_command("/adopt t-candidate --handoff-complete extra"),
        Err(CommandParseError::HandoffConfirmationRequired)
    );
    assert_eq!(
        parse_command("/adopt selector with spaces --handoff-complete"),
        Err(CommandParseError::InvalidSelector)
    );
    assert_eq!(
        parse_command(r#"/adopt "unterminated --handoff-complete"#),
        Err(CommandParseError::InvalidSelector)
    );
    assert_eq!(
        parse_command(&format!("/threads {}", "c".repeat(513))),
        Err(CommandParseError::TooLong)
    );
    assert_eq!(
        parse_command(&format!("/adopt {} --handoff-complete", "s".repeat(129))),
        Err(CommandParseError::TooLong)
    );
}

#[test]
fn command_table_is_the_single_exact_help_source() {
    let specs = command_specs();
    assert_eq!(specs.len(), 11);
    assert_eq!(
        specs.iter().map(|spec| spec.name).collect::<Vec<_>>(),
        vec![
            "/new", "/stop", "/status", "/resume", "/cd", "/threads", "/adopt", "/release",
            "/help", "/info", "/config"
        ]
    );
    assert!(specs.iter().all(|spec| !spec.usage.is_empty()));
    assert!(specs.iter().all(|spec| !spec.description.is_empty()));
}

#[test]
fn help_text_is_rendered_from_the_single_command_table() {
    let help = render_help();
    assert_eq!(help.lines().next(), Some("Available commands:"));
    assert_eq!(help.lines().count(), command_specs().len() + 1);
    for spec in command_specs() {
        assert!(
            help.lines()
                .any(|line| line == format!("{} — {}", spec.usage, spec.description)),
            "missing help entry for {}",
            spec.name
        );
    }
}

#[test]
fn command_limits_and_debug_never_expose_a_workspace_path() {
    let oversized = format!("/cd {}", "x".repeat(16 * 1024));
    assert_eq!(parse_command(&oversized), Err(CommandParseError::TooLong));
    let command = BridgeCommand::Resume {
        selector: Some("sensitive-thread-selector".to_owned()),
    };
    let debug = format!("{command:?}");
    assert!(!debug.contains("sensitive"));
    assert!(debug.contains("selector_bytes"));

    let command = BridgeCommand::Cd {
        path: Some(PathBuf::from("/sensitive/customer/workspace")),
    };
    let debug = format!("{command:?}");
    assert!(!debug.contains("sensitive"));
    assert!(!debug.contains("customer"));
    assert!(debug.contains("path_bytes"));

    let cursor = "sensitive-cursor".to_owned();
    let command = BridgeCommand::Threads {
        cursor: Some(cursor.clone()),
    };
    let debug = format!("{command:?}");
    assert!(!debug.contains(&cursor));
    assert!(debug.contains("cursor_bytes"));

    let selector = "sensitive-thread-selector".to_owned();
    let command = BridgeCommand::Adopt {
        selector: selector.clone(),
    };
    let debug = format!("{command:?}");
    assert!(!debug.contains(&selector));
    assert!(debug.contains("selector_bytes"));
}

#[test]
fn card_actions_round_trip_to_slash_commands() {
    assert_eq!(
        command_text_from_card_action("new", None).as_deref(),
        Some("/new")
    );
    assert_eq!(
        command_text_from_card_action("resume", None).as_deref(),
        Some("/resume")
    );
    assert_eq!(
        command_text_from_card_action("resume.use", Some("thread-9")).as_deref(),
        Some("/resume use thread-9")
    );
    assert_eq!(
        command_text_from_card_action("cd", Some("~/src")).as_deref(),
        Some("/cd ~/src")
    );
    assert_eq!(
        command_text_from_card_action("config", None).as_deref(),
        Some("/config")
    );
    assert_eq!(
        command_text_from_card_action("config.cancel", None).as_deref(),
        Some("/config cancel")
    );
    assert_eq!(
        command_text_from_card_action("info", None).as_deref(),
        Some("/info")
    );
    assert_eq!(command_text_from_card_action("unknown", None), None);
}
