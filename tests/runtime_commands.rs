use std::path::PathBuf;

use lark_codex_bridge::runtime::commands::{
    BridgeCommand, CommandParseError, command_specs, parse_command, render_help,
};

#[test]
fn parses_the_eight_first_stage_commands() {
    assert_eq!(parse_command("/new"), Ok(Some(BridgeCommand::New)));
    assert_eq!(parse_command(" /stop "), Ok(Some(BridgeCommand::Stop)));
    assert_eq!(parse_command("/status"), Ok(Some(BridgeCommand::Status)));
    assert_eq!(parse_command("/help"), Ok(Some(BridgeCommand::Help)));
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
    assert_eq!(parse_command("/release"), Ok(Some(BridgeCommand::Release)));
    assert_eq!(
        parse_command("/cd ./workspace with spaces"),
        Ok(Some(BridgeCommand::Cd {
            path: PathBuf::from("./workspace with spaces")
        }))
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
        parse_command("/cd"),
        Err(CommandParseError::MissingArgument { command: "/cd" })
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
        parse_command(r#"/adopt "selector with spaces" --handoff-complete"#),
        Ok(Some(BridgeCommand::Adopt {
            selector: "selector with spaces".to_owned()
        }))
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
    assert_eq!(specs.len(), 8);
    assert_eq!(
        specs.iter().map(|spec| spec.name).collect::<Vec<_>>(),
        vec![
            "/new", "/stop", "/status", "/cd", "/threads", "/adopt", "/release", "/help"
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
    let command = BridgeCommand::Cd {
        path: PathBuf::from("/sensitive/customer/workspace"),
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
