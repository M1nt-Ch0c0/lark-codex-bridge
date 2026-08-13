use std::path::PathBuf;

use lark_codex_bridge::runtime::commands::{
    BridgeCommand, CommandParseError, command_specs, parse_command,
};

#[test]
fn parses_the_five_first_stage_commands() {
    assert_eq!(parse_command("/new"), Ok(Some(BridgeCommand::New)));
    assert_eq!(parse_command(" /stop "), Ok(Some(BridgeCommand::Stop)));
    assert_eq!(parse_command("/status"), Ok(Some(BridgeCommand::Status)));
    assert_eq!(parse_command("/help"), Ok(Some(BridgeCommand::Help)));
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
}

#[test]
fn command_table_is_the_single_exact_help_source() {
    let specs = command_specs();
    assert_eq!(specs.len(), 5);
    assert_eq!(
        specs.iter().map(|spec| spec.name).collect::<Vec<_>>(),
        vec!["/new", "/stop", "/status", "/cd", "/help"]
    );
    assert!(specs.iter().all(|spec| !spec.usage.is_empty()));
    assert!(specs.iter().all(|spec| !spec.description.is_empty()));
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
}
