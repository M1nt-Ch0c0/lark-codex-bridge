//! Pure parsing contracts for the first-stage bridge commands.
//!
//! Runtime handlers and their durable replies are wired only after the
//! outbox lands. Keeping recognition pure makes unknown slash-prefixed text
//! safely fall back to ordinary Codex input without a temporary send path.

use std::fmt;
use std::path::PathBuf;

use crate::limits::BRIDGE_COMMAND_MAX_BYTES;

/// One recognized first-stage command.
#[derive(Clone, Eq, PartialEq)]
pub enum BridgeCommand {
    /// Archive the active session and retain its workspace.
    New,
    /// Interrupt the active turn, when one exists.
    Stop,
    /// Show a redacted structural runtime summary.
    Status,
    /// Change to a policy-validated workspace and reset the session.
    Cd {
        /// Raw user-supplied path. The handler must validate and canonicalize
        /// it through `AccessPolicy` before any persistence or RPC.
        path: PathBuf,
    },
    /// Render the command table.
    Help,
}

impl fmt::Debug for BridgeCommand {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::New => formatter.write_str("New"),
            Self::Stop => formatter.write_str("Stop"),
            Self::Status => formatter.write_str("Status"),
            Self::Cd { path } => formatter
                .debug_struct("Cd")
                .field("path_bytes", &path.as_os_str().len())
                .finish(),
            Self::Help => formatter.write_str("Help"),
        }
    }
}

/// Static parse failure safe to display without echoing command arguments.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum CommandParseError {
    /// A recognized command is larger than the bounded command parser input.
    #[error("the bridge command is too long")]
    TooLong,
    /// A required argument is absent.
    #[error("{command} requires an argument")]
    MissingArgument {
        /// Static recognized command name.
        command: &'static str,
    },
    /// A no-argument command received trailing input.
    #[error("{command} does not accept arguments")]
    UnexpectedArgument {
        /// Static recognized command name.
        command: &'static str,
    },
}

/// Stable command metadata used to render `/help` and audit command drift.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandSpec {
    /// Exact command token.
    pub name: &'static str,
    /// One-line usage.
    pub usage: &'static str,
    /// One-line description.
    pub description: &'static str,
}

const COMMAND_SPECS: [CommandSpec; 5] = [
    CommandSpec {
        name: "/new",
        usage: "/new",
        description: "start a new session in the current workspace",
    },
    CommandSpec {
        name: "/stop",
        usage: "/stop",
        description: "interrupt the active turn",
    },
    CommandSpec {
        name: "/status",
        usage: "/status",
        description: "show a redacted runtime status",
    },
    CommandSpec {
        name: "/cd",
        usage: "/cd <path>",
        description: "change workspace and reset the session",
    },
    CommandSpec {
        name: "/help",
        usage: "/help",
        description: "list bridge commands",
    },
];

/// Returns the single stable metadata table for first-stage commands.
#[must_use]
pub const fn command_specs() -> &'static [CommandSpec] {
    &COMMAND_SPECS
}

/// Parses one possible command.
///
/// `Ok(None)` deliberately means “ordinary user input”, including unknown
/// slash-prefixed text. Handlers must not silently discard that input.
///
/// # Errors
///
/// Returns a static error only for malformed *recognized* commands.
pub fn parse_command(text: &str) -> Result<Option<BridgeCommand>, CommandParseError> {
    let trimmed = text.trim();
    let Some((name, arguments)) = split_command(trimmed) else {
        return Ok(None);
    };
    let recognized = matches!(name, "/new" | "/stop" | "/status" | "/cd" | "/help");
    if !recognized {
        return Ok(None);
    }
    if trimmed.len() > BRIDGE_COMMAND_MAX_BYTES {
        return Err(CommandParseError::TooLong);
    }
    match name {
        "/new" => no_argument(arguments, "/new", BridgeCommand::New),
        "/stop" => no_argument(arguments, "/stop", BridgeCommand::Stop),
        "/status" => no_argument(arguments, "/status", BridgeCommand::Status),
        "/help" => no_argument(arguments, "/help", BridgeCommand::Help),
        "/cd" => {
            let path = arguments.trim();
            if path.is_empty() {
                Err(CommandParseError::MissingArgument { command: "/cd" })
            } else {
                Ok(Some(BridgeCommand::Cd {
                    path: PathBuf::from(path),
                }))
            }
        }
        _ => Ok(None),
    }
}

fn split_command(text: &str) -> Option<(&str, &str)> {
    if !text.starts_with('/') {
        return None;
    }
    let split_at = text.find(char::is_whitespace).unwrap_or(text.len());
    Some((&text[..split_at], &text[split_at..]))
}

fn no_argument(
    arguments: &str,
    command: &'static str,
    value: BridgeCommand,
) -> Result<Option<BridgeCommand>, CommandParseError> {
    if arguments.trim().is_empty() {
        Ok(Some(value))
    } else {
        Err(CommandParseError::UnexpectedArgument { command })
    }
}
