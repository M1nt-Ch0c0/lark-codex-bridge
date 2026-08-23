//! Hand-written compatibility boundary between generated wire DTOs and the
//! bridge's stable domain types.
//!
//! Generated modules never leak into the runtime API. Every conversion takes
//! place here so a schema promotion must explicitly compile and pass contract
//! tests. Conversion errors retain only a static contract label, never a wire
//! payload or a remote error message.

#![allow(clippy::missing_errors_doc)] // Every conversion has the identical redacted failure mode.

use std::fmt;

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

/// A redacted generated-wire to stable-domain conversion failure.
pub struct CompatError {
    contract: &'static str,
}

impl fmt::Debug for CompatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl fmt::Display for CompatError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Codex wire value does not satisfy stable {0}",
            self.contract
        )
    }
}

impl std::error::Error for CompatError {}

impl CompatError {
    const fn new(contract: &'static str) -> Self {
        Self { contract }
    }
}

fn convert<T, S>(source: S, contract: &'static str) -> Result<T, CompatError>
where
    T: DeserializeOwned,
    S: Serialize,
{
    let value = serde_json::to_value(source).map_err(|_| CompatError { contract })?;
    serde_json::from_value(value).map_err(|_| CompatError { contract })
}

macro_rules! compatibility_namespace {
    ($namespace:ident, $wire_version:ident) => {
        pub mod $namespace {
            use crate::codex::{types, wire::$wire_version as wire};

            use super::{CompatError, convert};

            pub fn initialize_params(
                value: &types::InitializeParams,
            ) -> Result<wire::InitializeParams, CompatError> {
                convert(value, "initialize params")
            }

            pub fn initialize_response(
                value: wire::InitializeResponse,
            ) -> Result<types::InitializeResult, CompatError> {
                convert(value, "initialize response")
            }

            pub fn thread_start_params(
                value: &types::ThreadStartParams,
            ) -> Result<wire::ThreadStartParams, CompatError> {
                convert(value, "thread/start params")
            }

            pub fn thread_start_response(
                value: wire::ThreadStartResponse,
            ) -> Result<types::ThreadStartResult, CompatError> {
                convert(value, "thread/start response")
            }

            pub fn thread_list_params(
                value: &types::ThreadListParams,
            ) -> Result<wire::ThreadListParams, CompatError> {
                convert(value, "thread/list params")
            }

            pub fn thread_list_response(
                value: wire::ThreadListResponse,
            ) -> Result<types::ThreadListResult, CompatError> {
                convert(value, "thread/list response")
            }

            pub fn thread_read_params(
                value: &types::ThreadReadParams,
            ) -> Result<wire::ThreadReadParams, CompatError> {
                convert(value, "thread/read params")
            }

            pub fn thread_read_response(
                value: wire::ThreadReadResponse,
            ) -> Result<types::ThreadReadResult, CompatError> {
                convert(value, "thread/read response")
            }

            pub fn thread_resume_params(
                value: &types::ThreadResumeParams,
            ) -> Result<wire::ThreadResumeParams, CompatError> {
                convert(value, "thread/resume params")
            }

            pub fn thread_resume_response(
                value: wire::ThreadResumeResponse,
            ) -> Result<types::ThreadResumeResult, CompatError> {
                convert(value, "thread/resume response")
            }

            pub fn turn_start_params(
                value: &types::TurnStartParams,
            ) -> Result<wire::TurnStartParams, CompatError> {
                convert(value, "turn/start params")
            }

            pub fn turn_start_response(
                value: wire::TurnStartResponse,
            ) -> Result<types::TurnStartResult, CompatError> {
                convert(value, "turn/start response")
            }

            pub fn turn_interrupt_params(
                value: &types::TurnInterruptParams,
            ) -> Result<wire::TurnInterruptParams, CompatError> {
                convert(value, "turn/interrupt params")
            }

            pub fn turn_interrupt_response(
                value: wire::TurnInterruptResponse,
            ) -> Result<types::TurnInterruptResult, CompatError> {
                convert(value, "turn/interrupt response")
            }

            pub fn thread_started_notification(
                value: wire::ThreadStartedNotification,
            ) -> Result<types::ThreadStartedNotification, CompatError> {
                convert(value, "thread/started notification")
            }

            pub fn turn_started_notification(
                value: wire::TurnStartedNotification,
            ) -> Result<types::TurnStartedNotification, CompatError> {
                convert(value, "turn/started notification")
            }

            pub fn item_started_notification(
                value: wire::ItemStartedNotification,
            ) -> Result<types::ItemStartedNotification, CompatError> {
                convert(value, "item/started notification")
            }

            pub fn agent_message_delta_notification(
                value: wire::AgentMessageDeltaNotification,
            ) -> Result<types::AgentMessageDeltaNotification, CompatError> {
                convert(value, "item/agentMessage/delta notification")
            }

            pub fn command_output_delta_notification(
                value: wire::CommandExecutionOutputDeltaNotification,
            ) -> Result<types::CommandExecutionOutputDeltaNotification, CompatError> {
                convert(value, "item/commandExecution/outputDelta notification")
            }

            pub fn item_completed_notification(
                value: wire::ItemCompletedNotification,
            ) -> Result<types::ItemCompletedNotification, CompatError> {
                convert(value, "item/completed notification")
            }

            pub fn token_usage_updated_notification(
                value: wire::ThreadTokenUsageUpdatedNotification,
            ) -> Result<types::ThreadTokenUsageUpdatedNotification, CompatError> {
                convert(value, "thread/tokenUsage/updated notification")
            }

            pub fn error_notification(
                value: wire::ErrorNotification,
            ) -> Result<types::ErrorNotification, CompatError> {
                convert(value, "error notification")
            }

            pub fn turn_completed_notification(
                value: wire::TurnCompletedNotification,
            ) -> Result<types::TurnCompletedNotification, CompatError> {
                convert(value, "turn/completed notification")
            }

            pub fn dynamic_tool_call_params(
                value: wire::DynamicToolCallParams,
            ) -> Result<types::DynamicToolCallParams, CompatError> {
                convert(value, "item/tool/call params")
            }

            pub fn dynamic_tool_call_response(
                value: &types::DynamicToolCallResponse,
            ) -> Result<wire::DynamicToolCallResponse, CompatError> {
                convert(value, "item/tool/call response")
            }
        }
    };
}

compatibility_namespace!(v0_146_0, v0_146_0);
compatibility_namespace!(v0_149_0, v0_149_0);

/// The exact generated wire contract selected for one probed app-server.
///
/// Candidate schemas intentionally have no variant here. Adding one is the
/// final, explicit promotion step after diff, mapper, and contract review.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WireAdapter {
    V0_146_0,
}

impl WireAdapter {
    #[must_use]
    pub fn for_version(version: &semver::Version) -> Option<Self> {
        if version.major == 0
            && version.minor == 146
            && version.patch == 0
            && version.pre.is_empty()
            && version.build.is_empty()
        {
            Some(Self::V0_146_0)
        } else {
            None
        }
    }

    #[must_use]
    pub const fn codex_version(self) -> &'static str {
        match self {
            Self::V0_146_0 => "0.146.0",
        }
    }

    pub fn initialize_params(
        self,
        value: &crate::codex::types::InitializeParams,
    ) -> Result<Value, CompatError> {
        match self {
            Self::V0_146_0 => encode(v0_146_0::initialize_params(value)?, "initialize params"),
        }
    }

    pub fn initialize_response(
        self,
        value: Value,
    ) -> Result<crate::codex::types::InitializeResult, CompatError> {
        match self {
            Self::V0_146_0 => v0_146_0::initialize_response(decode(value, "initialize response")?),
        }
    }

    pub fn thread_start_params(
        self,
        value: &crate::codex::types::ThreadStartParams,
    ) -> Result<Value, CompatError> {
        match self {
            Self::V0_146_0 => {
                validate_v0_146_thread_start_params(value)?;
                encode(v0_146_0::thread_start_params(value)?, "thread/start params")
            }
        }
    }

    pub fn thread_start_response(
        self,
        value: Value,
    ) -> Result<crate::codex::types::ThreadStartResult, CompatError> {
        match self {
            Self::V0_146_0 => {
                v0_146_0::thread_start_response(decode(value, "thread/start response")?)
            }
        }
    }

    pub fn thread_list_params(
        self,
        value: &crate::codex::types::ThreadListParams,
    ) -> Result<Value, CompatError> {
        match self {
            Self::V0_146_0 => {
                validate_v0_146_thread_list_params(value)?;
                encode(v0_146_0::thread_list_params(value)?, "thread/list params")
            }
        }
    }

    pub fn thread_list_response(
        self,
        value: Value,
    ) -> Result<crate::codex::types::ThreadListResult, CompatError> {
        match self {
            Self::V0_146_0 => {
                v0_146_0::thread_list_response(decode(value, "thread/list response")?)
            }
        }
    }

    pub fn thread_read_params(
        self,
        value: &crate::codex::types::ThreadReadParams,
    ) -> Result<Value, CompatError> {
        match self {
            Self::V0_146_0 => encode(v0_146_0::thread_read_params(value)?, "thread/read params"),
        }
    }

    pub fn thread_read_response(
        self,
        value: Value,
    ) -> Result<crate::codex::types::ThreadReadResult, CompatError> {
        match self {
            Self::V0_146_0 => {
                v0_146_0::thread_read_response(decode(value, "thread/read response")?)
            }
        }
    }

    pub fn thread_resume_params(
        self,
        value: &crate::codex::types::ThreadResumeParams,
    ) -> Result<Value, CompatError> {
        match self {
            Self::V0_146_0 => {
                validate_v0_146_approval(
                    value.overrides.approval_policy.as_ref(),
                    value.overrides.approvals_reviewer.as_deref(),
                    "thread/resume params",
                )?;
                encode(
                    v0_146_0::thread_resume_params(value)?,
                    "thread/resume params",
                )
            }
        }
    }

    pub fn thread_resume_response(
        self,
        value: Value,
    ) -> Result<crate::codex::types::ThreadResumeResult, CompatError> {
        match self {
            Self::V0_146_0 => {
                v0_146_0::thread_resume_response(decode(value, "thread/resume response")?)
            }
        }
    }

    pub fn turn_start_params(
        self,
        value: &crate::codex::types::TurnStartParams,
    ) -> Result<Value, CompatError> {
        match self {
            Self::V0_146_0 => {
                validate_v0_146_turn_start_params(value)?;
                encode(v0_146_0::turn_start_params(value)?, "turn/start params")
            }
        }
    }

    pub fn turn_start_response(
        self,
        value: Value,
    ) -> Result<crate::codex::types::TurnStartResult, CompatError> {
        match self {
            Self::V0_146_0 => v0_146_0::turn_start_response(decode(value, "turn/start response")?),
        }
    }

    pub fn turn_interrupt_params(
        self,
        value: &crate::codex::types::TurnInterruptParams,
    ) -> Result<Value, CompatError> {
        match self {
            Self::V0_146_0 => encode(
                v0_146_0::turn_interrupt_params(value)?,
                "turn/interrupt params",
            ),
        }
    }

    pub fn turn_interrupt_response(
        self,
        value: Value,
    ) -> Result<crate::codex::types::TurnInterruptResult, CompatError> {
        match self {
            Self::V0_146_0 => {
                v0_146_0::turn_interrupt_response(decode(value, "turn/interrupt response")?)
            }
        }
    }

    pub fn thread_started_notification(
        self,
        value: Value,
    ) -> Result<crate::codex::types::ThreadStartedNotification, CompatError> {
        match self {
            Self::V0_146_0 => {
                v0_146_0::thread_started_notification(decode(value, "thread/started notification")?)
            }
        }
    }

    pub fn turn_started_notification(
        self,
        value: Value,
    ) -> Result<crate::codex::types::TurnStartedNotification, CompatError> {
        match self {
            Self::V0_146_0 => {
                v0_146_0::turn_started_notification(decode(value, "turn/started notification")?)
            }
        }
    }

    pub fn item_started_notification(
        self,
        value: Value,
    ) -> Result<crate::codex::types::ItemStartedNotification, CompatError> {
        match self {
            Self::V0_146_0 => {
                v0_146_0::item_started_notification(decode(value, "item/started notification")?)
            }
        }
    }

    pub fn agent_message_delta_notification(
        self,
        value: Value,
    ) -> Result<crate::codex::types::AgentMessageDeltaNotification, CompatError> {
        match self {
            Self::V0_146_0 => v0_146_0::agent_message_delta_notification(decode(
                value,
                "item/agentMessage/delta notification",
            )?),
        }
    }

    pub fn command_output_delta_notification(
        self,
        value: Value,
    ) -> Result<crate::codex::types::CommandExecutionOutputDeltaNotification, CompatError> {
        match self {
            Self::V0_146_0 => v0_146_0::command_output_delta_notification(decode(
                value,
                "item/commandExecution/outputDelta notification",
            )?),
        }
    }

    pub fn item_completed_notification(
        self,
        value: Value,
    ) -> Result<crate::codex::types::ItemCompletedNotification, CompatError> {
        match self {
            Self::V0_146_0 => {
                v0_146_0::item_completed_notification(decode(value, "item/completed notification")?)
            }
        }
    }

    pub fn token_usage_updated_notification(
        self,
        value: Value,
    ) -> Result<crate::codex::types::ThreadTokenUsageUpdatedNotification, CompatError> {
        match self {
            Self::V0_146_0 => v0_146_0::token_usage_updated_notification(decode(
                value,
                "thread/tokenUsage/updated notification",
            )?),
        }
    }

    pub fn error_notification(
        self,
        value: Value,
    ) -> Result<crate::codex::types::ErrorNotification, CompatError> {
        match self {
            Self::V0_146_0 => v0_146_0::error_notification(decode(value, "error notification")?),
        }
    }

    pub fn turn_completed_notification(
        self,
        value: Value,
    ) -> Result<crate::codex::types::TurnCompletedNotification, CompatError> {
        match self {
            Self::V0_146_0 => {
                v0_146_0::turn_completed_notification(decode(value, "turn/completed notification")?)
            }
        }
    }

    pub fn dynamic_tool_call_params(
        self,
        value: Value,
    ) -> Result<crate::codex::types::DynamicToolCallParams, CompatError> {
        match self {
            Self::V0_146_0 => {
                v0_146_0::dynamic_tool_call_params(decode(value, "item/tool/call params")?)
            }
        }
    }

    pub fn dynamic_tool_call_response(
        self,
        value: &crate::codex::types::DynamicToolCallResponse,
    ) -> Result<Value, CompatError> {
        match self {
            Self::V0_146_0 => encode(
                v0_146_0::dynamic_tool_call_response(value)?,
                "item/tool/call response",
            ),
        }
    }
}

fn validate_v0_146_thread_list_params(
    value: &crate::codex::types::ThreadListParams,
) -> Result<(), CompatError> {
    use crate::codex::types::{SortDirection, ThreadSortKey, ThreadSourceKind};

    if matches!(value.sort_key.as_ref(), Some(ThreadSortKey::Unknown(_)))
        || matches!(
            value.sort_direction.as_ref(),
            Some(SortDirection::Unknown(_))
        )
        || value.source_kinds.as_ref().is_some_and(|values| {
            values
                .iter()
                .any(|value| matches!(value, ThreadSourceKind::Unknown(_)))
        })
    {
        return Err(CompatError::new("thread/list params"));
    }
    Ok(())
}

fn validate_v0_146_approval(
    policy: Option<&crate::codex::types::ApprovalPolicy>,
    reviewer: Option<&str>,
    contract: &'static str,
) -> Result<(), CompatError> {
    if matches!(
        policy,
        Some(crate::codex::types::ApprovalPolicy::Named(value))
            if !matches!(value.as_str(), "never" | "on-request" | "untrusted")
    ) || reviewer
        .is_some_and(|value| !matches!(value, "auto_review" | "guardian_subagent" | "user"))
    {
        return Err(CompatError::new(contract));
    }
    Ok(())
}

fn validate_v0_146_thread_start_params(
    value: &crate::codex::types::ThreadStartParams,
) -> Result<(), CompatError> {
    validate_v0_146_approval(
        value.approval_policy.as_ref(),
        value.approvals_reviewer.as_deref(),
        "thread/start params",
    )?;
    if value
        .session_start_source
        .as_deref()
        .is_some_and(|value| !matches!(value, "clear" | "startup"))
    {
        return Err(CompatError::new("thread/start params"));
    }
    Ok(())
}

fn validate_v0_146_turn_start_params(
    value: &crate::codex::types::TurnStartParams,
) -> Result<(), CompatError> {
    validate_v0_146_approval(
        value.approval_policy.as_ref(),
        value.approvals_reviewer.as_deref(),
        "turn/start params",
    )?;
    if value.effort.as_deref().is_some_and(str::is_empty)
        || value
            .summary
            .as_deref()
            .is_some_and(|value| !matches!(value, "auto" | "concise" | "detailed" | "none"))
    {
        return Err(CompatError::new("turn/start params"));
    }
    Ok(())
}

fn encode<T: Serialize>(value: T, contract: &'static str) -> Result<Value, CompatError> {
    serde_json::to_value(value).map_err(|_| CompatError::new(contract))
}

fn decode<T: DeserializeOwned>(value: Value, contract: &'static str) -> Result<T, CompatError> {
    serde_json::from_value(value).map_err(|_| CompatError::new(contract))
}
