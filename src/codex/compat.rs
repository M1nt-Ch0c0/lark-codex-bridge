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
