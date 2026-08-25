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

mod shared_v0_149_0 {
    use crate::codex::{types, wire::v0_149_0 as wire};

    use super::{CompatError, convert};

    macro_rules! outgoing {
        ($name:ident, $stable:ty, $wire:ty, $contract:literal) => {
            pub fn $name(value: &$stable) -> Result<$wire, CompatError> {
                convert(value, $contract)
            }
        };
    }

    macro_rules! incoming {
        ($name:ident, $wire:ty, $stable:ty, $contract:literal) => {
            pub fn $name(value: $wire) -> Result<$stable, CompatError> {
                convert(value, $contract)
            }
        };
    }

    outgoing!(
        thread_unsubscribe_params,
        types::ThreadUnsubscribeParams,
        wire::ThreadUnsubscribeParams,
        "thread/unsubscribe params"
    );
    incoming!(
        thread_unsubscribe_response,
        wire::ThreadUnsubscribeResponse,
        types::ThreadUnsubscribeResult,
        "thread/unsubscribe response"
    );
    outgoing!(
        turn_steer_params,
        types::TurnSteerParams,
        wire::TurnSteerParams,
        "turn/steer params"
    );
    incoming!(
        turn_steer_response,
        wire::TurnSteerResponse,
        types::TurnSteerResult,
        "turn/steer response"
    );
    outgoing!(
        thread_queue_add_params,
        types::ThreadQueueAddParams,
        wire::ThreadQueueAddParams,
        "thread/queue/add params"
    );
    incoming!(
        thread_queue_add_response,
        wire::ThreadQueueAddResponse,
        types::ThreadQueueAddResult,
        "thread/queue/add response"
    );
    outgoing!(
        thread_queue_list_params,
        types::ThreadQueueListParams,
        wire::ThreadQueueListParams,
        "thread/queue/list params"
    );
    incoming!(
        thread_queue_list_response,
        wire::ThreadQueueListResponse,
        types::ThreadQueueListResult,
        "thread/queue/list response"
    );
    outgoing!(
        thread_queue_start_params,
        types::ThreadQueueStartParams,
        wire::ThreadQueueStartParams,
        "thread/queue/start params"
    );
    incoming!(
        thread_queue_start_response,
        wire::ThreadQueueStartResponse,
        types::ThreadQueueStartResult,
        "thread/queue/start response"
    );
    outgoing!(
        thread_turns_list_params,
        types::ThreadTurnsListParams,
        wire::ThreadTurnsListParams,
        "thread/turns/list params"
    );
    incoming!(
        thread_turns_list_response,
        wire::ThreadTurnsListResponse,
        types::ThreadTurnsListResult,
        "thread/turns/list response"
    );
    outgoing!(
        thread_items_list_params,
        types::ThreadItemsListParams,
        wire::ThreadItemsListParams,
        "thread/items/list params"
    );
    incoming!(
        thread_items_list_response,
        wire::ThreadItemsListResponse,
        types::ThreadItemsListResult,
        "thread/items/list response"
    );
    incoming!(
        thread_status_changed_notification,
        wire::ThreadStatusChangedNotification,
        types::ThreadStatusChangedNotification,
        "thread/status/changed notification"
    );
    incoming!(
        thread_queue_changed_notification,
        wire::ThreadQueueChangedNotification,
        types::ThreadQueueChangedNotification,
        "thread/queue/changed notification"
    );
    incoming!(
        server_request_resolved_notification,
        wire::ServerRequestResolvedNotification,
        types::ServerRequestResolvedNotification,
        "serverRequest/resolved notification"
    );
    incoming!(
        command_execution_request_approval_params,
        wire::CommandExecutionRequestApprovalParams,
        types::CommandExecutionRequestApprovalParams,
        "item/commandExecution/requestApproval params"
    );
    outgoing!(
        command_execution_request_approval_response,
        types::CommandExecutionRequestApprovalResult,
        wire::CommandExecutionRequestApprovalResponse,
        "item/commandExecution/requestApproval response"
    );
    incoming!(
        file_change_request_approval_params,
        wire::FileChangeRequestApprovalParams,
        types::FileChangeRequestApprovalParams,
        "item/fileChange/requestApproval params"
    );
    outgoing!(
        file_change_request_approval_response,
        types::FileChangeRequestApprovalResult,
        wire::FileChangeRequestApprovalResponse,
        "item/fileChange/requestApproval response"
    );
    incoming!(
        permissions_request_approval_params,
        wire::PermissionsRequestApprovalParams,
        types::PermissionsRequestApprovalParams,
        "item/permissions/requestApproval params"
    );
    outgoing!(
        permissions_request_approval_response,
        types::PermissionsRequestApprovalResult,
        wire::PermissionsRequestApprovalResponse,
        "item/permissions/requestApproval response"
    );
}

/// Exact capability groups required by the external shared-endpoint design.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SharedWireProfile {
    ObserveShared,
    ResumeShared,
    MutateShared,
    QueueShared,
}

impl SharedWireProfile {
    #[must_use]
    pub const fn required_methods(self) -> &'static [&'static str] {
        match self {
            Self::ObserveShared => &["initialize", "thread/list", "thread/read"],
            Self::ResumeShared => &[
                "initialize",
                "thread/list",
                "thread/read",
                "thread/resume",
                "thread/unsubscribe",
                "thread/turns/list",
                "thread/items/list",
            ],
            Self::MutateShared => &[
                "initialize",
                "thread/list",
                "thread/read",
                "thread/resume",
                "thread/unsubscribe",
                "thread/turns/list",
                "thread/items/list",
                "turn/start",
                "turn/steer",
                "turn/interrupt",
            ],
            Self::QueueShared => &[
                "initialize",
                "thread/list",
                "thread/read",
                "thread/resume",
                "thread/unsubscribe",
                "thread/turns/list",
                "thread/items/list",
                "turn/start",
                "turn/steer",
                "turn/interrupt",
                "thread/queue/add",
                "thread/queue/list",
                "thread/queue/start",
            ],
        }
    }

    #[must_use]
    pub const fn required_notifications(self) -> &'static [&'static str] {
        match self {
            Self::ObserveShared => &[],
            Self::ResumeShared => &["thread/status/changed", "item/completed", "turn/completed"],
            Self::MutateShared => &[
                "thread/status/changed",
                "item/completed",
                "turn/completed",
                "serverRequest/resolved",
            ],
            Self::QueueShared => &[
                "thread/status/changed",
                "thread/queue/changed",
                "item/completed",
                "turn/completed",
                "serverRequest/resolved",
            ],
        }
    }

    #[must_use]
    pub const fn required_reverse_requests(self) -> &'static [&'static str] {
        match self {
            Self::ObserveShared | Self::ResumeShared => &[],
            Self::MutateShared | Self::QueueShared => &[
                "item/commandExecution/requestApproval",
                "item/fileChange/requestApproval",
                "item/permissions/requestApproval",
            ],
        }
    }
}

/// The exact generated wire contract selected for one probed app-server.
///
/// A variant exists only after the exact schema, compatibility review, mapper,
/// and contract matrix have all been promoted together.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WireAdapter {
    V0_146_0,
    V0_149_0,
}

macro_rules! shared_outgoing_adapter {
    ($name:ident, $stable:ty, $mapper:ident, $contract:literal) => {
        pub fn $name(self, value: &$stable) -> Result<Value, CompatError> {
            match self {
                Self::V0_149_0 => encode(shared_v0_149_0::$mapper(value)?, $contract),
                Self::V0_146_0 => Err(CompatError::new($contract)),
            }
        }
    };
}

macro_rules! shared_incoming_adapter {
    ($name:ident, $stable:ty, $mapper:ident, $wire:ty, $contract:literal) => {
        pub fn $name(self, value: Value) -> Result<$stable, CompatError> {
            match self {
                Self::V0_149_0 => shared_v0_149_0::$mapper(decode::<$wire>(value, $contract)?),
                Self::V0_146_0 => Err(CompatError::new($contract)),
            }
        }
    };
}

impl WireAdapter {
    #[must_use]
    pub fn for_version(version: &semver::Version) -> Option<Self> {
        if !version.pre.is_empty() || !version.build.is_empty() {
            return None;
        }
        match (version.major, version.minor, version.patch) {
            (0, 146, 0) => Some(Self::V0_146_0),
            (0, 149, 0) => Some(Self::V0_149_0),
            _ => None,
        }
    }

    #[must_use]
    pub const fn codex_version(self) -> &'static str {
        match self {
            Self::V0_146_0 => "0.146.0",
            Self::V0_149_0 => "0.149.0",
        }
    }

    /// Returns whether this exact adapter contains every reviewed shape in a
    /// shared-endpoint profile. A base stdio adapter is never inferred as a
    /// partial shared profile.
    #[must_use]
    pub const fn supports_shared_profile(self, _profile: SharedWireProfile) -> bool {
        matches!(self, Self::V0_149_0)
    }

    pub fn initialize_params(
        self,
        value: &crate::codex::types::InitializeParams,
    ) -> Result<Value, CompatError> {
        match self {
            Self::V0_146_0 => encode(v0_146_0::initialize_params(value)?, "initialize params"),
            Self::V0_149_0 => encode(v0_149_0::initialize_params(value)?, "initialize params"),
        }
    }

    pub fn initialize_response(
        self,
        value: Value,
    ) -> Result<crate::codex::types::InitializeResult, CompatError> {
        match self {
            Self::V0_146_0 => v0_146_0::initialize_response(decode(value, "initialize response")?),
            Self::V0_149_0 => v0_149_0::initialize_response(decode(value, "initialize response")?),
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
            Self::V0_149_0 => {
                validate_v0_149_thread_start_params(value)?;
                encode(v0_149_0::thread_start_params(value)?, "thread/start params")
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
            Self::V0_149_0 => {
                v0_149_0::thread_start_response(decode(value, "thread/start response")?)
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
            Self::V0_149_0 => {
                validate_v0_149_thread_list_params(value)?;
                encode(v0_149_0::thread_list_params(value)?, "thread/list params")
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
            Self::V0_149_0 => {
                v0_149_0::thread_list_response(decode(value, "thread/list response")?)
            }
        }
    }

    pub fn thread_read_params(
        self,
        value: &crate::codex::types::ThreadReadParams,
    ) -> Result<Value, CompatError> {
        match self {
            Self::V0_146_0 => encode(v0_146_0::thread_read_params(value)?, "thread/read params"),
            Self::V0_149_0 => encode(v0_149_0::thread_read_params(value)?, "thread/read params"),
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
            Self::V0_149_0 => {
                v0_149_0::thread_read_response(decode(value, "thread/read response")?)
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
            Self::V0_149_0 => {
                validate_v0_149_approval(
                    value.overrides.approval_policy.as_ref(),
                    value.overrides.approvals_reviewer.as_deref(),
                    "thread/resume params",
                )?;
                encode(
                    v0_149_0::thread_resume_params(value)?,
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
            Self::V0_149_0 => {
                v0_149_0::thread_resume_response(decode(value, "thread/resume response")?)
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
            Self::V0_149_0 => {
                validate_v0_149_turn_start_params(value)?;
                encode(v0_149_0::turn_start_params(value)?, "turn/start params")
            }
        }
    }

    pub fn turn_start_response(
        self,
        value: Value,
    ) -> Result<crate::codex::types::TurnStartResult, CompatError> {
        match self {
            Self::V0_146_0 => v0_146_0::turn_start_response(decode(value, "turn/start response")?),
            Self::V0_149_0 => v0_149_0::turn_start_response(decode(value, "turn/start response")?),
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
            Self::V0_149_0 => encode(
                v0_149_0::turn_interrupt_params(value)?,
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
            Self::V0_149_0 => {
                v0_149_0::turn_interrupt_response(decode(value, "turn/interrupt response")?)
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
            Self::V0_149_0 => {
                v0_149_0::thread_started_notification(decode(value, "thread/started notification")?)
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
            Self::V0_149_0 => {
                v0_149_0::turn_started_notification(decode(value, "turn/started notification")?)
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
            Self::V0_149_0 => {
                v0_149_0::item_started_notification(decode(value, "item/started notification")?)
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
            Self::V0_149_0 => v0_149_0::agent_message_delta_notification(decode(
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
            Self::V0_149_0 => v0_149_0::command_output_delta_notification(decode(
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
            Self::V0_149_0 => {
                v0_149_0::item_completed_notification(decode(value, "item/completed notification")?)
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
            Self::V0_149_0 => v0_149_0::token_usage_updated_notification(decode(
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
            Self::V0_149_0 => v0_149_0::error_notification(decode(value, "error notification")?),
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
            Self::V0_149_0 => {
                v0_149_0::turn_completed_notification(decode(value, "turn/completed notification")?)
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
            Self::V0_149_0 => {
                v0_149_0::dynamic_tool_call_params(decode(value, "item/tool/call params")?)
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
            Self::V0_149_0 => encode(
                v0_149_0::dynamic_tool_call_response(value)?,
                "item/tool/call response",
            ),
        }
    }

    shared_outgoing_adapter!(
        thread_unsubscribe_params,
        crate::codex::types::ThreadUnsubscribeParams,
        thread_unsubscribe_params,
        "thread/unsubscribe params"
    );
    shared_incoming_adapter!(
        thread_unsubscribe_response,
        crate::codex::types::ThreadUnsubscribeResult,
        thread_unsubscribe_response,
        crate::codex::wire::v0_149_0::ThreadUnsubscribeResponse,
        "thread/unsubscribe response"
    );
    shared_outgoing_adapter!(
        turn_steer_params,
        crate::codex::types::TurnSteerParams,
        turn_steer_params,
        "turn/steer params"
    );
    shared_incoming_adapter!(
        turn_steer_response,
        crate::codex::types::TurnSteerResult,
        turn_steer_response,
        crate::codex::wire::v0_149_0::TurnSteerResponse,
        "turn/steer response"
    );
    shared_outgoing_adapter!(
        thread_queue_add_params,
        crate::codex::types::ThreadQueueAddParams,
        thread_queue_add_params,
        "thread/queue/add params"
    );
    shared_incoming_adapter!(
        thread_queue_add_response,
        crate::codex::types::ThreadQueueAddResult,
        thread_queue_add_response,
        crate::codex::wire::v0_149_0::ThreadQueueAddResponse,
        "thread/queue/add response"
    );
    shared_outgoing_adapter!(
        thread_queue_list_params,
        crate::codex::types::ThreadQueueListParams,
        thread_queue_list_params,
        "thread/queue/list params"
    );
    shared_incoming_adapter!(
        thread_queue_list_response,
        crate::codex::types::ThreadQueueListResult,
        thread_queue_list_response,
        crate::codex::wire::v0_149_0::ThreadQueueListResponse,
        "thread/queue/list response"
    );
    shared_outgoing_adapter!(
        thread_queue_start_params,
        crate::codex::types::ThreadQueueStartParams,
        thread_queue_start_params,
        "thread/queue/start params"
    );
    shared_incoming_adapter!(
        thread_queue_start_response,
        crate::codex::types::ThreadQueueStartResult,
        thread_queue_start_response,
        crate::codex::wire::v0_149_0::ThreadQueueStartResponse,
        "thread/queue/start response"
    );
    pub fn thread_turns_list_params(
        self,
        value: &crate::codex::types::ThreadTurnsListParams,
    ) -> Result<Value, CompatError> {
        match self {
            Self::V0_149_0 => {
                validate_v0_149_shared_sort_direction(
                    value.sort_direction.as_ref(),
                    "thread/turns/list params",
                )?;
                encode(
                    shared_v0_149_0::thread_turns_list_params(value)?,
                    "thread/turns/list params",
                )
            }
            Self::V0_146_0 => Err(CompatError::new("thread/turns/list params")),
        }
    }
    shared_incoming_adapter!(
        thread_turns_list_response,
        crate::codex::types::ThreadTurnsListResult,
        thread_turns_list_response,
        crate::codex::wire::v0_149_0::ThreadTurnsListResponse,
        "thread/turns/list response"
    );
    pub fn thread_items_list_params(
        self,
        value: &crate::codex::types::ThreadItemsListParams,
    ) -> Result<Value, CompatError> {
        match self {
            Self::V0_149_0 => {
                validate_v0_149_shared_sort_direction(
                    value.sort_direction.as_ref(),
                    "thread/items/list params",
                )?;
                encode(
                    shared_v0_149_0::thread_items_list_params(value)?,
                    "thread/items/list params",
                )
            }
            Self::V0_146_0 => Err(CompatError::new("thread/items/list params")),
        }
    }
    shared_incoming_adapter!(
        thread_items_list_response,
        crate::codex::types::ThreadItemsListResult,
        thread_items_list_response,
        crate::codex::wire::v0_149_0::ThreadItemsListResponse,
        "thread/items/list response"
    );
    shared_incoming_adapter!(
        thread_status_changed_notification,
        crate::codex::types::ThreadStatusChangedNotification,
        thread_status_changed_notification,
        crate::codex::wire::v0_149_0::ThreadStatusChangedNotification,
        "thread/status/changed notification"
    );
    shared_incoming_adapter!(
        thread_queue_changed_notification,
        crate::codex::types::ThreadQueueChangedNotification,
        thread_queue_changed_notification,
        crate::codex::wire::v0_149_0::ThreadQueueChangedNotification,
        "thread/queue/changed notification"
    );
    shared_incoming_adapter!(
        server_request_resolved_notification,
        crate::codex::types::ServerRequestResolvedNotification,
        server_request_resolved_notification,
        crate::codex::wire::v0_149_0::ServerRequestResolvedNotification,
        "serverRequest/resolved notification"
    );
    shared_incoming_adapter!(
        command_execution_request_approval_params,
        crate::codex::types::CommandExecutionRequestApprovalParams,
        command_execution_request_approval_params,
        crate::codex::wire::v0_149_0::CommandExecutionRequestApprovalParams,
        "item/commandExecution/requestApproval params"
    );
    shared_outgoing_adapter!(
        command_execution_request_approval_response,
        crate::codex::types::CommandExecutionRequestApprovalResult,
        command_execution_request_approval_response,
        "item/commandExecution/requestApproval response"
    );
    shared_incoming_adapter!(
        file_change_request_approval_params,
        crate::codex::types::FileChangeRequestApprovalParams,
        file_change_request_approval_params,
        crate::codex::wire::v0_149_0::FileChangeRequestApprovalParams,
        "item/fileChange/requestApproval params"
    );
    shared_outgoing_adapter!(
        file_change_request_approval_response,
        crate::codex::types::FileChangeRequestApprovalResult,
        file_change_request_approval_response,
        "item/fileChange/requestApproval response"
    );
    shared_incoming_adapter!(
        permissions_request_approval_params,
        crate::codex::types::PermissionsRequestApprovalParams,
        permissions_request_approval_params,
        crate::codex::wire::v0_149_0::PermissionsRequestApprovalParams,
        "item/permissions/requestApproval params"
    );
    shared_outgoing_adapter!(
        permissions_request_approval_response,
        crate::codex::types::PermissionsRequestApprovalResult,
        permissions_request_approval_response,
        "item/permissions/requestApproval response"
    );
}

fn validate_v0_146_thread_list_params(
    value: &crate::codex::types::ThreadListParams,
) -> Result<(), CompatError> {
    use crate::codex::types::{SortDirection, ThreadSortKey, ThreadSourceKind};

    if matches!(
        value.sort_key.as_ref(),
        Some(ThreadSortKey::SectionPosition | ThreadSortKey::Unknown(_))
    ) || value.project_id.is_some()
        || value.section_id.is_some()
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
    if value.project_id.is_some() {
        return Err(CompatError::new("thread/start params"));
    }
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

fn validate_v0_149_thread_list_params(
    value: &crate::codex::types::ThreadListParams,
) -> Result<(), CompatError> {
    use crate::codex::types::{SortDirection, ThreadSortKey, ThreadSourceKind};

    if value.is_pinned.is_some()
        || matches!(value.sort_key.as_ref(), Some(ThreadSortKey::Unknown(_)))
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

fn validate_v0_149_approval(
    policy: Option<&crate::codex::types::ApprovalPolicy>,
    reviewer: Option<&str>,
    contract: &'static str,
) -> Result<(), CompatError> {
    validate_v0_146_approval(policy, reviewer, contract)
}

fn validate_v0_149_thread_start_params(
    value: &crate::codex::types::ThreadStartParams,
) -> Result<(), CompatError> {
    validate_v0_149_approval(
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

fn validate_v0_149_turn_start_params(
    value: &crate::codex::types::TurnStartParams,
) -> Result<(), CompatError> {
    validate_v0_149_approval(
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

fn validate_v0_149_shared_sort_direction(
    value: Option<&crate::codex::types::SortDirection>,
    contract: &'static str,
) -> Result<(), CompatError> {
    if matches!(value, Some(crate::codex::types::SortDirection::Unknown(_))) {
        return Err(CompatError::new(contract));
    }
    Ok(())
}

fn encode<T: Serialize>(value: T, contract: &'static str) -> Result<Value, CompatError> {
    serde_json::to_value(value).map_err(|_| CompatError::new(contract))
}

fn decode<T: DeserializeOwned>(value: Value, contract: &'static str) -> Result<T, CompatError> {
    serde_json::from_value(value).map_err(|_| CompatError::new(contract))
}
