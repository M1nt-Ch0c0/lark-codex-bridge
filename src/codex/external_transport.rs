//! Bounded authenticated WebSocket transport for an operator-owned Codex app-server.
//!
//! The external connection owns one admitted socket and nothing process-like. It deliberately
//! exposes promoted read/resume operations to reconciliation and a separately typed mutation
//! client only for explicitly promoted write profiles. Reconnect and process ownership remain
//! outside this type.

use std::{fmt, time::Duration};

use serde_json::Value;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::codex::{
    compat::WireAdapter,
    external::{
        ExternalCapabilityProfile, ExternalEndpointGate, ExternalGateError, ExternalGateReport,
    },
    rpc::{
        ConnectionEpoch, RpcConnection, RpcError, RpcEvent, RpcHandle, RpcProtocolPolicy,
        ServerRequest, spawn_rpc_with_policy,
    },
    transport::{TransportExit, spawn_websocket_transport},
    types::{
        AccountRateLimitsUpdatedNotification, AgentMessageDeltaNotification,
        CommandExecutionOutputDeltaNotification, CommandExecutionRequestApprovalParams,
        CommandExecutionRequestApprovalResult, ErrorNotification, FileChangeRequestApprovalParams,
        FileChangeRequestApprovalResult, ItemCompletedNotification, ItemStartedNotification,
        PermissionsRequestApprovalParams, PermissionsRequestApprovalResult,
        RemoteControlStatusChangedNotification, ServerRequestResolvedNotification,
        ThreadGoalClearedNotification, ThreadItemsListParams, ThreadItemsListResult,
        ThreadListParams, ThreadListResult, ThreadQueueAddParams, ThreadQueueAddResult,
        ThreadQueueChangedNotification, ThreadQueueListParams, ThreadQueueListResult,
        ThreadQueueStartParams, ThreadQueueStartResult, ThreadReadParams, ThreadReadResult,
        ThreadResumeParams, ThreadResumeResult, ThreadSettingsUpdatedNotification,
        ThreadStatusChangedNotification, ThreadTokenUsageUpdatedNotification,
        ThreadTurnsListParams, ThreadTurnsListResult, TurnCompletedNotification,
        TurnInterruptParams, TurnInterruptResult, TurnStartParams, TurnStartResult,
        TurnStartedNotification, TurnSteerParams, TurnSteerResult,
    },
};
use crate::limits::CONTROL_RPC_TIMEOUT;

/// Static external-transport failures. Endpoint text, token paths, credentials, payloads, remote
/// error strings, and thread identifiers are never retained.
#[derive(Clone, Copy, Debug, Error, Eq, PartialEq)]
pub enum ExternalTransportError {
    #[error("external Codex admission failed")]
    Admission(#[source] ExternalGateError),
    #[error("external Codex read-only RPC failed")]
    Rpc,
    #[error("external Codex read-only RPC was rejected with code {code}")]
    ServerRejected { code: i64 },
    #[error("external Codex read-only RPC timed out")]
    RequestTimeout,
    #[error("external Codex socket epoch was lost")]
    ConnectionLost,
    #[error("external Codex response violated the promoted contract")]
    ProtocolViolation,
    #[error("external Codex operation is not available in this capability profile")]
    UnsupportedProfile,
}

/// Exact promoted approval request retained with its epoch-bound response token.
pub enum ExternalApprovalRequest {
    Command {
        params: CommandExecutionRequestApprovalParams,
        request: ServerRequest,
    },
    FileChange {
        params: FileChangeRequestApprovalParams,
        request: ServerRequest,
    },
    Permissions {
        params: PermissionsRequestApprovalParams,
        request: ServerRequest,
    },
}

impl fmt::Debug for ExternalApprovalRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExternalApprovalRequest")
            .field("kind", &self.kind())
            .field("epoch", &self.epoch())
            .finish_non_exhaustive()
    }
}

impl ExternalApprovalRequest {
    #[must_use]
    pub const fn request_id(&self) -> &crate::codex::protocol::RequestId {
        match self {
            Self::Command { request, .. }
            | Self::FileChange { request, .. }
            | Self::Permissions { request, .. } => request.id(),
        }
    }

    #[must_use]
    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Command { .. } => "command",
            Self::FileChange { .. } => "file_change",
            Self::Permissions { .. } => "permissions",
        }
    }

    #[must_use]
    pub const fn epoch(&self) -> ConnectionEpoch {
        match self {
            Self::Command { request, .. }
            | Self::FileChange { request, .. }
            | Self::Permissions { request, .. } => request.epoch(),
        }
    }

    #[must_use]
    pub fn thread_id(&self) -> &str {
        match self {
            Self::Command { params, .. } => &params.thread_id,
            Self::FileChange { params, .. } => &params.thread_id,
            Self::Permissions { params, .. } => &params.thread_id,
        }
    }

    #[must_use]
    pub fn turn_id(&self) -> &str {
        match self {
            Self::Command { params, .. } => &params.turn_id,
            Self::FileChange { params, .. } => &params.turn_id,
            Self::Permissions { params, .. } => &params.turn_id,
        }
    }

    #[must_use]
    pub fn item_id(&self) -> &str {
        match self {
            Self::Command { params, .. } => &params.item_id,
            Self::FileChange { params, .. } => &params.item_id,
            Self::Permissions { params, .. } => &params.item_id,
        }
    }

    #[must_use]
    pub fn auto_resolution_ms(&self) -> Option<u64> {
        let details = match self {
            Self::Command { params, .. } => &params.details,
            Self::FileChange { params, .. } => &params.details,
            Self::Permissions { params, .. } => &params.details,
        };
        details.get("autoResolutionMs").and_then(Value::as_u64)
    }
}

/// Typed, allowlisted traffic from a `resume_shared` socket epoch.
pub enum ExternalReadEvent {
    AccountRateLimitsUpdated(AccountRateLimitsUpdatedNotification),
    RemoteControlStatusChanged(RemoteControlStatusChangedNotification),
    ThreadGoalCleared(ThreadGoalClearedNotification),
    ThreadSettingsUpdated(ThreadSettingsUpdatedNotification),
    ThreadStatusChanged(ThreadStatusChangedNotification),
    ThreadQueueChanged(ThreadQueueChangedNotification),
    ServerRequestResolved(ServerRequestResolvedNotification),
    Approval(ExternalApprovalRequest),
    TurnStarted(TurnStartedNotification),
    ItemStarted(ItemStartedNotification),
    AgentMessageDelta(AgentMessageDeltaNotification),
    CommandOutputDelta(CommandExecutionOutputDeltaNotification),
    ItemCompleted(ItemCompletedNotification),
    TokenUsageUpdated(ThreadTokenUsageUpdatedNotification),
    Error(ErrorNotification),
    TurnCompleted(TurnCompletedNotification),
    Closed(TransportExit),
}

impl fmt::Debug for ExternalReadEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AccountRateLimitsUpdated(_) => formatter.write_str("AccountRateLimitsUpdated"),
            Self::RemoteControlStatusChanged(_) => {
                formatter.write_str("RemoteControlStatusChanged")
            }
            Self::ThreadGoalCleared(_) => formatter.write_str("ThreadGoalCleared"),
            Self::ThreadSettingsUpdated(_) => formatter.write_str("ThreadSettingsUpdated"),
            Self::ThreadStatusChanged(_) => formatter.write_str("ThreadStatusChanged"),
            Self::ThreadQueueChanged(_) => formatter.write_str("ThreadQueueChanged"),
            Self::ServerRequestResolved(_) => formatter.write_str("ServerRequestResolved"),
            Self::Approval(request) => formatter.debug_tuple("Approval").field(request).finish(),
            Self::TurnStarted(_) => formatter.write_str("TurnStarted"),
            Self::ItemStarted(_) => formatter.write_str("ItemStarted"),
            Self::AgentMessageDelta(_) => formatter.write_str("AgentMessageDelta"),
            Self::CommandOutputDelta(_) => formatter.write_str("CommandOutputDelta"),
            Self::ItemCompleted(_) => formatter.write_str("ItemCompleted"),
            Self::TokenUsageUpdated(_) => formatter.write_str("TokenUsageUpdated"),
            Self::Error(_) => formatter.write_str("Error"),
            Self::TurnCompleted(_) => formatter.write_str("TurnCompleted"),
            Self::Closed(exit) => formatter.debug_tuple("Closed").field(exit).finish(),
        }
    }
}

/// Cloneable request half for one admitted external socket epoch.
///
/// Its API intentionally contains no thread/turn mutation, queue, interrupt, approval, process,
/// or reconnect operation.
#[derive(Clone)]
pub struct ExternalReadOnlyClient {
    report: ExternalGateReport,
    wire: WireAdapter,
    handle: RpcHandle,
}

impl fmt::Debug for ExternalReadOnlyClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExternalReadOnlyClient")
            .field("endpoint_label", &self.report.endpoint_label)
            .field("codex_version", &self.report.codex_version)
            .field("capability_profile", &self.report.capability_profile)
            .field("epoch", &self.handle.epoch())
            .finish_non_exhaustive()
    }
}

impl ExternalReadOnlyClient {
    #[must_use]
    pub fn epoch(&self) -> ConnectionEpoch {
        self.handle.epoch()
    }

    #[must_use]
    pub const fn report(&self) -> &ExternalGateReport {
        &self.report
    }

    fn require_resume(&self) -> Result<(), ExternalTransportError> {
        match self.report.capability_profile {
            ExternalCapabilityProfile::ResumeShared
            | ExternalCapabilityProfile::MutateShared
            | ExternalCapabilityProfile::QueueShared => Ok(()),
            ExternalCapabilityProfile::ObserveShared => {
                Err(ExternalTransportError::UnsupportedProfile)
            }
        }
    }

    /// Lists threads through the bounded read-only RPC surface.
    ///
    /// # Errors
    ///
    /// Returns a content-free timeout, connection, server, RPC, or wire-contract classification.
    pub async fn list_threads(
        &self,
        params: &ThreadListParams,
    ) -> Result<ThreadListResult, ExternalTransportError> {
        self.list_threads_with_timeout(params, CONTROL_RPC_TIMEOUT)
            .await
    }

    /// Lists threads with an explicit request deadline.
    ///
    /// # Errors
    ///
    /// Returns the same content-free classifications as [`Self::list_threads`].
    pub async fn list_threads_with_timeout(
        &self,
        params: &ThreadListParams,
        request_timeout: Duration,
    ) -> Result<ThreadListResult, ExternalTransportError> {
        let params = self
            .wire
            .thread_list_params(params)
            .map_err(|_| ExternalTransportError::ProtocolViolation)?;
        let response: Value = self
            .handle
            .request("thread/list", &params, request_timeout)
            .await
            .map_err(|error| map_rpc_error(&error))?;
        self.wire
            .thread_list_response(response)
            .map_err(|_| ExternalTransportError::ProtocolViolation)
    }

    /// Resumes a thread without admitting any turn mutation.
    ///
    /// # Errors
    ///
    /// Returns `UnsupportedProfile` outside `resume_shared`, or a content-free transport failure.
    pub async fn resume_thread(
        &self,
        params: &ThreadResumeParams,
    ) -> Result<ThreadResumeResult, ExternalTransportError> {
        self.resume_thread_with_timeout(params, CONTROL_RPC_TIMEOUT)
            .await
    }

    /// Resumes a thread with an explicit request deadline.
    ///
    /// # Errors
    ///
    /// Returns the same content-free classifications as [`Self::resume_thread`].
    pub async fn resume_thread_with_timeout(
        &self,
        params: &ThreadResumeParams,
        request_timeout: Duration,
    ) -> Result<ThreadResumeResult, ExternalTransportError> {
        self.require_resume()?;
        let params = self
            .wire
            .thread_resume_params(params)
            .map_err(|_| ExternalTransportError::ProtocolViolation)?;
        let response: Value = self
            .handle
            .request("thread/resume", &params, request_timeout)
            .await
            .map_err(|error| map_rpc_error(&error))?;
        self.wire
            .thread_resume_response(response)
            .map_err(|_| ExternalTransportError::ProtocolViolation)
    }

    /// Reads one thread through the promoted reconciliation profile.
    ///
    /// # Errors
    ///
    /// Returns `UnsupportedProfile` outside `resume_shared`, or a content-free transport failure.
    pub async fn read_thread(
        &self,
        params: &ThreadReadParams,
    ) -> Result<ThreadReadResult, ExternalTransportError> {
        self.read_thread_with_timeout(params, CONTROL_RPC_TIMEOUT)
            .await
    }

    /// Reads one thread with an explicit request deadline.
    ///
    /// # Errors
    ///
    /// Returns the same content-free classifications as [`Self::read_thread`].
    pub async fn read_thread_with_timeout(
        &self,
        params: &ThreadReadParams,
        request_timeout: Duration,
    ) -> Result<ThreadReadResult, ExternalTransportError> {
        self.require_resume()?;
        let params = self
            .wire
            .thread_read_params(params)
            .map_err(|_| ExternalTransportError::ProtocolViolation)?;
        let response: Value = self
            .handle
            .request("thread/read", &params, request_timeout)
            .await
            .map_err(|error| map_rpc_error(&error))?;
        self.wire
            .thread_read_response(response)
            .map_err(|_| ExternalTransportError::ProtocolViolation)
    }

    /// Lists a managed thread's turn page through the reconciliation profile.
    ///
    /// # Errors
    ///
    /// Returns `UnsupportedProfile` outside `resume_shared`, or a content-free transport failure.
    pub async fn list_thread_turns(
        &self,
        params: &ThreadTurnsListParams,
    ) -> Result<ThreadTurnsListResult, ExternalTransportError> {
        self.list_thread_turns_with_timeout(params, CONTROL_RPC_TIMEOUT)
            .await
    }

    /// Lists a managed thread's turn page with an explicit request deadline.
    ///
    /// # Errors
    ///
    /// Returns the same content-free classifications as [`Self::list_thread_turns`].
    pub async fn list_thread_turns_with_timeout(
        &self,
        params: &ThreadTurnsListParams,
        request_timeout: Duration,
    ) -> Result<ThreadTurnsListResult, ExternalTransportError> {
        self.require_resume()?;
        let params = self
            .wire
            .thread_turns_list_params(params)
            .map_err(|_| ExternalTransportError::ProtocolViolation)?;
        let response: Value = self
            .handle
            .request("thread/turns/list", &params, request_timeout)
            .await
            .map_err(|error| map_rpc_error(&error))?;
        self.wire
            .thread_turns_list_response(response)
            .map_err(|_| ExternalTransportError::ProtocolViolation)
    }

    /// Lists a managed thread's item page through the reconciliation profile.
    ///
    /// # Errors
    ///
    /// Returns `UnsupportedProfile` outside `resume_shared`, or a content-free transport failure.
    pub async fn list_thread_items(
        &self,
        params: &ThreadItemsListParams,
    ) -> Result<ThreadItemsListResult, ExternalTransportError> {
        self.list_thread_items_with_timeout(params, CONTROL_RPC_TIMEOUT)
            .await
    }

    /// Lists a managed thread's item page with an explicit request deadline.
    ///
    /// # Errors
    ///
    /// Returns the same content-free classifications as [`Self::list_thread_items`].
    pub async fn list_thread_items_with_timeout(
        &self,
        params: &ThreadItemsListParams,
        request_timeout: Duration,
    ) -> Result<ThreadItemsListResult, ExternalTransportError> {
        self.require_resume()?;
        let params = self
            .wire
            .thread_items_list_params(params)
            .map_err(|_| ExternalTransportError::ProtocolViolation)?;
        let response: Value = self
            .handle
            .request("thread/items/list", &params, request_timeout)
            .await
            .map_err(|error| map_rpc_error(&error))?;
        self.wire
            .thread_items_list_response(response)
            .map_err(|_| ExternalTransportError::ProtocolViolation)
    }
}

/// Cloneable, exact-version mutation half for one admitted external socket epoch.
///
/// This surface is unavailable to observe/resume profiles. It contains no reconnect, process, or
/// retry API; callers must durably fence every non-idempotent request before invoking it.
#[derive(Clone)]
pub struct ExternalMutationClient {
    report: ExternalGateReport,
    wire: WireAdapter,
    handle: RpcHandle,
}

impl fmt::Debug for ExternalMutationClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExternalMutationClient")
            .field("endpoint_label", &self.report.endpoint_label)
            .field("capability_profile", &self.report.capability_profile)
            .field("epoch", &self.handle.epoch())
            .finish_non_exhaustive()
    }
}

impl ExternalMutationClient {
    #[must_use]
    pub fn epoch(&self) -> ConnectionEpoch {
        self.handle.epoch()
    }

    fn require_queue(&self) -> Result<(), ExternalTransportError> {
        if self.report.capability_profile == ExternalCapabilityProfile::QueueShared {
            Ok(())
        } else {
            Err(ExternalTransportError::UnsupportedProfile)
        }
    }

    /// Starts one exact-target turn through the promoted external mutation profile.
    ///
    /// # Errors
    ///
    /// Returns a content-free profile, wire, server, timeout, or connection classification.
    pub async fn start_turn(
        &self,
        params: &TurnStartParams,
        request_timeout: Duration,
    ) -> Result<TurnStartResult, ExternalTransportError> {
        let params = self
            .wire
            .turn_start_params(params)
            .map_err(|_| ExternalTransportError::ProtocolViolation)?;
        let response: Value = self
            .handle
            .request("turn/start", &params, request_timeout)
            .await
            .map_err(|error| map_rpc_error(&error))?;
        self.wire
            .turn_start_response(response)
            .map_err(|_| ExternalTransportError::ProtocolViolation)
    }

    /// Steers one exact active turn through the promoted external mutation profile.
    ///
    /// # Errors
    ///
    /// Returns a content-free profile, wire, server, timeout, or connection classification.
    pub async fn steer_turn(
        &self,
        params: &TurnSteerParams,
        request_timeout: Duration,
    ) -> Result<TurnSteerResult, ExternalTransportError> {
        let params = self
            .wire
            .turn_steer_params(params)
            .map_err(|_| ExternalTransportError::ProtocolViolation)?;
        let response: Value = self
            .handle
            .request("turn/steer", &params, request_timeout)
            .await
            .map_err(|error| map_rpc_error(&error))?;
        self.wire
            .turn_steer_response(response)
            .map_err(|_| ExternalTransportError::ProtocolViolation)
    }

    /// Interrupts one exact active turn at high RPC priority.
    ///
    /// # Errors
    ///
    /// Returns a content-free profile, wire, server, timeout, or connection classification.
    pub async fn interrupt_turn(
        &self,
        params: &TurnInterruptParams,
        request_timeout: Duration,
    ) -> Result<TurnInterruptResult, ExternalTransportError> {
        let params = self
            .wire
            .turn_interrupt_params(params)
            .map_err(|_| ExternalTransportError::ProtocolViolation)?;
        let response: Value = self
            .handle
            .request_high("turn/interrupt", &params, request_timeout)
            .await
            .map_err(|error| map_rpc_error(&error))?;
        self.wire
            .turn_interrupt_response(response)
            .map_err(|_| ExternalTransportError::ProtocolViolation)
    }

    /// Adds one client-correlated input to the promoted external queue.
    ///
    /// # Errors
    ///
    /// Returns a content-free profile, wire, server, timeout, or connection classification.
    pub async fn add_to_queue(
        &self,
        params: &ThreadQueueAddParams,
        request_timeout: Duration,
    ) -> Result<ThreadQueueAddResult, ExternalTransportError> {
        self.require_queue()?;
        let params = self
            .wire
            .thread_queue_add_params(params)
            .map_err(|_| ExternalTransportError::ProtocolViolation)?;
        let response: Value = self
            .handle
            .request("thread/queue/add", &params, request_timeout)
            .await
            .map_err(|error| map_rpc_error(&error))?;
        self.wire
            .thread_queue_add_response(response)
            .map_err(|_| ExternalTransportError::ProtocolViolation)
    }

    /// Lists a bounded page of the promoted external queue.
    ///
    /// # Errors
    ///
    /// Returns a content-free profile, wire, server, timeout, or connection classification.
    pub async fn list_queue(
        &self,
        params: &ThreadQueueListParams,
        request_timeout: Duration,
    ) -> Result<ThreadQueueListResult, ExternalTransportError> {
        self.require_queue()?;
        let params = self
            .wire
            .thread_queue_list_params(params)
            .map_err(|_| ExternalTransportError::ProtocolViolation)?;
        let response: Value = self
            .handle
            .request("thread/queue/list", &params, request_timeout)
            .await
            .map_err(|error| map_rpc_error(&error))?;
        self.wire
            .thread_queue_list_response(response)
            .map_err(|_| ExternalTransportError::ProtocolViolation)
    }

    /// Starts one exact queued submission.
    ///
    /// # Errors
    ///
    /// Returns a content-free profile, wire, server, timeout, or connection classification.
    pub async fn start_queued(
        &self,
        params: &ThreadQueueStartParams,
        request_timeout: Duration,
    ) -> Result<ThreadQueueStartResult, ExternalTransportError> {
        self.require_queue()?;
        let params = self
            .wire
            .thread_queue_start_params(params)
            .map_err(|_| ExternalTransportError::ProtocolViolation)?;
        let response: Value = self
            .handle
            .request("thread/queue/start", &params, request_timeout)
            .await
            .map_err(|error| map_rpc_error(&error))?;
        self.wire
            .thread_queue_start_response(response)
            .map_err(|_| ExternalTransportError::ProtocolViolation)
    }

    /// Answers one command approval token from this exact connection epoch.
    ///
    /// # Errors
    ///
    /// Returns a content-free classification for an invalid token, response, or connection.
    pub async fn respond_command_approval(
        &self,
        approval: &mut ExternalApprovalRequest,
        result: &CommandExecutionRequestApprovalResult,
    ) -> Result<(), ExternalTransportError> {
        let ExternalApprovalRequest::Command { request, .. } = approval else {
            return Err(ExternalTransportError::ProtocolViolation);
        };
        let result = self
            .wire
            .command_execution_request_approval_response(result)
            .map_err(|_| ExternalTransportError::ProtocolViolation)?;
        self.handle
            .respond_request(request, &result)
            .await
            .map_err(|error| map_rpc_error(&error))
    }

    /// Answers one file-change approval token from this exact connection epoch.
    ///
    /// # Errors
    ///
    /// Returns a content-free classification for an invalid token, response, or connection.
    pub async fn respond_file_change_approval(
        &self,
        approval: &mut ExternalApprovalRequest,
        result: &FileChangeRequestApprovalResult,
    ) -> Result<(), ExternalTransportError> {
        let ExternalApprovalRequest::FileChange { request, .. } = approval else {
            return Err(ExternalTransportError::ProtocolViolation);
        };
        let result = self
            .wire
            .file_change_request_approval_response(result)
            .map_err(|_| ExternalTransportError::ProtocolViolation)?;
        self.handle
            .respond_request(request, &result)
            .await
            .map_err(|error| map_rpc_error(&error))
    }

    /// Answers one permissions approval token from this exact connection epoch.
    ///
    /// # Errors
    ///
    /// Returns a content-free classification for an invalid token, response, or connection.
    pub async fn respond_permissions_approval(
        &self,
        approval: &mut ExternalApprovalRequest,
        result: &PermissionsRequestApprovalResult,
    ) -> Result<(), ExternalTransportError> {
        let ExternalApprovalRequest::Permissions { request, .. } = approval else {
            return Err(ExternalTransportError::ProtocolViolation);
        };
        let result = self
            .wire
            .permissions_request_approval_response(result)
            .map_err(|_| ExternalTransportError::ProtocolViolation)?;
        self.handle
            .respond_request(request, &result)
            .await
            .map_err(|error| map_rpc_error(&error))
    }

    /// Fails this epoch closed when an approval cannot be routed safely.
    ///
    /// # Errors
    ///
    /// Returns a content-free classification when the approval token is already stale.
    pub fn abandon_approval(
        &self,
        approval: &mut ExternalApprovalRequest,
    ) -> Result<(), ExternalTransportError> {
        let request = match approval {
            ExternalApprovalRequest::Command { request, .. }
            | ExternalApprovalRequest::FileChange { request, .. }
            | ExternalApprovalRequest::Permissions { request, .. } => request,
        };
        self.handle
            .abandon_request(request)
            .map_err(|error| map_rpc_error(&error))
    }
}

fn map_rpc_error(error: &RpcError) -> ExternalTransportError {
    match error {
        RpcError::Timeout { .. } => ExternalTransportError::RequestTimeout,
        RpcError::ConnectionLost(_) => ExternalTransportError::ConnectionLost,
        RpcError::Server { code, .. } => ExternalTransportError::ServerRejected { code: *code },
        _ => ExternalTransportError::Rpc,
    }
}

/// One exact-version, authenticated, already initialized external socket epoch.
///
/// There is intentionally no process factory, command, child, PID, wait, kill, terminate,
/// restart, or reconfiguration capability in this type.
pub struct ExternalReadOnlyConnection {
    report: ExternalGateReport,
    wire: WireAdapter,
    connection: RpcConnection,
}

impl fmt::Debug for ExternalReadOnlyConnection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExternalReadOnlyConnection")
            .field("endpoint_label", &self.report.endpoint_label)
            .field("codex_version", &self.report.codex_version)
            .field("capability_profile", &self.report.capability_profile)
            .field("epoch", &self.connection.handle.epoch())
            .finish_non_exhaustive()
    }
}

impl ExternalReadOnlyConnection {
    /// Opens one authenticated socket, runs the exact-version initialize and one-row list gate on
    /// that same connection, then transfers the admitted socket to the bounded transport owner.
    ///
    /// # Errors
    ///
    /// Returns only static, content-free admission classifications.
    pub async fn connect(
        gate: &ExternalEndpointGate,
        epoch: ConnectionEpoch,
        parent_cancellation: CancellationToken,
    ) -> Result<Self, ExternalTransportError> {
        let admitted = gate
            .admit_socket()
            .await
            .map_err(ExternalTransportError::Admission)?;
        let transport = spawn_websocket_transport(admitted.socket, parent_cancellation.clone());
        let policy = match admitted.report.capability_profile {
            ExternalCapabilityProfile::ObserveShared => {
                RpcProtocolPolicy::FailClosedExternalObserve
            }
            ExternalCapabilityProfile::ResumeShared => RpcProtocolPolicy::FailClosedExternalResume,
            ExternalCapabilityProfile::MutateShared => RpcProtocolPolicy::FailClosedExternalMutate,
            ExternalCapabilityProfile::QueueShared => RpcProtocolPolicy::FailClosedExternalQueue,
        };
        let connection = spawn_rpc_with_policy(transport, epoch, parent_cancellation, policy);
        Ok(Self {
            report: admitted.report,
            wire: admitted.wire,
            connection,
        })
    }

    #[must_use]
    pub const fn report(&self) -> &ExternalGateReport {
        &self.report
    }

    #[must_use]
    pub fn epoch(&self) -> ConnectionEpoch {
        self.connection.handle.epoch()
    }

    #[must_use]
    pub fn client(&self) -> ExternalReadOnlyClient {
        ExternalReadOnlyClient {
            report: self.report.clone(),
            wire: self.wire,
            handle: self.connection.handle.clone(),
        }
    }

    /// Returns a mutation client only for an explicitly promoted write profile.
    ///
    /// # Errors
    ///
    /// Returns `UnsupportedProfile` for observe/resume connections.
    pub fn mutation_client(&self) -> Result<ExternalMutationClient, ExternalTransportError> {
        match self.report.capability_profile {
            ExternalCapabilityProfile::MutateShared | ExternalCapabilityProfile::QueueShared => {
                Ok(ExternalMutationClient {
                    report: self.report.clone(),
                    wire: self.wire,
                    handle: self.connection.handle.clone(),
                })
            }
            ExternalCapabilityProfile::ObserveShared | ExternalCapabilityProfile::ResumeShared => {
                Err(ExternalTransportError::UnsupportedProfile)
            }
        }
    }

    /// Executes a bounded typed `thread/list` read for either promoted external profile.
    /// Profile-gated resume/read pagination is available only through [`Self::client`], and no
    /// mutation method is exposed.
    ///
    /// # Errors
    ///
    /// Fails with a content-free classification on RPC, timeout, connection, or wire drift.
    pub async fn list_threads(
        &self,
        params: &ThreadListParams,
    ) -> Result<ThreadListResult, ExternalTransportError> {
        self.list_threads_with_timeout(params, CONTROL_RPC_TIMEOUT)
            .await
    }

    /// Same read-only request with a caller-supplied bounded deadline, primarily for deterministic
    /// overload and stale-response tests.
    ///
    /// # Errors
    ///
    /// Returns the same content-free classifications as [`Self::list_threads`].
    pub async fn list_threads_with_timeout(
        &self,
        params: &ThreadListParams,
        request_timeout: Duration,
    ) -> Result<ThreadListResult, ExternalTransportError> {
        self.client()
            .list_threads_with_timeout(params, request_timeout)
            .await
    }

    /// Waits for the next exact promoted terminal/status notification or socket close.
    ///
    /// # Errors
    ///
    /// Fails closed if an impossible event reaches this layer or an allowlisted notification does
    /// not satisfy the exact promoted wire shape.
    #[allow(clippy::too_many_lines)]
    pub async fn recv_event(
        &mut self,
    ) -> Result<Option<ExternalReadEvent>, ExternalTransportError> {
        let Some(event) = self.connection.events.recv().await else {
            return Ok(None);
        };
        match event {
            RpcEvent::Notification { method, params } => {
                let params = params.ok_or(ExternalTransportError::ProtocolViolation)?;
                match method.as_str() {
                    "account/rateLimits/updated" => serde_json::from_value(params)
                        .map(ExternalReadEvent::AccountRateLimitsUpdated)
                        .map(Some)
                        .map_err(|_| ExternalTransportError::ProtocolViolation),
                    "remoteControl/status/changed" => self
                        .wire
                        .remote_control_status_changed_notification(params)
                        .map(ExternalReadEvent::RemoteControlStatusChanged)
                        .map(Some)
                        .map_err(|_| ExternalTransportError::ProtocolViolation),
                    "thread/goal/cleared" => self
                        .wire
                        .thread_goal_cleared_notification(params)
                        .map(ExternalReadEvent::ThreadGoalCleared)
                        .map(Some)
                        .map_err(|_| ExternalTransportError::ProtocolViolation),
                    "thread/settings/updated" => serde_json::from_value(params)
                        .map(ExternalReadEvent::ThreadSettingsUpdated)
                        .map(Some)
                        .map_err(|_| ExternalTransportError::ProtocolViolation),
                    "thread/status/changed" => self
                        .wire
                        .thread_status_changed_notification(params)
                        .map(ExternalReadEvent::ThreadStatusChanged)
                        .map(Some)
                        .map_err(|_| ExternalTransportError::ProtocolViolation),
                    "thread/queue/changed" => self
                        .wire
                        .thread_queue_changed_notification(params)
                        .map(ExternalReadEvent::ThreadQueueChanged)
                        .map(Some)
                        .map_err(|_| ExternalTransportError::ProtocolViolation),
                    "serverRequest/resolved" => self
                        .wire
                        .server_request_resolved_notification(params)
                        .map(ExternalReadEvent::ServerRequestResolved)
                        .map(Some)
                        .map_err(|_| ExternalTransportError::ProtocolViolation),
                    "turn/started" => self
                        .wire
                        .turn_started_notification(params)
                        .map(ExternalReadEvent::TurnStarted)
                        .map(Some)
                        .map_err(|_| ExternalTransportError::ProtocolViolation),
                    "item/started" => self
                        .wire
                        .item_started_notification(params)
                        .map(ExternalReadEvent::ItemStarted)
                        .map(Some)
                        .map_err(|_| ExternalTransportError::ProtocolViolation),
                    "item/agentMessage/delta" => self
                        .wire
                        .agent_message_delta_notification(params)
                        .map(ExternalReadEvent::AgentMessageDelta)
                        .map(Some)
                        .map_err(|_| ExternalTransportError::ProtocolViolation),
                    "item/commandExecution/outputDelta" => self
                        .wire
                        .command_output_delta_notification(params)
                        .map(ExternalReadEvent::CommandOutputDelta)
                        .map(Some)
                        .map_err(|_| ExternalTransportError::ProtocolViolation),
                    "item/completed" => self
                        .wire
                        .item_completed_notification(params)
                        .map(ExternalReadEvent::ItemCompleted)
                        .map(Some)
                        .map_err(|_| ExternalTransportError::ProtocolViolation),
                    "thread/tokenUsage/updated" => self
                        .wire
                        .token_usage_updated_notification(params)
                        .map(ExternalReadEvent::TokenUsageUpdated)
                        .map(Some)
                        .map_err(|_| ExternalTransportError::ProtocolViolation),
                    "error" => self
                        .wire
                        .error_notification(params)
                        .map(ExternalReadEvent::Error)
                        .map(Some)
                        .map_err(|_| ExternalTransportError::ProtocolViolation),
                    "turn/completed" => self
                        .wire
                        .turn_completed_notification(params)
                        .map(ExternalReadEvent::TurnCompleted)
                        .map(Some)
                        .map_err(|_| ExternalTransportError::ProtocolViolation),
                    _ => Err(ExternalTransportError::ProtocolViolation),
                }
            }
            RpcEvent::TransportClosed(exit) => Ok(Some(ExternalReadEvent::Closed(exit))),
            RpcEvent::ServerRequest(mut request) => {
                let params = request
                    .params
                    .take()
                    .ok_or(ExternalTransportError::ProtocolViolation)?;
                let approval = match request.method.as_str() {
                    "item/commandExecution/requestApproval" => self
                        .wire
                        .command_execution_request_approval_params(params)
                        .map(|params| ExternalApprovalRequest::Command { params, request }),
                    "item/fileChange/requestApproval" => self
                        .wire
                        .file_change_request_approval_params(params)
                        .map(|params| ExternalApprovalRequest::FileChange { params, request }),
                    "item/permissions/requestApproval" => self
                        .wire
                        .permissions_request_approval_params(params)
                        .map(|params| ExternalApprovalRequest::Permissions { params, request }),
                    _ => return Err(ExternalTransportError::ProtocolViolation),
                }
                .map_err(|_| ExternalTransportError::ProtocolViolation)?;
                Ok(Some(ExternalReadEvent::Approval(approval)))
            }
            RpcEvent::ProtocolDrift => Err(ExternalTransportError::ProtocolViolation),
        }
    }

    /// Performs an orderly socket-only shutdown and returns the exact close-handshake report from
    /// the transport.
    pub async fn shutdown(&mut self) -> TransportExit {
        self.connection.shutdown().await
    }

    /// Simulates abrupt bridge loss by dropping the socket owner without a close handshake. It
    /// cannot signal or otherwise control the external server process.
    pub fn abort(&mut self) -> TransportExit {
        self.connection.abort()
    }
}
