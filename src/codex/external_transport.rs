//! Bounded authenticated WebSocket transport for an operator-owned Codex app-server.
//!
//! The external connection owns one admitted socket and nothing process-like. It deliberately
//! exposes only promoted read and resume operations. Reconnect, reconciliation, and all write
//! methods remain outside this type.

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
        spawn_rpc_with_policy,
    },
    transport::{TransportExit, spawn_websocket_transport},
    types::{
        ItemCompletedNotification, RemoteControlStatusChangedNotification,
        ThreadGoalClearedNotification, ThreadItemsListParams, ThreadItemsListResult,
        ThreadListParams, ThreadListResult, ThreadReadParams, ThreadReadResult, ThreadResumeParams,
        ThreadResumeResult, ThreadStatusChangedNotification, ThreadTurnsListParams,
        ThreadTurnsListResult, TurnCompletedNotification,
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

/// Typed, allowlisted traffic from a `resume_shared` socket epoch.
pub enum ExternalReadEvent {
    RemoteControlStatusChanged(RemoteControlStatusChangedNotification),
    ThreadGoalCleared(ThreadGoalClearedNotification),
    ThreadStatusChanged(ThreadStatusChangedNotification),
    ItemCompleted(ItemCompletedNotification),
    TurnCompleted(TurnCompletedNotification),
    Closed(TransportExit),
}

impl fmt::Debug for ExternalReadEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RemoteControlStatusChanged(_) => {
                formatter.write_str("RemoteControlStatusChanged")
            }
            Self::ThreadGoalCleared(_) => formatter.write_str("ThreadGoalCleared"),
            Self::ThreadStatusChanged(_) => formatter.write_str("ThreadStatusChanged"),
            Self::ItemCompleted(_) => formatter.write_str("ItemCompleted"),
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
        if self.report.capability_profile == ExternalCapabilityProfile::ResumeShared {
            Ok(())
        } else {
            Err(ExternalTransportError::UnsupportedProfile)
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
                    "thread/status/changed" => self
                        .wire
                        .thread_status_changed_notification(params)
                        .map(ExternalReadEvent::ThreadStatusChanged)
                        .map(Some)
                        .map_err(|_| ExternalTransportError::ProtocolViolation),
                    "item/completed" => self
                        .wire
                        .item_completed_notification(params)
                        .map(ExternalReadEvent::ItemCompleted)
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
            RpcEvent::ServerRequest(_) | RpcEvent::ProtocolDrift => {
                Err(ExternalTransportError::ProtocolViolation)
            }
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
