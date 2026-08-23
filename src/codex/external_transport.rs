//! Bounded authenticated WebSocket transport for an operator-owned Codex app-server.
//!
//! The external connection owns one admitted socket and nothing process-like. It deliberately
//! exposes only the `observe_shared` read canary in this issue; reconnect, reconciliation, and all
//! mutation methods remain outside this type.

use std::{fmt, time::Duration};

use serde_json::Value;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use crate::codex::{
    compat::WireAdapter,
    external::{ExternalEndpointGate, ExternalGateError, ExternalGateReport},
    rpc::{ConnectionEpoch, RpcConnection, RpcProtocolPolicy, spawn_rpc_with_policy},
    transport::{TransportExit, spawn_websocket_transport},
    types::{ThreadListParams, ThreadListResult},
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
    #[error("external Codex response violated the promoted contract")]
    ProtocolViolation,
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
        let connection = spawn_rpc_with_policy(
            transport,
            epoch,
            parent_cancellation,
            RpcProtocolPolicy::FailClosedExternalObserve,
        );
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

    /// Executes the only public operation admitted by this issue's external connection: a bounded
    /// typed `thread/list` read. No mutation method is exposed.
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
        let params = self
            .wire
            .thread_list_params(params)
            .map_err(|_| ExternalTransportError::ProtocolViolation)?;
        let response: Value = self
            .connection
            .handle
            .request("thread/list", &params, request_timeout)
            .await
            .map_err(|_| ExternalTransportError::Rpc)?;
        self.wire
            .thread_list_response(response)
            .map_err(|_| ExternalTransportError::ProtocolViolation)
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
