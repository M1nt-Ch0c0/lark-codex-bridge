use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt, io,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use tokio::{
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use crate::{
    codex::{
        compat::WireAdapter,
        protocol::{
            InboundMessage, OutboundMessage, RequestId, RpcErrorObject, request_id_memory_weight,
            value_memory_weight,
        },
        transport::{
            TransportEvent, TransportExit, TransportHandle, TransportSendError, TransportSender,
        },
        types::{ClientInfo, InitializeCapabilities, InitializeParams, InitializeResult},
    },
    limits::{
        CONTROL_RPC_TIMEOUT, EVENT_CAPACITY, HIGH_PRIORITY_BURST, INITIALIZE_TIMEOUT,
        MAX_JSONL_LINE_BYTES, MAX_OUTBOUND_VALUE_WIRE_BYTES, RPC_BYTE_BUDGET, RPC_HIGH_BYTE_BUDGET,
        RPC_HIGH_CAPACITY, RPC_INFLIGHT_CAPACITY, RPC_NORMAL_CAPACITY,
        RPC_RELIABLE_EVENT_BYTE_BUDGET, RPC_RELIABLE_EVENT_CAPACITY, RPC_SERVER_REQUEST_CAPACITY,
        RPC_TOTAL_PENDING_CAPACITY,
    },
};

const STATE_OPEN: u8 = 0;
const STATE_LOST: u8 = 1;
const INIT_NEW: u8 = 0;
const INIT_RUNNING: u8 = 1;
const INIT_READY: u8 = 2;
const INIT_FAILED: u8 = 3;
const SERVER_REQUEST_ARMED: u8 = 0;
const SERVER_REQUEST_ACTOR_OWNED: u8 = 1;
const SERVER_REQUEST_RESOLVED: u8 = 2;

/// Identifies one app-server connection. IDs from different epochs never correlate.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ConnectionEpoch(u64);

impl ConnectionEpoch {
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A request initiated by app-server and requiring a high-priority response.
pub struct ServerRequest {
    id: RequestId,
    pub method: String,
    pub params: Option<Value>,
    epoch: ConnectionEpoch,
    retention: Option<OwnedSemaphorePermit>,
    transport_retention: Option<OwnedSemaphorePermit>,
    lease: Arc<ServerRequestLease>,
}

impl ServerRequest {
    /// Returns the opaque app-server request token.  It is intentionally
    /// read-only: callers must answer through `respond_request` instead of
    /// constructing or mutating response IDs.
    #[must_use]
    pub const fn id(&self) -> &RequestId {
        &self.id
    }

    #[must_use]
    pub const fn epoch(&self) -> ConnectionEpoch {
        self.epoch
    }

    pub(crate) fn retain_with(&mut self, permit: OwnedSemaphorePermit) {
        self.retention = Some(permit);
    }
}

impl Drop for ServerRequest {
    fn drop(&mut self) {
        self.lease.abandon_if_armed();
    }
}

struct ServerRequestLease {
    state: AtomicU8,
    abandonment: CancellationToken,
}

impl ServerRequestLease {
    fn new(abandonment: CancellationToken) -> Self {
        Self {
            state: AtomicU8::new(SERVER_REQUEST_ARMED),
            abandonment,
        }
    }

    fn is_armed(&self) -> bool {
        self.state.load(Ordering::Acquire) == SERVER_REQUEST_ARMED
    }

    fn handoff_to_actor(&self) {
        let _ = self.state.compare_exchange(
            SERVER_REQUEST_ARMED,
            SERVER_REQUEST_ACTOR_OWNED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    fn resolve_actor_owned(&self) {
        let _ = self.state.compare_exchange(
            SERVER_REQUEST_ACTOR_OWNED,
            SERVER_REQUEST_RESOLVED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    fn fail_actor_owned(&self) {
        if self
            .state
            .compare_exchange(
                SERVER_REQUEST_ACTOR_OWNED,
                SERVER_REQUEST_RESOLVED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.abandonment.cancel();
        }
    }

    fn abandon_if_armed(&self) {
        if self
            .state
            .compare_exchange(
                SERVER_REQUEST_ARMED,
                SERVER_REQUEST_RESOLVED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            self.abandonment.cancel();
        }
    }
}

impl fmt::Debug for ServerRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerRequest")
            .field("id_kind", &request_id_kind(&self.id))
            .field("method", &self.method)
            .field("has_params", &self.params.is_some())
            .field("has_retention", &self.retention.is_some())
            .field(
                "has_transport_retention",
                &self.transport_retention.is_some(),
            )
            .field("armed", &self.lease.is_armed())
            .field("epoch", &self.epoch)
            .finish_non_exhaustive()
    }
}

/// Non-response traffic emitted by the RPC owner.
pub enum RpcEvent {
    Notification {
        method: String,
        params: Option<Value>,
    },
    ServerRequest(ServerRequest),
    TransportClosed(TransportExit),
    ProtocolDrift,
}

impl fmt::Debug for RpcEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Notification { method, params } => formatter
                .debug_struct("Notification")
                .field("method", method)
                .field("has_params", &params.is_some())
                .finish(),
            Self::ServerRequest(request) => formatter
                .debug_tuple("ServerRequest")
                .field(request)
                .finish(),
            Self::TransportClosed(exit) => formatter
                .debug_tuple("TransportClosed")
                .field(exit)
                .finish(),
            Self::ProtocolDrift => formatter.write_str("ProtocolDrift"),
        }
    }
}

/// Safe RPC failures. Remote text and data are deliberately discarded.
pub enum RpcError {
    Timeout { method: &'static str },
    Server { method: &'static str, code: i64 },
    ConnectionLost(ConnectionEpoch),
    AlreadyInitialized,
    Serialize { method: &'static str },
    Deserialize { method: &'static str },
    PayloadTooLarge { method: &'static str },
    UnknownServerRequest,
    RequestIdExhausted,
}

pub struct BudgetedResponse<T> {
    pub value: T,
    transport_budget: OwnedSemaphorePermit,
}

impl<T> BudgetedResponse<T> {
    #[must_use]
    pub fn into_inner(self) -> T {
        self.value
    }

    pub fn into_parts(self) -> (T, OwnedSemaphorePermit) {
        (self.value, self.transport_budget)
    }

    /// Maps a decoded response while retaining its inbound memory permit.
    ///
    /// # Errors
    ///
    /// Returns the mapping closure's error without discarding it or exposing the response.
    pub fn try_map<U, E>(
        self,
        map: impl FnOnce(T) -> Result<U, E>,
    ) -> Result<BudgetedResponse<U>, E> {
        Ok(BudgetedResponse {
            value: map(self.value)?,
            transport_budget: self.transport_budget,
        })
    }
}

impl fmt::Debug for RpcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl fmt::Display for RpcError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout { method } => write!(formatter, "RPC {method} timed out"),
            Self::Server { method, code } => {
                write!(formatter, "RPC {method} failed with server code {code}")
            }
            Self::ConnectionLost(epoch) => {
                write!(formatter, "app-server connection {} was lost", epoch.get())
            }
            Self::AlreadyInitialized => formatter.write_str("connection was already initialized"),
            Self::Serialize { method } => write!(formatter, "RPC {method} parameters are invalid"),
            Self::Deserialize { method } => write!(formatter, "RPC {method} result is invalid"),
            Self::PayloadTooLarge { method } => {
                write!(formatter, "RPC {method} exceeds the protocol size limit")
            }
            Self::UnknownServerRequest => {
                formatter.write_str("server request is no longer pending")
            }
            Self::RequestIdExhausted => formatter.write_str("connection request IDs are exhausted"),
        }
    }
}

impl std::error::Error for RpcError {}

#[derive(Clone)]
pub struct RpcHandle {
    high_tx: mpsc::Sender<RpcCommand>,
    normal_tx: mpsc::Sender<RpcCommand>,
    cancel_tx: mpsc::Sender<RequestId>,
    epoch: ConnectionEpoch,
    state: Arc<AtomicU8>,
    initialize_state: Arc<AtomicU8>,
    next_id: Arc<AtomicU64>,
    high_inflight: Arc<Semaphore>,
    normal_inflight: Arc<Semaphore>,
    high_command_budget: Arc<Semaphore>,
    normal_command_budget: Arc<Semaphore>,
    pending_count: Arc<AtomicUsize>,
    protocol_drift_count: Arc<AtomicU64>,
    dropped_notification_count: Arc<AtomicU64>,
    cancellation: CancellationToken,
}

impl RpcHandle {
    /// Sends a normal-priority request and decodes its typed result.
    ///
    /// # Errors
    ///
    /// Returns a safe [`RpcError`] for admission, encoding, timeout, server,
    /// connection, or result-decoding failures.
    pub async fn request<P, R>(
        &self,
        method: &'static str,
        params: &P,
        timeout: Duration,
    ) -> Result<R, RpcError>
    where
        P: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        self.request_budgeted_with_priority(false, method, params, timeout)
            .await
            .map(BudgetedResponse::into_inner)
    }

    /// Sends a control request ahead of normal work.
    ///
    /// # Errors
    ///
    /// Returns a safe [`RpcError`] for admission, encoding, timeout, server,
    /// connection, or result-decoding failures.
    pub async fn request_high<P, R>(
        &self,
        method: &'static str,
        params: &P,
        timeout: Duration,
    ) -> Result<R, RpcError>
    where
        P: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        self.request_budgeted_with_priority(true, method, params, timeout)
            .await
            .map(BudgetedResponse::into_inner)
    }

    /// Returns a typed response while retaining its inbound memory budget.
    ///
    /// # Errors
    ///
    /// Returns a safe [`RpcError`] for admission, encoding, timeout, server,
    /// connection, or result-decoding failures.
    pub async fn request_budgeted<P, R>(
        &self,
        method: &'static str,
        params: &P,
        timeout: Duration,
    ) -> Result<BudgetedResponse<R>, RpcError>
    where
        P: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        self.request_budgeted_with_priority(false, method, params, timeout)
            .await
    }

    async fn request_budgeted_with_priority<P, R>(
        &self,
        high: bool,
        method: &'static str,
        params: &P,
        timeout: Duration,
    ) -> Result<BudgetedResponse<R>, RpcError>
    where
        P: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        self.ensure_open()?;
        let deadline = deadline_after(timeout);
        let inflight = self
            .acquire_until(
                Arc::clone(if high {
                    &self.high_inflight
                } else {
                    &self.normal_inflight
                }),
                1,
                method,
                deadline,
            )
            .await?;
        let command_budget = if high {
            &self.high_command_budget
        } else {
            &self.normal_command_budget
        };
        let (params, budget) = self
            .serialize_bounded(command_budget, method, params, deadline)
            .await?;
        let id = self.next_request_id()?;
        let (reply_tx, reply_rx) = oneshot::channel();
        let command = RpcCommand::Request {
            id: id.clone(),
            method,
            params,
            deadline,
            reply: reply_tx,
            _inflight: inflight,
            _budget: budget,
        };
        self.enqueue(high, command, method, deadline).await?;

        let mut cancel_guard = RequestCancelGuard {
            id,
            tx: self.cancel_tx.clone(),
            active: true,
        };
        let response = tokio::select! {
            biased;
            () = self.cancellation.cancelled() => Err(RpcError::ConnectionLost(self.epoch)),
            () = tokio::time::sleep_until(deadline) => Err(RpcError::Timeout { method }),
            response = reply_rx => {
                cancel_guard.active = false;
                response.unwrap_or(Err(RpcError::ConnectionLost(self.epoch)))
            },
        };
        let response = response?;
        let value =
            serde_json::from_value(response.value).map_err(|_| RpcError::Deserialize { method })?;
        Ok(BudgetedResponse {
            value,
            transport_budget: response.transport_budget,
        })
    }

    /// Sends a normal-priority notification and waits for transport admission.
    ///
    /// # Errors
    ///
    /// Returns a safe [`RpcError`] if serialization, admission, or transport fails.
    pub async fn notify<P>(&self, method: &'static str, params: &P) -> Result<(), RpcError>
    where
        P: Serialize + ?Sized,
    {
        self.notify_with_priority(false, method, params).await
    }

    async fn notify_with_priority<P>(
        &self,
        high: bool,
        method: &'static str,
        params: &P,
    ) -> Result<(), RpcError>
    where
        P: Serialize + ?Sized,
    {
        let deadline = deadline_after(CONTROL_RPC_TIMEOUT);
        let command_budget = if high {
            &self.high_command_budget
        } else {
            &self.normal_command_budget
        };
        let (params, budget) = self
            .serialize_bounded(command_budget, method, params, deadline)
            .await?;
        self.send_fire(
            high,
            method,
            OutboundMessage::Notification {
                method: method.to_owned(),
                params: Some(params),
            },
            None,
            budget,
            deadline,
        )
        .await
    }

    async fn notify_empty_params_high(&self, method: &'static str) -> Result<(), RpcError> {
        let deadline = deadline_after(CONTROL_RPC_TIMEOUT);
        let budget = self
            .acquire_until(
                Arc::clone(&self.high_command_budget),
                bounded_permits(method.len().saturating_add(32)),
                method,
                deadline,
            )
            .await?;
        self.send_fire(
            true,
            method,
            OutboundMessage::Notification {
                method: method.to_owned(),
                params: Some(Value::Object(serde_json::Map::new())),
            },
            None,
            budget,
            deadline,
        )
        .await
    }

    /// Answers one still-pending app-server request at high priority.
    ///
    /// # Errors
    ///
    /// Returns a safe [`RpcError`] if the request is stale or serialization,
    /// admission, or transport fails.
    async fn respond_id<R>(&self, request: &ServerRequest, result: &R) -> Result<(), RpcError>
    where
        R: Serialize + ?Sized,
    {
        let method = "server/respond";
        let deadline = deadline_after(CONTROL_RPC_TIMEOUT);
        let (result, budget) = self
            .serialize_bounded(&self.high_command_budget, method, result, deadline)
            .await?;
        self.send_fire(
            true,
            method,
            OutboundMessage::Response {
                id: request.id.clone(),
                result,
            },
            Some(ServerResponseLease {
                id: request.id.clone(),
                lease: Arc::clone(&request.lease),
            }),
            budget,
            deadline,
        )
        .await
    }

    /// Answers a request token emitted by this exact connection epoch.
    ///
    /// # Errors
    ///
    /// Returns [`RpcError::UnknownServerRequest`] for a stale token, or another
    /// safe RPC failure if serialization, admission, or transport fails.
    pub async fn respond_request<R>(
        &self,
        request: &mut ServerRequest,
        result: &R,
    ) -> Result<(), RpcError>
    where
        R: Serialize + ?Sized,
    {
        if request.epoch != self.epoch {
            return Err(RpcError::UnknownServerRequest);
        }
        if !request.lease.is_armed() {
            return Err(RpcError::UnknownServerRequest);
        }
        self.respond_id(request, result).await
    }

    /// Rejects one still-pending app-server request at high priority.
    ///
    /// # Errors
    ///
    /// Returns a safe [`RpcError`] if the request is stale or admission or
    /// transport fails.
    async fn respond_error_id(
        &self,
        request: &ServerRequest,
        code: i64,
        message: &str,
    ) -> Result<(), RpcError> {
        let method = "server/respond_error";
        let deadline = deadline_after(CONTROL_RPC_TIMEOUT);
        let wire_size = count_serialized(method, message)?.saturating_add(128);
        if wire_size > MAX_JSONL_LINE_BYTES {
            return Err(RpcError::PayloadTooLarge { method });
        }
        let permits = bounded_permits(message.len().saturating_add(128).max(wire_size));
        let budget = self
            .acquire_until(
                Arc::clone(&self.high_command_budget),
                permits,
                method,
                deadline,
            )
            .await?;
        self.send_fire(
            true,
            method,
            OutboundMessage::ErrorResponse {
                id: request.id.clone(),
                error: RpcErrorObject {
                    code,
                    message: message.to_owned(),
                    data: None,
                },
            },
            Some(ServerResponseLease {
                id: request.id.clone(),
                lease: Arc::clone(&request.lease),
            }),
            budget,
            deadline,
        )
        .await
    }

    /// Rejects a request token emitted by this exact connection epoch.
    ///
    /// # Errors
    ///
    /// Returns [`RpcError::UnknownServerRequest`] for a stale token, or another
    /// safe RPC failure if admission or transport fails.
    pub async fn respond_request_error(
        &self,
        request: &mut ServerRequest,
        code: i64,
        message: &str,
    ) -> Result<(), RpcError> {
        if request.epoch != self.epoch {
            return Err(RpcError::UnknownServerRequest);
        }
        if !request.lease.is_armed() {
            return Err(RpcError::UnknownServerRequest);
        }
        self.respond_error_id(request, code, message).await
    }

    /// Fails this epoch when an upper layer cannot safely answer a server request.
    ///
    /// # Errors
    ///
    /// Returns [`RpcError::UnknownServerRequest`] for another epoch, or a
    /// connection error when cleanup can no longer be queued safely.
    pub fn abandon_request(&self, request: &mut ServerRequest) -> Result<(), RpcError> {
        if request.epoch != self.epoch {
            return Err(RpcError::UnknownServerRequest);
        }
        if !request.lease.is_armed() {
            return Err(RpcError::UnknownServerRequest);
        }
        request.lease.abandon_if_armed();
        Ok(())
    }

    async fn send_fire(
        &self,
        high: bool,
        method: &'static str,
        message: OutboundMessage,
        server_request: Option<ServerResponseLease>,
        budget: OwnedSemaphorePermit,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        self.ensure_open()?;
        let (ack_tx, ack_rx) = oneshot::channel();
        let server_lease = server_request
            .as_ref()
            .map(|response| Arc::clone(&response.lease));
        if let Some(lease) = &server_lease {
            // Arm actor ownership before publishing the command.  The command's
            // lease then fail-closes on every queue/actor/pump drop path, while
            // serialization failures above remain retryable.
            lease.handoff_to_actor();
        }
        let command = RpcCommand::Fire {
            method,
            message,
            server_request,
            deadline,
            ack: ack_tx,
            _budget: budget,
        };
        self.enqueue(high, command, method, deadline).await?;
        tokio::select! {
            biased;
            () = self.cancellation.cancelled() => Err(RpcError::ConnectionLost(self.epoch)),
            () = tokio::time::sleep_until(deadline) => Err(RpcError::Timeout { method }),
            result = ack_rx => result.unwrap_or(Err(RpcError::ConnectionLost(self.epoch))),
        }
    }

    async fn enqueue(
        &self,
        high: bool,
        command: RpcCommand,
        method: &'static str,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        self.ensure_open()?;
        let sender = if high { &self.high_tx } else { &self.normal_tx };
        tokio::select! {
            biased;
            () = self.cancellation.cancelled() => Err(RpcError::ConnectionLost(self.epoch)),
            () = tokio::time::sleep_until(deadline) => Err(RpcError::Timeout { method }),
            result = sender.send(command) => result.map_err(|_| RpcError::ConnectionLost(self.epoch)),
        }
    }

    async fn serialize_bounded<T>(
        &self,
        command_budget: &Arc<Semaphore>,
        method: &'static str,
        value: &T,
        deadline: Instant,
    ) -> Result<(Value, OwnedSemaphorePermit), RpcError>
    where
        T: Serialize + ?Sized,
    {
        self.ensure_open()?;
        let wire_size = count_serialized(method, value)?;
        let mut budget = self
            .acquire_until(
                Arc::clone(command_budget),
                bounded_permits(MAX_JSONL_LINE_BYTES),
                method,
                deadline,
            )
            .await?;
        let value = serde_json::to_value(value).map_err(|_| RpcError::Serialize { method })?;
        if Instant::now() >= deadline {
            return Err(RpcError::Timeout { method });
        }
        let retained = value_memory_weight(&value)
            .saturating_add(method.len())
            .saturating_add(128)
            .max(wire_size.saturating_add(method.len()).saturating_add(128));
        if retained > MAX_JSONL_LINE_BYTES {
            return Err(RpcError::PayloadTooLarge { method });
        }
        let excess = budget.num_permits().saturating_sub(retained.max(1));
        drop(budget.split(excess));
        Ok((value, budget))
    }

    async fn acquire_until(
        &self,
        semaphore: Arc<Semaphore>,
        permits: u32,
        method: &'static str,
        deadline: Instant,
    ) -> Result<OwnedSemaphorePermit, RpcError> {
        tokio::select! {
            biased;
            () = self.cancellation.cancelled() => Err(RpcError::ConnectionLost(self.epoch)),
            () = tokio::time::sleep_until(deadline) => Err(RpcError::Timeout { method }),
            permit = semaphore.acquire_many_owned(permits) => {
                permit.map_err(|_| RpcError::ConnectionLost(self.epoch))
            }
        }
    }

    fn next_request_id(&self) -> Result<RequestId, RpcError> {
        let sequence = self
            .next_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                current.checked_add(1)
            })
            .map_err(|_| RpcError::RequestIdExhausted)?;
        Ok(RequestId::String(format!(
            "c:{}:{sequence}",
            self.epoch.get()
        )))
    }

    fn ensure_open(&self) -> Result<(), RpcError> {
        if self.state.load(Ordering::Acquire) == STATE_OPEN && !self.cancellation.is_cancelled() {
            Ok(())
        } else {
            Err(RpcError::ConnectionLost(self.epoch))
        }
    }

    #[must_use]
    pub const fn epoch(&self) -> ConnectionEpoch {
        self.epoch
    }

    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.pending_count.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn protocol_drift_count(&self) -> u64 {
        self.protocol_drift_count.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn dropped_notification_count(&self) -> u64 {
        self.dropped_notification_count.load(Ordering::Acquire)
    }
}

struct RequestCancelGuard {
    id: RequestId,
    tx: mpsc::Sender<RequestId>,
    active: bool,
}

impl Drop for RequestCancelGuard {
    fn drop(&mut self) {
        if self.active {
            let _ = self.tx.try_send(self.id.clone());
        }
    }
}

pub struct RpcEventReceiver {
    normal_rx: mpsc::Receiver<InternalRpcEvent>,
    reliable_rx: mpsc::Receiver<InternalRpcEvent>,
    terminal_rx: Option<oneshot::Receiver<RpcEvent>>,
    normal_closed: bool,
    reliable_closed: bool,
    next_sequence: u64,
    buffered: BTreeMap<u64, InternalRpcEvent>,
}

impl RpcEventReceiver {
    pub async fn recv(&mut self) -> Option<RpcEvent> {
        loop {
            if let Some(event) = self.buffered.remove(&self.next_sequence) {
                self.next_sequence = self.next_sequence.saturating_add(1);
                return Some(event.event);
            }
            if self.normal_closed && self.reliable_closed {
                return self.terminal_rx.take()?.await.ok();
            }
            tokio::select! {
                event = self.normal_rx.recv(), if !self.normal_closed => match event {
                    Some(event) => {
                        self.buffered.insert(event.sequence, event);
                    }
                    None => self.normal_closed = true,
                },
                event = self.reliable_rx.recv(), if !self.reliable_closed => match event {
                    Some(event) => {
                        self.buffered.insert(event.sequence, event);
                    }
                    None => self.reliable_closed = true,
                },
            }
        }
    }
}

pub struct RpcConnection {
    pub handle: RpcHandle,
    pub events: RpcEventReceiver,
    cancellation: CancellationToken,
    actor: Option<JoinHandle<TransportExit>>,
    exit: Option<TransportExit>,
}

impl RpcConnection {
    pub async fn shutdown(&mut self) -> TransportExit {
        if let Some(exit) = self.exit {
            return exit;
        }
        self.cancellation.cancel();
        let exit = match self.actor.take() {
            Some(actor) => actor.await.unwrap_or(TransportExit::TaskFailed),
            None => TransportExit::TaskFailed,
        };
        self.exit = Some(exit);
        exit
    }

    /// Abruptly drops this connection epoch without waiting for an orderly transport close. This
    /// is used for crash-path verification; it never reaches through the transport to a server
    /// process.
    pub fn abort(&mut self) -> TransportExit {
        if let Some(exit) = self.exit {
            return exit;
        }
        if let Some(actor) = self.actor.take() {
            actor.abort();
        }
        self.cancellation.cancel();
        self.exit = Some(TransportExit::Aborted);
        TransportExit::Aborted
    }
}

impl Drop for RpcConnection {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

/// Starts the sole RPC owner for one bounded transport.
#[must_use]
pub fn spawn_rpc(
    transport: TransportHandle,
    epoch: ConnectionEpoch,
    parent_cancellation: CancellationToken,
) -> RpcConnection {
    spawn_rpc_with_policy(
        transport,
        epoch,
        parent_cancellation,
        RpcProtocolPolicy::Permissive,
    )
}

/// Inbound protocol policy selected by the connection owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RpcProtocolPolicy {
    /// Preserve the existing spawned-stdio behavior: surface unknown traffic as protocol drift.
    Permissive,
    /// External observe-only connections fault on stale/duplicate responses, server requests,
    /// notifications, and malformed transport records.
    FailClosedExternalObserve,
    /// External resume/reconciliation connections accept only the promoted thread status and
    /// terminal item/turn notifications. Reverse requests and every other message still fault.
    FailClosedExternalResume,
    /// External mutation connections additionally admit the three exact approval reverse-request
    /// methods and their durable resolution notification.
    FailClosedExternalMutate,
    /// External queue connections add the reviewed queue-change notification to the mutation
    /// surface.
    FailClosedExternalQueue,
}

impl RpcProtocolPolicy {
    const fn is_fail_closed_external(self) -> bool {
        matches!(
            self,
            Self::FailClosedExternalObserve
                | Self::FailClosedExternalResume
                | Self::FailClosedExternalMutate
                | Self::FailClosedExternalQueue
        )
    }

    fn allows_notification(self, method: &str) -> bool {
        match self {
            Self::Permissive => true,
            Self::FailClosedExternalObserve => false,
            Self::FailClosedExternalResume => matches!(
                method,
                "remoteControl/status/changed"
                    | "thread/status/changed"
                    | "thread/goal/cleared"
                    | "item/completed"
                    | "turn/completed"
            ),
            Self::FailClosedExternalMutate => matches!(
                method,
                "account/rateLimits/updated"
                    | "remoteControl/status/changed"
                    | "thread/status/changed"
                    | "thread/goal/cleared"
                    | "thread/settings/updated"
                    | "turn/started"
                    | "item/started"
                    | "item/agentMessage/delta"
                    | "item/commandExecution/outputDelta"
                    | "item/completed"
                    | "thread/tokenUsage/updated"
                    | "error"
                    | "turn/completed"
                    | "serverRequest/resolved"
            ),
            Self::FailClosedExternalQueue => matches!(
                method,
                "account/rateLimits/updated"
                    | "remoteControl/status/changed"
                    | "thread/status/changed"
                    | "thread/goal/cleared"
                    | "thread/settings/updated"
                    | "turn/started"
                    | "item/started"
                    | "item/agentMessage/delta"
                    | "item/commandExecution/outputDelta"
                    | "item/completed"
                    | "thread/tokenUsage/updated"
                    | "error"
                    | "turn/completed"
                    | "thread/queue/changed"
                    | "serverRequest/resolved"
            ),
        }
    }

    fn allows_server_request(self, method: &str) -> bool {
        match self {
            Self::Permissive => true,
            Self::FailClosedExternalObserve | Self::FailClosedExternalResume => false,
            Self::FailClosedExternalMutate | Self::FailClosedExternalQueue => matches!(
                method,
                "item/commandExecution/requestApproval"
                    | "item/fileChange/requestApproval"
                    | "item/permissions/requestApproval"
            ),
        }
    }
}

/// Starts the sole RPC owner with an explicit inbound protocol policy.
#[must_use]
pub fn spawn_rpc_with_policy(
    transport: TransportHandle,
    epoch: ConnectionEpoch,
    parent_cancellation: CancellationToken,
    policy: RpcProtocolPolicy,
) -> RpcConnection {
    let cancellation = parent_cancellation.child_token();
    drop(parent_cancellation);
    let (high_tx, high_rx) = mpsc::channel(RPC_HIGH_CAPACITY);
    let (normal_tx, normal_rx) = mpsc::channel(RPC_NORMAL_CAPACITY);
    let (cancel_tx, cancel_rx) = mpsc::channel(RPC_TOTAL_PENDING_CAPACITY);
    let (event_tx, event_rx) = mpsc::channel(EVENT_CAPACITY);
    let (reliable_event_tx, reliable_event_rx) = mpsc::channel(RPC_RELIABLE_EVENT_CAPACITY);
    let (terminal_tx, terminal_rx) = oneshot::channel();
    let state = Arc::new(AtomicU8::new(STATE_OPEN));
    let pending_count = Arc::new(AtomicUsize::new(0));
    let protocol_drift_count = Arc::new(AtomicU64::new(0));
    let dropped_notification_count = Arc::new(AtomicU64::new(0));
    let event_budget = Arc::new(Semaphore::new(RPC_BYTE_BUDGET));
    let reliable_event_budget = Arc::new(Semaphore::new(RPC_RELIABLE_EVENT_BYTE_BUDGET));

    let handle = RpcHandle {
        high_tx,
        normal_tx,
        cancel_tx,
        epoch,
        state: Arc::clone(&state),
        initialize_state: Arc::new(AtomicU8::new(INIT_NEW)),
        next_id: Arc::new(AtomicU64::new(0)),
        high_inflight: Arc::new(Semaphore::new(RPC_HIGH_CAPACITY)),
        normal_inflight: Arc::new(Semaphore::new(RPC_INFLIGHT_CAPACITY)),
        high_command_budget: Arc::new(Semaphore::new(RPC_HIGH_BYTE_BUDGET)),
        normal_command_budget: Arc::new(Semaphore::new(RPC_BYTE_BUDGET)),
        pending_count: Arc::clone(&pending_count),
        protocol_drift_count: Arc::clone(&protocol_drift_count),
        dropped_notification_count: Arc::clone(&dropped_notification_count),
        cancellation: cancellation.clone(),
    };
    let actor_cancel = cancellation.clone();
    let actor = tokio::spawn(async move {
        run_actor(
            transport,
            high_rx,
            normal_rx,
            cancel_rx,
            event_tx,
            reliable_event_tx,
            terminal_tx,
            event_budget,
            reliable_event_budget,
            epoch,
            state,
            pending_count,
            protocol_drift_count,
            dropped_notification_count,
            policy,
            actor_cancel,
        )
        .await
    });

    RpcConnection {
        handle,
        events: RpcEventReceiver {
            normal_rx: event_rx,
            reliable_rx: reliable_event_rx,
            terminal_rx: Some(terminal_rx),
            normal_closed: false,
            reliable_closed: false,
            next_sequence: 0,
            buffered: BTreeMap::new(),
        },
        cancellation,
        actor: Some(actor),
        exit: None,
    }
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
async fn run_actor(
    mut transport: TransportHandle,
    mut high_rx: mpsc::Receiver<RpcCommand>,
    mut normal_rx: mpsc::Receiver<RpcCommand>,
    mut cancel_rx: mpsc::Receiver<RequestId>,
    event_tx: mpsc::Sender<InternalRpcEvent>,
    reliable_event_tx: mpsc::Sender<InternalRpcEvent>,
    terminal_tx: oneshot::Sender<RpcEvent>,
    event_budget: Arc<Semaphore>,
    reliable_event_budget: Arc<Semaphore>,
    epoch: ConnectionEpoch,
    state: Arc<AtomicU8>,
    pending_count: Arc<AtomicUsize>,
    protocol_drift_count: Arc<AtomicU64>,
    dropped_notification_count: Arc<AtomicU64>,
    policy: RpcProtocolPolicy,
    cancellation: CancellationToken,
) -> TransportExit {
    let (pump_high_tx, pump_high_rx) = mpsc::channel(RPC_HIGH_CAPACITY);
    let (pump_normal_tx, pump_normal_rx) = mpsc::channel(RPC_NORMAL_CAPACITY);
    let (completion_tx, mut completion_rx) = mpsc::channel(RPC_INFLIGHT_CAPACITY);
    let high_pump = tokio::spawn(run_sender_pump(
        transport.high_tx.clone(),
        pump_high_rx,
        completion_tx.clone(),
        epoch,
        cancellation.clone(),
    ));
    let normal_pump = tokio::spawn(run_sender_pump(
        transport.normal_tx.clone(),
        pump_normal_rx,
        completion_tx,
        epoch,
        cancellation.clone(),
    ));
    let mut pending = HashMap::<RequestId, PendingRequest>::new();
    let mut server_pending = HashSet::<RequestId>::new();
    let mut high_burst = 0_usize;
    let mut event_sequence = 0_u64;
    let exit = loop {
        let allow_high = pump_high_tx.capacity() > 0;
        let allow_normal = pump_normal_tx.capacity() > 0;
        let prefer_normal = high_burst >= HIGH_PRIORITY_BURST;
        let next_deadline = pending.values().map(|entry| entry.deadline).min();
        tokio::select! {
            () = cancellation.cancelled() => break TransportExit::Cancelled,
            event = transport.events.recv() => {
                match event {
                    Some(TransportEvent::Message(message)) => {
                        let (message, budget) = message.into_parts();
                        if !handle_inbound(
                            message,
                            budget,
                            &mut pending,
                            &mut server_pending,
                            &event_tx,
                            &event_budget,
                            &reliable_event_tx,
                            &reliable_event_budget,
                            &cancellation,
                            epoch,
                            &mut event_sequence,
                            &pending_count,
                            &protocol_drift_count,
                            &dropped_notification_count,
                            policy,
                        ).await {
                            break if cancellation.is_cancelled() {
                                TransportExit::Cancelled
                            } else if policy.is_fail_closed_external() {
                                TransportExit::ProtocolViolation
                            } else {
                                TransportExit::TaskFailed
                            };
                        }
                    }
                    Some(TransportEvent::ProtocolError(_)) => {
                        if policy.is_fail_closed_external() {
                            break TransportExit::ProtocolViolation;
                        }
                        increment_saturating(&protocol_drift_count);
                        if !try_emit_small_event(
                            RpcEvent::ProtocolDrift,
                            &event_tx,
                            &event_budget,
                            &mut event_sequence,
                        ) {
                            break TransportExit::TaskFailed;
                        }
                    }
                    Some(TransportEvent::ReadError(error)) => break TransportExit::ReadError(error.kind),
                    Some(TransportEvent::WriteError(error)) => break TransportExit::WriteError(error.kind),
                    Some(TransportEvent::StdoutEof) => break TransportExit::StdoutEof,
                    Some(TransportEvent::WebSocketClosed(report)) => {
                        break TransportExit::WebSocketClosed(report);
                    }
                    Some(TransportEvent::ConnectionError) => break TransportExit::ConnectionFailed,
                    Some(TransportEvent::Cancelled) => break TransportExit::Cancelled,
                    Some(TransportEvent::StderrLine { .. }) => {}
                    None => break transport.shutdown().await,
                }
            }
            Some(completion) = completion_rx.recv() => {
                handle_completion(completion, &mut pending, &pending_count);
            }
            Some(id) = cancel_rx.recv() => {
                remove_pending(&mut pending, &id, &pending_count);
            }
            () = wait_for_deadline(next_deadline) => {
                sweep_pending(&mut pending, &pending_count);
            }
            command = receive_command(
                &mut high_rx,
                &mut normal_rx,
                allow_high,
                allow_normal,
                prefer_normal,
            ) => {
                if let Some((high, command)) = command {
                    if high {
                        high_burst = high_burst.saturating_add(1);
                    } else {
                        high_burst = 0;
                    }
                    let pump_tx = if high { &pump_high_tx } else { &pump_normal_tx };
                    if !dispatch_command(
                        command,
                        pump_tx,
                        &mut pending,
                        &mut server_pending,
                        &pending_count,
                        epoch,
                    ) {
                        break TransportExit::TaskFailed;
                    }
                }
            }
        }
    };

    state.store(STATE_LOST, Ordering::Release);
    cancellation.cancel();
    completion_rx.close();
    drop(completion_rx);
    fail_all_pending(&mut pending, epoch, &pending_count);
    high_rx.close();
    normal_rx.close();
    reject_queued(&mut high_rx, epoch);
    reject_queued(&mut normal_rx, epoch);
    drop(pump_high_tx);
    drop(pump_normal_tx);
    let _ = high_pump.await;
    let _ = normal_pump.await;
    let transport_exit = transport.shutdown().await;
    let final_exit =
        if exit == TransportExit::Cancelled && transport_exit != TransportExit::Cancelled {
            transport_exit
        } else {
            exit
        };
    let _ = terminal_tx.send(RpcEvent::TransportClosed(final_exit));
    final_exit
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
async fn handle_inbound(
    message: InboundMessage,
    transport_budget: OwnedSemaphorePermit,
    pending: &mut HashMap<RequestId, PendingRequest>,
    server_pending: &mut HashSet<RequestId>,
    event_tx: &mpsc::Sender<InternalRpcEvent>,
    event_budget: &Arc<Semaphore>,
    reliable_event_tx: &mpsc::Sender<InternalRpcEvent>,
    reliable_event_budget: &Arc<Semaphore>,
    cancellation: &CancellationToken,
    epoch: ConnectionEpoch,
    event_sequence: &mut u64,
    pending_count: &AtomicUsize,
    protocol_drift_count: &AtomicU64,
    dropped_notification_count: &AtomicU64,
    policy: RpcProtocolPolicy,
) -> bool {
    match message {
        InboundMessage::Response { id, result } => {
            if let Some(entry) = take_pending(pending, &id, pending_count) {
                let _ = entry.reply.send(Ok(BudgetedResponse {
                    value: result,
                    transport_budget,
                }));
            } else {
                drop(transport_budget);
                if policy.is_fail_closed_external() {
                    return false;
                }
                increment_saturating(protocol_drift_count);
                return try_emit_small_event(
                    RpcEvent::ProtocolDrift,
                    event_tx,
                    event_budget,
                    event_sequence,
                );
            }
        }
        InboundMessage::ErrorResponse { id, error } => {
            drop(transport_budget);
            if let Some(entry) = take_pending(pending, &id, pending_count) {
                let _ = entry.reply.send(Err(RpcError::Server {
                    method: entry.method,
                    code: error.code,
                }));
            } else {
                if policy.is_fail_closed_external() {
                    return false;
                }
                increment_saturating(protocol_drift_count);
                return try_emit_small_event(
                    RpcEvent::ProtocolDrift,
                    event_tx,
                    event_budget,
                    event_sequence,
                );
            }
        }
        InboundMessage::Request { id, method, params } => {
            if !policy.allows_server_request(&method) {
                return false;
            }
            if server_pending.len() >= RPC_SERVER_REQUEST_CAPACITY
                || !server_pending.insert(id.clone())
            {
                return false;
            }
            return emit_reliable_event(
                RpcEvent::ServerRequest(ServerRequest {
                    id,
                    method,
                    params,
                    epoch,
                    retention: None,
                    transport_retention: Some(transport_budget),
                    lease: Arc::new(ServerRequestLease::new(cancellation.clone())),
                }),
                None,
                Arc::clone(reliable_event_budget),
                reliable_event_tx,
                event_sequence,
                cancellation,
            )
            .await;
        }
        InboundMessage::Notification { method, params } => {
            if !policy.allows_notification(&method) {
                return false;
            }
            let authoritative = is_authoritative_notification(&method);
            let event = RpcEvent::Notification { method, params };
            let emitted = if authoritative {
                emit_reliable_event(
                    event,
                    Some(transport_budget),
                    Arc::clone(reliable_event_budget),
                    reliable_event_tx,
                    event_sequence,
                    cancellation,
                )
                .await
            } else {
                try_emit_event(
                    event,
                    Some(transport_budget),
                    Arc::clone(event_budget),
                    event_tx,
                    event_sequence,
                )
            };
            if emitted {
                return true;
            }
            if event_tx.is_closed() || authoritative {
                return false;
            }
            increment_saturating(dropped_notification_count);
            return true;
        }
    }
    true
}

fn dispatch_command(
    command: RpcCommand,
    pump_tx: &mpsc::Sender<OutboundJob>,
    pending: &mut HashMap<RequestId, PendingRequest>,
    server_pending: &mut HashSet<RequestId>,
    pending_count: &AtomicUsize,
    epoch: ConnectionEpoch,
) -> bool {
    match command {
        RpcCommand::Request {
            id,
            method,
            params,
            deadline,
            reply,
            _inflight: inflight_permit,
            _budget: budget_permit,
        } => {
            if reply.is_closed() {
                return true;
            }
            if Instant::now() >= deadline {
                let _ = reply.send(Err(RpcError::Timeout { method }));
                return true;
            }
            let entry = PendingRequest {
                method,
                deadline,
                reply,
                _inflight: inflight_permit,
            };
            if pending.insert(id.clone(), entry).is_some() {
                return false;
            }
            pending_count.fetch_add(1, Ordering::AcqRel);
            let job = OutboundJob {
                method,
                message: OutboundMessage::Request {
                    id: id.clone(),
                    method: method.to_owned(),
                    params: Some(params),
                },
                deadline,
                completion: JobCompletion::Request(id.clone()),
                server_response: None,
                _budget: budget_permit,
            };
            if let Err(error) = pump_tx.try_send(job) {
                remove_pending(pending, &id, pending_count);
                return !matches!(error, mpsc::error::TrySendError::Closed(_));
            }
        }
        RpcCommand::Fire {
            method,
            message,
            server_request,
            deadline,
            ack,
            _budget: budget_permit,
        } => {
            if ack.is_closed() && server_request.is_none() {
                return true;
            }
            if Instant::now() >= deadline {
                let _ = ack.send(Err(RpcError::Timeout { method }));
                return true;
            }
            if let Some(response) = &server_request {
                if !server_pending.remove(&response.id) {
                    let _ = ack.send(Err(RpcError::UnknownServerRequest));
                    return true;
                }
            }
            let job = OutboundJob {
                method,
                message,
                deadline,
                completion: JobCompletion::Fire(ack),
                server_response: server_request,
                _budget: budget_permit,
            };
            if let Err(error) = pump_tx.try_send(job) {
                let job = match error {
                    mpsc::error::TrySendError::Full(job)
                    | mpsc::error::TrySendError::Closed(job) => job,
                };
                if let JobCompletion::Fire(ack) = job.completion {
                    let _ = ack.send(Err(RpcError::ConnectionLost(epoch)));
                }
                return false;
            }
        }
    }
    true
}

async fn run_sender_pump(
    sender: TransportSender,
    mut rx: mpsc::Receiver<OutboundJob>,
    completion_tx: mpsc::Sender<OutboundCompletion>,
    epoch: ConnectionEpoch,
    cancellation: CancellationToken,
) {
    loop {
        let job = tokio::select! {
            biased;
            () = cancellation.cancelled() => return,
            job = rx.recv() => job,
        };
        let Some(job) = job else {
            return;
        };
        let OutboundJob {
            method,
            message,
            deadline,
            completion,
            server_response,
            _budget,
        } = job;
        let result = tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(RpcError::ConnectionLost(epoch)),
            () = tokio::time::sleep_until(deadline) => Err(RpcError::Timeout { method }),
            result = async {
                if server_response.is_some() {
                    sender.send_confirmed(message).await
                } else {
                    sender.send(message).await
                }
            } => result.map_err(|error| map_send_error(&error, method, epoch)),
        };
        if let Some(response) = &server_response {
            if result.is_ok() {
                response.resolve();
            } else {
                response.fail_closed();
            }
        }
        match completion {
            JobCompletion::Request(id) => {
                let sent = tokio::select! {
                    biased;
                    () = cancellation.cancelled() => false,
                    result = completion_tx.send(OutboundCompletion { id, result }) => {
                        result.is_ok()
                    }
                };
                if !sent {
                    return;
                }
            }
            JobCompletion::Fire(ack) => {
                let _ = ack.send(result);
            }
        }
    }
}

fn map_send_error(
    error: &TransportSendError,
    method: &'static str,
    epoch: ConnectionEpoch,
) -> RpcError {
    match error {
        TransportSendError::Protocol(_) => RpcError::PayloadTooLarge { method },
        TransportSendError::Closed | TransportSendError::Cancelled | TransportSendError::Io(_) => {
            RpcError::ConnectionLost(epoch)
        }
    }
}

fn handle_completion(
    completion: OutboundCompletion,
    pending: &mut HashMap<RequestId, PendingRequest>,
    pending_count: &AtomicUsize,
) {
    if let Err(error) = completion.result {
        if let Some(entry) = take_pending(pending, &completion.id, pending_count) {
            let _ = entry.reply.send(Err(error));
        }
    }
}

fn sweep_pending(pending: &mut HashMap<RequestId, PendingRequest>, pending_count: &AtomicUsize) {
    let now = Instant::now();
    let expired = pending
        .iter()
        .filter(|(_, entry)| entry.reply.is_closed() || now >= entry.deadline)
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    for id in expired {
        if let Some(entry) = take_pending(pending, &id, pending_count) {
            if !entry.reply.is_closed() {
                let _ = entry.reply.send(Err(RpcError::Timeout {
                    method: entry.method,
                }));
            }
        }
    }
}

async fn receive_command(
    high_rx: &mut mpsc::Receiver<RpcCommand>,
    normal_rx: &mut mpsc::Receiver<RpcCommand>,
    allow_high: bool,
    allow_normal: bool,
    prefer_normal: bool,
) -> Option<(bool, RpcCommand)> {
    if !allow_high && !allow_normal {
        return std::future::pending().await;
    }
    if prefer_normal {
        tokio::select! {
            biased;
            command = normal_rx.recv(), if allow_normal => command.map(|command| (false, command)),
            command = high_rx.recv(), if allow_high => command.map(|command| (true, command)),
            else => std::future::pending().await,
        }
    } else {
        tokio::select! {
            biased;
            command = high_rx.recv(), if allow_high => command.map(|command| (true, command)),
            command = normal_rx.recv(), if allow_normal => command.map(|command| (false, command)),
            else => std::future::pending().await,
        }
    }
}

async fn wait_for_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

fn take_pending(
    pending: &mut HashMap<RequestId, PendingRequest>,
    id: &RequestId,
    pending_count: &AtomicUsize,
) -> Option<PendingRequest> {
    let entry = pending.remove(id)?;
    pending_count.fetch_sub(1, Ordering::AcqRel);
    Some(entry)
}

fn remove_pending(
    pending: &mut HashMap<RequestId, PendingRequest>,
    id: &RequestId,
    pending_count: &AtomicUsize,
) {
    drop(take_pending(pending, id, pending_count));
}

fn fail_all_pending(
    pending: &mut HashMap<RequestId, PendingRequest>,
    epoch: ConnectionEpoch,
    pending_count: &AtomicUsize,
) {
    for (_, entry) in pending.drain() {
        let _ = entry.reply.send(Err(RpcError::ConnectionLost(epoch)));
    }
    pending_count.store(0, Ordering::Release);
}

fn reject_queued(rx: &mut mpsc::Receiver<RpcCommand>, epoch: ConnectionEpoch) {
    while let Ok(command) = rx.try_recv() {
        match command {
            RpcCommand::Request { reply, .. } => {
                let _ = reply.send(Err(RpcError::ConnectionLost(epoch)));
            }
            RpcCommand::Fire { ack, .. } => {
                let _ = ack.send(Err(RpcError::ConnectionLost(epoch)));
            }
        }
    }
}

fn try_emit_event(
    event: RpcEvent,
    transport_budget: Option<OwnedSemaphorePermit>,
    event_budget: Arc<Semaphore>,
    event_tx: &mpsc::Sender<InternalRpcEvent>,
    event_sequence: &mut u64,
) -> bool {
    let weight = rpc_event_weight(&event);
    if weight > RPC_BYTE_BUDGET {
        return false;
    }
    let permits = bounded_permits(weight);
    let Ok(budget) = event_budget.try_acquire_many_owned(permits) else {
        return false;
    };
    let Ok(slot) = event_tx.try_reserve() else {
        return false;
    };
    let Some(sequence) = take_event_sequence(event_sequence) else {
        return false;
    };
    slot.send(InternalRpcEvent {
        sequence,
        event,
        _budget: budget,
        _transport_budget: transport_budget,
    });
    true
}

async fn emit_reliable_event(
    event: RpcEvent,
    transport_budget: Option<OwnedSemaphorePermit>,
    event_budget: Arc<Semaphore>,
    event_tx: &mpsc::Sender<InternalRpcEvent>,
    event_sequence: &mut u64,
    cancellation: &CancellationToken,
) -> bool {
    let weight = rpc_event_weight(&event);
    if weight > RPC_RELIABLE_EVENT_BYTE_BUDGET {
        return false;
    }
    let budget = tokio::select! {
        biased;
        () = cancellation.cancelled() => return false,
        permit = event_budget.acquire_many_owned(bounded_reliable_permits(weight)) => {
            let Ok(permit) = permit else { return false; };
            permit
        }
    };
    let slot = tokio::select! {
        biased;
        () = cancellation.cancelled() => return false,
        permit = event_tx.reserve() => {
            let Ok(permit) = permit else { return false; };
            permit
        }
    };
    let Some(sequence) = take_event_sequence(event_sequence) else {
        return false;
    };
    slot.send(InternalRpcEvent {
        sequence,
        event,
        _budget: budget,
        _transport_budget: transport_budget,
    });
    true
}

fn try_emit_small_event(
    event: RpcEvent,
    event_tx: &mpsc::Sender<InternalRpcEvent>,
    event_budget: &Arc<Semaphore>,
    event_sequence: &mut u64,
) -> bool {
    let weight = rpc_event_weight(&event);
    let Ok(budget) = Arc::clone(event_budget).try_acquire_many_owned(bounded_permits(weight))
    else {
        return false;
    };
    let Ok(slot) = event_tx.try_reserve() else {
        return false;
    };
    let Some(sequence) = take_event_sequence(event_sequence) else {
        return false;
    };
    slot.send(InternalRpcEvent {
        sequence,
        event,
        _budget: budget,
        _transport_budget: None,
    });
    true
}

fn take_event_sequence(next: &mut u64) -> Option<u64> {
    let sequence = *next;
    *next = next.checked_add(1)?;
    Some(sequence)
}

fn rpc_event_weight(event: &RpcEvent) -> usize {
    match event {
        RpcEvent::Notification { method, params } => method
            .len()
            .saturating_add(params.as_ref().map_or(0, value_memory_weight))
            .saturating_add(128),
        RpcEvent::ServerRequest(request) => request
            .method
            .len()
            .saturating_add(request_id_memory_weight(&request.id))
            .saturating_add(request.params.as_ref().map_or(0, value_memory_weight))
            .saturating_add(192),
        RpcEvent::TransportClosed(_) | RpcEvent::ProtocolDrift => 64,
    }
}

fn bounded_permits(bytes: usize) -> u32 {
    u32::try_from(bytes.clamp(1, RPC_BYTE_BUDGET))
        .expect("RPC byte budget is bounded below u32::MAX")
}

fn bounded_reliable_permits(bytes: usize) -> u32 {
    u32::try_from(bytes.clamp(1, RPC_RELIABLE_EVENT_BYTE_BUDGET))
        .expect("reliable RPC event budget is bounded below u32::MAX")
}

fn count_serialized<T>(method: &'static str, value: &T) -> Result<usize, RpcError>
where
    T: Serialize + ?Sized,
{
    let maximum = MAX_OUTBOUND_VALUE_WIRE_BYTES.saturating_sub(method.len().saturating_add(256));
    let mut counter = LimitedCounter::new(maximum);
    match serde_json::to_writer(&mut counter, value) {
        Ok(()) => Ok(counter.written),
        Err(_) if counter.exceeded => Err(RpcError::PayloadTooLarge { method }),
        Err(_) => Err(RpcError::Serialize { method }),
    }
}

struct LimitedCounter {
    written: usize,
    maximum: usize,
    exceeded: bool,
}

impl LimitedCounter {
    const fn new(maximum: usize) -> Self {
        Self {
            written: 0,
            maximum,
            exceeded: false,
        }
    }
}

impl io::Write for LimitedCounter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let Some(total) = self.written.checked_add(buffer.len()) else {
            self.exceeded = true;
            return Err(io::Error::other("serialized RPC payload exceeds limit"));
        };
        if total > self.maximum {
            self.exceeded = true;
            return Err(io::Error::other("serialized RPC payload exceeds limit"));
        }
        self.written = total;
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn is_authoritative_notification(method: &str) -> bool {
    matches!(
        method,
        "thread/status/changed"
            | "thread/settings/updated"
            | "thread/queue/changed"
            | "serverRequest/resolved"
            | "item/completed"
            | "turn/completed"
            | "error"
    )
}

fn deadline_after(timeout: Duration) -> Instant {
    Instant::now()
        .checked_add(timeout)
        .unwrap_or_else(Instant::now)
}

fn increment_saturating(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
        Some(value.saturating_add(1))
    });
}

fn request_id_kind(id: &RequestId) -> &'static str {
    match id {
        RequestId::String(_) => "string",
        RequestId::Integer(_) => "integer",
    }
}

/// Performs the stable initialize/initialized handshake exactly once per epoch.
///
/// # Errors
///
/// Returns [`RpcError::AlreadyInitialized`] after any prior attempt on the same
/// epoch, or another safe RPC failure if the handshake cannot complete.
pub async fn initialize_connection(handle: &RpcHandle) -> Result<InitializeResult, RpcError> {
    initialize_connection_for_wire(handle, WireAdapter::V0_146_0).await
}

/// Performs the initialize handshake using an explicitly selected wire contract.
///
/// # Errors
///
/// Returns the same redacted failures as [`initialize_connection`].
pub async fn initialize_connection_for_wire(
    handle: &RpcHandle,
    wire: WireAdapter,
) -> Result<InitializeResult, RpcError> {
    initialize_connection_with_capabilities(handle, wire, false).await
}

/// Performs the initialize handshake while opting into app-server dynamic
/// tools for the long-running bridge runtime.
///
/// The opt-in is kept separate from [`initialize_connection`] so probes and
/// protocol tests can continue exercising the stable handshake.
///
/// # Errors
///
/// Returns the same failures as [`initialize_connection`].
pub async fn initialize_connection_with_dynamic_tools(
    handle: &RpcHandle,
    wire: WireAdapter,
) -> Result<InitializeResult, RpcError> {
    initialize_connection_with_capabilities(handle, wire, true).await
}

async fn initialize_connection_with_capabilities(
    handle: &RpcHandle,
    wire: WireAdapter,
    dynamic_tools: bool,
) -> Result<InitializeResult, RpcError> {
    handle
        .initialize_state
        .compare_exchange(INIT_NEW, INIT_RUNNING, Ordering::AcqRel, Ordering::Acquire)
        .map_err(|_| RpcError::AlreadyInitialized)?;

    let mut client_info = ClientInfo::new("lark_codex_bridge", env!("CARGO_PKG_VERSION"));
    client_info.title = Some("Lark Codex Bridge".to_owned());
    let mut params = InitializeParams::new(client_info);
    if dynamic_tools {
        params.capabilities = Some(InitializeCapabilities {
            experimental_api: Some(true),
            ..InitializeCapabilities::default()
        });
    }
    let Ok(params) = wire.initialize_params(&params) else {
        handle
            .initialize_state
            .store(INIT_FAILED, Ordering::Release);
        return Err(RpcError::Serialize {
            method: "initialize",
        });
    };
    let result = handle
        .request_high::<_, Value>("initialize", &params, INITIALIZE_TIMEOUT)
        .await
        .and_then(|value| {
            wire.initialize_response(value)
                .map_err(|_| RpcError::Deserialize {
                    method: "initialize",
                })
        });
    let result = match result {
        Ok(result) => result,
        Err(error) => {
            handle
                .initialize_state
                .store(INIT_FAILED, Ordering::Release);
            return Err(error);
        }
    };
    if !result.codex_home.is_absolute() {
        handle
            .initialize_state
            .store(INIT_FAILED, Ordering::Release);
        return Err(RpcError::Deserialize {
            method: "initialize",
        });
    }
    if let Err(error) = handle.notify_empty_params_high("initialized").await {
        handle
            .initialize_state
            .store(INIT_FAILED, Ordering::Release);
        return Err(error);
    }
    handle.initialize_state.store(INIT_READY, Ordering::Release);
    Ok(result)
}

enum RpcCommand {
    Request {
        id: RequestId,
        method: &'static str,
        params: Value,
        deadline: Instant,
        reply: oneshot::Sender<Result<BudgetedResponse<Value>, RpcError>>,
        _inflight: OwnedSemaphorePermit,
        _budget: OwnedSemaphorePermit,
    },
    Fire {
        method: &'static str,
        message: OutboundMessage,
        server_request: Option<ServerResponseLease>,
        deadline: Instant,
        ack: oneshot::Sender<Result<(), RpcError>>,
        _budget: OwnedSemaphorePermit,
    },
}

struct ServerResponseLease {
    id: RequestId,
    lease: Arc<ServerRequestLease>,
}

impl ServerResponseLease {
    fn resolve(&self) {
        self.lease.resolve_actor_owned();
    }

    fn fail_closed(&self) {
        self.lease.fail_actor_owned();
    }
}

impl Drop for ServerResponseLease {
    fn drop(&mut self) {
        self.fail_closed();
    }
}

struct PendingRequest {
    method: &'static str,
    deadline: Instant,
    reply: oneshot::Sender<Result<BudgetedResponse<Value>, RpcError>>,
    _inflight: OwnedSemaphorePermit,
}

struct OutboundJob {
    method: &'static str,
    message: OutboundMessage,
    deadline: Instant,
    completion: JobCompletion,
    server_response: Option<ServerResponseLease>,
    _budget: OwnedSemaphorePermit,
}

enum JobCompletion {
    Request(RequestId),
    Fire(oneshot::Sender<Result<(), RpcError>>),
}

struct OutboundCompletion {
    id: RequestId,
    result: Result<(), RpcError>,
}

struct InternalRpcEvent {
    sequence: u64,
    event: RpcEvent,
    _budget: OwnedSemaphorePermit,
    _transport_budget: Option<OwnedSemaphorePermit>,
}
