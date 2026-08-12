use std::{
    collections::{HashMap, HashSet},
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
        protocol::{InboundMessage, OutboundMessage, RequestId, RpcErrorObject},
        transport::{
            TransportEvent, TransportExit, TransportHandle, TransportSendError, TransportSender,
        },
        types::{ClientInfo, InitializeParams, InitializeResult},
    },
    limits::{
        CONTROL_RPC_TIMEOUT, EVENT_CAPACITY, HIGH_PRIORITY_BURST, INITIALIZE_TIMEOUT,
        MAX_JSONL_LINE_BYTES, RPC_BYTE_BUDGET, RPC_HIGH_CAPACITY, RPC_INFLIGHT_CAPACITY,
        RPC_NORMAL_CAPACITY, RPC_SERVER_REQUEST_CAPACITY,
    },
};

const STATE_OPEN: u8 = 0;
const STATE_LOST: u8 = 1;
const INIT_NEW: u8 = 0;
const INIT_RUNNING: u8 = 1;
const INIT_READY: u8 = 2;
const INIT_FAILED: u8 = 3;

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
    pub id: RequestId,
    pub method: String,
    pub params: Option<Value>,
    epoch: ConnectionEpoch,
}

impl ServerRequest {
    #[must_use]
    pub const fn epoch(&self) -> ConnectionEpoch {
        self.epoch
    }
}

impl fmt::Debug for ServerRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerRequest")
            .field("id_kind", &request_id_kind(&self.id))
            .field("method", &self.method)
            .field("has_params", &self.params.is_some())
            .field("epoch", &self.epoch)
            .finish()
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
    inflight: Arc<Semaphore>,
    command_budget: Arc<Semaphore>,
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
        self.request_with_priority(false, method, params, timeout)
            .await
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
        self.request_with_priority(true, method, params, timeout)
            .await
    }

    async fn request_with_priority<P, R>(
        &self,
        high: bool,
        method: &'static str,
        params: &P,
        timeout: Duration,
    ) -> Result<R, RpcError>
    where
        P: Serialize + ?Sized,
        R: DeserializeOwned,
    {
        self.ensure_open()?;
        let deadline = deadline_after(timeout);
        let inflight = self
            .acquire_until(Arc::clone(&self.inflight), 1, method, deadline)
            .await?;
        let (params, budget) = self.serialize_bounded(method, params, deadline).await?;
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
        let value = response?;
        serde_json::from_value(value).map_err(|_| RpcError::Deserialize { method })
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
        let (params, budget) = self.serialize_bounded(method, params, deadline).await?;
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
                Arc::clone(&self.command_budget),
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
    async fn respond_id<R>(&self, id: RequestId, result: &R) -> Result<(), RpcError>
    where
        R: Serialize + ?Sized,
    {
        let method = "server/respond";
        let deadline = deadline_after(CONTROL_RPC_TIMEOUT);
        let (result, budget) = self.serialize_bounded(method, result, deadline).await?;
        self.send_fire(
            true,
            method,
            OutboundMessage::Response {
                id: id.clone(),
                result,
            },
            Some(id),
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
        request: &ServerRequest,
        result: &R,
    ) -> Result<(), RpcError>
    where
        R: Serialize + ?Sized,
    {
        if request.epoch != self.epoch {
            return Err(RpcError::UnknownServerRequest);
        }
        self.respond_id(request.id.clone(), result).await
    }

    /// Rejects one still-pending app-server request at high priority.
    ///
    /// # Errors
    ///
    /// Returns a safe [`RpcError`] if the request is stale or admission or
    /// transport fails.
    async fn respond_error_id(
        &self,
        id: RequestId,
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
            .acquire_until(Arc::clone(&self.command_budget), permits, method, deadline)
            .await?;
        self.send_fire(
            true,
            method,
            OutboundMessage::ErrorResponse {
                id: id.clone(),
                error: RpcErrorObject {
                    code,
                    message: message.to_owned(),
                    data: None,
                },
            },
            Some(id),
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
        request: &ServerRequest,
        code: i64,
        message: &str,
    ) -> Result<(), RpcError> {
        if request.epoch != self.epoch {
            return Err(RpcError::UnknownServerRequest);
        }
        self.respond_error_id(request.id.clone(), code, message)
            .await
    }

    async fn send_fire(
        &self,
        high: bool,
        method: &'static str,
        message: OutboundMessage,
        server_request_id: Option<RequestId>,
        budget: OwnedSemaphorePermit,
        deadline: Instant,
    ) -> Result<(), RpcError> {
        self.ensure_open()?;
        let (ack_tx, ack_rx) = oneshot::channel();
        let command = RpcCommand::Fire {
            method,
            message,
            server_request_id,
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
                Arc::clone(&self.command_budget),
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
    rx: mpsc::Receiver<InternalRpcEvent>,
    terminal_rx: Option<oneshot::Receiver<RpcEvent>>,
    normal_closed: bool,
}

impl RpcEventReceiver {
    pub async fn recv(&mut self) -> Option<RpcEvent> {
        if !self.normal_closed {
            if let Some(event) = self.rx.recv().await {
                return Some(event.event);
            }
            self.normal_closed = true;
        }
        self.terminal_rx.take()?.await.ok()
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
    let cancellation = parent_cancellation.child_token();
    drop(parent_cancellation);
    let (high_tx, high_rx) = mpsc::channel(RPC_HIGH_CAPACITY);
    let (normal_tx, normal_rx) = mpsc::channel(RPC_NORMAL_CAPACITY);
    let (cancel_tx, cancel_rx) = mpsc::channel(RPC_INFLIGHT_CAPACITY);
    let (event_tx, event_rx) = mpsc::channel(EVENT_CAPACITY);
    let (terminal_tx, terminal_rx) = oneshot::channel();
    let state = Arc::new(AtomicU8::new(STATE_OPEN));
    let pending_count = Arc::new(AtomicUsize::new(0));
    let protocol_drift_count = Arc::new(AtomicU64::new(0));
    let dropped_notification_count = Arc::new(AtomicU64::new(0));
    let event_budget = Arc::new(Semaphore::new(RPC_BYTE_BUDGET));

    let handle = RpcHandle {
        high_tx,
        normal_tx,
        cancel_tx,
        epoch,
        state: Arc::clone(&state),
        initialize_state: Arc::new(AtomicU8::new(INIT_NEW)),
        next_id: Arc::new(AtomicU64::new(0)),
        inflight: Arc::new(Semaphore::new(RPC_INFLIGHT_CAPACITY)),
        command_budget: Arc::new(Semaphore::new(RPC_BYTE_BUDGET)),
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
            terminal_tx,
            event_budget,
            epoch,
            state,
            pending_count,
            protocol_drift_count,
            dropped_notification_count,
            actor_cancel,
        )
        .await
    });

    RpcConnection {
        handle,
        events: RpcEventReceiver {
            rx: event_rx,
            terminal_rx: Some(terminal_rx),
            normal_closed: false,
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
    terminal_tx: oneshot::Sender<RpcEvent>,
    event_budget: Arc<Semaphore>,
    epoch: ConnectionEpoch,
    state: Arc<AtomicU8>,
    pending_count: Arc<AtomicUsize>,
    protocol_drift_count: Arc<AtomicU64>,
    dropped_notification_count: Arc<AtomicU64>,
    cancellation: CancellationToken,
) -> TransportExit {
    let (pump_high_tx, pump_high_rx) = mpsc::channel(RPC_HIGH_CAPACITY);
    let (pump_normal_tx, pump_normal_rx) = mpsc::channel(RPC_NORMAL_CAPACITY);
    let (completion_tx, mut completion_rx) = mpsc::channel(RPC_INFLIGHT_CAPACITY);
    let pump = tokio::spawn(run_sender_pump(
        transport.high_tx.clone(),
        transport.normal_tx.clone(),
        pump_high_rx,
        pump_normal_rx,
        completion_tx,
        epoch,
        cancellation.clone(),
    ));
    let mut pending = HashMap::<RequestId, PendingRequest>::new();
    let mut server_pending = HashSet::<RequestId>::new();
    let mut high_burst = 0_usize;
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
                        if !handle_inbound(
                            message,
                            &mut pending,
                            &mut server_pending,
                            &event_tx,
                            &event_budget,
                            epoch,
                            &pending_count,
                            &protocol_drift_count,
                            &dropped_notification_count,
                        ) {
                            break TransportExit::TaskFailed;
                        }
                    }
                    Some(TransportEvent::ProtocolError(_)) => {
                        increment_saturating(&protocol_drift_count);
                        if !try_emit_event(RpcEvent::ProtocolDrift, &event_tx, &event_budget) {
                            break TransportExit::TaskFailed;
                        }
                    }
                    Some(TransportEvent::ReadError(error)) => break TransportExit::ReadError(error.kind),
                    Some(TransportEvent::WriteError(error)) => break TransportExit::WriteError(error.kind),
                    Some(TransportEvent::StdoutEof) => break TransportExit::StdoutEof,
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
    fail_all_pending(&mut pending, epoch, &pending_count);
    high_rx.close();
    normal_rx.close();
    reject_queued(&mut high_rx, epoch);
    reject_queued(&mut normal_rx, epoch);
    drop(pump_high_tx);
    drop(pump_normal_tx);
    let _ = pump.await;
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
fn handle_inbound(
    message: InboundMessage,
    pending: &mut HashMap<RequestId, PendingRequest>,
    server_pending: &mut HashSet<RequestId>,
    event_tx: &mpsc::Sender<InternalRpcEvent>,
    event_budget: &Arc<Semaphore>,
    epoch: ConnectionEpoch,
    pending_count: &AtomicUsize,
    protocol_drift_count: &AtomicU64,
    dropped_notification_count: &AtomicU64,
) -> bool {
    match message {
        InboundMessage::Response { id, result } => {
            if let Some(entry) = take_pending(pending, &id, pending_count) {
                let _ = entry.reply.send(Ok(result));
            } else {
                increment_saturating(protocol_drift_count);
                return try_emit_event(RpcEvent::ProtocolDrift, event_tx, event_budget);
            }
        }
        InboundMessage::ErrorResponse { id, error } => {
            if let Some(entry) = take_pending(pending, &id, pending_count) {
                let _ = entry.reply.send(Err(RpcError::Server {
                    method: entry.method,
                    code: error.code,
                }));
            } else {
                increment_saturating(protocol_drift_count);
                return try_emit_event(RpcEvent::ProtocolDrift, event_tx, event_budget);
            }
        }
        InboundMessage::Request { id, method, params } => {
            if server_pending.len() >= RPC_SERVER_REQUEST_CAPACITY
                || !server_pending.insert(id.clone())
            {
                return false;
            }
            return try_emit_event(
                RpcEvent::ServerRequest(ServerRequest {
                    id,
                    method,
                    params,
                    epoch,
                }),
                event_tx,
                event_budget,
            );
        }
        InboundMessage::Notification { method, params } => {
            let authoritative = is_authoritative_notification(&method);
            let emitted = try_emit_event(
                RpcEvent::Notification { method, params },
                event_tx,
                event_budget,
            );
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
            server_request_id,
            deadline,
            ack,
            _budget: budget_permit,
        } => {
            if ack.is_closed() {
                return true;
            }
            if Instant::now() >= deadline {
                let _ = ack.send(Err(RpcError::Timeout { method }));
                return true;
            }
            if let Some(id) = &server_request_id {
                if !server_pending.remove(id) {
                    let _ = ack.send(Err(RpcError::UnknownServerRequest));
                    return true;
                }
            }
            let job = OutboundJob {
                method,
                message,
                deadline,
                completion: JobCompletion::Fire(ack),
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
    high_sender: TransportSender,
    normal_sender: TransportSender,
    mut high_rx: mpsc::Receiver<OutboundJob>,
    mut normal_rx: mpsc::Receiver<OutboundJob>,
    completion_tx: mpsc::Sender<OutboundCompletion>,
    epoch: ConnectionEpoch,
    cancellation: CancellationToken,
) {
    let mut high_burst = 0_usize;
    loop {
        let job = if high_burst >= HIGH_PRIORITY_BURST {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return,
                Some(job) = normal_rx.recv() => {
                    high_burst = 0;
                    Some((job, normal_sender.clone()))
                }
                Some(job) = high_rx.recv() => {
                    high_burst = high_burst.saturating_add(1);
                    Some((job, high_sender.clone()))
                }
                else => None,
            }
        } else {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return,
                Some(job) = high_rx.recv() => {
                    high_burst = high_burst.saturating_add(1);
                    Some((job, high_sender.clone()))
                }
                Some(job) = normal_rx.recv() => {
                    high_burst = 0;
                    Some((job, normal_sender.clone()))
                }
                else => None,
            }
        };
        let Some((job, sender)) = job else {
            return;
        };
        let OutboundJob {
            method,
            message,
            deadline,
            completion,
            _budget,
        } = job;
        let result = tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(RpcError::ConnectionLost(epoch)),
            () = tokio::time::sleep_until(deadline) => Err(RpcError::Timeout { method }),
            result = sender.send(message) => result.map_err(|error| map_send_error(&error, method, epoch)),
        };
        match completion {
            JobCompletion::Request(id) => {
                if completion_tx
                    .send(OutboundCompletion { id, result })
                    .await
                    .is_err()
                {
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
        TransportSendError::Closed | TransportSendError::Cancelled => {
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
    event_tx: &mpsc::Sender<InternalRpcEvent>,
    event_budget: &Arc<Semaphore>,
) -> bool {
    let weight = rpc_event_weight(&event);
    if weight > RPC_BYTE_BUDGET {
        return false;
    }
    let permits = bounded_permits(weight);
    let Ok(budget) = Arc::clone(event_budget).try_acquire_many_owned(permits) else {
        return false;
    };
    event_tx
        .try_send(InternalRpcEvent {
            event,
            _budget: budget,
        })
        .is_ok()
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
            .saturating_add(request.params.as_ref().map_or(0, value_memory_weight))
            .saturating_add(192),
        RpcEvent::TransportClosed(_) | RpcEvent::ProtocolDrift => 64,
    }
}

fn value_memory_weight(value: &Value) -> usize {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => 24,
        Value::String(value) => value.len().saturating_add(32),
        Value::Array(values) => values.iter().fold(32, |total, value| {
            total.saturating_add(value_memory_weight(value).saturating_add(24))
        }),
        Value::Object(values) => values.iter().fold(48, |total, (key, value)| {
            total
                .saturating_add(key.len())
                .saturating_add(value_memory_weight(value))
                .saturating_add(56)
        }),
    }
}

fn bounded_permits(bytes: usize) -> u32 {
    u32::try_from(bytes.clamp(1, RPC_BYTE_BUDGET))
        .expect("RPC byte budget is bounded below u32::MAX")
}

fn count_serialized<T>(method: &'static str, value: &T) -> Result<usize, RpcError>
where
    T: Serialize + ?Sized,
{
    let maximum = MAX_JSONL_LINE_BYTES.saturating_sub(method.len().saturating_add(256));
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
    matches!(method, "item/completed" | "turn/completed" | "error")
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
    handle
        .initialize_state
        .compare_exchange(INIT_NEW, INIT_RUNNING, Ordering::AcqRel, Ordering::Acquire)
        .map_err(|_| RpcError::AlreadyInitialized)?;

    let mut client_info = ClientInfo::new("lark_codex_bridge", env!("CARGO_PKG_VERSION"));
    client_info.title = Some("Lark Codex Bridge".to_owned());
    let params = InitializeParams::new(client_info);
    let result = handle
        .request_high::<_, InitializeResult>("initialize", &params, INITIALIZE_TIMEOUT)
        .await;
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
        reply: oneshot::Sender<Result<Value, RpcError>>,
        _inflight: OwnedSemaphorePermit,
        _budget: OwnedSemaphorePermit,
    },
    Fire {
        method: &'static str,
        message: OutboundMessage,
        server_request_id: Option<RequestId>,
        deadline: Instant,
        ack: oneshot::Sender<Result<(), RpcError>>,
        _budget: OwnedSemaphorePermit,
    },
}

struct PendingRequest {
    method: &'static str,
    deadline: Instant,
    reply: oneshot::Sender<Result<Value, RpcError>>,
    _inflight: OwnedSemaphorePermit,
}

struct OutboundJob {
    method: &'static str,
    message: OutboundMessage,
    deadline: Instant,
    completion: JobCompletion,
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
    event: RpcEvent,
    _budget: OwnedSemaphorePermit,
}
