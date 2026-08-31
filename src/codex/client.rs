use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    hash::{DefaultHasher, Hasher},
    io,
    sync::{Arc, Mutex, MutexGuard, Weak},
};

use serde::Serialize;
use serde_json::Value;
use tokio::{
    sync::{Mutex as AsyncMutex, Semaphore, mpsc, oneshot},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    codex::{
        compat::WireAdapter,
        protocol::value_memory_weight,
        rpc::{ConnectionEpoch, RpcConnection, RpcError, RpcEvent, RpcHandle, ServerRequest},
        transport::TransportExit,
        types::{
            ErrorNotification, ItemCompletedNotification, Thread, ThreadItem, ThreadListParams,
            ThreadListResult, ThreadReadParams, ThreadReadResult, ThreadResumeParams,
            ThreadStartParams, ThreadStartResult, ThreadTokenUsageUpdatedNotification,
            TokenUsageBreakdown, Turn, TurnCompletedNotification, TurnError, TurnInterruptParams,
            TurnInterruptResult, TurnStartParams, TurnStatus,
        },
    },
    limits::{
        CLIENT_COMMAND_CAPACITY, CLIENT_CONTROL_BYTE_BUDGET, CLIENT_CONTROL_CAPACITY,
        CLIENT_CONTROL_EVENT_BYTE_LIMIT, CLIENT_PROJECTION_CAPACITY, CLIENT_SUBSCRIBER_CAPACITY,
        CONTROL_RPC_TIMEOUT, INTERRUPT_TIMEOUT, ROUTING_ID_BYTE_LIMIT, THREAD_DELTA_BYTE_LIMIT,
        THREAD_EVENT_CAPACITY, THREAD_MAILBOX_BYTE_BUDGET, THREAD_OUTCOME_CAPACITY,
        THREAD_PROJECTION_BYTE_BUDGET, THREAD_SUBSCRIBER_CAPACITY, THREAD_TERMINAL_CAPACITY,
    },
};

/// Reverse request currently consumed by the bridge runtime.
pub const DYNAMIC_TOOL_CALL_METHOD: &str = "item/tool/call";

/// Canonical successful lifecycle order exercised by the production router.
pub const NORMAL_NOTIFICATION_ORDER: &[&str] = &[
    "thread/started",
    "turn/started",
    "item/started",
    "item/agentMessage/delta",
    "item/commandExecution/outputDelta",
    "item/completed",
    "thread/tokenUsage/updated",
    "turn/completed",
];

/// Every notification whose parameters the production router interprets.
pub const CONSUMED_NOTIFICATION_METHODS: &[&str] = &[
    "thread/started",
    "turn/started",
    "item/started",
    "item/agentMessage/delta",
    "item/commandExecution/outputDelta",
    "item/completed",
    "thread/tokenUsage/updated",
    "error",
    "turn/completed",
];

/// Opaque proof that this initialized client uses a reviewed wire contract
/// with exact active-writer conflict semantics for persisted-thread adoption.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ThreadAdoptionContract(());

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ThreadId(Arc<str>);

impl ThreadId {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(Arc::from(value.into()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn is_valid(&self) -> bool {
        valid_routing_id(self.as_str())
    }
}

impl From<String> for ThreadId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for ThreadId {
    fn from(value: &str) -> Self {
        Self(Arc::from(value))
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TurnId(Arc<str>);

impl TurnId {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(Arc::from(value.into()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for TurnId {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for TurnId {
    fn from(value: &str) -> Self {
        Self(Arc::from(value))
    }
}

#[derive(Clone)]
pub struct TurnOutcome {
    pub thread_id: ThreadId,
    pub turn_id: TurnId,
    pub status: TurnStatus,
    pub error: Option<TurnError>,
    pub completed_items: Vec<ThreadItem>,
    pub token_usage: Option<TokenUsageBreakdown>,
}

impl fmt::Debug for TurnOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TurnOutcome")
            .field("thread_id", &self.thread_id)
            .field("turn_id", &self.turn_id)
            .field("status", &turn_status_label(&self.status))
            .field("has_error", &self.error.is_some())
            .field("completed_item_count", &self.completed_items.len())
            .field("has_token_usage", &self.token_usage.is_some())
            .finish()
    }
}

fn turn_status_label(status: &TurnStatus) -> &'static str {
    match status {
        TurnStatus::Completed => "completed",
        TurnStatus::Interrupted => "interrupted",
        TurnStatus::Failed => "failed",
        TurnStatus::InProgress => "inProgress",
        TurnStatus::Unknown(_) => "unknown",
    }
}

#[derive(Clone)]
pub enum AppServerEvent {
    ThreadStarted {
        thread_id: ThreadId,
    },
    TurnStarted {
        turn: Turn,
    },
    ItemStarted {
        turn_id: TurnId,
        item: ThreadItem,
    },
    AgentMessageDelta {
        turn_id: TurnId,
        item_id: String,
        delta: String,
    },
    CommandOutputDelta {
        turn_id: TurnId,
        item_id: String,
        delta: String,
    },
    ItemCompleted {
        turn_id: TurnId,
        item: ThreadItem,
    },
    TokenUsageUpdated {
        turn_id: TurnId,
        usage: TokenUsageBreakdown,
    },
    TurnCompleted(TurnOutcome),
    Error {
        turn_id: TurnId,
        error: TurnError,
        will_retry: bool,
    },
    SubscriptionInvalidated {
        thread_id: ThreadId,
        reason: SubscriptionInvalidation,
    },
    Unknown {
        method: String,
    },
    ConnectionClosed {
        exit: TransportExit,
    },
}

impl fmt::Debug for AppServerEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ThreadStarted { thread_id } => formatter
                .debug_struct("ThreadStarted")
                .field("thread_id", thread_id)
                .finish(),
            Self::TurnStarted { turn } => formatter
                .debug_struct("TurnStarted")
                .field("turn_id", &turn.id)
                .finish(),
            Self::ItemStarted { turn_id, item } | Self::ItemCompleted { turn_id, item } => {
                formatter
                    .debug_struct(match self {
                        Self::ItemStarted { .. } => "ItemStarted",
                        _ => "ItemCompleted",
                    })
                    .field("turn_id", turn_id)
                    .field("item_kind", &item.kind())
                    .field("item_id", &item.id())
                    .finish()
            }
            Self::AgentMessageDelta {
                turn_id, item_id, ..
            } => formatter
                .debug_struct("AgentMessageDelta")
                .field("turn_id", turn_id)
                .field("item_id", item_id)
                .finish(),
            Self::CommandOutputDelta {
                turn_id, item_id, ..
            } => formatter
                .debug_struct("CommandOutputDelta")
                .field("turn_id", turn_id)
                .field("item_id", item_id)
                .finish(),
            Self::TokenUsageUpdated { turn_id, .. } => formatter
                .debug_struct("TokenUsageUpdated")
                .field("turn_id", turn_id)
                .finish(),
            Self::TurnCompleted(outcome) => formatter
                .debug_tuple("TurnCompleted")
                .field(outcome)
                .finish(),
            Self::Error {
                turn_id,
                will_retry,
                ..
            } => formatter
                .debug_struct("Error")
                .field("turn_id", turn_id)
                .field("will_retry", will_retry)
                .finish(),
            Self::SubscriptionInvalidated { thread_id, reason } => formatter
                .debug_struct("SubscriptionInvalidated")
                .field("thread_id", thread_id)
                .field("reason", reason)
                .finish(),
            Self::Unknown { method } => formatter
                .debug_struct("Unknown")
                .field("method", method)
                .finish(),
            Self::ConnectionClosed { exit } => formatter
                .debug_struct("ConnectionClosed")
                .field("exit", exit)
                .finish(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubscriptionInvalidation {
    Lagged,
    ProtocolDrift,
}

pub enum ControlEvent {
    ServerRequest(ServerRequest),
    ProtocolDrift,
    UnknownNotification { method: String },
    InvalidNotification { method: String, authoritative: bool },
    ConnectionClosed(TransportExit),
}

impl fmt::Debug for ControlEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ServerRequest(request) => formatter
                .debug_tuple("ServerRequest")
                .field(request)
                .finish(),
            Self::ProtocolDrift => formatter.write_str("ProtocolDrift"),
            Self::UnknownNotification { method } => formatter
                .debug_struct("UnknownNotification")
                .field("method", method)
                .finish(),
            Self::InvalidNotification {
                method,
                authoritative,
            } => formatter
                .debug_struct("InvalidNotification")
                .field("method", method)
                .field("authoritative", authoritative)
                .finish(),
            Self::ConnectionClosed(exit) => formatter
                .debug_tuple("ConnectionClosed")
                .field(exit)
                .finish(),
        }
    }
}

pub struct ControlEventReceiver {
    rx: mpsc::Receiver<ControlEvent>,
    terminal_rx: Option<oneshot::Receiver<ControlEvent>>,
    normal_closed: bool,
}

impl ControlEventReceiver {
    pub async fn recv(&mut self) -> Option<ControlEvent> {
        if !self.normal_closed {
            if let Some(event) = self.rx.recv().await {
                return Some(event);
            }
            self.normal_closed = true;
        }
        self.terminal_rx.take()?.await.ok()
    }
}

pub enum ClientError {
    Rpc(RpcError),
    RouterClosed(ConnectionEpoch),
    RouterTaskFailed(ConnectionEpoch),
    RouterTimeout(ConnectionEpoch),
    Capacity,
    ControlEventsTaken,
    ConfirmedThreadUntracked {
        method: &'static str,
        thread: Box<Thread>,
    },
    ConfirmedTurnUntracked {
        turn: Box<Turn>,
    },
    InvalidNotification {
        method: String,
    },
}

impl fmt::Debug for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl fmt::Display for ClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rpc(error) => fmt::Display::fmt(error, formatter),
            Self::RouterClosed(epoch) => {
                write!(
                    formatter,
                    "client router for epoch {} is closed",
                    epoch.get()
                )
            }
            Self::RouterTaskFailed(epoch) => {
                write!(formatter, "client router for epoch {} failed", epoch.get())
            }
            Self::RouterTimeout(epoch) => {
                write!(
                    formatter,
                    "client router for epoch {} timed out",
                    epoch.get()
                )
            }
            Self::Capacity => formatter.write_str("client routing capacity is exhausted"),
            Self::ControlEventsTaken => {
                formatter.write_str("client control event stream already has an owner")
            }
            Self::ConfirmedThreadUntracked { method, .. } => write!(
                formatter,
                "RPC {method} was confirmed but its local event projection was lost"
            ),
            Self::ConfirmedTurnUntracked { .. } => formatter
                .write_str("RPC turn/start was confirmed but its local event projection was lost"),
            Self::InvalidNotification { method } => {
                write!(formatter, "invalid app-server notification for {method}")
            }
        }
    }
}

impl std::error::Error for ClientError {}

impl ClientError {
    /// Reports whether a failed `turn/start` is known not to have reached a
    /// point where Codex could execute it. Callers may safely finalize local
    /// resources immediately only in this case; every other error remains
    /// uncertain until the owning connection epoch ends.
    #[must_use]
    pub fn turn_start_definitely_not_applied(&self) -> bool {
        match self {
            Self::Rpc(error) => definitely_not_applied(error),
            Self::RouterClosed(_)
            | Self::RouterTaskFailed(_)
            | Self::RouterTimeout(_)
            | Self::Capacity
            | Self::ControlEventsTaken
            | Self::InvalidNotification { .. } => true,
            Self::ConfirmedThreadUntracked { .. } | Self::ConfirmedTurnUntracked { .. } => false,
        }
    }
}

impl From<RpcError> for ClientError {
    fn from(value: RpcError) -> Self {
        Self::Rpc(value)
    }
}

#[derive(Clone)]
pub struct AppServerClient {
    rpc: RpcHandle,
    wire: WireAdapter,
    router_tx: mpsc::Sender<RouterCommand>,
    cancellation: CancellationToken,
    faulted: Arc<std::sync::atomic::AtomicBool>,
    epoch: ConnectionEpoch,
    router: Arc<AsyncMutex<RouterLifecycle>>,
    control_rx: Arc<Mutex<Option<ControlEventReceiver>>>,
}

struct RouterLifecycle {
    task: Option<JoinHandle<TransportExit>>,
    exit: Option<TransportExit>,
    failed: bool,
}

impl AppServerClient {
    #[must_use]
    pub fn spawn(mut connection: RpcConnection, wire: WireAdapter) -> Self {
        let rpc = connection.handle.clone();
        let epoch = rpc.epoch();
        let cancellation = CancellationToken::new();
        let faulted = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let router_cancel = cancellation.clone();
        let router_faulted = Arc::clone(&faulted);
        let (router_tx, router_rx) = mpsc::channel(CLIENT_COMMAND_CAPACITY);
        let (control_tx, control_rx) = mpsc::channel(CLIENT_CONTROL_CAPACITY);
        let (control_terminal_tx, control_terminal_rx) = oneshot::channel();
        let control_budget = Arc::new(Semaphore::new(CLIENT_CONTROL_BYTE_BUDGET));
        let router = tokio::spawn(async move {
            run_router(
                &mut connection,
                router_rx,
                control_tx,
                control_terminal_tx,
                control_budget,
                router_faulted,
                router_cancel,
                wire,
            )
            .await
        });
        Self {
            rpc,
            wire,
            router_tx,
            cancellation,
            faulted,
            epoch,
            router: Arc::new(AsyncMutex::new(RouterLifecycle {
                task: Some(router),
                exit: None,
                failed: false,
            })),
            control_rx: Arc::new(Mutex::new(Some(ControlEventReceiver {
                rx: control_rx,
                terminal_rx: Some(control_terminal_rx),
                normal_closed: false,
            }))),
        }
    }

    /// Returns capability proof only for the exact reviewed adoption wires.
    #[must_use]
    pub const fn thread_adoption_contract(&self) -> Option<ThreadAdoptionContract> {
        match self.wire {
            WireAdapter::V0_149_0 | WireAdapter::SidecarV1 => Some(ThreadAdoptionContract(())),
            WireAdapter::V0_146_0 => None,
        }
    }

    /// Lists Codex threads without binding any returned thread to bridge state.
    ///
    /// # Errors
    ///
    /// Returns a safe client error when the read-only RPC fails.
    pub async fn list_threads(
        &self,
        params: ThreadListParams,
    ) -> Result<ThreadListResult, ClientError> {
        let params = Self::encode_params("thread/list", self.wire.thread_list_params(&params))?;
        self.rpc
            .request_budgeted::<_, Value>("thread/list", &params, CONTROL_RPC_TIMEOUT)
            .await?
            .try_map(|value| {
                Self::decode_result("thread/list", self.wire.thread_list_response(value))
            })
            .map(crate::codex::rpc::BudgetedResponse::into_inner)
    }

    /// Reads one Codex thread without binding it to bridge state.
    ///
    /// # Errors
    ///
    /// Returns a safe client error when the read-only RPC fails.
    pub async fn read_thread(
        &self,
        params: ThreadReadParams,
    ) -> Result<ThreadReadResult, ClientError> {
        let params = Self::encode_params("thread/read", self.wire.thread_read_params(&params))?;
        self.rpc
            .request_budgeted::<_, Value>("thread/read", &params, CONTROL_RPC_TIMEOUT)
            .await?
            .try_map(|value| {
                Self::decode_result("thread/read", self.wire.thread_read_response(value))
            })
            .map(crate::codex::rpc::BudgetedResponse::into_inner)
    }

    /// Creates a Codex thread. This non-idempotent RPC is never retried locally.
    ///
    /// # Errors
    ///
    /// Returns a safe client error when the RPC fails.
    pub async fn start_thread(&self, params: ThreadStartParams) -> Result<Thread, ClientError> {
        let params = Self::encode_params("thread/start", self.wire.thread_start_params(&params))?;
        let mut guard =
            NonIdempotentGuard::new(self.cancellation.clone(), Arc::clone(&self.faulted));
        let result = match self
            .rpc
            .request_budgeted::<_, Value>("thread/start", &params, CONTROL_RPC_TIMEOUT)
            .await
        {
            Ok(result) => result.try_map(|value| {
                Self::decode_result("thread/start", self.wire.thread_start_response(value))
            })?,
            Err(error) => {
                if definitely_not_applied(&error) {
                    guard.disarm();
                }
                return Err(error.into());
            }
        };
        let thread = result.value.thread.clone();
        self.observe_thread_started(ThreadId::from(thread.id.as_str()), result)
            .await
            .map_err(|_| ClientError::ConfirmedThreadUntracked {
                method: "thread/start",
                thread: Box::new(thread.clone()),
            })?;
        // The response only confirms that the remote operation happened.  Keep
        // the epoch fail-closed until its local routing observation is durable.
        guard.disarm();
        Ok(thread)
    }

    /// Resumes a persisted Codex thread without retrying an uncertain request.
    ///
    /// # Errors
    ///
    /// Returns a safe client error when the RPC fails.
    pub async fn resume_thread(&self, params: ThreadResumeParams) -> Result<Thread, ClientError> {
        self.ensure_thread_route(ThreadId::from(params.thread_id.as_str()))
            .await?;
        let wire_params =
            Self::encode_params("thread/resume", self.wire.thread_resume_params(&params))?;
        let mut guard =
            NonIdempotentGuard::new(self.cancellation.clone(), Arc::clone(&self.faulted));
        let result = match self
            .rpc
            .request_thread_resume_budgeted::<_, Value>(
                &wire_params,
                params.thread_id.as_str(),
                self.wire,
                CONTROL_RPC_TIMEOUT,
            )
            .await
        {
            Ok(result) => result.try_map(|value| {
                Self::decode_result("thread/resume", self.wire.thread_resume_response(value))
            })?,
            Err(error) => {
                if definitely_not_applied(&error) {
                    guard.disarm();
                }
                return Err(error.into());
            }
        };
        let thread = result.value.thread.clone();
        self.observe_thread_started(ThreadId::from(thread.id.as_str()), result)
            .await
            .map_err(|_| ClientError::ConfirmedThreadUntracked {
                method: "thread/resume",
                thread: Box::new(thread.clone()),
            })?;
        guard.disarm();
        Ok(thread)
    }

    /// Starts a Codex turn. This non-idempotent RPC is never retried locally.
    ///
    /// # Errors
    ///
    /// Returns a safe client error when the RPC fails.
    pub async fn start_turn(&self, params: TurnStartParams) -> Result<Turn, ClientError> {
        let thread_id = ThreadId::from(params.thread_id.as_str());
        let params = Self::encode_params("turn/start", self.wire.turn_start_params(&params))?;
        let attempt_id = TurnStartAttemptId::new();
        self.ensure_thread_route(thread_id.clone()).await?;
        self.begin_turn_start(thread_id.clone(), attempt_id).await?;
        let mut pending_start = PendingTurnStartGuard::new(
            thread_id.clone(),
            attempt_id,
            self.router_tx.clone(),
            self.cancellation.clone(),
            Arc::clone(&self.faulted),
        );
        let result = match self
            .rpc
            .request_budgeted::<_, Value>("turn/start", &params, CONTROL_RPC_TIMEOUT)
            .await
        {
            Ok(result) => result.try_map(|value| {
                Self::decode_result("turn/start", self.wire.turn_start_response(value))
            })?,
            Err(error) => {
                if definitely_not_applied(&error) {
                    pending_start.abort_known_failure();
                }
                return Err(error.into());
            }
        };
        let (result, response_budget) = result.into_parts();
        let turn = result.turn;
        let confirmed = turn.clone();
        let (ack_tx, ack_rx) = oneshot::channel();
        if self
            .send_router(RouterCommand::ObserveTurnStarted {
                thread_id,
                attempt_id,
                turn: Box::new(turn),
                ack: ack_tx,
                _budget: response_budget,
            })
            .await
            .is_err()
        {
            return Err(ClientError::ConfirmedTurnUntracked {
                turn: Box::new(confirmed),
            });
        }
        match wait_router_ack(ack_rx, self.epoch).await {
            Ok(Some(turn)) => {
                pending_start.disarm();
                Ok(*turn)
            }
            Ok(None) | Err(_) => Err(ClientError::ConfirmedTurnUntracked {
                turn: Box::new(confirmed),
            }),
        }
    }

    /// Requests high-priority interruption; completion still requires turn/completed.
    ///
    /// # Errors
    ///
    /// Returns a safe client error when the interrupt RPC fails.
    pub async fn interrupt_turn(
        &self,
        thread_id: &ThreadId,
        turn_id: &TurnId,
    ) -> Result<(), ClientError> {
        if !thread_id.is_valid() || !valid_routing_id(turn_id.as_str()) {
            return Err(ClientError::Capacity);
        }
        let params = TurnInterruptParams::new(thread_id.as_str(), turn_id.as_str());
        let params =
            Self::encode_params("turn/interrupt", self.wire.turn_interrupt_params(&params))?;
        let value: Value = self
            .rpc
            .request_high("turn/interrupt", &params, INTERRUPT_TIMEOUT)
            .await?;
        let _: TurnInterruptResult =
            Self::decode_result("turn/interrupt", self.wire.turn_interrupt_response(value))?;
        Ok(())
    }

    /// Registers one bounded thread mailbox.
    ///
    /// # Errors
    ///
    /// Returns a safe error if the router connection is already closed.
    pub async fn subscribe(&self, thread_id: ThreadId) -> Result<ThreadSubscription, ClientError> {
        if !thread_id.is_valid() {
            return Err(ClientError::Capacity);
        }
        let id = SubscriptionId::new();
        let mailbox = Arc::new(Mailbox::new());
        let (ack_tx, ack_rx) = oneshot::channel();
        self.send_router(RouterCommand::Subscribe {
            id,
            thread_id: thread_id.clone(),
            mailbox: Arc::clone(&mailbox),
            ack: ack_tx,
        })
        .await?;
        let state = wait_router_ack(ack_rx, self.epoch)
            .await?
            .ok_or(ClientError::Capacity)?;
        Ok(ThreadSubscription {
            id,
            thread_id,
            mailbox,
            state,
            router_tx: self.router_tx.clone(),
        })
    }

    /// Releases a completed, unsubscribed thread route and its retained projection.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Capacity`] when the route has a live subscription,
    /// active turn, or pending turn start.  Those routes are never released
    /// implicitly.
    pub async fn release_thread(&self, thread_id: &ThreadId) -> Result<(), ClientError> {
        if !thread_id.is_valid() {
            return Err(ClientError::Capacity);
        }
        let (ack_tx, ack_rx) = oneshot::channel();
        self.send_router(RouterCommand::ReleaseRoute {
            thread_id: thread_id.clone(),
            ack: ack_tx,
        })
        .await?;
        if wait_router_ack(ack_rx, self.epoch).await? {
            Ok(())
        } else {
            Err(ClientError::Capacity)
        }
    }

    #[must_use]
    pub const fn epoch(&self) -> ConnectionEpoch {
        self.epoch
    }

    /// Takes the single reliable stream for approvals and global protocol events.
    ///
    /// # Errors
    ///
    /// Returns a safe error if another owner already took the stream.
    pub fn take_control_events(&self) -> Result<ControlEventReceiver, ClientError> {
        lock_unpoison(&self.control_rx)
            .take()
            .ok_or(ClientError::ControlEventsTaken)
    }

    /// Answers an app-server request emitted on the control stream.
    ///
    /// # Errors
    ///
    /// Returns a safe RPC error for stale tokens, serialization, or transport loss.
    pub async fn respond_request<R>(
        &self,
        request: &mut ServerRequest,
        result: &R,
    ) -> Result<(), ClientError>
    where
        R: Serialize + ?Sized,
    {
        self.rpc.respond_request(request, result).await?;
        Ok(())
    }

    /// Decodes the only promoted reverse-request contract.
    ///
    /// # Errors
    ///
    /// Returns a redacted error if the method or generated wire value is invalid.
    pub fn decode_dynamic_tool_call(
        &self,
        request: &ServerRequest,
    ) -> Result<crate::codex::types::DynamicToolCallParams, ClientError> {
        if request.method != DYNAMIC_TOOL_CALL_METHOD {
            return Err(ClientError::Rpc(RpcError::UnknownServerRequest));
        }
        Self::decode_result(
            DYNAMIC_TOOL_CALL_METHOD,
            self.wire
                .dynamic_tool_call_params(request.params.clone().unwrap_or(Value::Null)),
        )
    }

    /// Encodes and answers the promoted dynamic-tool reverse request.
    ///
    /// # Errors
    ///
    /// Returns a redacted error for incompatible output, a stale request, or transport loss.
    pub async fn respond_dynamic_tool_call(
        &self,
        request: &mut ServerRequest,
        result: &crate::codex::types::DynamicToolCallResponse,
    ) -> Result<(), ClientError> {
        let result = Self::encode_params(
            DYNAMIC_TOOL_CALL_METHOD,
            self.wire.dynamic_tool_call_response(result),
        )?;
        self.rpc.respond_request(request, &result).await?;
        Ok(())
    }

    fn encode_params<T>(
        method: &'static str,
        result: Result<T, crate::codex::compat::CompatError>,
    ) -> Result<T, ClientError> {
        result.map_err(|_| ClientError::Rpc(RpcError::Serialize { method }))
    }

    fn decode_result<T>(
        method: &'static str,
        result: Result<T, crate::codex::compat::CompatError>,
    ) -> Result<T, ClientError> {
        result.map_err(|_| ClientError::Rpc(RpcError::Deserialize { method }))
    }

    /// Rejects an app-server request emitted on the control stream.
    ///
    /// # Errors
    ///
    /// Returns a safe RPC error for stale tokens or transport loss.
    pub async fn respond_request_error(
        &self,
        request: &mut ServerRequest,
        code: i64,
        message: &str,
    ) -> Result<(), ClientError> {
        self.rpc
            .respond_request_error(request, code, message)
            .await?;
        Ok(())
    }

    /// Releases an app-server request that an upper-layer policy will not answer.
    ///
    /// # Errors
    ///
    /// Returns a safe error for stale request tokens or a closed connection.
    pub fn abandon_request(&self, request: &mut ServerRequest) -> Result<(), ClientError> {
        self.rpc.abandon_request(request)?;
        Ok(())
    }

    /// Stops the router and its underlying RPC transport.
    ///
    /// # Errors
    ///
    /// Returns a redacted error if the router task panicked or was aborted.
    pub async fn shutdown(&self) -> Result<TransportExit, ClientError> {
        self.cancellation.cancel();
        let mut lifecycle = self.router.lock().await;
        if let Some(exit) = lifecycle.exit {
            return Ok(exit);
        }
        if lifecycle.failed {
            return Err(ClientError::RouterTaskFailed(self.epoch));
        }
        let Some(task) = lifecycle.task.take() else {
            lifecycle.failed = true;
            return Err(ClientError::RouterTaskFailed(self.epoch));
        };
        if let Ok(exit) = task.await {
            lifecycle.exit = Some(exit);
            Ok(exit)
        } else {
            lifecycle.failed = true;
            Err(ClientError::RouterTaskFailed(self.epoch))
        }
    }

    async fn observe_thread_started(
        &self,
        thread_id: ThreadId,
        budget: crate::codex::rpc::BudgetedResponse<ThreadStartResult>,
    ) -> Result<(), ClientError> {
        if !thread_id.is_valid() {
            return Err(ClientError::Capacity);
        }
        let (ack_tx, ack_rx) = oneshot::channel();
        self.send_router(RouterCommand::ObserveThreadStarted {
            thread_id,
            ack: ack_tx,
            _budget: Box::new(budget),
        })
        .await?;
        if wait_router_ack(ack_rx, self.epoch).await? {
            Ok(())
        } else {
            Err(ClientError::Capacity)
        }
    }

    async fn ensure_thread_route(&self, thread_id: ThreadId) -> Result<(), ClientError> {
        if !thread_id.is_valid() {
            return Err(ClientError::Capacity);
        }
        let (ack_tx, ack_rx) = oneshot::channel();
        self.send_router(RouterCommand::EnsureRoute {
            thread_id,
            ack: ack_tx,
        })
        .await?;
        if wait_router_ack(ack_rx, self.epoch).await? {
            Ok(())
        } else {
            Err(ClientError::Capacity)
        }
    }

    async fn begin_turn_start(
        &self,
        thread_id: ThreadId,
        attempt_id: TurnStartAttemptId,
    ) -> Result<(), ClientError> {
        if !thread_id.is_valid() {
            return Err(ClientError::Capacity);
        }
        let (ack_tx, ack_rx) = oneshot::channel();
        self.send_router(RouterCommand::BeginTurnStart {
            thread_id,
            attempt_id,
            ack: ack_tx,
        })
        .await?;
        if wait_router_ack(ack_rx, self.epoch).await? {
            Ok(())
        } else {
            Err(ClientError::Capacity)
        }
    }

    async fn send_router(&self, command: RouterCommand) -> Result<(), ClientError> {
        match tokio::time::timeout(CONTROL_RPC_TIMEOUT, self.router_tx.send(command)).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(_)) => Err(ClientError::RouterClosed(self.epoch)),
            Err(_) => Err(ClientError::RouterTimeout(self.epoch)),
        }
    }
}

fn definitely_not_applied(error: &RpcError) -> bool {
    matches!(
        error,
        RpcError::Serialize { .. }
            | RpcError::PayloadTooLarge { .. }
            | RpcError::RequestIdExhausted
            | RpcError::Server { .. }
            | RpcError::ThreadResumeActiveWriter
    )
}

struct NonIdempotentGuard {
    armed: bool,
    cancellation: CancellationToken,
    faulted: Arc<std::sync::atomic::AtomicBool>,
}

impl NonIdempotentGuard {
    fn new(cancellation: CancellationToken, faulted: Arc<std::sync::atomic::AtomicBool>) -> Self {
        Self {
            armed: true,
            cancellation,
            faulted,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for NonIdempotentGuard {
    fn drop(&mut self) {
        if self.armed {
            self.faulted
                .store(true, std::sync::atomic::Ordering::Release);
            self.cancellation.cancel();
        }
    }
}

struct PendingTurnStartGuard {
    thread_id: Option<ThreadId>,
    attempt_id: TurnStartAttemptId,
    router_tx: mpsc::Sender<RouterCommand>,
    cancellation: CancellationToken,
    faulted: Arc<std::sync::atomic::AtomicBool>,
}

impl PendingTurnStartGuard {
    fn new(
        thread_id: ThreadId,
        attempt_id: TurnStartAttemptId,
        router_tx: mpsc::Sender<RouterCommand>,
        cancellation: CancellationToken,
        faulted: Arc<std::sync::atomic::AtomicBool>,
    ) -> Self {
        Self {
            thread_id: Some(thread_id),
            attempt_id,
            router_tx,
            cancellation,
            faulted,
        }
    }

    fn abort_known_failure(&mut self) {
        if let Some(thread_id) = self.thread_id.take() {
            if self
                .router_tx
                .try_send(RouterCommand::AbortTurnStart {
                    thread_id,
                    attempt_id: self.attempt_id,
                })
                .is_err()
            {
                self.cancellation.cancel();
            }
        }
    }

    fn disarm(&mut self) {
        self.thread_id = None;
    }

    fn fail_closed(&mut self) {
        if self.thread_id.take().is_some() {
            self.faulted
                .store(true, std::sync::atomic::Ordering::Release);
            self.cancellation.cancel();
        }
    }
}

impl Drop for PendingTurnStartGuard {
    fn drop(&mut self) {
        self.fail_closed();
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
struct TurnStartAttemptId(u64);

impl TurnStartAttemptId {
    fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

async fn wait_router_ack<T>(
    receiver: oneshot::Receiver<T>,
    epoch: ConnectionEpoch,
) -> Result<T, ClientError> {
    match tokio::time::timeout(CONTROL_RPC_TIMEOUT, receiver).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(_)) => Err(ClientError::RouterClosed(epoch)),
        Err(_) => Err(ClientError::RouterTimeout(epoch)),
    }
}

pub struct ThreadSubscription {
    id: SubscriptionId,
    thread_id: ThreadId,
    mailbox: Arc<Mailbox>,
    state: Arc<Mutex<ThreadProjection>>,
    router_tx: mpsc::Sender<RouterCommand>,
}

impl ThreadSubscription {
    pub async fn recv(&mut self) -> Option<AppServerEvent> {
        self.mailbox.recv().await
    }

    #[must_use]
    pub fn outcome(&self, turn_id: &TurnId) -> Option<TurnOutcome> {
        lock_unpoison(&self.state).outcomes.get(turn_id).cloned()
    }

    #[must_use]
    pub fn thread_id(&self) -> &ThreadId {
        &self.thread_id
    }
}

impl Drop for ThreadSubscription {
    fn drop(&mut self) {
        self.mailbox.close();
        let _ = self.router_tx.try_send(RouterCommand::Unsubscribe {
            id: self.id,
            thread_id: self.thread_id.clone(),
        });
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct SubscriptionId(u64);

impl SubscriptionId {
    fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

enum RouterCommand {
    Subscribe {
        id: SubscriptionId,
        thread_id: ThreadId,
        mailbox: Arc<Mailbox>,
        ack: oneshot::Sender<Option<Arc<Mutex<ThreadProjection>>>>,
    },
    Unsubscribe {
        id: SubscriptionId,
        thread_id: ThreadId,
    },
    ObserveTurnStarted {
        thread_id: ThreadId,
        attempt_id: TurnStartAttemptId,
        turn: Box<Turn>,
        ack: oneshot::Sender<Option<Box<Turn>>>,
        _budget: tokio::sync::OwnedSemaphorePermit,
    },
    BeginTurnStart {
        thread_id: ThreadId,
        attempt_id: TurnStartAttemptId,
        ack: oneshot::Sender<bool>,
    },
    AbortTurnStart {
        thread_id: ThreadId,
        attempt_id: TurnStartAttemptId,
    },
    ObserveThreadStarted {
        thread_id: ThreadId,
        ack: oneshot::Sender<bool>,
        _budget: Box<crate::codex::rpc::BudgetedResponse<ThreadStartResult>>,
    },
    EnsureRoute {
        thread_id: ThreadId,
        ack: oneshot::Sender<bool>,
    },
    ReleaseRoute {
        thread_id: ThreadId,
        ack: oneshot::Sender<bool>,
    },
}

struct Subscriber {
    id: SubscriptionId,
    mailbox: Weak<Mailbox>,
}

#[derive(Default)]
struct ThreadProjection {
    thread_started: bool,
    invalidated: Option<SubscriptionInvalidation>,
    started_turns: HashSet<TurnId>,
    active_turns: HashSet<TurnId>,
    started_items: HashSet<(TurnId, String)>,
    completed_items: HashMap<TurnId, CompletedItems>,
    completed_id_bytes: usize,
    usage: HashMap<TurnId, TokenUsageBreakdown>,
    outcomes: HashMap<TurnId, TurnOutcome>,
    terminal_fingerprints: HashMap<TurnId, TerminalFingerprint>,
    outcome_order: VecDeque<TurnId>,
    outcome_weights: HashMap<TurnId, usize>,
    outcome_bytes: usize,
}

struct ThreadRoute {
    subscribers: Vec<Subscriber>,
    projection: Arc<Mutex<ThreadProjection>>,
    pending_turn_start: Option<TurnStartAttemptId>,
    defer_turn_notifications: bool,
    deferred_notifications: VecDeque<(String, Option<Value>)>,
    deferred_bytes: usize,
}

#[derive(Clone, Copy)]
struct TerminalFingerprint(u64);

impl Default for ThreadRoute {
    fn default() -> Self {
        Self {
            subscribers: Vec::new(),
            projection: Arc::new(Mutex::new(ThreadProjection::default())),
            pending_turn_start: None,
            defer_turn_notifications: false,
            deferred_notifications: VecDeque::new(),
            deferred_bytes: 0,
        }
    }
}

#[derive(Default)]
struct CompletedItems {
    by_id: HashMap<String, u64>,
    anonymous: HashSet<u64>,
    retained_bytes: usize,
}

impl CompletedItems {
    fn upsert(&mut self, item: &ThreadItem) -> ItemUpsert {
        let fingerprint = serialized_fingerprint(item);
        if let Some(id) = item.id().map(ToOwned::to_owned) {
            let previous = self.by_id.insert(id.clone(), fingerprint);
            if previous.is_none() {
                self.retained_bytes = self.retained_bytes.saturating_add(id.len());
            }
            match previous {
                None => ItemUpsert::New,
                Some(previous) if previous == fingerprint => ItemUpsert::Same,
                Some(_) => ItemUpsert::Conflict,
            }
        } else if self.anonymous.insert(fingerprint) {
            ItemUpsert::New
        } else {
            ItemUpsert::Same
        }
    }

    fn len(&self) -> usize {
        self.by_id.len().saturating_add(self.anonymous.len())
    }

    fn retained_bytes(&self) -> usize {
        self.retained_bytes
            .saturating_add(self.anonymous.len().saturating_mul(size_of::<u64>()))
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ItemUpsert {
    New,
    Same,
    Conflict,
}

#[allow(clippy::too_many_arguments)]
async fn run_router(
    connection: &mut RpcConnection,
    mut command_rx: mpsc::Receiver<RouterCommand>,
    control_tx: mpsc::Sender<ControlEvent>,
    control_terminal_tx: oneshot::Sender<ControlEvent>,
    control_budget: Arc<Semaphore>,
    faulted: Arc<std::sync::atomic::AtomicBool>,
    cancellation: CancellationToken,
    wire: WireAdapter,
) -> TransportExit {
    let mut routes = HashMap::<ThreadId, ThreadRoute>::new();
    let mut router_fault = None;
    let exit = loop {
        tokio::select! {
            () = cancellation.cancelled() => {
                if faulted.load(std::sync::atomic::Ordering::Acquire) {
                    router_fault = Some(TransportExit::TaskFailed);
                }
                break connection.shutdown().await;
            }
            command = command_rx.recv() => {
                if let Some(command) = command {
                    if !handle_router_command(command, &mut routes, &control_tx, wire) {
                        router_fault = Some(TransportExit::TaskFailed);
                        break connection.shutdown().await;
                    }
                } else {
                    break connection.shutdown().await;
                }
            },
            event = connection.events.recv() => match event {
                Some(RpcEvent::Notification { method, params }) => {
                    if !route_notification(method, params, &mut routes, &control_tx, wire) {
                        router_fault = Some(TransportExit::ProtocolViolation);
                        break connection.shutdown().await;
                    }
                }
                Some(RpcEvent::TransportClosed(exit)) => {
                    let joined_exit = connection.shutdown().await;
                    break if joined_exit == exit { exit } else { joined_exit };
                }
                Some(RpcEvent::ServerRequest(mut request)) => {
                    let weight = server_request_memory_weight(&request);
                    let permit = if weight <= CLIENT_CONTROL_EVENT_BYTE_LIMIT {
                        Arc::clone(&control_budget)
                            .try_acquire_many_owned(byte_permits(weight))
                            .ok()
                    } else {
                        None
                    };
                    let budgeted = permit.is_some();
                    if let Some(permit) = permit {
                        request.retain_with(permit);
                    }
                    if !budgeted
                        || control_tx.try_send(ControlEvent::ServerRequest(request)).is_err()
                    {
                        router_fault = Some(TransportExit::TaskFailed);
                        break connection.shutdown().await;
                    }
                }
                Some(RpcEvent::ProtocolDrift) => {
                    if control_tx.try_send(ControlEvent::ProtocolDrift).is_err()
                        && control_tx.is_closed()
                    {
                        break connection.shutdown().await;
                    }
                }
                None => {
                    break connection.shutdown().await;
                }
            }
        }
    };
    let exit = router_fault.unwrap_or(exit);
    let _ = control_terminal_tx.send(ControlEvent::ConnectionClosed(exit));
    close_subscribers(&mut routes, exit);
    exit
}

#[allow(clippy::too_many_lines)]
fn handle_router_command(
    command: RouterCommand,
    routes: &mut HashMap<ThreadId, ThreadRoute>,
    control_tx: &mpsc::Sender<ControlEvent>,
    wire: WireAdapter,
) -> bool {
    match command {
        RouterCommand::Subscribe {
            id,
            thread_id,
            mailbox,
            ack,
        } => {
            prune_subscribers(routes);
            let subscriber_count = routes
                .values()
                .map(|route| route.subscribers.len())
                .fold(0_usize, usize::saturating_add);
            if subscriber_count >= CLIENT_SUBSCRIBER_CAPACITY {
                let _ = ack.send(None);
                return true;
            }
            let Some(route) = route_for_update(routes, &thread_id) else {
                let _ = ack.send(None);
                return true;
            };
            if route.subscribers.len() >= THREAD_SUBSCRIBER_CAPACITY {
                let _ = ack.send(None);
                return true;
            }
            let projection = Arc::clone(&route.projection);
            let (thread_started, invalidated) = {
                let projection = lock_unpoison(&route.projection);
                (projection.thread_started, projection.invalidated)
            };
            if let Some(reason) = invalidated {
                mailbox.invalidate(thread_id.clone(), reason);
            } else {
                if thread_started {
                    let _ = mailbox.push_terminal(AppServerEvent::ThreadStarted {
                        thread_id: thread_id.clone(),
                    });
                }
                route.subscribers.push(Subscriber {
                    id,
                    mailbox: Arc::downgrade(&mailbox),
                });
            }
            if ack.send(Some(projection)).is_err() {
                route.subscribers.retain(|subscriber| subscriber.id != id);
            }
            true
        }
        RouterCommand::Unsubscribe { id, thread_id } => {
            if let Some(route) = routes.get_mut(&thread_id) {
                route.subscribers.retain(|subscriber| subscriber.id != id);
            }
            true
        }
        RouterCommand::ObserveTurnStarted {
            thread_id,
            attempt_id,
            turn,
            ack,
            _budget,
        } => {
            let accepted = route_turn_started(&thread_id, (*turn).clone(), routes);
            let keep_open = finish_turn_start(&thread_id, attempt_id, routes, control_tx, wire);
            let _ = ack.send(accepted.then_some(turn));
            keep_open
        }
        RouterCommand::BeginTurnStart {
            thread_id,
            attempt_id,
            ack,
        } => {
            let accepted = route_for_update(routes, &thread_id).is_some_and(|route| {
                if route.pending_turn_start.is_some() {
                    false
                } else {
                    route.pending_turn_start = Some(attempt_id);
                    route.defer_turn_notifications = true;
                    true
                }
            });
            if ack.send(accepted).is_err() && accepted {
                finish_turn_start(&thread_id, attempt_id, routes, control_tx, wire)
            } else {
                true
            }
        }
        RouterCommand::AbortTurnStart {
            thread_id,
            attempt_id,
        } => finish_turn_start(&thread_id, attempt_id, routes, control_tx, wire),
        RouterCommand::ObserveThreadStarted {
            thread_id,
            ack,
            _budget,
        } => {
            let accepted = route_thread_started(&thread_id, routes);
            let _ = ack.send(accepted);
            true
        }
        RouterCommand::EnsureRoute { thread_id, ack } => {
            let accepted = route_for_update(routes, &thread_id).is_some();
            let _ = ack.send(accepted);
            true
        }
        RouterCommand::ReleaseRoute { thread_id, ack } => {
            prune_subscribers(routes);
            let releasable = routes.get(&thread_id).is_some_and(|route| {
                route.subscribers.is_empty()
                    && route.pending_turn_start.is_none()
                    && !route.defer_turn_notifications
                    && route.deferred_notifications.is_empty()
                    && lock_unpoison(&route.projection).active_turns.is_empty()
            });
            if releasable {
                routes.remove(&thread_id);
            }
            let _ = ack.send(releasable);
            true
        }
    }
}

fn route_for_update<'a>(
    routes: &'a mut HashMap<ThreadId, ThreadRoute>,
    thread_id: &ThreadId,
) -> Option<&'a mut ThreadRoute> {
    if thread_id.as_str().is_empty() || thread_id.as_str().len() > ROUTING_ID_BYTE_LIMIT {
        return None;
    }
    prune_subscribers(routes);
    if routes.contains_key(thread_id) {
        let invalidated = routes
            .get(thread_id)
            .is_some_and(|route| lock_unpoison(&route.projection).invalidated.is_some());
        if invalidated {
            return None;
        }
        return routes.get_mut(thread_id);
    }
    if routes.len() >= CLIENT_PROJECTION_CAPACITY {
        let evictable = routes.iter().find_map(|(candidate_id, route)| {
            let projection = lock_unpoison(&route.projection);
            let idle = route.subscribers.is_empty()
                && route.pending_turn_start.is_none()
                && !route.defer_turn_notifications
                && route.deferred_notifications.is_empty()
                && !projection.thread_started
                && projection.started_turns.is_empty()
                && projection.active_turns.is_empty()
                && projection.started_items.is_empty()
                && projection.completed_items.is_empty()
                && projection.usage.is_empty()
                && projection.outcomes.is_empty();
            idle.then(|| candidate_id.clone())
        });
        let evictable = evictable?;
        routes.remove(&evictable);
    }
    Some(routes.entry(thread_id.clone()).or_default())
}

fn prune_subscribers(routes: &mut HashMap<ThreadId, ThreadRoute>) {
    for route in routes.values_mut() {
        route.subscribers.retain(|subscriber| {
            subscriber
                .mailbox
                .upgrade()
                .is_some_and(|mailbox| !mailbox.is_closed())
        });
    }
}

#[allow(clippy::too_many_lines)]
fn route_notification(
    method: String,
    params: Option<Value>,
    routes: &mut HashMap<ThreadId, ThreadRoute>,
    control_tx: &mpsc::Sender<ControlEvent>,
    wire: WireAdapter,
) -> bool {
    let raw_thread_id = extract_thread_id(params.as_ref());
    if method != "turn/started" {
        if let Some(thread_id) = raw_thread_id.as_ref() {
            let weight = method
                .len()
                .saturating_add(params.as_ref().map_or(0, value_memory_weight))
                .saturating_add(128);
            let mut deferred = false;
            let mut overflow = false;
            if let Some(route) = routes.get_mut(thread_id) {
                if route.defer_turn_notifications {
                    deferred = true;
                    overflow = route.deferred_notifications.len() >= THREAD_EVENT_CAPACITY
                        || route.deferred_bytes.saturating_add(weight) > THREAD_MAILBOX_BYTE_BUDGET;
                    if !overflow {
                        route.deferred_bytes = route.deferred_bytes.saturating_add(weight);
                        route
                            .deferred_notifications
                            .push_back((method.clone(), params.clone()));
                    }
                }
            }
            if overflow {
                if let Some(route) = routes.get_mut(thread_id) {
                    invalidate_route(route, thread_id, SubscriptionInvalidation::Lagged);
                }
                let _ = control_tx.try_send(ControlEvent::InvalidNotification {
                    method: bounded_method(method.clone()),
                    authoritative: is_client_authoritative_notification(&method),
                });
                return !is_client_authoritative_notification(&method);
            }
            if deferred {
                return true;
            }
        }
    }
    match method.as_str() {
        "thread/started" => {
            match decode_notification(params, |value| wire.thread_started_notification(value)) {
                Ok(params) => {
                    let thread_id = ThreadId::from(params.thread.id);
                    route_thread_started(&thread_id, routes);
                }
                Err(_) => {
                    report_invalid_notification(method, false, raw_thread_id, routes, control_tx);
                }
            }
        }
        "turn/started" => match decode_notification(params, |value| {
            wire.turn_started_notification(value)
        }) {
            Ok(params) => {
                let thread_id = ThreadId::from(params.thread_id);
                route_turn_started(&thread_id, params.turn, routes);
                return stop_deferring_notifications(&thread_id, routes, control_tx, wire);
            }
            Err(_) => report_invalid_notification(method, false, raw_thread_id, routes, control_tx),
        },
        "item/started" => match decode_notification(params, |value| {
            wire.item_started_notification(value)
        }) {
            Ok(params) => {
                let thread_id = ThreadId::from(params.thread_id);
                route_item_started(
                    &thread_id,
                    TurnId::from(params.turn_id),
                    params.item,
                    routes,
                );
            }
            Err(_) => report_invalid_notification(method, false, raw_thread_id, routes, control_tx),
        },
        "item/agentMessage/delta" => {
            match decode_notification(params, |value| wire.agent_message_delta_notification(value))
            {
                Ok(params) => {
                    let thread_id = ThreadId::from(params.thread_id);
                    if valid_routing_id(thread_id.as_str())
                        && valid_routing_id(&params.turn_id)
                        && valid_routing_id(&params.item_id)
                    {
                        dispatch(
                            &thread_id,
                            &AppServerEvent::AgentMessageDelta {
                                turn_id: TurnId::from(params.turn_id),
                                item_id: params.item_id,
                                delta: params.delta,
                            },
                            routes,
                        );
                    } else {
                        report_invalid_notification(
                            method,
                            false,
                            Some(thread_id),
                            routes,
                            control_tx,
                        );
                    }
                }
                Err(_) => {
                    report_invalid_notification(method, false, raw_thread_id, routes, control_tx);
                }
            }
        }
        "item/commandExecution/outputDelta" => {
            match decode_notification(params, |value| {
                wire.command_output_delta_notification(value)
            }) {
                Ok(params) => {
                    let thread_id = ThreadId::from(params.thread_id);
                    if valid_routing_id(thread_id.as_str())
                        && valid_routing_id(&params.turn_id)
                        && valid_routing_id(&params.item_id)
                    {
                        dispatch(
                            &thread_id,
                            &AppServerEvent::CommandOutputDelta {
                                turn_id: TurnId::from(params.turn_id),
                                item_id: params.item_id,
                                delta: params.delta,
                            },
                            routes,
                        );
                    } else {
                        report_invalid_notification(
                            method,
                            false,
                            Some(thread_id),
                            routes,
                            control_tx,
                        );
                    }
                }
                Err(_) => {
                    report_invalid_notification(method, false, raw_thread_id, routes, control_tx);
                }
            }
        }
        "item/completed" => {
            if let Ok(params) =
                decode_notification(params, |value| wire.item_completed_notification(value))
            {
                if !route_item_completed(params, routes) {
                    report_routing_capacity(control_tx);
                    return false;
                }
            } else {
                report_invalid_notification(method, true, raw_thread_id, routes, control_tx);
                return false;
            }
        }
        "thread/tokenUsage/updated" => {
            match decode_notification(params, |value| wire.token_usage_updated_notification(value))
            {
                Ok(params) => route_usage(params, routes),
                Err(_) => {
                    report_invalid_notification(method, false, raw_thread_id, routes, control_tx);
                }
            }
        }
        "error" => {
            if let Ok(params) = decode_notification(params, |value| wire.error_notification(value))
            {
                if !route_error(params, routes) {
                    report_routing_capacity(control_tx);
                    return false;
                }
            } else {
                report_invalid_notification(method, true, raw_thread_id, routes, control_tx);
                return false;
            }
        }
        "turn/completed" => {
            if let Ok(params) =
                decode_notification(params, |value| wire.turn_completed_notification(value))
            {
                if !route_turn_completed(params, routes) {
                    report_routing_capacity(control_tx);
                    return false;
                }
            } else {
                report_invalid_notification(method, true, raw_thread_id, routes, control_tx);
                return false;
            }
        }
        _ if CONSUMED_NOTIFICATION_METHODS.contains(&method.as_str()) => {
            // The catalog and dispatch must change atomically. If a future edit
            // updates only the catalog, fail closed instead of treating a
            // bridge-owned notification as an opaque extension.
            report_invalid_notification(method, true, raw_thread_id, routes, control_tx);
            return false;
        }
        _ => {
            if let Some(thread_id) = extract_thread_id(params.as_ref()) {
                dispatch(
                    &thread_id,
                    &AppServerEvent::Unknown {
                        method: bounded_method(method),
                    },
                    routes,
                );
            } else {
                let _ = control_tx.try_send(ControlEvent::UnknownNotification {
                    method: bounded_method(method),
                });
            }
        }
    }
    true
}

fn route_error(params: ErrorNotification, routes: &mut HashMap<ThreadId, ThreadRoute>) -> bool {
    let thread_id = ThreadId::from(params.thread_id);
    let turn_id = TurnId::from(params.turn_id);
    let Some(route) = route_for_update(routes, &thread_id) else {
        return false;
    };
    if turn_id.as_str().len() > ROUTING_ID_BYTE_LIMIT {
        invalidate_route(route, &thread_id, SubscriptionInvalidation::ProtocolDrift);
        return true;
    }
    dispatch_terminal_to_route(
        route,
        &AppServerEvent::Error {
            turn_id,
            error: params.error,
            will_retry: params.will_retry,
        },
        &thread_id,
    );
    true
}

fn report_routing_capacity(control_tx: &mpsc::Sender<ControlEvent>) {
    let _ = control_tx.try_send(ControlEvent::InvalidNotification {
        method: "routing-capacity".to_owned(),
        authoritative: true,
    });
}

fn finish_turn_start(
    thread_id: &ThreadId,
    attempt_id: TurnStartAttemptId,
    routes: &mut HashMap<ThreadId, ThreadRoute>,
    control_tx: &mpsc::Sender<ControlEvent>,
    wire: WireAdapter,
) -> bool {
    let matching = routes
        .get(thread_id)
        .is_some_and(|route| route.pending_turn_start == Some(attempt_id));
    if !matching {
        return true;
    }
    if let Some(route) = routes.get_mut(thread_id) {
        route.pending_turn_start = None;
    }
    stop_deferring_notifications(thread_id, routes, control_tx, wire)
}

fn stop_deferring_notifications(
    thread_id: &ThreadId,
    routes: &mut HashMap<ThreadId, ThreadRoute>,
    control_tx: &mpsc::Sender<ControlEvent>,
    wire: WireAdapter,
) -> bool {
    let deferred = routes.get_mut(thread_id).map(|route| {
        route.defer_turn_notifications = false;
        route.deferred_bytes = 0;
        std::mem::take(&mut route.deferred_notifications)
    });
    if let Some(deferred) = deferred {
        for (method, params) in deferred {
            if !route_notification(method, params, routes, control_tx, wire) {
                return false;
            }
        }
    }
    true
}

fn bounded_method(method: String) -> String {
    if method.len() <= ROUTING_ID_BYTE_LIMIT {
        method
    } else {
        "oversized-method".to_owned()
    }
}

fn valid_routing_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= ROUTING_ID_BYTE_LIMIT
}

fn is_client_authoritative_notification(method: &str) -> bool {
    matches!(method, "item/completed" | "turn/completed" | "error")
}

fn report_invalid_notification(
    method: String,
    authoritative: bool,
    thread_id: Option<ThreadId>,
    routes: &mut HashMap<ThreadId, ThreadRoute>,
    control_tx: &mpsc::Sender<ControlEvent>,
) {
    let method = bounded_method(method);
    if authoritative {
        if let Some(thread_id) = thread_id {
            if let Some(route) = routes.get_mut(&thread_id) {
                invalidate_route(route, &thread_id, SubscriptionInvalidation::ProtocolDrift);
            }
        }
    }
    let _ = control_tx.try_send(ControlEvent::InvalidNotification {
        method,
        authoritative,
    });
}

fn decode_notification<T>(
    params: Option<Value>,
    decode: impl FnOnce(Value) -> Result<T, crate::codex::compat::CompatError>,
) -> Result<T, crate::codex::compat::CompatError> {
    decode(params.unwrap_or(Value::Null))
}

fn extract_thread_id(params: Option<&Value>) -> Option<ThreadId> {
    params?
        .get("threadId")
        .and_then(Value::as_str)
        .map(ThreadId::from)
}

fn route_thread_started(thread_id: &ThreadId, routes: &mut HashMap<ThreadId, ThreadRoute>) -> bool {
    let Some(route) = route_for_update(routes, thread_id) else {
        return false;
    };
    let should_dispatch = {
        let mut projection = lock_unpoison(&route.projection);
        if projection.invalidated.is_some() || projection.thread_started {
            false
        } else {
            projection.thread_started = true;
            true
        }
    };
    if should_dispatch {
        dispatch_terminal_to_route(
            route,
            &AppServerEvent::ThreadStarted {
                thread_id: thread_id.clone(),
            },
            thread_id,
        );
    }
    true
}

fn route_turn_started(
    thread_id: &ThreadId,
    turn: Turn,
    routes: &mut HashMap<ThreadId, ThreadRoute>,
) -> bool {
    let Some(route) = route_for_update(routes, thread_id) else {
        return false;
    };
    let turn_id = TurnId::from(turn.id.as_str());
    let (should_dispatch, invalid) = {
        let mut projection = lock_unpoison(&route.projection);
        if turn_id.as_str().len() > ROUTING_ID_BYTE_LIMIT
            || (projection.started_turns.len() >= THREAD_TERMINAL_CAPACITY
                && !projection.started_turns.contains(&turn_id))
        {
            (false, true)
        } else if projection.invalidated.is_some() || projection.outcomes.contains_key(&turn_id) {
            (false, false)
        } else {
            let inserted = projection.started_turns.insert(turn_id.clone());
            projection.active_turns.insert(turn_id);
            (inserted, false)
        }
    };
    if invalid {
        invalidate_route(route, thread_id, SubscriptionInvalidation::ProtocolDrift);
        return false;
    }
    if should_dispatch {
        dispatch_terminal_to_route(route, &AppServerEvent::TurnStarted { turn }, thread_id);
    }
    true
}

fn route_item_started(
    thread_id: &ThreadId,
    turn_id: TurnId,
    item: ThreadItem,
    routes: &mut HashMap<ThreadId, ThreadRoute>,
) {
    let Some(route) = route_for_update(routes, thread_id) else {
        return;
    };
    let (should_dispatch, invalid) = match item.id() {
        Some(item_id) => {
            let mut projection = lock_unpoison(&route.projection);
            if item_id.len() > ROUTING_ID_BYTE_LIMIT
                || turn_id.as_str().len() > ROUTING_ID_BYTE_LIMIT
                || (projection.started_items.len() >= THREAD_EVENT_CAPACITY
                    && !projection
                        .started_items
                        .contains(&(turn_id.clone(), item_id.to_owned())))
            {
                (false, true)
            } else {
                (
                    projection
                        .started_items
                        .insert((turn_id.clone(), item_id.to_owned())),
                    false,
                )
            }
        }
        None => (true, false),
    };
    if invalid {
        invalidate_route(route, thread_id, SubscriptionInvalidation::ProtocolDrift);
        return;
    }
    if should_dispatch {
        dispatch_to_route(route, &AppServerEvent::ItemStarted { turn_id, item });
    }
}

fn route_item_completed(
    params: ItemCompletedNotification,
    routes: &mut HashMap<ThreadId, ThreadRoute>,
) -> bool {
    let thread_id = ThreadId::from(params.thread_id);
    let turn_id = TurnId::from(params.turn_id);
    let Some(route) = route_for_update(routes, &thread_id) else {
        return false;
    };
    let (changed, invalid) = {
        let mut projection = lock_unpoison(&route.projection);
        if projection.outcomes.contains_key(&turn_id) {
            return true;
        }
        let new_turn = !projection.completed_items.contains_key(&turn_id);
        if new_turn && projection.completed_items.len() >= THREAD_TERMINAL_CAPACITY {
            (false, true)
        } else {
            let (upsert, previous_bytes, retained_bytes, item_count) = {
                let items = projection
                    .completed_items
                    .entry(turn_id.clone())
                    .or_default();
                let previous_bytes = items.retained_bytes();
                let upsert = items.upsert(&params.item);
                (upsert, previous_bytes, items.retained_bytes(), items.len())
            };
            projection.completed_id_bytes = projection
                .completed_id_bytes
                .saturating_sub(previous_bytes)
                .saturating_add(retained_bytes);
            let invalid = upsert == ItemUpsert::Conflict
                || turn_id.as_str().len() > ROUTING_ID_BYTE_LIMIT
                || item_count > THREAD_EVENT_CAPACITY
                || projection.completed_id_bytes > THREAD_PROJECTION_BYTE_BUDGET;
            (upsert == ItemUpsert::New, invalid)
        }
    };
    if invalid {
        invalidate_route(route, &thread_id, SubscriptionInvalidation::ProtocolDrift);
        return true;
    }
    if changed {
        dispatch_terminal_to_route(
            route,
            &AppServerEvent::ItemCompleted {
                turn_id,
                item: params.item,
            },
            &thread_id,
        );
    }
    true
}

fn route_usage(
    params: ThreadTokenUsageUpdatedNotification,
    routes: &mut HashMap<ThreadId, ThreadRoute>,
) {
    let thread_id = ThreadId::from(params.thread_id);
    let turn_id = TurnId::from(params.turn_id);
    let usage = params.token_usage.last;
    let Some(route) = route_for_update(routes, &thread_id) else {
        return;
    };
    if turn_id.as_str().len() > ROUTING_ID_BYTE_LIMIT {
        invalidate_route(route, &thread_id, SubscriptionInvalidation::ProtocolDrift);
        return;
    }
    let invalid = {
        let mut projection = lock_unpoison(&route.projection);
        if projection.outcomes.contains_key(&turn_id) {
            false
        } else if !projection.usage.contains_key(&turn_id)
            && projection.usage.len() >= THREAD_TERMINAL_CAPACITY
        {
            true
        } else {
            projection.usage.insert(turn_id.clone(), usage.clone());
            false
        }
    };
    if invalid {
        invalidate_route(route, &thread_id, SubscriptionInvalidation::ProtocolDrift);
        return;
    }
    dispatch_to_route(route, &AppServerEvent::TokenUsageUpdated { turn_id, usage });
}

fn route_turn_completed(
    params: TurnCompletedNotification,
    routes: &mut HashMap<ThreadId, ThreadRoute>,
) -> bool {
    let thread_id = ThreadId::from(params.thread_id);
    let turn_id = TurnId::from(params.turn.id.clone());
    let terminal_fingerprint = TerminalFingerprint(serialized_fingerprint(&params.turn));
    let Some(route) = route_for_update(routes, &thread_id) else {
        return false;
    };
    if turn_id.as_str().len() > ROUTING_ID_BYTE_LIMIT {
        invalidate_route(route, &thread_id, SubscriptionInvalidation::ProtocolDrift);
        return true;
    }
    let outcome = {
        let mut projection = lock_unpoison(&route.projection);
        if let Some(previous) = projection.terminal_fingerprints.get(&turn_id) {
            let conflict = previous.0 != terminal_fingerprint.0;
            drop(projection);
            if conflict {
                invalidate_route(route, &thread_id, SubscriptionInvalidation::ProtocolDrift);
            }
            return true;
        }
        let outcome = TurnOutcome {
            thread_id: thread_id.clone(),
            turn_id: turn_id.clone(),
            status: params.turn.status,
            error: params.turn.error,
            completed_items: params.turn.items,
            token_usage: projection.usage.remove(&turn_id),
        };
        let weight = outcome_memory_weight(&outcome);
        if weight > THREAD_PROJECTION_BYTE_BUDGET {
            drop(projection);
            invalidate_route(route, &thread_id, SubscriptionInvalidation::ProtocolDrift);
            return true;
        }
        if let Some(completed) = projection.completed_items.remove(&turn_id) {
            projection.completed_id_bytes = projection
                .completed_id_bytes
                .saturating_sub(completed.retained_bytes());
        }
        projection.started_turns.remove(&turn_id);
        projection.active_turns.remove(&turn_id);
        projection
            .started_items
            .retain(|(started_turn, _)| started_turn != &turn_id);
        while projection.outcomes.len() >= THREAD_OUTCOME_CAPACITY
            || projection.outcome_bytes.saturating_add(weight) > THREAD_PROJECTION_BYTE_BUDGET
        {
            let Some(oldest) = projection.outcome_order.pop_front() else {
                break;
            };
            projection.outcomes.remove(&oldest);
            projection.terminal_fingerprints.remove(&oldest);
            if let Some(old_weight) = projection.outcome_weights.remove(&oldest) {
                projection.outcome_bytes = projection.outcome_bytes.saturating_sub(old_weight);
            }
        }
        projection.outcome_bytes = projection.outcome_bytes.saturating_add(weight);
        projection.outcome_order.push_back(turn_id.clone());
        projection.outcome_weights.insert(turn_id.clone(), weight);
        projection
            .terminal_fingerprints
            .insert(turn_id.clone(), terminal_fingerprint);
        projection.outcomes.insert(turn_id, outcome.clone());
        outcome
    };
    dispatch_terminal_to_route(route, &AppServerEvent::TurnCompleted(outcome), &thread_id);
    true
}

fn dispatch(
    thread_id: &ThreadId,
    event: &AppServerEvent,
    routes: &mut HashMap<ThreadId, ThreadRoute>,
) {
    if let Some(route) = routes.get_mut(thread_id) {
        dispatch_to_route(route, event);
    }
}

fn dispatch_to_route(route: &mut ThreadRoute, event: &AppServerEvent) {
    route.subscribers.retain(|subscriber| {
        let Some(mailbox) = subscriber.mailbox.upgrade() else {
            return false;
        };
        if mailbox.is_closed() {
            false
        } else {
            mailbox.push_lossy(event.clone());
            true
        }
    });
}

fn dispatch_terminal_to_route(
    route: &mut ThreadRoute,
    event: &AppServerEvent,
    thread_id: &ThreadId,
) {
    let mut lagged = Vec::new();
    route.subscribers.retain(|subscriber| {
        let Some(mailbox) = subscriber.mailbox.upgrade() else {
            return false;
        };
        if mailbox.is_closed() {
            false
        } else if mailbox.push_terminal(event.clone()) {
            true
        } else {
            lagged.push(mailbox);
            false
        }
    });
    for mailbox in lagged {
        mailbox.invalidate(thread_id.clone(), SubscriptionInvalidation::Lagged);
    }
}

fn invalidate_route(
    route: &mut ThreadRoute,
    thread_id: &ThreadId,
    reason: SubscriptionInvalidation,
) {
    {
        let mut projection = lock_unpoison(&route.projection);
        projection.invalidated = Some(reason);
        projection.started_turns.clear();
        projection.active_turns.clear();
        projection.started_items.clear();
        projection.completed_items.clear();
        projection.completed_id_bytes = 0;
        projection.usage.clear();
    }
    for subscriber in route.subscribers.drain(..) {
        if let Some(mailbox) = subscriber.mailbox.upgrade() {
            mailbox.invalidate(thread_id.clone(), reason);
        }
    }
}

fn close_subscribers(routes: &mut HashMap<ThreadId, ThreadRoute>, exit: TransportExit) {
    for (thread_id, route) in routes.iter() {
        for subscriber in &route.subscribers {
            if let Some(mailbox) = subscriber.mailbox.upgrade() {
                if mailbox.push_terminal(AppServerEvent::ConnectionClosed { exit }) {
                    mailbox.close_after_drain();
                } else {
                    mailbox.invalidate(thread_id.clone(), SubscriptionInvalidation::Lagged);
                }
            }
        }
    }
    routes.clear();
}

struct Mailbox {
    state: Mutex<MailboxState>,
    notify: tokio::sync::Notify,
}

#[derive(Default)]
struct MailboxState {
    events: VecDeque<QueuedEvent>,
    terminal_count: usize,
    regular_count: usize,
    queued_bytes: usize,
    closed: bool,
}

struct QueuedEvent {
    byte_len: usize,
    terminal: bool,
    event: AppServerEvent,
}

impl Mailbox {
    fn new() -> Self {
        Self {
            state: Mutex::new(MailboxState::default()),
            notify: tokio::sync::Notify::new(),
        }
    }

    async fn recv(&self) -> Option<AppServerEvent> {
        loop {
            let notified = self.notify.notified();
            {
                let mut state = lock_unpoison(&self.state);
                if let Some(queued) = state.events.pop_front() {
                    state.queued_bytes = state.queued_bytes.saturating_sub(queued.byte_len);
                    if queued.terminal {
                        state.terminal_count = state.terminal_count.saturating_sub(1);
                    } else {
                        state.regular_count = state.regular_count.saturating_sub(1);
                    }
                    return Some(queued.event);
                }
                if state.closed {
                    return None;
                }
            }
            notified.await;
        }
    }

    fn push_terminal(&self, event: AppServerEvent) -> bool {
        let mut state = lock_unpoison(&self.state);
        if state.closed || state.terminal_count >= THREAD_TERMINAL_CAPACITY {
            return false;
        }
        let byte_len = event_memory_weight(&event);
        if state.queued_bytes.saturating_add(byte_len) > THREAD_MAILBOX_BYTE_BUDGET {
            return false;
        }
        state.queued_bytes = state.queued_bytes.saturating_add(byte_len);
        state.terminal_count = state.terminal_count.saturating_add(1);
        state.events.push_back(QueuedEvent {
            byte_len,
            terminal: true,
            event,
        });
        drop(state);
        self.notify.notify_one();
        true
    }

    fn push_lossy(&self, event: AppServerEvent) {
        let mut state = lock_unpoison(&self.state);
        if state.closed {
            return;
        }
        let byte_len = event_memory_weight(&event);
        let is_delta = delta_stream(&event).is_some();
        if is_delta && byte_len > THREAD_DELTA_BYTE_LIMIT {
            return;
        }
        if is_delta {
            let merged_weight = state.events.back().and_then(|queued| {
                if queued.terminal || !same_delta_stream(&queued.event, &event) {
                    None
                } else {
                    Some(queued.byte_len.saturating_add(delta_text_len(&event)))
                }
            });
            if let Some(merged_weight) = merged_weight {
                let previous_weight = state.events.back().map_or(0, |queued| queued.byte_len);
                if merged_weight <= THREAD_DELTA_BYTE_LIMIT
                    && state
                        .queued_bytes
                        .saturating_sub(previous_weight)
                        .saturating_add(merged_weight)
                        <= THREAD_MAILBOX_BYTE_BUDGET
                {
                    let updated_weight = {
                        let queued = state.events.back_mut().expect("back was checked");
                        if merge_delta(&mut queued.event, event, THREAD_DELTA_BYTE_LIMIT) {
                            queued.byte_len = event_memory_weight(&queued.event);
                            Some(queued.byte_len)
                        } else {
                            None
                        }
                    };
                    if let Some(updated_weight) = updated_weight {
                        state.queued_bytes = state
                            .queued_bytes
                            .saturating_sub(previous_weight)
                            .saturating_add(updated_weight);
                    }
                }
                drop(state);
                self.notify.notify_one();
                return;
            }
        }
        if state.regular_count >= THREAD_EVENT_CAPACITY
            || state.queued_bytes.saturating_add(byte_len) > THREAD_MAILBOX_BYTE_BUDGET
        {
            return;
        }
        state.queued_bytes = state.queued_bytes.saturating_add(byte_len);
        state.regular_count = state.regular_count.saturating_add(1);
        state.events.push_back(QueuedEvent {
            byte_len,
            terminal: false,
            event,
        });
        drop(state);
        self.notify.notify_one();
    }

    fn close(&self) {
        lock_unpoison(&self.state).closed = true;
        self.notify.notify_waiters();
    }

    fn close_after_drain(&self) {
        lock_unpoison(&self.state).closed = true;
        self.notify.notify_waiters();
    }

    fn is_closed(&self) -> bool {
        lock_unpoison(&self.state).closed
    }

    fn invalidate(&self, thread_id: ThreadId, reason: SubscriptionInvalidation) {
        let mut state = lock_unpoison(&self.state);
        state.events.clear();
        state.terminal_count = 0;
        state.regular_count = 0;
        state.queued_bytes = 0;
        let event = AppServerEvent::SubscriptionInvalidated { thread_id, reason };
        let byte_len = event_memory_weight(&event);
        state.events.push_back(QueuedEvent {
            byte_len,
            terminal: true,
            event,
        });
        state.terminal_count = 1;
        state.queued_bytes = byte_len;
        state.closed = true;
        drop(state);
        self.notify.notify_waiters();
    }
}

fn event_memory_weight(event: &AppServerEvent) -> usize {
    const FIXED: usize = 128;
    match event {
        AppServerEvent::AgentMessageDelta {
            turn_id,
            item_id,
            delta,
        }
        | AppServerEvent::CommandOutputDelta {
            turn_id,
            item_id,
            delta,
        } => FIXED
            .saturating_add(turn_id.as_str().len())
            .saturating_add(item_id.len())
            .saturating_add(delta.len()),
        AppServerEvent::Unknown { method } => FIXED.saturating_add(method.len()),
        AppServerEvent::ThreadStarted { thread_id } => {
            FIXED.saturating_add(thread_id.as_str().len())
        }
        AppServerEvent::TurnStarted { turn } => {
            FIXED.saturating_add(typed_memory_weight(turn, THREAD_MAILBOX_BYTE_BUDGET))
        }
        AppServerEvent::ItemStarted { turn_id, item }
        | AppServerEvent::ItemCompleted { turn_id, item } => FIXED
            .saturating_add(turn_id.as_str().len())
            .saturating_add(typed_memory_weight(item, THREAD_MAILBOX_BYTE_BUDGET)),
        AppServerEvent::TokenUsageUpdated { turn_id, .. } => {
            FIXED.saturating_add(turn_id.as_str().len())
        }
        AppServerEvent::TurnCompleted(outcome) => outcome_memory_weight(outcome),
        AppServerEvent::Error { turn_id, error, .. } => FIXED
            .saturating_add(turn_id.as_str().len())
            .saturating_add(typed_memory_weight(error, THREAD_MAILBOX_BYTE_BUDGET)),
        _ => FIXED,
    }
}

fn server_request_memory_weight(request: &ServerRequest) -> usize {
    256_usize
        .saturating_add(request.method.len())
        .saturating_add(serialized_memory_weight(
            request.id(),
            CLIENT_CONTROL_EVENT_BYTE_LIMIT,
        ))
        .saturating_add(request.params.as_ref().map_or(0, value_memory_weight))
}

fn byte_permits(bytes: usize) -> u32 {
    u32::try_from(bytes.max(1)).unwrap_or(u32::MAX)
}

fn outcome_memory_weight(outcome: &TurnOutcome) -> usize {
    let mut weight = 256_usize
        .saturating_add(outcome.thread_id.as_str().len())
        .saturating_add(outcome.turn_id.as_str().len())
        .saturating_add(match &outcome.status {
            TurnStatus::Unknown(value) => value.len().saturating_add(32),
            _ => 24,
        });
    if let Some(error) = &outcome.error {
        weight = weight.saturating_add(typed_memory_weight(error, THREAD_PROJECTION_BYTE_BUDGET));
    }
    outcome.completed_items.iter().fold(weight, |total, item| {
        total.saturating_add(typed_memory_weight(item, THREAD_PROJECTION_BYTE_BUDGET))
    })
}

fn serialized_memory_weight<T>(value: &T, maximum: usize) -> usize
where
    T: Serialize + ?Sized,
{
    let mut counter = ByteCounter { count: 0, maximum };
    serde_json::to_writer(&mut counter, value).map_or(maximum.saturating_add(1), |()| counter.count)
}

fn typed_memory_weight<T>(value: &T, maximum: usize) -> usize
where
    T: Serialize + ?Sized,
{
    let wire = serialized_memory_weight(value, maximum);
    if wire > maximum {
        return wire;
    }
    match serde_json::to_value(value) {
        Ok(value) => value_memory_weight(&value).max(wire),
        Err(_) => maximum.saturating_add(1),
    }
}

struct ByteCounter {
    count: usize,
    maximum: usize,
}

impl io::Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.count = self.count.saturating_add(bytes.len());
        if self.count > self.maximum {
            Err(io::Error::other("serialized value exceeds client budget"))
        } else {
            Ok(bytes.len())
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn serialized_fingerprint<T>(item: &T) -> u64
where
    T: Serialize + ?Sized,
{
    let mut writer = HashWriter(DefaultHasher::new());
    if serde_json::to_writer(&mut writer, item).is_err() {
        writer.0.write(b"serialization-error");
    }
    writer.0.finish()
}

struct HashWriter(DefaultHasher);

impl io::Write for HashWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.write(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn delta_stream(event: &AppServerEvent) -> Option<(&TurnId, &str, bool)> {
    match event {
        AppServerEvent::AgentMessageDelta {
            turn_id, item_id, ..
        } => Some((turn_id, item_id, false)),
        AppServerEvent::CommandOutputDelta {
            turn_id, item_id, ..
        } => Some((turn_id, item_id, true)),
        _ => None,
    }
}

fn same_delta_stream(left: &AppServerEvent, right: &AppServerEvent) -> bool {
    delta_stream(left) == delta_stream(right)
}

fn delta_text_len(event: &AppServerEvent) -> usize {
    match event {
        AppServerEvent::AgentMessageDelta { delta, .. }
        | AppServerEvent::CommandOutputDelta { delta, .. } => delta.len(),
        _ => 0,
    }
}

fn merge_delta(existing: &mut AppServerEvent, incoming: AppServerEvent, maximum: usize) -> bool {
    match (existing, incoming) {
        (
            AppServerEvent::AgentMessageDelta { delta, .. },
            AppServerEvent::AgentMessageDelta {
                delta: incoming, ..
            },
        )
        | (
            AppServerEvent::CommandOutputDelta { delta, .. },
            AppServerEvent::CommandOutputDelta {
                delta: incoming, ..
            },
        ) => {
            if delta.len().saturating_add(incoming.len()) > maximum {
                return false;
            }
            delta.push_str(&incoming);
            true
        }
        _ => false,
    }
}

fn lock_unpoison<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
