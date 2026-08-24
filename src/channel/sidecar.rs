//! Supervised Node channel sidecar.
//!
//! Credentials cross only the child's stdin in its initial configuration
//! frame. Stdout is reserved for bounded NDJSON, while stderr is drained but
//! never copied into tracing. Every inbound event keeps its correlation id
//! until the Rust durable handler returns a positive or negative ack.

use std::collections::HashSet;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
#[cfg(windows)]
use process_wrap::tokio::JobObject;
#[cfg(unix)]
use process_wrap::tokio::ProcessGroup;
use process_wrap::tokio::{KillOnDrop, TokioChildWrapper, TokioCommandWrap};
use secrecy::ExposeSecret as _;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdout, Command};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, timeout, timeout_at};
use tokio_util::codec::{FramedRead, LinesCodec};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::wire::{PROTOCOL, REQUIRED_CAPABILITIES, VERSION};
use super::{ConnectionState, InboundSource};
use crate::codex::supervisor::AppServerSupervisor;
use crate::lark::bridge::InboundEventHandler;
use crate::lark::credentials::LarkCredentials;
use crate::lark::error::LarkError;
use crate::limits::{
    CHANNEL_SIDECAR_ACK_GRACE, CHANNEL_SIDECAR_CONNECT_TIMEOUT, CHANNEL_SIDECAR_EVENT_CAPACITY,
    CHANNEL_SIDECAR_FRAME_BYTES, CHANNEL_SIDECAR_HANDLER_TIMEOUT,
    CHANNEL_SIDECAR_HANDSHAKE_TIMEOUT, CHANNEL_SIDECAR_HEALTHY_UPTIME,
    CHANNEL_SIDECAR_SHUTDOWN_GRACE, CHANNEL_SIDECAR_WRITE_CAPACITY,
};

/// Bounded process and wire settings. Paths are never printed by `Debug`.
#[derive(Clone)]
pub struct NodeSidecarConfig {
    /// Node executable. A single component is resolved by the OS `PATH`.
    pub node_binary: PathBuf,
    /// Checked-in sidecar entrypoint.
    pub entrypoint: PathBuf,
    /// Non-secret adapter arguments (primarily useful for fake-sidecar tests).
    pub arguments: Vec<String>,
    /// Maximum bytes before the newline of one frame.
    pub max_frame_bytes: usize,
    /// Events waiting for the durable handler.
    pub event_capacity: usize,
    /// Frames waiting for child stdin.
    pub write_capacity: usize,
    /// Initial hello/configure deadline.
    pub handshake_timeout: Duration,
    /// Deadline for the SDK to report its first live connection after it
    /// accepts configuration.
    pub initial_connect_timeout: Duration,
    /// Continuous connected time required before restart backoff resets.
    pub healthy_uptime: Duration,
    /// Deadline for one durable event decision.
    pub handler_timeout: Duration,
    /// Grace for a correlated shutdown response and process exit.
    pub shutdown_grace: Duration,
}

impl Default for NodeSidecarConfig {
    fn default() -> Self {
        Self {
            node_binary: PathBuf::from("node"),
            entrypoint: PathBuf::from("sidecar/index.cjs"),
            arguments: Vec::new(),
            max_frame_bytes: CHANNEL_SIDECAR_FRAME_BYTES,
            event_capacity: CHANNEL_SIDECAR_EVENT_CAPACITY,
            write_capacity: CHANNEL_SIDECAR_WRITE_CAPACITY,
            handshake_timeout: CHANNEL_SIDECAR_HANDSHAKE_TIMEOUT,
            initial_connect_timeout: CHANNEL_SIDECAR_CONNECT_TIMEOUT,
            healthy_uptime: CHANNEL_SIDECAR_HEALTHY_UPTIME,
            handler_timeout: CHANNEL_SIDECAR_HANDLER_TIMEOUT,
            shutdown_grace: CHANNEL_SIDECAR_SHUTDOWN_GRACE,
        }
    }
}

impl fmt::Debug for NodeSidecarConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NodeSidecarConfig")
            .field("node_binary", &"[configured]")
            .field("entrypoint", &"[configured]")
            .field("argument_count", &self.arguments.len())
            .field("max_frame_bytes", &self.max_frame_bytes)
            .field("event_capacity", &self.event_capacity)
            .field("write_capacity", &self.write_capacity)
            .field("handshake_timeout", &self.handshake_timeout)
            .field("initial_connect_timeout", &self.initial_connect_timeout)
            .field("healthy_uptime", &self.healthy_uptime)
            .field("handler_timeout", &self.handler_timeout)
            .field("shutdown_grace", &self.shutdown_grace)
            .finish()
    }
}

impl NodeSidecarConfig {
    fn validate(&self) -> Result<(), LarkError> {
        if self.node_binary.as_os_str().is_empty() || self.entrypoint.as_os_str().is_empty() {
            return Err(LarkError::protocol(
                "node sidecar executable paths must be configured",
            ));
        }
        if self.arguments.len() > 8
            || self
                .arguments
                .iter()
                .any(|argument| argument.len() > 1024 || argument.contains('\0'))
        {
            return Err(LarkError::protocol("node sidecar arguments are invalid"));
        }
        if self.max_frame_bytes == 0 || self.max_frame_bytes > CHANNEL_SIDECAR_FRAME_BYTES {
            return Err(LarkError::exhausted(
                "node sidecar frame bound",
                u64::try_from(CHANNEL_SIDECAR_FRAME_BYTES).unwrap_or(u64::MAX),
            ));
        }
        if self.event_capacity == 0
            || self.event_capacity > CHANNEL_SIDECAR_EVENT_CAPACITY
            || self.write_capacity == 0
            || self.write_capacity > CHANNEL_SIDECAR_WRITE_CAPACITY
        {
            return Err(LarkError::protocol("node sidecar queue bounds are invalid"));
        }
        if self.handshake_timeout.is_zero()
            || self.initial_connect_timeout.is_zero()
            || self.healthy_uptime.is_zero()
            || self.handler_timeout.is_zero()
            || self.shutdown_grace.is_zero()
            || self.handshake_timeout > CHANNEL_SIDECAR_HANDSHAKE_TIMEOUT
            || self.initial_connect_timeout > CHANNEL_SIDECAR_CONNECT_TIMEOUT
            || self.healthy_uptime > CHANNEL_SIDECAR_HEALTHY_UPTIME
            || self.handler_timeout > CHANNEL_SIDECAR_HANDLER_TIMEOUT
            || self.shutdown_grace > CHANNEL_SIDECAR_SHUTDOWN_GRACE
        {
            return Err(LarkError::protocol("node sidecar time bounds are invalid"));
        }
        Ok(())
    }
}

/// Entry point for the official-SDK sidecar.
pub struct NodeSidecar;

impl NodeSidecar {
    /// Starts supervision and waits until the first process completes the
    /// version/capability/configuration handshake and the SDK reports an
    /// established provider connection.
    ///
    /// # Errors
    ///
    /// Returns a static classification if the executable cannot start, the
    /// first handshake is malformed/incompatible, or configuration times out.
    pub async fn start(
        config: NodeSidecarConfig,
        credentials: LarkCredentials,
        handler: InboundEventHandler,
    ) -> Result<NodeSidecarHandle, LarkError> {
        config.validate()?;
        let shutdown_grace = config.shutdown_grace;
        let shutdown = CancellationToken::new();
        let (state_tx, state) = watch::channel(ConnectionState::Connecting { attempt: 1 });
        let (ready_tx, ready_rx) = oneshot::channel();
        let task = tokio::spawn(supervise(
            config,
            credentials,
            handler,
            state_tx,
            shutdown.clone(),
            ready_tx,
        ));
        let mut startup = StartupGuard {
            shutdown: shutdown.clone(),
            task: Some(task),
        };
        match ready_rx.await {
            Ok(Ok(())) => {
                let task = startup.task.take();
                Ok(NodeSidecarHandle {
                    state,
                    shutdown,
                    task,
                    shutdown_grace,
                })
            }
            Ok(Err(error)) => {
                shutdown.cancel();
                if let Some(task) = startup.task.take() {
                    let _ = task.await;
                }
                Err(error)
            }
            Err(_) => {
                shutdown.cancel();
                if let Some(task) = startup.task.take() {
                    let _ = task.await;
                }
                Err(LarkError::retryable("starting the node sidecar supervisor"))
            }
        }
    }
}

struct StartupGuard {
    shutdown: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl Drop for StartupGuard {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            self.shutdown.cancel();
            task.abort();
        }
    }
}

/// Supervised source handle.
pub struct NodeSidecarHandle {
    state: watch::Receiver<ConnectionState>,
    shutdown: CancellationToken,
    task: Option<JoinHandle<()>>,
    shutdown_grace: Duration,
}

impl fmt::Debug for NodeSidecarHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NodeSidecarHandle")
            .field("state", &*self.state.borrow())
            .finish_non_exhaustive()
    }
}

impl NodeSidecarHandle {
    /// Returns the latest lifecycle state.
    #[must_use]
    pub fn state(&self) -> ConnectionState {
        self.state.borrow().clone()
    }

    /// Subscribes to connection-state changes.
    #[must_use]
    pub fn subscribe_state(&self) -> watch::Receiver<ConnectionState> {
        self.state.clone()
    }

    /// Requests correlated graceful shutdown, then joins the supervisor.
    pub async fn shutdown(mut self) {
        self.shutdown.cancel();
        if let Some(mut task) = self.task.take() {
            let join_bound = self.shutdown_grace.saturating_mul(3);
            if timeout(join_bound, &mut task).await.is_err() {
                task.abort();
                let _ = task.await;
            }
        }
    }
}

impl InboundSource for NodeSidecarHandle {
    fn subscribe_state(&self) -> watch::Receiver<ConnectionState> {
        self.subscribe_state()
    }

    fn shutdown(self: Box<Self>) -> futures_util::future::BoxFuture<'static, ()> {
        async move { (*self).shutdown().await }.boxed()
    }
}

impl Drop for NodeSidecarHandle {
    fn drop(&mut self) {
        self.shutdown.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn supervise(
    config: NodeSidecarConfig,
    credentials: LarkCredentials,
    handler: InboundEventHandler,
    state: watch::Sender<ConnectionState>,
    shutdown: CancellationToken,
    ready: oneshot::Sender<Result<(), LarkError>>,
) {
    let mut ready = Some(ready);
    let mut failures = 0_u32;
    loop {
        if shutdown.is_cancelled() {
            publish(&state, ConnectionState::Stopped);
            return;
        }
        let attempt = failures.saturating_add(1);
        publish(&state, ConnectionState::Connecting { attempt });
        match ChildSession::start(&config, &credentials, Arc::clone(&handler), state.clone()).await
        {
            Ok(session) => {
                if let Some(ready) = ready.take() {
                    let _ = ready.send(Ok(()));
                }
                match session.run(&shutdown).await {
                    SessionEnd::Shutdown => {
                        publish(&state, ConnectionState::Stopped);
                        return;
                    }
                    SessionEnd::Crashed { was_healthy } => {
                        if was_healthy {
                            failures = 0;
                        }
                    }
                }
            }
            Err(error) => {
                if let Some(ready) = ready.take() {
                    let _ = ready.send(Err(error));
                    publish(
                        &state,
                        ConnectionState::Degraded {
                            reason: "node_sidecar_startup_failed".to_owned(),
                        },
                    );
                    return;
                }
                tracing::warn!("node sidecar restart attempt failed");
            }
        }
        failures = failures.saturating_add(1);
        let delay = AppServerSupervisor::retry_delay(0, failures);
        publish(
            &state,
            ConnectionState::Backoff {
                attempt: failures,
                delay,
            },
        );
        tokio::select! {
            () = shutdown.cancelled() => {
                publish(&state, ConnectionState::Stopped);
                return;
            }
            () = tokio::time::sleep(delay) => {}
        }
    }
}

fn publish(state: &watch::Sender<ConnectionState>, next: ConnectionState) {
    state.send_replace(next);
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SessionEnd {
    Crashed { was_healthy: bool },
    Shutdown,
}

/// The sidecar is always the leader of an owned POSIX process group or, on
/// Windows, an owned Job object. `start_kill` targets that whole ownership
/// boundary, including non-exec wrapper descendants.
struct OwnedChildGroup {
    child: Box<dyn TokioChildWrapper>,
}

impl OwnedChildGroup {
    fn new(child: Box<dyn TokioChildWrapper>) -> Self {
        Self { child }
    }

    fn inner(&mut self) -> &mut tokio::process::Child {
        self.child.inner_mut()
    }

    async fn wait_leader(&mut self) -> std::io::Result<std::process::ExitStatus> {
        self.child.inner_mut().wait().await
    }

    async fn terminate_and_reap(&mut self, grace: Duration) {
        // This is deliberately attempted even after the leader has exited:
        // descendants may still own the process group/Job and inherited pipes.
        let _ = self.child.start_kill();
        if timeout(grace, Box::into_pin(self.child.wait()))
            .await
            .is_err()
        {
            tracing::warn!("node sidecar process group did not reap within its bound");
            let _ = self.child.start_kill();
            let _ = timeout(grace, self.child.inner_mut().wait()).await;
        }
    }
}

impl Drop for OwnedChildGroup {
    fn drop(&mut self) {
        // Synchronous group/Job termination is the last-resort guarantee when
        // an owning async task is aborted or a public handle is dropped.
        let _ = self.child.start_kill();
    }
}

#[derive(Clone, Default)]
struct ActiveEventIds(Arc<Mutex<HashSet<String>>>);

impl ActiveEventIds {
    fn insert(&self, id: &str) -> Result<bool, LarkError> {
        self.0
            .lock()
            .map_err(|_| LarkError::protocol("locking node sidecar event correlations"))
            .map(|mut ids| ids.insert(id.to_owned()))
    }

    fn remove(&self, id: &str) {
        if let Ok(mut ids) = self.0.lock() {
            ids.remove(id);
        }
    }

    fn clear(&self) {
        if let Ok(mut ids) = self.0.lock() {
            ids.clear();
        }
    }
}

struct ChildSession {
    child: OwnedChildGroup,
    stdout: FramedRead<BufReader<ChildStdout>, LinesCodec>,
    writes: mpsc::Sender<Vec<u8>>,
    events: mpsc::Sender<PendingEvent>,
    writer: JoinHandle<()>,
    worker: JoinHandle<()>,
    stderr: JoinHandle<()>,
    active_ids: ActiveEventIds,
    state: watch::Sender<ConnectionState>,
    max_frame_bytes: usize,
    shutdown_grace: Duration,
    healthy_uptime: Duration,
}

impl ChildSession {
    async fn start(
        config: &NodeSidecarConfig,
        credentials: &LarkCredentials,
        handler: InboundEventHandler,
        state: watch::Sender<ConnectionState>,
    ) -> Result<Self, LarkError> {
        let mut command = Command::new(&config.node_binary);
        let search_path = std::env::var_os("PATH");
        command
            .arg(&config.entrypoint)
            .args(&config.arguments)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .env_clear()
            .env("NO_COLOR", "1");
        if let Some(search_path) = search_path {
            command.env("PATH", search_path);
        }
        #[cfg(windows)]
        for name in ["PATHEXT", "SystemRoot", "WINDIR"] {
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }
        let mut command = TokioCommandWrap::from(command);
        command.wrap(KillOnDrop);
        #[cfg(unix)]
        command.wrap(ProcessGroup::leader());
        #[cfg(windows)]
        command.wrap(JobObject);
        let child = command
            .spawn()
            .map_err(|_| LarkError::retryable("spawning the node sidecar"))?;
        let mut child = OwnedChildGroup::new(child);
        let (stdin, stdout, stderr_pipe) = {
            let inner = child.inner();
            (inner.stdin.take(), inner.stdout.take(), inner.stderr.take())
        };
        let (Some(stdin), Some(stdout), Some(stderr_pipe)) = (stdin, stdout, stderr_pipe) else {
            child.terminate_and_reap(config.shutdown_grace).await;
            return Err(LarkError::protocol(
                "node sidecar standard streams are unavailable",
            ));
        };
        let stdout = FramedRead::new(
            BufReader::new(stdout),
            LinesCodec::new_with_max_length(config.max_frame_bytes),
        );
        let (write_tx, write_rx) = mpsc::channel(config.write_capacity);
        let writer = tokio::spawn(write_loop(stdin, write_rx, config.max_frame_bytes));
        let stderr = tokio::spawn(drain_stderr(stderr_pipe));
        let (event_tx, event_rx) = mpsc::channel(config.event_capacity);
        let active_ids = ActiveEventIds::default();
        let worker = tokio::spawn(event_loop(
            event_rx,
            write_tx.clone(),
            handler,
            config.handler_timeout,
            config.max_frame_bytes,
            config.event_capacity,
            active_ids.clone(),
        ));

        let mut session = Self {
            child,
            stdout,
            writes: write_tx,
            events: event_tx,
            writer,
            worker,
            stderr,
            active_ids,
            state,
            max_frame_bytes: config.max_frame_bytes,
            shutdown_grace: config.shutdown_grace,
            healthy_uptime: config.healthy_uptime,
        };
        if let Err(error) = session.bootstrap(config, credentials).await {
            session.cleanup_failed_bootstrap().await;
            return Err(error);
        }
        Ok(session)
    }

    async fn bootstrap(
        &mut self,
        config: &NodeSidecarConfig,
        credentials: &LarkCredentials,
    ) -> Result<(), LarkError> {
        self.read_hello(config.handshake_timeout).await?;
        self.configure(config, credentials).await?;
        self.wait_until_connected(config.initial_connect_timeout)
            .await
    }

    async fn read_hello(&mut self, deadline: Duration) -> Result<(), LarkError> {
        let line = timeout(deadline, self.stdout.next())
            .await
            .map_err(|_| LarkError::retryable("waiting for node sidecar hello"))?
            .ok_or_else(|| LarkError::retryable("reading node sidecar hello"))?
            .map_err(|_| LarkError::protocol("node sidecar hello exceeds the frame bound"))?;
        let hello: HelloFrame = serde_json::from_str(&line)
            .map_err(|_| LarkError::protocol("decoding node sidecar hello"))?;
        hello.validate(self.max_frame_bytes)
    }

    async fn configure(
        &mut self,
        config: &NodeSidecarConfig,
        credentials: &LarkCredentials,
    ) -> Result<(), LarkError> {
        let id = new_id("configure");
        enqueue(
            &self.writes,
            &json!({
                "v": VERSION,
                "type": "configure",
                "id": id.clone(),
                "app_id": &credentials.app_id,
                "app_secret": credentials.app_secret.expose_secret(),
                "tenant": credentials.tenant.as_str(),
                "max_frame_bytes": config.max_frame_bytes,
                "max_in_flight": config.event_capacity,
                "ack_timeout_ms": duration_ms(
                    config.handler_timeout.saturating_add(CHANNEL_SIDECAR_ACK_GRACE)
                ),
            }),
            self.max_frame_bytes,
        )?;
        let line = timeout(config.handshake_timeout, self.stdout.next())
            .await
            .map_err(|_| LarkError::retryable("waiting for node sidecar configuration"))?
            .ok_or_else(|| LarkError::retryable("reading node sidecar configuration"))?
            .map_err(|_| {
                LarkError::protocol("node sidecar configuration response exceeds the frame bound")
            })?;
        let response: ResponseFrame = serde_json::from_str(&line)
            .map_err(|_| LarkError::protocol("decoding node sidecar configuration response"))?;
        response.validate(&id)
    }

    async fn wait_until_connected(&mut self, deadline: Duration) -> Result<(), LarkError> {
        timeout(deadline, async {
            loop {
                tokio::select! {
                    status = self.child.wait_leader() => {
                        if status.is_err() {
                            tracing::warn!("waiting for node sidecar bootstrap failed");
                        }
                        return Err(LarkError::retryable(
                            "establishing the initial node sidecar connection",
                        ));
                    }
                    line = self.stdout.next() => {
                        let line = line
                            .ok_or_else(|| LarkError::retryable(
                                "reading initial node sidecar connection state",
                            ))?
                            .map_err(|_| LarkError::protocol(
                                "node sidecar initial state exceeds the frame bound",
                            ))?;
                        match handle_line(
                            &line,
                            &self.state,
                            &self.writes,
                            &self.events,
                            &self.active_ids,
                            self.max_frame_bytes,
                        )? {
                            FrameEffect::Connected => return Ok(()),
                            FrameEffect::Failed | FrameEffect::Stopped => {
                                return Err(LarkError::retryable(
                                    "establishing the initial node sidecar connection",
                                ));
                            }
                            FrameEffect::Continue | FrameEffect::Disconnected => {}
                        }
                    }
                }
            }
        })
        .await
        .map_err(|_| LarkError::retryable("waiting for initial node sidecar connection"))?
    }

    async fn cleanup_failed_bootstrap(&mut self) {
        self.active_ids.clear();
        self.writer.abort();
        self.worker.abort();
        self.child.terminate_and_reap(self.shutdown_grace).await;
        self.stderr.abort();
    }

    async fn run(mut self, shutdown: &CancellationToken) -> SessionEnd {
        let mut connected_since = Some(Instant::now());
        let mut was_healthy = false;
        loop {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => return self.graceful_shutdown().await,
                status = self.child.wait_leader() => {
                    if status.is_err() {
                        tracing::warn!("waiting for node sidecar failed");
                    }
                    was_healthy |= connected_for(connected_since.as_ref(), self.healthy_uptime);
                    return self.crashed(was_healthy).await;
                }
                line = self.stdout.next() => {
                    match line {
                        Some(Ok(line)) => {
                            match handle_line(
                                &line,
                                &self.state,
                                &self.writes,
                                &self.events,
                                &self.active_ids,
                                self.max_frame_bytes,
                            ) {
                                Ok(FrameEffect::Connected) => {
                                    connected_since.get_or_insert_with(Instant::now);
                                }
                                Ok(FrameEffect::Disconnected | FrameEffect::Failed | FrameEffect::Stopped) => {
                                    was_healthy |= connected_for(
                                        connected_since.as_ref(),
                                        self.healthy_uptime,
                                    );
                                    connected_since = None;
                                    if matches!(
                                        *self.state.borrow(),
                                        ConnectionState::Degraded { .. } | ConnectionState::Stopped
                                    ) {
                                        return self.crashed(was_healthy).await;
                                    }
                                }
                                Ok(FrameEffect::Continue) => {}
                                Err(_) => return self.crashed(was_healthy).await,
                            }
                        }
                        Some(Err(_)) => {
                            tracing::warn!("node sidecar emitted an oversized or invalid line");
                            was_healthy |= connected_for(
                                connected_since.as_ref(),
                                self.healthy_uptime,
                            );
                            return self.crashed(was_healthy).await;
                        }
                        None => {
                            tracing::warn!("node sidecar protocol stdout closed unexpectedly");
                            was_healthy |= connected_for(
                                connected_since.as_ref(),
                                self.healthy_uptime,
                            );
                            return self.crashed(was_healthy).await;
                        }
                    }
                }
            }
        }
    }

    async fn crashed(&mut self, was_healthy: bool) -> SessionEnd {
        self.child.terminate_and_reap(self.shutdown_grace).await;
        self.abort_tasks();
        SessionEnd::Crashed { was_healthy }
    }

    async fn graceful_shutdown(&mut self) -> SessionEnd {
        let deadline = Instant::now() + self.shutdown_grace;
        let id = new_id("shutdown");
        let request = json!({"v": VERSION, "type": "shutdown", "id": id.clone()});
        if enqueue(&self.writes, &request, self.max_frame_bytes).is_ok() {
            let wait = async {
                loop {
                    let Some(line) = self.stdout.next().await else {
                        return false;
                    };
                    let Ok(line) = line else {
                        return false;
                    };
                    let Ok(base) = serde_json::from_str::<BaseFrame>(&line) else {
                        return false;
                    };
                    if base.kind == "response" && base.id == id {
                        let Ok(response) = serde_json::from_str::<ResponseFrame>(&line) else {
                            return false;
                        };
                        return response.validate(&id).is_ok();
                    }
                    if base.kind == "event" {
                        let _ = send_negative_ack(
                            &self.writes,
                            &base.id,
                            "shutting_down",
                            self.max_frame_bytes,
                        );
                    }
                }
            };
            let _ = timeout_at(deadline, wait).await;
        }
        if Instant::now() < deadline {
            let _ = timeout_at(deadline, self.child.wait_leader()).await;
        }
        // Even a cleanly exited wrapper may have left descendants behind.
        self.child.terminate_and_reap(self.shutdown_grace).await;
        self.abort_tasks();
        SessionEnd::Shutdown
    }

    fn abort_tasks(&self) {
        self.active_ids.clear();
        self.writer.abort();
        self.worker.abort();
        self.stderr.abort();
    }
}

fn connected_for(since: Option<&Instant>, threshold: Duration) -> bool {
    since.is_some_and(|started| started.elapsed() >= threshold)
}

impl Drop for ChildSession {
    fn drop(&mut self) {
        self.abort_tasks();
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FrameEffect {
    Continue,
    Connected,
    Disconnected,
    Failed,
    Stopped,
}

fn handle_line(
    line: &str,
    state: &watch::Sender<ConnectionState>,
    writes: &mpsc::Sender<Vec<u8>>,
    events: &mpsc::Sender<PendingEvent>,
    active_ids: &ActiveEventIds,
    max_frame_bytes: usize,
) -> Result<FrameEffect, LarkError> {
    let base: BaseFrame = serde_json::from_str(line)
        .map_err(|_| LarkError::protocol("decoding a node sidecar frame"))?;
    base.validate()?;
    match base.kind.as_str() {
        "state" => {
            let frame: StateFrame = serde_json::from_str(line)
                .map_err(|_| LarkError::protocol("decoding node sidecar state"))?;
            return frame.publish(state);
        }
        "event" => {
            let frame: EventFrame = serde_json::from_str(line)
                .map_err(|_| LarkError::protocol("decoding node sidecar event"))?;
            frame.validate()?;
            let payload = serde_json::to_vec(&frame.payload)
                .map_err(|_| LarkError::protocol("encoding node sidecar event payload"))?;
            if payload.len() > max_frame_bytes {
                send_negative_ack(writes, &frame.id, "payload_too_large", max_frame_bytes)?;
                return Ok(FrameEffect::Continue);
            }
            if !active_ids.insert(&frame.id)? {
                return Err(LarkError::protocol(
                    "node sidecar reused an active event correlation",
                ));
            }
            match events.try_send(PendingEvent {
                id: frame.id,
                payload: Bytes::from(payload),
            }) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(event)) => {
                    active_ids.remove(&event.id);
                    send_negative_ack(writes, &event.id, "backpressure", max_frame_bytes)?;
                }
                Err(mpsc::error::TrySendError::Closed(event)) => {
                    active_ids.remove(&event.id);
                    send_negative_ack(writes, &event.id, "intake_unavailable", max_frame_bytes)?;
                }
            }
        }
        "response" => {
            return Err(LarkError::protocol(
                "node sidecar sent an unsolicited response",
            ));
        }
        "error" => {
            let frame: ErrorFrame = serde_json::from_str(line)
                .map_err(|_| LarkError::protocol("decoding node sidecar error"))?;
            frame.validate()?;
            return Err(LarkError::protocol(
                "node sidecar rejected a protocol message",
            ));
        }
        _ => {
            enqueue(
                writes,
                &json!({
                    "v": VERSION,
                    "type": "error",
                    "id": base.id,
                    "code": "unknown_message",
                }),
                max_frame_bytes,
            )?;
        }
    }
    Ok(FrameEffect::Continue)
}

struct PendingEvent {
    id: String,
    payload: Bytes,
}

async fn event_loop(
    mut events: mpsc::Receiver<PendingEvent>,
    writes: mpsc::Sender<Vec<u8>>,
    handler: InboundEventHandler,
    handler_timeout: Duration,
    max_frame_bytes: usize,
    concurrency: usize,
    active_ids: ActiveEventIds,
) {
    let mut active = FuturesUnordered::new();
    let mut input_closed = false;
    loop {
        if input_closed && active.is_empty() {
            return;
        }
        tokio::select! {
            event = events.recv(), if !input_closed && active.len() < concurrency => {
                match event {
                    Some(event) => active.push(process_event(
                        event,
                        Arc::clone(&handler),
                        handler_timeout,
                    )),
                    None => input_closed = true,
                }
            }
            completion = active.next(), if !active.is_empty() => {
                if let Some(completion) = completion {
                    if enqueue(&writes, &completion.frame, max_frame_bytes).is_err() {
                        active_ids.clear();
                        return;
                    }
                    active_ids.remove(&completion.id);
                }
            }
        }
    }
}

async fn process_event(
    event: PendingEvent,
    handler: InboundEventHandler,
    handler_timeout: Duration,
) -> CompletedEvent {
    let PendingEvent { id, payload } = event;
    let frame = match timeout(handler_timeout, handler(payload)).await {
        Ok(Ok(data)) => json!({
            "v": VERSION,
            "type": "event_ack",
            "id": &id,
            "ok": true,
            "data": data,
        }),
        Ok(Err(_)) => json!({
            "v": VERSION,
            "type": "event_ack",
            "id": &id,
            "ok": false,
            "error": "durable_intake_failed",
        }),
        Err(_) => json!({
            "v": VERSION,
            "type": "event_ack",
            "id": &id,
            "ok": false,
            "error": "durable_intake_timeout",
        }),
    };
    CompletedEvent { id, frame }
}

struct CompletedEvent {
    id: String,
    frame: Value,
}

async fn write_loop(
    mut stdin: tokio::process::ChildStdin,
    mut frames: mpsc::Receiver<Vec<u8>>,
    max_frame_bytes: usize,
) {
    while let Some(frame) = frames.recv().await {
        if frame.len() > max_frame_bytes
            || stdin.write_all(&frame).await.is_err()
            || stdin.write_all(b"\n").await.is_err()
            || stdin.flush().await.is_err()
        {
            return;
        }
    }
}

async fn drain_stderr(mut stderr: tokio::process::ChildStderr) {
    const CHUNK_BYTES: usize = 8 * 1024;
    let mut chunk = [0_u8; CHUNK_BYTES];
    let mut line_bytes = 0_usize;
    let mut discarding_oversized = false;

    loop {
        let read = match stderr.read(&mut chunk).await {
            Ok(0) => break,
            Ok(read) => read,
            Err(_) => {
                tracing::warn!("reading node sidecar stderr failed");
                return;
            }
        };
        for byte in &chunk[..read] {
            if *byte == b'\n' {
                if !discarding_oversized && line_bytes > 0 {
                    tracing::warn!(line_bytes, "node sidecar stderr");
                }
                line_bytes = 0;
                discarding_oversized = false;
            } else if !discarding_oversized {
                line_bytes = line_bytes.saturating_add(1);
                if line_bytes > crate::limits::MAX_STDERR_LINE_BYTES {
                    tracing::warn!("node sidecar stderr line exceeded its bound");
                    discarding_oversized = true;
                }
            }
        }
    }

    if !discarding_oversized && line_bytes > 0 {
        tracing::warn!(line_bytes, "node sidecar stderr");
    }
}

fn enqueue(
    writes: &mpsc::Sender<Vec<u8>>,
    value: &Value,
    max_frame_bytes: usize,
) -> Result<(), LarkError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|_| LarkError::protocol("encoding a node sidecar frame"))?;
    if bytes.len() > max_frame_bytes {
        return Err(LarkError::exhausted(
            "node sidecar outbound frame",
            u64::try_from(max_frame_bytes).unwrap_or(u64::MAX),
        ));
    }
    writes.try_send(bytes).map_err(|_| {
        LarkError::exhausted(
            "node sidecar write queue",
            u64::try_from(writes.max_capacity()).unwrap_or(u64::MAX),
        )
    })
}

fn send_negative_ack(
    writes: &mpsc::Sender<Vec<u8>>,
    id: &str,
    code: &'static str,
    max_frame_bytes: usize,
) -> Result<(), LarkError> {
    enqueue(
        writes,
        &json!({
            "v": VERSION,
            "type": "event_ack",
            "id": id,
            "ok": false,
            "error": code,
        }),
        max_frame_bytes,
    )
}

fn new_id(prefix: &str) -> String {
    format!("{prefix}-{}", Uuid::new_v4().simple())
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HelloFrame {
    v: u16,
    #[serde(rename = "type")]
    kind: String,
    id: String,
    protocol: String,
    capabilities: Vec<String>,
    max_frame_bytes: usize,
}

impl HelloFrame {
    fn validate(&self, requested_max: usize) -> Result<(), LarkError> {
        if self.v != VERSION
            || self.kind != "hello"
            || self.protocol != PROTOCOL
            || !valid_id(&self.id)
            || self.max_frame_bytes < requested_max
            || self.max_frame_bytes > CHANNEL_SIDECAR_FRAME_BYTES
            || !REQUIRED_CAPABILITIES
                .iter()
                .all(|required| self.capabilities.iter().any(|actual| actual == required))
        {
            return Err(LarkError::protocol("node sidecar hello is incompatible"));
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseFrame {
    v: u16,
    #[serde(rename = "type")]
    kind: String,
    id: String,
    ok: bool,
}

impl ResponseFrame {
    fn validate(&self, expected_id: &str) -> Result<(), LarkError> {
        if self.v != VERSION
            || self.kind != "response"
            || self.id != expected_id
            || !self.ok
            || !valid_id(&self.id)
        {
            return Err(LarkError::protocol("node sidecar response is incompatible"));
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct BaseFrame {
    v: u16,
    #[serde(rename = "type")]
    kind: String,
    id: String,
}

impl BaseFrame {
    fn validate(&self) -> Result<(), LarkError> {
        if self.v != VERSION || !valid_id(&self.id) || self.kind.len() > 64 {
            return Err(LarkError::protocol("node sidecar frame header is invalid"));
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EventFrame {
    v: u16,
    #[serde(rename = "type")]
    kind: String,
    id: String,
    payload: Value,
}

impl EventFrame {
    fn validate(&self) -> Result<(), LarkError> {
        if self.v != VERSION || self.kind != "event" || !valid_id(&self.id) {
            return Err(LarkError::protocol("node sidecar event header is invalid"));
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ErrorFrame {
    v: u16,
    #[serde(rename = "type")]
    kind: String,
    id: String,
    code: String,
}

impl ErrorFrame {
    fn validate(&self) -> Result<(), LarkError> {
        if self.v != VERSION
            || self.kind != "error"
            || !valid_id(&self.id)
            || self.code.is_empty()
            || self.code.len() > 64
            || !self
                .code
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
        {
            return Err(LarkError::protocol("node sidecar error frame is invalid"));
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StateFrame {
    v: u16,
    #[serde(rename = "type")]
    kind: String,
    id: String,
    state: String,
    #[serde(default)]
    attempt: Option<u32>,
    #[serde(default)]
    delay_ms: Option<u64>,
}

impl StateFrame {
    fn publish(&self, sink: &watch::Sender<ConnectionState>) -> Result<FrameEffect, LarkError> {
        if self.v != VERSION || self.kind != "state" || !valid_id(&self.id) {
            return Err(LarkError::protocol("node sidecar state header is invalid"));
        }
        let (state, effect) = match self.state.as_str() {
            "connecting" | "reconnecting" => (
                ConnectionState::Connecting {
                    attempt: self.attempt.unwrap_or(1).max(1),
                },
                FrameEffect::Disconnected,
            ),
            "connected" => (ConnectionState::Connected, FrameEffect::Connected),
            "backoff" => (
                ConnectionState::Backoff {
                    attempt: self.attempt.unwrap_or(1).max(1),
                    delay: Duration::from_millis(self.delay_ms.unwrap_or(0)),
                },
                FrameEffect::Disconnected,
            ),
            "failed" => (
                ConnectionState::Degraded {
                    reason: "node_sidecar_connection_failed".to_owned(),
                },
                FrameEffect::Failed,
            ),
            "stopped" => (ConnectionState::Stopped, FrameEffect::Stopped),
            _ => return Err(LarkError::protocol("node sidecar state is unknown")),
        };
        sink.send_replace(state);
        Ok(effect)
    }
}
