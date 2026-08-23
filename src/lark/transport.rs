//! WebSocket endpoint bootstrap, transport actor, receipts, and reconnect.
//!
//! Bootstrap is `POST {open_base}/callback/ws/endpoint` with a JSON
//! `{AppID, AppSecret}` body and a `locale: zh` header under the shared 15 s
//! HTTP timeout; `device_id`/`service_id` are parsed from the returned `URL`
//! query string exactly like the reference SDK.
//!
//! Deliberate deviations from the reference SDK, per the design:
//!
//! - **Bootstrap code classification.** The reference retries only
//!   `1000040343` and treats every other non-zero code as fatal. This client
//!   additionally retries `1` (system busy), transport errors, timeouts, and
//!   HTTP 5xx, because those are transient server-side conditions. `403`/
//!   `514` stay [`LarkError::PermanentAuth`], `1000040350` (connection limit)
//!   is [`LarkError::Exhausted`], and any other unknown non-zero code remains
//!   non-retryable (`PermanentAuth`), matching the reference's conservative
//!   default.
//! - **Reconnect scheduling.** The reference sleeps a fixed server-supplied
//!   `ReconnectInterval` between attempts. This client instead reuses the
//!   supervisor's deterministic jittered exponential backoff (0.5–30 s) keyed
//!   by consecutive failure count, honoring the server `ReconnectNonce` as the
//!   first delay and `ReconnectCount >= 0` as the attempt cap.
//!   `ReconnectInterval` is parsed but intentionally unused for scheduling.
//!   Note the cap semantics deliberately differ from the reference: the
//!   reference counts only attempts inside one reconnect loop after a
//!   successful connection, while this client counts consecutive failed
//!   attempts since the last successful session (including the very first
//!   connect), resetting the counter on every successful connect.
//!
//! Receipts: `{code: 200}` is sent only after the inbound handler completes
//! successfully (with `data` = base64(JSON of the handler's return value)
//! when it returns one); handler failure — including exceeding
//! [`LARK_HANDLER_TIMEOUT`] — sends `{code: 500}`. A receipt send failure on a
//! closing socket is logged, never retried. `PermanentAuth`/`Exhausted` from
//! bootstrap or a `handshake-autherrcode` header enters
//! [`TransportState::Degraded`] without further retries; a `ProtocolViolation`
//! from bootstrap (a malformed endpoint response) also fails closed into
//! `Degraded`, since retrying an unparsable response cannot succeed.

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::{SinkExt, StreamExt};
use secrecy::ExposeSecret as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{Sleep, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};
use tokio_util::sync::CancellationToken;
use url::Url;

use super::credentials::LarkCredentials;
use super::error::{LarkError, LarkErrorKind};
use super::fragments::{Reassembler, Reassembly};
use super::frame::{Frame, FrameHeaders, FrameMethod, Header, MessageType, header_key};
use super::http::LarkHttp;
pub use crate::channel::ConnectionState as TransportState;
use crate::codex::supervisor::AppServerSupervisor;
use crate::limits::{
    LARK_DEFAULT_PING_INTERVAL, LARK_FRAGMENT_MESSAGE_BYTES, LARK_HANDLER_TIMEOUT,
    LARK_PONG_TIMEOUT, LARK_TRANSPORT_EVENT_BYTE_BUDGET, LARK_TRANSPORT_EVENT_CAPACITY,
    LARK_TRANSPORT_SHUTDOWN_GRACE, LARK_WS_CONNECT_TIMEOUT, PROBE_TIMEOUT,
};

/// One pulled WebSocket endpoint plus its server-supplied client config.
///
/// `Debug` is manual: the full URL can carry one-time connection tickets, so
/// only the host and the extracted IDs are shown.
#[derive(Clone, PartialEq, Eq)]
pub struct WsEndpoint {
    /// The full connect URL (contains `device_id` and `service_id` query
    /// parameters, and possibly one-time tickets — never log it verbatim).
    pub url: Url,
    /// The `device_id` query parameter of [`WsEndpoint::url`].
    pub device_id: String,
    /// The `service_id` query parameter of [`WsEndpoint::url`].
    pub service_id: i32,
    /// Ping cadence requested by the server (`PingInterval`, seconds).
    pub ping_interval: Duration,
    /// Reconnect attempt cap requested by the server; `< 0` means unlimited.
    pub reconnect_count: i64,
    /// Server-suggested fixed reconnect delay (`ReconnectInterval`, seconds).
    /// Parsed for completeness but intentionally unused: reconnects use the
    /// supervisor's jittered backoff instead (see module docs).
    pub reconnect_interval: Duration,
    /// Server-suggested initial reconnect delay (`ReconnectNonce`, seconds),
    /// honored as the first delay after an outage.
    pub reconnect_nonce: Duration,
}

impl fmt::Debug for WsEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WsEndpoint")
            .field("host", &self.url.host_str().unwrap_or(""))
            .field("device_id", &self.device_id)
            .field("service_id", &self.service_id)
            .field("ping_interval", &self.ping_interval)
            .field("reconnect_count", &self.reconnect_count)
            .field("reconnect_interval", &self.reconnect_interval)
            .field("reconnect_nonce", &self.reconnect_nonce)
            .finish()
    }
}

/// Observation events emitted by the transport actor.
///
/// This channel is observational and bounded (count via
/// [`LARK_TRANSPORT_EVENT_CAPACITY`], payload bytes via
/// [`LARK_TRANSPORT_EVENT_BYTE_BUDGET`]); when saturated, events are dropped
/// with a warning rather than slowing the wire path. The authoritative event
/// stream is the handler itself.
pub enum TransportEvent {
    /// A lifecycle state transition.
    State(TransportState),
    /// A complete, reassembled inbound payload delivered to the handler.
    Message {
        /// Headers of the frame that completed the message.
        headers: FrameHeaders,
        /// The complete payload.
        payload: Bytes,
        /// Byte-budget permit, held until the receiver dequeues.
        permit: Option<OwnedSemaphorePermit>,
    },
    /// A protocol anomaly (bad frame, fragment rejection, bad pong). Never
    /// fatal to the connection.
    Anomaly {
        /// Stable anomaly kind string.
        kind: &'static str,
        /// The affected message, when known.
        message_id: Option<String>,
    },
}

impl fmt::Debug for TransportEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::State(state) => formatter.debug_tuple("State").field(state).finish(),
            Self::Message {
                headers, payload, ..
            } => formatter
                .debug_struct("Message")
                .field("headers", headers)
                .field("payload_len", &payload.len())
                .finish(),
            Self::Anomaly { kind, message_id } => formatter
                .debug_struct("Anomaly")
                .field("kind", kind)
                .field("message_id", message_id)
                .finish(),
        }
    }
}

/// The inbound handler invoked for every complete `event`/`card` payload.
///
/// Returns an optional JSON value embedded base64-encoded into the receipt's
/// `data` field; `Err` produces a `{code: 500}` receipt. Milestone 3 swaps in
/// SQLite-persisted receipt semantics behind this seam.
pub type InboundFrameHandler = Arc<
    dyn Fn(FrameHeaders, Bytes) -> BoxFuture<'static, Result<Option<Value>, LarkError>>
        + Send
        + Sync,
>;

/// Tunables for the transport; defaults match production limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportConfig {
    /// After a ping is sent, any inbound frame within this window proves
    /// liveness; otherwise the socket is dropped to trigger a reconnect.
    pub pong_timeout: Duration,
    /// Upper bound for one handler invocation; on expiry the handler is
    /// treated as failed and a `{code: 500}` receipt is sent, so a stuck
    /// handler cannot stall the ping loop, liveness, or shutdown.
    pub handler_timeout: Duration,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            pong_timeout: LARK_PONG_TIMEOUT,
            handler_timeout: LARK_HANDLER_TIMEOUT,
        }
    }
}

/// Outcome of a one-shot endpoint liveness probe (`lark probe`).
///
/// Carries only the endpoint host (never the full URL, which can contain
/// one-time tickets) and the negotiated timing values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeOutcome {
    /// Host of the pulled WebSocket endpoint.
    pub endpoint_host: String,
    /// Ping interval negotiated for the session: the bootstrap value,
    /// updated by the first pong's `ClientConfig` payload when present.
    pub ping_interval: Duration,
    /// Wall time of the whole probe (bootstrap, connect, first round trip).
    pub elapsed: Duration,
}

/// Entry points for the Lark WebSocket transport.
pub struct LarkTransport;

impl LarkTransport {
    /// Pulls the WebSocket endpoint for the given credentials.
    ///
    /// `POST {open_base}/callback/ws/endpoint` with `{AppID, AppSecret}` and a
    /// `locale: zh` header. See the module docs for the code classification,
    /// which deliberately deviates from the reference SDK by retrying
    /// transient server errors.
    ///
    /// # Errors
    ///
    /// Returns a classified [`LarkError`]: `PermanentAuth`/`Exhausted` are
    /// non-retryable, everything else is `Retryable` or `ProtocolViolation`
    /// (malformed response).
    pub async fn pull_endpoint(
        http: &LarkHttp,
        creds: &LarkCredentials,
    ) -> Result<WsEndpoint, LarkError> {
        let body = EndpointRequest {
            app_id: &creds.app_id,
            app_secret: creds.app_secret.expose_secret(),
        };
        let response: EndpointResponse = http
            .post_json_with_headers("/callback/ws/endpoint", &body, &[("locale", "zh")])
            .await?;
        classify_endpoint_code(response.code)?;
        let data = response
            .data
            .ok_or_else(|| LarkError::protocol("endpoint response has no data"))?;
        build_endpoint(data)
    }

    /// Starts the transport actor with production limits.
    #[must_use]
    pub fn start(
        http: LarkHttp,
        creds: LarkCredentials,
        handler: InboundFrameHandler,
    ) -> TransportHandle {
        Self::start_with_config(http, creds, handler, TransportConfig::default())
    }

    /// Starts the transport actor with explicit tunables.
    ///
    /// The returned handle observes the first bootstrap attempt immediately;
    /// the actor owns the socket, ping loop, receipts, and reconnect policy.
    #[must_use]
    pub fn start_with_config(
        http: LarkHttp,
        creds: LarkCredentials,
        handler: InboundFrameHandler,
        config: TransportConfig,
    ) -> TransportHandle {
        let (event_tx, events) = mpsc::channel(LARK_TRANSPORT_EVENT_CAPACITY);
        let (state_tx, state) = watch::channel(TransportState::Connecting { attempt: 1 });
        let shutdown = CancellationToken::new();
        let actor = Actor {
            http,
            creds,
            handler,
            config,
            event_tx,
            event_bytes: Arc::new(Semaphore::new(LARK_TRANSPORT_EVENT_BYTE_BUDGET)),
            state_tx,
            shutdown: shutdown.clone(),
            live: LiveConfig::default(),
        };
        let task = tokio::spawn(actor.run());
        TransportHandle {
            events,
            state,
            shutdown,
            task: Some(task),
        }
    }

    /// Runs a one-shot liveness probe: pulls the endpoint, opens the socket,
    /// sends one ping, and waits for the first pong within
    /// [`PROBE_TIMEOUT`], then closes.
    ///
    /// Unlike [`LarkTransport::start`] this performs exactly one bootstrap and
    /// one connect with no retries — a probe reports reachability, it does not
    /// hold a session.
    ///
    /// # Errors
    ///
    /// Returns a classified [`LarkError`]: `PermanentAuth` for bad
    /// credentials or a handshake auth error, `Retryable` for connect
    /// failures, a closed socket, or the overall timeout.
    pub async fn probe(
        http: &LarkHttp,
        creds: &LarkCredentials,
    ) -> Result<ProbeOutcome, LarkError> {
        let started = Instant::now();
        let endpoint = Self::pull_endpoint(http, creds).await?;
        let host = endpoint.url.host_str().unwrap_or("").to_owned();
        let round_trip = async {
            let ws_config = WebSocketConfig::default()
                .max_message_size(Some(LARK_FRAGMENT_MESSAGE_BYTES))
                .max_frame_size(Some(LARK_FRAGMENT_MESSAGE_BYTES));
            let (socket, _response) = timeout(
                LARK_WS_CONNECT_TIMEOUT,
                connect_async_with_config(endpoint.url.as_str(), Some(ws_config), false),
            )
            .await
            .map_err(|_| LarkError::retryable("lark probe connect timed out"))?
            .map_err(|_| LarkError::retryable("lark probe connect failed"))?;
            let (mut sink, mut stream) = socket.split();
            send_ping(&mut sink, endpoint.service_id)
                .await
                .map_err(|()| LarkError::retryable("lark probe failed to send the ping frame"))?;
            let mut live = LiveConfig::default();
            live.apply_endpoint(&endpoint);
            loop {
                match stream.next().await {
                    Some(Ok(Message::Binary(bytes))) => {
                        let Ok(frame) = Frame::decode_bytes(&bytes) else {
                            continue;
                        };
                        let headers = frame.frame_headers();
                        if let Some(code) = headers.handshake_autherrcode() {
                            return Err(LarkError::PermanentAuth {
                                context: "the lark probe handshake",
                                code: code.parse::<i64>().ok(),
                            });
                        }
                        let is_pong = matches!(
                            FrameMethod::from_wire(frame.method),
                            Some(FrameMethod::Control)
                        ) && matches!(headers.ty(), Some(MessageType::Pong));
                        if is_pong {
                            if let Some(payload) = frame.payload.as_deref() {
                                let _ = live.apply_pong(payload);
                            }
                            let close = async {
                                let _ = sink.send(Message::Close(None)).await;
                                let _ = sink.flush().await;
                            };
                            let _ = timeout(LARK_TRANSPORT_SHUTDOWN_GRACE, close).await;
                            return Ok(live.ping_interval);
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => {
                        return Err(LarkError::retryable(
                            "lark probe socket closed before the first pong",
                        ));
                    }
                    Some(Err(_)) => {
                        return Err(LarkError::retryable("lark probe socket error"));
                    }
                    // Protocol pings are answered by tungstenite; text frames
                    // are not part of pbbp2.
                    Some(Ok(_)) => {}
                }
            }
        };
        let ping_interval = timeout(PROBE_TIMEOUT, round_trip).await.map_err(|_| {
            LarkError::retryable("lark probe timed out waiting for the first pong")
        })??;
        Ok(ProbeOutcome {
            endpoint_host: host,
            ping_interval,
            elapsed: started.elapsed(),
        })
    }
}

/// Handle to the running transport actor.
pub struct TransportHandle {
    events: mpsc::Receiver<TransportEvent>,
    state: watch::Receiver<TransportState>,
    shutdown: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl TransportHandle {
    /// Returns the most recently published lifecycle state.
    #[must_use]
    pub fn state(&self) -> TransportState {
        self.state.borrow().clone()
    }

    /// Subscribes to lifecycle state transitions.
    #[must_use]
    pub fn subscribe_state(&self) -> watch::Receiver<TransportState> {
        self.state.clone()
    }

    /// Receives the next observation event, or `None` after the actor stops.
    pub async fn next_event(&mut self) -> Option<TransportEvent> {
        self.events.recv().await
    }

    /// Closes the socket and joins the actor with a bounded grace; the task
    /// is aborted if it outlives the grace.
    pub async fn shutdown(mut self) {
        self.shutdown.cancel();
        if let Some(mut task) = self.task.take() {
            if timeout(LARK_TRANSPORT_SHUTDOWN_GRACE, &mut task)
                .await
                .is_err()
            {
                task.abort();
                let _ = task.await;
            }
        }
    }
}

impl Drop for TransportHandle {
    fn drop(&mut self) {
        self.shutdown.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[derive(Serialize)]
struct EndpointRequest<'a> {
    #[serde(rename = "AppID")]
    app_id: &'a str,
    #[serde(rename = "AppSecret")]
    app_secret: &'a str,
}

#[derive(Deserialize)]
struct EndpointResponse {
    code: i64,
    // `msg` is deliberately not deserialized: it is free-form server text
    // that must never reach logs or errors.
    data: Option<EndpointData>,
}

#[derive(Deserialize)]
struct EndpointData {
    #[serde(rename = "URL")]
    url: String,
    #[serde(rename = "ClientConfig")]
    client_config: Option<ClientConfigDto>,
}

#[derive(Deserialize, Default)]
struct ClientConfigDto {
    #[serde(rename = "PingInterval")]
    ping_interval: Option<i64>,
    #[serde(rename = "ReconnectCount")]
    reconnect_count: Option<i64>,
    #[serde(rename = "ReconnectInterval")]
    reconnect_interval: Option<i64>,
    #[serde(rename = "ReconnectNonce")]
    reconnect_nonce: Option<i64>,
}

/// Classifies the bootstrap response code; see the module docs for the
/// deliberate deviation from the reference SDK.
fn classify_endpoint_code(code: i64) -> Result<(), LarkError> {
    let context = "pulling the Lark WebSocket endpoint";
    match code {
        0 => Ok(()),
        // Deliberate deviation from the reference (which retries only
        // 1000040343): system busy is transient, so retry it too.
        code @ (1 | 1_000_040_343) => Err(LarkError::Retryable {
            context,
            code: Some(code),
        }),
        1_000_040_350 => Err(LarkError::Exhausted {
            // The connection limit is server-side and unknown to us.
            context: "the Lark WebSocket connection limit (code 1000040350)",
            limit: 0,
        }),
        // 403/514 are auth failures; any other unknown code follows the
        // reference's conservative non-retryable default.
        code => Err(LarkError::PermanentAuth {
            context,
            code: Some(code),
        }),
    }
}

/// Parses the endpoint URL query and client config into a [`WsEndpoint`].
fn build_endpoint(data: EndpointData) -> Result<WsEndpoint, LarkError> {
    let url = Url::parse(&data.url).map_err(|_| LarkError::protocol("parsing the endpoint URL"))?;
    let mut device_id = None;
    let mut service_id = None;
    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "device_id" => device_id = Some(value.into_owned()),
            "service_id" => service_id = value.parse::<i32>().ok(),
            _ => {}
        }
    }
    let device_id = device_id
        .filter(|value| !value.is_empty())
        .ok_or_else(|| LarkError::protocol("endpoint URL has no device_id"))?;
    let service_id =
        service_id.ok_or_else(|| LarkError::protocol("endpoint URL has no service_id"))?;

    let config = data.client_config.unwrap_or_default();
    let seconds = |value: Option<i64>| {
        value
            .filter(|seconds| *seconds > 0)
            .map_or(Duration::ZERO, |seconds| {
                Duration::from_secs(seconds.unsigned_abs())
            })
    };
    let ping_interval = {
        let interval = seconds(config.ping_interval);
        if interval.is_zero() {
            LARK_DEFAULT_PING_INTERVAL
        } else {
            interval
        }
    };
    Ok(WsEndpoint {
        url,
        device_id,
        service_id,
        ping_interval,
        reconnect_count: config.reconnect_count.unwrap_or(-1),
        reconnect_interval: seconds(config.reconnect_interval),
        reconnect_nonce: seconds(config.reconnect_nonce),
    })
}

/// Server-supplied live connection configuration, updated by every bootstrap
/// and every pong payload.
#[derive(Debug, Clone, Copy)]
struct LiveConfig {
    service_id: i32,
    ping_interval: Duration,
    reconnect_count: i64,
    reconnect_nonce: Duration,
}

impl Default for LiveConfig {
    fn default() -> Self {
        Self {
            service_id: 0,
            ping_interval: LARK_DEFAULT_PING_INTERVAL,
            reconnect_count: -1,
            reconnect_nonce: Duration::ZERO,
        }
    }
}

impl LiveConfig {
    fn apply_endpoint(&mut self, endpoint: &WsEndpoint) {
        self.service_id = endpoint.service_id;
        self.ping_interval = endpoint.ping_interval;
        self.reconnect_count = endpoint.reconnect_count;
        self.reconnect_nonce = endpoint.reconnect_nonce;
    }

    /// Applies a pong payload (`{PingInterval, ReconnectCount,
    /// ReconnectInterval, ReconnectNonce}`, seconds), exactly like the
    /// reference. `ReconnectInterval` is parsed but unused for scheduling.
    fn apply_pong(&mut self, payload: &[u8]) -> Result<(), ()> {
        #[derive(Deserialize)]
        struct PongConfig {
            #[serde(rename = "PingInterval")]
            ping_interval: Option<i64>,
            #[serde(rename = "ReconnectCount")]
            reconnect_count: Option<i64>,
            #[serde(rename = "ReconnectNonce")]
            reconnect_nonce: Option<i64>,
        }
        let pong: PongConfig = serde_json::from_slice(payload).map_err(|_| ())?;
        if let Some(interval) = pong.ping_interval.filter(|seconds| *seconds > 0) {
            self.ping_interval = Duration::from_secs(interval.unsigned_abs());
        }
        if let Some(count) = pong.reconnect_count {
            self.reconnect_count = count;
        }
        if let Some(nonce) = pong.reconnect_nonce.filter(|seconds| *seconds > 0) {
            self.reconnect_nonce = Duration::from_secs(nonce.unsigned_abs());
        }
        Ok(())
    }
}

/// Deterministic reconnect delay: the server nonce seeds the first delay of
/// an outage, then the supervisor's jittered backoff takes over.
fn reconnect_delay(live: &LiveConfig, failures: u32) -> Duration {
    if failures <= 1 && !live.reconnect_nonce.is_zero() {
        live.reconnect_nonce
    } else {
        AppServerSupervisor::retry_delay(0, failures)
    }
}

/// Why one WebSocket session ended.
enum SessionEnd {
    /// The socket was lost; reconnect per policy.
    Reconnect,
    /// A permanent failure; no further retries.
    Degraded(String),
    /// Shutdown was requested.
    Stopped,
}

struct Actor {
    http: LarkHttp,
    creds: LarkCredentials,
    handler: InboundFrameHandler,
    config: TransportConfig,
    event_tx: mpsc::Sender<TransportEvent>,
    event_bytes: Arc<Semaphore>,
    state_tx: watch::Sender<TransportState>,
    shutdown: CancellationToken,
    live: LiveConfig,
}

type WsSink = futures_util::stream::SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>;

impl Actor {
    async fn run(mut self) {
        let mut failures = 0_u32;
        loop {
            if self.shutdown.is_cancelled() {
                break;
            }
            self.publish_state(TransportState::Connecting {
                attempt: failures.saturating_add(1),
            });
            match self.connect_once().await {
                Err(error) if is_fatal(&error) => {
                    tracing::warn!(error = %error, "lark transport degraded");
                    self.publish_state(TransportState::Degraded {
                        reason: error.to_string(),
                    });
                    return;
                }
                Err(error) => {
                    tracing::warn!(error = %error, "lark connect attempt failed");
                    failures = failures.saturating_add(1);
                    if self.live.reconnect_count >= 0
                        && i64::from(failures) >= self.live.reconnect_count
                    {
                        self.publish_state(TransportState::Degraded {
                            reason: "reconnect attempts exhausted".to_owned(),
                        });
                        return;
                    }
                    if !self.backoff(failures).await {
                        break;
                    }
                }
                Ok(socket) => {
                    failures = 0;
                    self.publish_state(TransportState::Connected);
                    match self.session(socket).await {
                        SessionEnd::Reconnect => {
                            if !self.backoff(1).await {
                                break;
                            }
                        }
                        SessionEnd::Degraded(reason) => {
                            self.publish_state(TransportState::Degraded { reason });
                            return;
                        }
                        SessionEnd::Stopped => break,
                    }
                }
            }
        }
        self.publish_state(TransportState::Stopped);
    }

    /// Sleeps one reconnect delay, interruptibly. Returns `false` when
    /// shutdown was requested during the sleep.
    async fn backoff(&self, failures: u32) -> bool {
        let delay = reconnect_delay(&self.live, failures);
        self.publish_state(TransportState::Backoff {
            attempt: failures,
            delay,
        });
        tokio::select! {
            () = self.shutdown.cancelled() => false,
            () = tokio::time::sleep(delay) => true,
        }
    }

    async fn connect_once(
        &mut self,
    ) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, LarkError> {
        let endpoint = LarkTransport::pull_endpoint(&self.http, &self.creds).await?;
        // Apply the server config as soon as the bootstrap succeeds so the
        // reconnect cap/nonce also govern failed connect attempts.
        self.live.apply_endpoint(&endpoint);
        let ws_config = WebSocketConfig::default()
            .max_message_size(Some(LARK_FRAGMENT_MESSAGE_BYTES))
            .max_frame_size(Some(LARK_FRAGMENT_MESSAGE_BYTES));
        let (socket, _response) = timeout(
            LARK_WS_CONNECT_TIMEOUT,
            connect_async_with_config(endpoint.url.as_str(), Some(ws_config), false),
        )
        .await
        .map_err(|_| LarkError::retryable("lark websocket connect timed out"))?
        .map_err(|_| LarkError::retryable("lark websocket connect failed"))?;
        Ok(socket)
    }

    async fn session(&mut self, socket: WebSocketStream<MaybeTlsStream<TcpStream>>) -> SessionEnd {
        let (mut sink, mut stream) = socket.split();
        let mut reassembler = Reassembler::new();
        let mut next_ping = std::pin::pin!(tokio::time::sleep(self.live.ping_interval));
        // Mirror the reference: ping immediately on open, then every interval.
        if send_ping(&mut sink, self.live.service_id).await.is_err() {
            return SessionEnd::Reconnect;
        }
        let mut liveness: Option<std::pin::Pin<Box<Sleep>>> =
            Some(Box::pin(tokio::time::sleep(self.config.pong_timeout)));
        loop {
            tokio::select! {
                biased;
                // Branch order matters under a sustained inbound flood:
                // shutdown first, then the timers, so a busy stream can never
                // starve the ping loop or the liveness watchdog.
                () = self.shutdown.cancelled() => {
                    let close = async {
                        let _ = sink.send(Message::Close(None)).await;
                        let _ = sink.flush().await;
                    };
                    let _ = timeout(LARK_TRANSPORT_SHUTDOWN_GRACE, close).await;
                    return SessionEnd::Stopped;
                }
                () = &mut next_ping => {
                    if send_ping(&mut sink, self.live.service_id).await.is_err() {
                        return SessionEnd::Reconnect;
                    }
                    liveness = Some(Box::pin(tokio::time::sleep(self.config.pong_timeout)));
                    next_ping
                        .as_mut()
                        .reset(tokio::time::Instant::now() + self.live.ping_interval);
                }
                () = async {
                    if let Some(deadline) = liveness.as_mut() {
                        deadline.await;
                    } else {
                        std::future::pending::<()>().await;
                    }
                } => {
                    tracing::warn!(
                        "no inbound frame within the pong timeout; dropping the socket"
                    );
                    return SessionEnd::Reconnect;
                }
                message = stream.next() => {
                    let Some(message) = message else {
                        return SessionEnd::Reconnect;
                    };
                    match message {
                        Ok(Message::Binary(bytes)) => {
                            liveness = None;
                            let ping_interval = self.live.ping_interval;
                            if let Some(end) = self
                                .handle_frame(&mut sink, &mut reassembler, bytes)
                                .await
                            {
                                return end;
                            }
                            // A pong may have shortened the ping interval;
                            // apply it to the pending schedule immediately.
                            if self.live.ping_interval != ping_interval {
                                next_ping.as_mut().reset(
                                    tokio::time::Instant::now() + self.live.ping_interval,
                                );
                            }
                        }
                        Ok(Message::Close(_)) | Err(_) => return SessionEnd::Reconnect,
                        Ok(_) => {
                            // Ping/Pong/Text: protocol pings are answered by
                            // tungstenite; text frames are not part of pbbp2.
                        }
                    }
                }
            }
        }
    }

    /// Handles one inbound binary frame. Returns `Some(SessionEnd)` when the
    /// session must end.
    async fn handle_frame(
        &mut self,
        sink: &mut WsSink,
        reassembler: &mut Reassembler,
        bytes: Bytes,
    ) -> Option<SessionEnd> {
        let Ok(frame) = Frame::decode_bytes(&bytes) else {
            self.anomaly("frame-decode", None);
            return None;
        };
        let headers = frame.frame_headers();
        if let Some(code) = headers.handshake_autherrcode() {
            tracing::warn!(code, "lark handshake authentication error");
            return Some(SessionEnd::Degraded(format!(
                "handshake authentication error (code {code})"
            )));
        }
        match FrameMethod::from_wire(frame.method) {
            Some(FrameMethod::Control) => self.handle_control(&headers, frame.payload.as_deref()),
            Some(FrameMethod::Data) => self.handle_data(sink, reassembler, &frame, &headers).await,
            None => self.anomaly("unknown-frame-method", None),
        }
        None
    }

    fn handle_control(&mut self, headers: &FrameHeaders, payload: Option<&[u8]>) {
        // Inbound pings and unknown control types need no answer; only pong
        // carries a live config update.
        if let (Some(MessageType::Pong), Some(payload)) = (headers.ty(), payload) {
            if self.live.apply_pong(payload).is_err() {
                self.anomaly("pong-config", None);
            }
        }
    }

    async fn handle_data(
        &mut self,
        sink: &mut WsSink,
        reassembler: &mut Reassembler,
        frame: &Frame,
        headers: &FrameHeaders,
    ) {
        if !matches!(headers.ty(), Some(MessageType::Event | MessageType::Card)) {
            self.anomaly(
                "unknown-message-type",
                headers.message_id().map(str::to_owned),
            );
            return;
        }
        let payload = frame.payload.clone().unwrap_or_default();
        let done = match reassembler.ingest(headers, payload, Instant::now()) {
            Ok(done) => done,
            Err(error) => {
                self.anomaly(error.as_str(), headers.message_id().map(str::to_owned));
                return;
            }
        };
        let Some(done) = done else {
            return; // Fragment buffered; no receipt until the message completes.
        };
        self.publish_message(headers.clone(), &done);
        let started = Instant::now();
        // The handler await is bounded: a stuck handler must not stall the
        // ping loop, the liveness watchdog, or shutdown. On timeout the
        // handler is treated as failed and a `{code: 500}` receipt is sent.
        let result = timeout(
            self.config.handler_timeout,
            (self.handler)(headers.clone(), done.payload.clone()),
        )
        .await
        .unwrap_or_else(|_| {
            tracing::warn!(
                message_id = done.message_id,
                "lark inbound handler timed out"
            );
            Err(LarkError::retryable("the inbound frame handler timed out"))
        });
        let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let receipt = build_receipt(frame, result, elapsed_ms);
        let encoded = receipt.encode_to_vec();
        if let Err(error) = sink.send(Message::Binary(encoded.into())).await {
            // A receipt failure on a closing socket is logged, never retried;
            // the read loop observes the dead socket on its own.
            tracing::warn!(
                message_id = done.message_id,
                error = %error,
                "lark receipt send failed"
            );
        }
    }

    fn publish_state(&self, state: TransportState) {
        let _ = self.state_tx.send(state.clone());
        self.push_event(TransportEvent::State(state));
    }

    fn publish_message(&self, headers: FrameHeaders, done: &Reassembly) {
        let size = u32::try_from(done.payload.len()).unwrap_or(u32::MAX);
        let permit = self.event_bytes.clone().try_acquire_many_owned(size).ok();
        if permit.is_none() {
            tracing::warn!(
                message_id = done.message_id,
                "lark transport observation byte budget exhausted; dropping message event"
            );
            return;
        }
        self.push_event(TransportEvent::Message {
            headers,
            payload: done.payload.clone(),
            permit,
        });
    }

    fn anomaly(&self, kind: &'static str, message_id: Option<String>) {
        tracing::warn!(
            kind,
            message_id = message_id.as_deref().unwrap_or(""),
            "lark transport anomaly"
        );
        self.push_event(TransportEvent::Anomaly { kind, message_id });
    }

    fn push_event(&self, event: TransportEvent) {
        if self.event_tx.try_send(event).is_err() {
            tracing::warn!("lark transport observation channel full; dropping event");
        }
    }
}

/// Fail-closed classification for the connect phase: permanent auth and
/// exhausted bounds cannot succeed on retry, and a protocol violation from
/// bootstrap means the endpoint response is unparsable — retrying the same
/// parse cannot help either. All three degrade without further attempts.
fn is_fatal(error: &LarkError) -> bool {
    matches!(
        error.kind(),
        LarkErrorKind::PermanentAuth | LarkErrorKind::Exhausted | LarkErrorKind::ProtocolViolation
    )
}

async fn send_ping(sink: &mut WsSink, service_id: i32) -> Result<(), ()> {
    let encoded = Frame::ping(service_id).encode_to_vec();
    sink.send(Message::Binary(encoded.into()))
        .await
        .map_err(|_| ())
}

/// Builds the receipt frame: the inbound frame's headers plus `biz_rt`, with
/// payload JSON `{code: 200, data?}` on handler success or `{code: 500}` on
/// failure. `data` is base64(JSON of the handler's return value).
fn build_receipt(
    frame: &Frame,
    result: Result<Option<Value>, LarkError>,
    elapsed_ms: u64,
) -> Frame {
    let mut receipt = frame.clone();
    receipt.headers.push(Header {
        key: header_key::BIZ_RT.to_owned(),
        value: elapsed_ms.to_string(),
    });
    let body = match result {
        Ok(value) => {
            let mut body = serde_json::json!({ "code": 200 });
            if let Some(value) = value {
                let encoded = serde_json::to_vec(&value)
                    .map(|json| BASE64.encode(json))
                    .unwrap_or_default();
                body["data"] = Value::String(encoded);
            }
            body
        }
        Err(_) => serde_json::json!({ "code": 500 }),
    };
    receipt.payload = Some(Bytes::from(
        serde_json::to_vec(&body).unwrap_or_else(|_| b"{\"code\":500}".to_vec()),
    ));
    receipt
}
