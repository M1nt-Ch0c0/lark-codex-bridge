use std::{fmt, io, sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot},
    task::JoinHandle,
    time::{Instant, timeout},
};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Error as WebSocketError, Message,
        protocol::{CloseFrame, frame::coding::CloseCode},
    },
};
use tokio_util::{
    codec::{FramedRead, LinesCodec, LinesCodecError},
    sync::CancellationToken,
};

use crate::{
    codex::protocol::{InboundMessage, OutboundMessage, ProtocolError, decode_line, encode_line},
    limits::{
        EVENT_CAPACITY, EXTERNAL_WS_CLOSE_TIMEOUT, EXTERNAL_WS_IO_TIMEOUT,
        EXTERNAL_WS_MESSAGE_BYTES, HIGH_PRIORITY_BURST, MAX_JSONL_LINE_BYTES,
        MAX_STDERR_LINE_BYTES, RPC_HIGH_CAPACITY, RPC_NORMAL_CAPACITY, TRANSPORT_BYTE_BUDGET,
        TRANSPORT_HIGH_BYTE_BUDGET,
    },
};

const STDERR_REPORT_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TransportIoError {
    pub kind: io::ErrorKind,
}

impl fmt::Display for TransportIoError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "transport I/O failed ({:?})", self.kind)
    }
}

impl std::error::Error for TransportIoError {}

impl From<io::Error> for TransportIoError {
    fn from(error: io::Error) -> Self {
        Self { kind: error.kind() }
    }
}

#[derive(Debug)]
pub enum TransportEvent {
    Message(BudgetedInboundMessage),
    ProtocolError(ProtocolError),
    ReadError(TransportIoError),
    WriteError(TransportIoError),
    StdoutEof,
    StderrLine { byte_len: usize },
    WebSocketClosed(WebSocketCloseReport),
    ConnectionError,
    Cancelled,
}

pub struct BudgetedInboundMessage {
    message: InboundMessage,
    budget: OwnedSemaphorePermit,
}

impl BudgetedInboundMessage {
    #[must_use]
    pub fn message(&self) -> &InboundMessage {
        &self.message
    }

    pub fn into_parts(self) -> (InboundMessage, OwnedSemaphorePermit) {
        (self.message, self.budget)
    }
}

impl fmt::Debug for BudgetedInboundMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportExit {
    Cancelled,
    StdoutEof,
    ProtocolViolation,
    ReadError(io::ErrorKind),
    WriteError(io::ErrorKind),
    WebSocketClosed(WebSocketCloseReport),
    ConnectionFailed,
    Aborted,
    TaskFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WebSocketCloseInitiator {
    Local,
    Peer,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WebSocketCloseHandshake {
    Complete,
    Incomplete,
}

/// Content-free WebSocket close evidence. A missing code is kept distinct from an abnormal 1006
/// observation, and remote close reasons are never retained.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WebSocketCloseReport {
    pub initiator: WebSocketCloseInitiator,
    pub handshake: WebSocketCloseHandshake,
    pub code: Option<u16>,
}

#[derive(Debug, Error)]
pub enum TransportSendError {
    #[error("outbound app-server message is invalid")]
    Protocol(#[source] ProtocolError),
    #[error("app-server transport is closed")]
    Closed,
    #[error("app-server transport was cancelled")]
    Cancelled,
    #[error("app-server transport write failed")]
    Io(#[source] TransportIoError),
}

struct QueuedFrame {
    bytes: Vec<u8>,
    _budget: OwnedSemaphorePermit,
    written: Option<oneshot::Sender<Result<(), TransportIoError>>>,
}

#[derive(Clone)]
pub struct TransportSender {
    tx: mpsc::Sender<QueuedFrame>,
    budget: Arc<Semaphore>,
    cancellation: CancellationToken,
}

impl TransportSender {
    /// Encodes and queues a message under both count and byte limits.
    ///
    /// # Errors
    ///
    /// Returns an error if encoding fails or the connection closes while waiting
    /// for bounded queue capacity.
    pub async fn send(&self, message: OutboundMessage) -> Result<(), TransportSendError> {
        self.enqueue(message, None).await
    }

    /// Queues a frame and waits until the writer has flushed it to app-server stdin.
    pub(crate) async fn send_confirmed(
        &self,
        message: OutboundMessage,
    ) -> Result<(), TransportSendError> {
        let (written_tx, written_rx) = oneshot::channel();
        self.enqueue(message, Some(written_tx)).await?;
        tokio::select! {
            biased;
            result = written_rx => match result {
                Ok(Ok(())) => Ok(()),
                Ok(Err(error)) => Err(TransportSendError::Io(error)),
                Err(_) => Err(TransportSendError::Closed),
            },
            () = self.cancellation.cancelled() => Err(TransportSendError::Cancelled),
        }
    }

    async fn enqueue(
        &self,
        message: OutboundMessage,
        written: Option<oneshot::Sender<Result<(), TransportIoError>>>,
    ) -> Result<(), TransportSendError> {
        let maximum_frame_permits = byte_permits(MAX_JSONL_LINE_BYTES.saturating_add(1));
        let mut budget = tokio::select! {
            biased;
            () = self.cancellation.cancelled() => return Err(TransportSendError::Cancelled),
            permit = Arc::clone(&self.budget).acquire_many_owned(maximum_frame_permits) => {
                permit.map_err(|_| TransportSendError::Closed)?
            }
        };
        let bytes = encode_line(&message).map_err(TransportSendError::Protocol)?;
        let excess = budget.num_permits().saturating_sub(bytes.len());
        drop(budget.split(excess));
        let frame = QueuedFrame {
            bytes,
            _budget: budget,
            written,
        };
        tokio::select! {
            biased;
            () = self.cancellation.cancelled() => Err(TransportSendError::Cancelled),
            result = self.tx.send(frame) => result.map_err(|_| TransportSendError::Closed),
        }
    }

    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed() || self.cancellation.is_cancelled()
    }
}

struct InternalEvent {
    event: TransportEvent,
}

pub struct TransportEventReceiver {
    rx: mpsc::Receiver<InternalEvent>,
    terminal_rx: Option<oneshot::Receiver<TransportEvent>>,
    normal_closed: bool,
}

impl TransportEventReceiver {
    pub async fn recv(&mut self) -> Option<TransportEvent> {
        if !self.normal_closed {
            if let Some(event) = self.rx.recv().await {
                return Some(event.event);
            }
            self.normal_closed = true;
        }
        self.terminal_rx.take()?.await.ok()
    }

    /// # Errors
    ///
    /// Returns Tokio's empty or disconnected receive error when no event is ready.
    pub fn try_recv(&mut self) -> Result<TransportEvent, mpsc::error::TryRecvError> {
        if !self.normal_closed {
            match self.rx.try_recv() {
                Ok(event) => return Ok(event.event),
                Err(mpsc::error::TryRecvError::Empty) => {
                    return Err(mpsc::error::TryRecvError::Empty);
                }
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    self.normal_closed = true;
                }
            }
        }

        match self.terminal_rx.as_mut() {
            Some(receiver) => match receiver.try_recv() {
                Ok(event) => {
                    self.terminal_rx = None;
                    Ok(event)
                }
                Err(oneshot::error::TryRecvError::Empty) => Err(mpsc::error::TryRecvError::Empty),
                Err(oneshot::error::TryRecvError::Closed) => {
                    self.terminal_rx = None;
                    Err(mpsc::error::TryRecvError::Disconnected)
                }
            },
            None => Err(mpsc::error::TryRecvError::Disconnected),
        }
    }
}

pub struct TransportHandle {
    pub high_tx: TransportSender,
    pub normal_tx: TransportSender,
    pub events: TransportEventReceiver,
    cancellation: CancellationToken,
    driver: Option<JoinHandle<TransportExit>>,
    exit: Option<TransportExit>,
}

impl TransportHandle {
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }

    /// Cancels all I/O tasks and waits until the writer has dropped app-server stdin.
    ///
    /// # Errors
    ///
    /// This operation has no error return; internal task failure is represented by
    /// [`TransportExit::TaskFailed`].
    pub async fn shutdown(&mut self) -> TransportExit {
        if let Some(exit) = self.exit {
            return exit;
        }
        self.cancellation.cancel();
        let exit = match self.driver.take() {
            Some(driver) => driver.await.unwrap_or(TransportExit::TaskFailed),
            None => TransportExit::TaskFailed,
        };
        self.exit = Some(exit);
        exit
    }
}

impl Drop for TransportHandle {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(driver) = &self.driver {
            driver.abort();
        }
    }
}

/// Starts bounded app-server stdio tasks under one cancellation and join owner.
#[must_use]
pub fn spawn_stream_transport<R, W, E>(
    stdout: R,
    stdin: W,
    stderr: E,
    parent_cancellation: CancellationToken,
) -> TransportHandle
where
    R: AsyncRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
    E: AsyncRead + Unpin + Send + 'static,
{
    let cancellation = parent_cancellation.child_token();
    drop(parent_cancellation);
    let outbound_high_budget = Arc::new(Semaphore::new(TRANSPORT_HIGH_BYTE_BUDGET));
    let outbound_normal_budget = Arc::new(Semaphore::new(TRANSPORT_BYTE_BUDGET));
    let inbound_budget = Arc::new(Semaphore::new(TRANSPORT_BYTE_BUDGET));
    let (high_tx, high_rx) = mpsc::channel(RPC_HIGH_CAPACITY);
    let (normal_tx, normal_rx) = mpsc::channel(RPC_NORMAL_CAPACITY);
    let (event_tx, event_rx) = mpsc::channel(EVENT_CAPACITY);
    let (terminal_tx, terminal_rx) = oneshot::channel();

    let reader = tokio::spawn(read_stdout(
        stdout,
        event_tx.clone(),
        inbound_budget,
        cancellation.clone(),
    ));
    let writer = tokio::spawn(write_stdin(stdin, high_rx, normal_rx, cancellation.clone()));
    let stderr = tokio::spawn(drain_stderr(stderr, event_tx, cancellation.clone()));
    let driver_cancel = cancellation.clone();
    let driver = tokio::spawn(async move {
        drive_transport(reader, writer, stderr, terminal_tx, driver_cancel).await
    });

    TransportHandle {
        high_tx: TransportSender {
            tx: high_tx,
            budget: outbound_high_budget,
            cancellation: cancellation.clone(),
        },
        normal_tx: TransportSender {
            tx: normal_tx,
            budget: outbound_normal_budget,
            cancellation: cancellation.clone(),
        },
        events: TransportEventReceiver {
            rx: event_rx,
            terminal_rx: Some(terminal_rx),
            normal_closed: false,
        },
        cancellation,
        driver: Some(driver),
        exit: None,
    }
}

/// Starts a bounded text-frame WebSocket transport. The socket is the only external resource this
/// owner can close; the type graph contains no process factory, child, PID, wait, or signal handle.
#[must_use]
pub fn spawn_websocket_transport<S>(
    socket: WebSocketStream<S>,
    parent_cancellation: CancellationToken,
) -> TransportHandle
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let cancellation = parent_cancellation.child_token();
    drop(parent_cancellation);
    let outbound_high_budget = Arc::new(Semaphore::new(TRANSPORT_HIGH_BYTE_BUDGET));
    let outbound_normal_budget = Arc::new(Semaphore::new(TRANSPORT_BYTE_BUDGET));
    let inbound_budget = Arc::new(Semaphore::new(TRANSPORT_BYTE_BUDGET));
    let (high_tx, high_rx) = mpsc::channel(RPC_HIGH_CAPACITY);
    let (normal_tx, normal_rx) = mpsc::channel(RPC_NORMAL_CAPACITY);
    let (event_tx, event_rx) = mpsc::channel(EVENT_CAPACITY);
    let (terminal_tx, terminal_rx) = oneshot::channel();
    let driver_cancel = cancellation.clone();
    let driver = tokio::spawn(drive_websocket(
        socket,
        high_rx,
        normal_rx,
        event_tx,
        inbound_budget,
        terminal_tx,
        driver_cancel,
    ));

    TransportHandle {
        high_tx: TransportSender {
            tx: high_tx,
            budget: outbound_high_budget,
            cancellation: cancellation.clone(),
        },
        normal_tx: TransportSender {
            tx: normal_tx,
            budget: outbound_normal_budget,
            cancellation: cancellation.clone(),
        },
        events: TransportEventReceiver {
            rx: event_rx,
            terminal_rx: Some(terminal_rx),
            normal_closed: false,
        },
        cancellation,
        driver: Some(driver),
        exit: None,
    }
}

enum WebSocketAction {
    Cancel,
    Inbound(Option<Result<Message, WebSocketError>>),
    Outbound(Option<(QueuedFrame, bool)>),
}

#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_lines)]
async fn drive_websocket<S>(
    mut socket: WebSocketStream<S>,
    mut high_rx: mpsc::Receiver<QueuedFrame>,
    mut normal_rx: mpsc::Receiver<QueuedFrame>,
    event_tx: mpsc::Sender<InternalEvent>,
    inbound_budget: Arc<Semaphore>,
    terminal_tx: oneshot::Sender<TransportEvent>,
    cancellation: CancellationToken,
) -> TransportExit
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut high_burst = 0_usize;
    let (exit, terminal) = loop {
        let outbound = receive_websocket_outbound(&mut high_rx, &mut normal_rx, high_burst);
        tokio::pin!(outbound);
        let action = tokio::select! {
            () = cancellation.cancelled() => WebSocketAction::Cancel,
            inbound = socket.next() => WebSocketAction::Inbound(inbound),
            outbound = &mut outbound => WebSocketAction::Outbound(outbound),
        };
        match action {
            WebSocketAction::Cancel | WebSocketAction::Outbound(None) => {
                let report = close_websocket(&mut socket).await;
                break (
                    TransportExit::WebSocketClosed(report),
                    TransportEvent::WebSocketClosed(report),
                );
            }
            WebSocketAction::Outbound(Some((frame, high))) => {
                if high {
                    high_burst = high_burst.saturating_add(1);
                } else {
                    high_burst = 0;
                }
                if let Err(error) = send_websocket_frame(&mut socket, frame).await {
                    break websocket_error_terminal(&error, WebSocketCloseInitiator::Local);
                }
            }
            WebSocketAction::Inbound(Some(Ok(Message::Text(text)))) => {
                if text.len() > EXTERNAL_WS_MESSAGE_BYTES {
                    break protocol_terminal(ProtocolError::LineTooLong {
                        length: text.len(),
                        maximum: EXTERNAL_WS_MESSAGE_BYTES,
                    });
                }
                let message = match decode_line(text.as_bytes()) {
                    Ok(message) => message,
                    Err(error) => break protocol_terminal(error),
                };
                let weight = text.len().max(message.retained_memory_weight());
                if weight > TRANSPORT_BYTE_BUDGET {
                    break protocol_terminal(ProtocolError::RetainedMessageTooLarge {
                        maximum: TRANSPORT_BYTE_BUDGET,
                    });
                }
                let permit = tokio::select! {
                    () = cancellation.cancelled() => {
                        let report = close_websocket(&mut socket).await;
                        break (
                            TransportExit::WebSocketClosed(report),
                            TransportEvent::WebSocketClosed(report),
                        );
                    }
                    permit = Arc::clone(&inbound_budget).acquire_many_owned(byte_permits(weight)) => {
                        match permit {
                            Ok(permit) => permit,
                            Err(_) => break (
                                TransportExit::ConnectionFailed,
                                TransportEvent::ConnectionError,
                            ),
                        }
                    }
                };
                let event = InternalEvent {
                    event: TransportEvent::Message(BudgetedInboundMessage {
                        message,
                        budget: permit,
                    }),
                };
                if !send_event(&event_tx, event, &cancellation).await {
                    let report = close_websocket(&mut socket).await;
                    break (
                        TransportExit::WebSocketClosed(report),
                        TransportEvent::WebSocketClosed(report),
                    );
                }
            }
            WebSocketAction::Inbound(Some(Ok(Message::Binary(_) | Message::Frame(_)))) => {
                break protocol_terminal(ProtocolError::InvalidEnvelope(
                    "external WebSocket accepts text RPC frames only",
                ));
            }
            WebSocketAction::Inbound(Some(Ok(Message::Ping(payload)))) => {
                let result =
                    timeout(EXTERNAL_WS_IO_TIMEOUT, socket.send(Message::Pong(payload))).await;
                if !matches!(result, Ok(Ok(()))) {
                    break (
                        TransportExit::ConnectionFailed,
                        TransportEvent::ConnectionError,
                    );
                }
            }
            WebSocketAction::Inbound(Some(Ok(Message::Pong(_)))) => {}
            WebSocketAction::Inbound(Some(Ok(Message::Close(frame)))) => {
                let code = frame.map(|frame| u16::from(frame.code));
                let _ = timeout(EXTERNAL_WS_IO_TIMEOUT, socket.flush()).await;
                let report = WebSocketCloseReport {
                    initiator: WebSocketCloseInitiator::Peer,
                    handshake: WebSocketCloseHandshake::Complete,
                    code,
                };
                break (
                    TransportExit::WebSocketClosed(report),
                    TransportEvent::WebSocketClosed(report),
                );
            }
            WebSocketAction::Inbound(Some(Err(error))) => {
                break websocket_error_terminal(&error, WebSocketCloseInitiator::Peer);
            }
            WebSocketAction::Inbound(None) => {
                let report = WebSocketCloseReport {
                    initiator: WebSocketCloseInitiator::Peer,
                    handshake: WebSocketCloseHandshake::Incomplete,
                    code: None,
                };
                break (
                    TransportExit::WebSocketClosed(report),
                    TransportEvent::WebSocketClosed(report),
                );
            }
        }
    };
    cancellation.cancel();
    high_rx.close();
    normal_rx.close();
    let _ = terminal_tx.send(terminal);
    exit
}

async fn receive_websocket_outbound(
    high_rx: &mut mpsc::Receiver<QueuedFrame>,
    normal_rx: &mut mpsc::Receiver<QueuedFrame>,
    high_burst: usize,
) -> Option<(QueuedFrame, bool)> {
    if high_burst >= HIGH_PRIORITY_BURST {
        tokio::select! {
            biased;
            frame = normal_rx.recv() => frame.map(|frame| (frame, false)),
            frame = high_rx.recv() => frame.map(|frame| (frame, true)),
        }
    } else {
        tokio::select! {
            biased;
            frame = high_rx.recv() => frame.map(|frame| (frame, true)),
            frame = normal_rx.recv() => frame.map(|frame| (frame, false)),
        }
    }
}

async fn send_websocket_frame<S>(
    socket: &mut WebSocketStream<S>,
    mut frame: QueuedFrame,
) -> Result<(), WebSocketError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if frame.bytes.last() == Some(&b'\n') {
        frame.bytes.pop();
    }
    let Ok(text) = String::from_utf8(frame.bytes) else {
        if let Some(written) = frame.written {
            let _ = written.send(Err(TransportIoError {
                kind: io::ErrorKind::InvalidData,
            }));
        }
        return Err(WebSocketError::Utf8(
            "outbound RPC was not UTF-8".to_owned(),
        ));
    };
    let result = timeout(
        EXTERNAL_WS_IO_TIMEOUT,
        socket.send(Message::Text(text.into())),
    )
    .await
    .map_err(|_| WebSocketError::Io(io::Error::new(io::ErrorKind::TimedOut, "write timeout")))?;
    match result {
        Ok(()) => {
            if let Some(written) = frame.written {
                let _ = written.send(Ok(()));
            }
            Ok(())
        }
        Err(error) => {
            if let Some(written) = frame.written {
                let _ = written.send(Err(TransportIoError {
                    kind: io::ErrorKind::BrokenPipe,
                }));
            }
            Err(error)
        }
    }
}

async fn close_websocket<S>(socket: &mut WebSocketStream<S>) -> WebSocketCloseReport
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let frame = CloseFrame {
        code: CloseCode::Normal,
        reason: "".into(),
    };
    if !matches!(
        timeout(
            EXTERNAL_WS_IO_TIMEOUT,
            socket.send(Message::Close(Some(frame)))
        )
        .await,
        Ok(Ok(()))
    ) {
        return WebSocketCloseReport {
            initiator: WebSocketCloseInitiator::Local,
            handshake: WebSocketCloseHandshake::Incomplete,
            code: None,
        };
    }
    let peer = timeout(EXTERNAL_WS_CLOSE_TIMEOUT, async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Close(frame))) => {
                    return Some(frame.map(|frame| u16::from(frame.code)));
                }
                Some(Ok(Message::Ping(payload))) => {
                    if socket.send(Message::Pong(payload)).await.is_err() {
                        return None;
                    }
                }
                Some(Ok(Message::Pong(_) | Message::Text(_) | Message::Binary(_))) => {}
                Some(Err(WebSocketError::ConnectionClosed)) => return Some(None),
                Some(Ok(Message::Frame(_)) | Err(_)) | None => return None,
            }
        }
    })
    .await;
    match peer {
        Ok(Some(code)) => WebSocketCloseReport {
            initiator: WebSocketCloseInitiator::Local,
            handshake: WebSocketCloseHandshake::Complete,
            code,
        },
        Ok(None) | Err(_) => WebSocketCloseReport {
            initiator: WebSocketCloseInitiator::Local,
            handshake: WebSocketCloseHandshake::Incomplete,
            code: None,
        },
    }
}

fn protocol_terminal(error: ProtocolError) -> (TransportExit, TransportEvent) {
    (
        TransportExit::ProtocolViolation,
        TransportEvent::ProtocolError(error),
    )
}

fn websocket_error_terminal(
    error: &WebSocketError,
    initiator: WebSocketCloseInitiator,
) -> (TransportExit, TransportEvent) {
    match error {
        WebSocketError::ConnectionClosed => {
            let report = WebSocketCloseReport {
                initiator,
                handshake: WebSocketCloseHandshake::Complete,
                code: None,
            };
            (
                TransportExit::WebSocketClosed(report),
                TransportEvent::WebSocketClosed(report),
            )
        }
        WebSocketError::Capacity(_)
        | WebSocketError::Protocol(_)
        | WebSocketError::Utf8(_)
        | WebSocketError::AttackAttempt => protocol_terminal(ProtocolError::InvalidEnvelope(
            "external WebSocket framing violated protocol policy",
        )),
        WebSocketError::AlreadyClosed
        | WebSocketError::Io(_)
        | WebSocketError::Tls(_)
        | WebSocketError::WriteBufferFull(_)
        | WebSocketError::Url(_)
        | WebSocketError::Http(_)
        | WebSocketError::HttpFormat(_) => (
            TransportExit::ConnectionFailed,
            TransportEvent::ConnectionError,
        ),
    }
}

async fn read_stdout<R>(
    stdout: R,
    event_tx: mpsc::Sender<InternalEvent>,
    budget: Arc<Semaphore>,
    cancellation: CancellationToken,
) -> ReaderExit
where
    R: AsyncRead + Unpin,
{
    let mut framed = FramedRead::new(
        stdout,
        LinesCodec::new_with_max_length(MAX_JSONL_LINE_BYTES),
    );
    loop {
        let frame = tokio::select! {
            biased;
            () = cancellation.cancelled() => return ReaderExit::Cancelled,
            frame = framed.next() => frame,
        };
        match frame {
            Some(Ok(line)) => match decode_line(line.as_bytes()) {
                Ok(message) => {
                    let weight = line.len().max(message.retained_memory_weight());
                    if weight > TRANSPORT_BYTE_BUDGET {
                        return ReaderExit::Protocol(ProtocolError::RetainedMessageTooLarge {
                            maximum: TRANSPORT_BYTE_BUDGET,
                        });
                    }
                    let permit = tokio::select! {
                        biased;
                        () = cancellation.cancelled() => return ReaderExit::Cancelled,
                        permit = Arc::clone(&budget).acquire_many_owned(byte_permits(weight)) => {
                            match permit {
                                Ok(permit) => permit,
                                Err(_) => return ReaderExit::Cancelled,
                            }
                        }
                    };
                    let event = InternalEvent {
                        event: TransportEvent::Message(BudgetedInboundMessage {
                            message,
                            budget: permit,
                        }),
                    };
                    if !send_event(&event_tx, event, &cancellation).await {
                        return ReaderExit::Cancelled;
                    }
                }
                Err(error) => {
                    let event = InternalEvent {
                        event: TransportEvent::ProtocolError(error),
                    };
                    if !send_event(&event_tx, event, &cancellation).await {
                        return ReaderExit::Cancelled;
                    }
                }
            },
            Some(Err(LinesCodecError::MaxLineLengthExceeded)) => {
                return ReaderExit::Protocol(ProtocolError::LineTooLong {
                    length: MAX_JSONL_LINE_BYTES.saturating_add(1),
                    maximum: MAX_JSONL_LINE_BYTES,
                });
            }
            Some(Err(LinesCodecError::Io(error))) => {
                return ReaderExit::Read(error.into());
            }
            None => return ReaderExit::Eof,
        }
    }
}

async fn write_stdin<W>(
    mut stdin: W,
    mut high_rx: mpsc::Receiver<QueuedFrame>,
    mut normal_rx: mpsc::Receiver<QueuedFrame>,
    cancellation: CancellationToken,
) -> WriterExit
where
    W: AsyncWrite + Unpin,
{
    let mut high_burst = 0;
    loop {
        let frame = if high_burst >= HIGH_PRIORITY_BURST {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return WriterExit::Cancelled,
                Some(frame) = normal_rx.recv() => {
                    high_burst = 0;
                    Some(frame)
                }
                Some(frame) = high_rx.recv() => {
                    high_burst = high_burst.saturating_add(1);
                    Some(frame)
                }
                else => None,
            }
        } else {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => return WriterExit::Cancelled,
                Some(frame) = high_rx.recv() => {
                    high_burst = high_burst.saturating_add(1);
                    Some(frame)
                }
                Some(frame) = normal_rx.recv() => {
                    high_burst = 0;
                    Some(frame)
                }
                else => None,
            }
        };

        let Some(frame) = frame else {
            return WriterExit::Cancelled;
        };
        if let Err(error) = stdin.write_all(&frame.bytes).await {
            let error = TransportIoError::from(error);
            if let Some(written) = frame.written {
                let _ = written.send(Err(error));
            }
            return WriterExit::Write(error);
        }
        if let Err(error) = stdin.flush().await {
            let error = TransportIoError::from(error);
            if let Some(written) = frame.written {
                let _ = written.send(Err(error));
            }
            return WriterExit::Write(error);
        }
        if let Some(written) = frame.written {
            let _ = written.send(Ok(()));
        }
    }
}

async fn drain_stderr<E>(
    mut stderr: E,
    event_tx: mpsc::Sender<InternalEvent>,
    cancellation: CancellationToken,
) where
    E: AsyncRead + Unpin,
{
    let mut buffer = [0_u8; 8 * 1024];
    let mut line_len = 0_usize;
    let mut previous_was_cr = false;
    let mut next_report = Instant::now();
    loop {
        let read = tokio::select! {
            biased;
            () = cancellation.cancelled() => return,
            read = stderr.read(&mut buffer) => read,
        };
        let count = match read {
            Ok(0) | Err(_) => return,
            Ok(count) => count,
        };
        for &byte in &buffer[..count] {
            if byte == b'\n' {
                let visible_len = line_len.saturating_sub(usize::from(previous_was_cr));
                if Instant::now() >= next_report {
                    let _ = event_tx.try_send(InternalEvent {
                        event: TransportEvent::StderrLine {
                            byte_len: visible_len,
                        },
                    });
                    next_report = Instant::now() + STDERR_REPORT_INTERVAL;
                }
                line_len = 0;
                previous_was_cr = false;
            } else {
                line_len = line_len
                    .saturating_add(1)
                    .min(MAX_STDERR_LINE_BYTES.saturating_add(1));
                previous_was_cr = byte == b'\r';
            }
        }
    }
}

async fn drive_transport(
    mut reader: JoinHandle<ReaderExit>,
    mut writer: JoinHandle<WriterExit>,
    stderr: JoinHandle<()>,
    terminal_tx: oneshot::Sender<TransportEvent>,
    cancellation: CancellationToken,
) -> TransportExit {
    enum FirstExit {
        Cancelled,
        Reader(Result<ReaderExit, tokio::task::JoinError>),
        Writer(Result<WriterExit, tokio::task::JoinError>),
    }

    let first = tokio::select! {
        biased;
        () = cancellation.cancelled() => FirstExit::Cancelled,
        result = &mut reader => FirstExit::Reader(result),
        result = &mut writer => FirstExit::Writer(result),
    };

    cancellation.cancel();
    let (exit, terminal) = match first {
        FirstExit::Cancelled => {
            reader.abort();
            writer.abort();
            let _ = reader.await;
            let _ = writer.await;
            (TransportExit::Cancelled, TransportEvent::Cancelled)
        }
        FirstExit::Reader(result) => {
            writer.abort();
            let _ = writer.await;
            reader_terminal(result)
        }
        FirstExit::Writer(result) => {
            reader.abort();
            let _ = reader.await;
            writer_terminal(&result)
        }
    };
    stderr.abort();
    let _ = stderr.await;
    let _ = terminal_tx.send(terminal);
    exit
}

fn reader_terminal(
    result: Result<ReaderExit, tokio::task::JoinError>,
) -> (TransportExit, TransportEvent) {
    match result {
        Ok(ReaderExit::Cancelled) => (TransportExit::Cancelled, TransportEvent::Cancelled),
        Ok(ReaderExit::Eof) => (TransportExit::StdoutEof, TransportEvent::StdoutEof),
        Ok(ReaderExit::Protocol(error)) => (
            TransportExit::ProtocolViolation,
            TransportEvent::ProtocolError(error),
        ),
        Ok(ReaderExit::Read(error)) => (
            TransportExit::ReadError(error.kind),
            TransportEvent::ReadError(error),
        ),
        Err(_) => (TransportExit::TaskFailed, TransportEvent::Cancelled),
    }
}

fn writer_terminal(
    result: &Result<WriterExit, tokio::task::JoinError>,
) -> (TransportExit, TransportEvent) {
    match result {
        Ok(WriterExit::Cancelled) => (TransportExit::Cancelled, TransportEvent::Cancelled),
        Ok(WriterExit::Write(error)) => (
            TransportExit::WriteError(error.kind),
            TransportEvent::WriteError(*error),
        ),
        Err(_) => (TransportExit::TaskFailed, TransportEvent::Cancelled),
    }
}

async fn send_event(
    event_tx: &mpsc::Sender<InternalEvent>,
    event: InternalEvent,
    cancellation: &CancellationToken,
) -> bool {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => false,
        result = event_tx.send(event) => result.is_ok(),
    }
}

fn byte_permits(length: usize) -> u32 {
    u32::try_from(length.max(1)).expect("protocol frame length is bounded below u32::MAX")
}

enum ReaderExit {
    Cancelled,
    Eof,
    Protocol(ProtocolError),
    Read(TransportIoError),
}

enum WriterExit {
    Cancelled,
    Write(TransportIoError),
}

#[cfg(test)]
mod websocket_queue_tests {
    use super::*;

    async fn queued(marker: u8) -> QueuedFrame {
        let budget = Arc::new(Semaphore::new(1))
            .acquire_owned()
            .await
            .expect("test byte permit is available");
        QueuedFrame {
            bytes: vec![marker],
            _budget: budget,
            written: None,
        }
    }

    #[tokio::test]
    async fn websocket_queues_prioritize_high_with_a_normal_starvation_bound() {
        let (high_tx, mut high_rx) = mpsc::channel(16);
        let (normal_tx, mut normal_rx) = mpsc::channel(2);
        normal_tx
            .send(queued(b'n').await)
            .await
            .expect("normal frame queues");
        for marker in b'a'..=b'i' {
            high_tx
                .send(queued(marker).await)
                .await
                .expect("high frame queues");
        }

        let mut high_burst = 0;
        for expected in b'a'..=b'h' {
            let (frame, high) =
                receive_websocket_outbound(&mut high_rx, &mut normal_rx, high_burst)
                    .await
                    .expect("queued frame is selected");
            assert!(high);
            assert_eq!(frame.bytes, vec![expected]);
            high_burst = high_burst.saturating_add(1);
        }
        let (normal, high) = receive_websocket_outbound(&mut high_rx, &mut normal_rx, high_burst)
            .await
            .expect("normal starvation bound selects a frame");
        assert!(!high);
        assert_eq!(normal.bytes, vec![b'n']);
    }
}
