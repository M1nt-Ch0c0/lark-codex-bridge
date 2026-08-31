//! Opt-in, exact-binary proof for production Router persisted-thread handoff.
//!
//! Owners A and C are deliberately small, independent stdio JSON-RPC clients. Only owner B uses
//! the bridge's production Supervisor, Router, durable Store, and Outbox reply sink. Ordinary test
//! runs ignore this smoke; its explicit gate fails closed on any missing or mismatched input.

use std::{
    collections::VecDeque,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, bail, ensure};
use lark_codex_bridge::{
    codex::{
        external::CodexBackendConfig,
        supervisor::{AppServerSupervisor, SupervisorHandle, SupervisorState},
        types::{ApprovalPolicy, SandboxMode},
    },
    config::{BridgeConfig, CodexSection, WorkspacePolicy},
    lark::{
        api::ChatMode,
        bridge::QueuedInboundEvent,
        config::TenantBrand,
        credentials::LarkCredentials,
        normalize::{InboundEvent, ScopeKey},
    },
    outbox::{OutboxOperation, OutboxReplySink},
    runtime::{
        commands::{BridgeCommand, parse_command},
        intake::TenantNamespace,
        policy::AccessPolicy,
        router::{Router, RouterSettings},
    },
    store::{
        DedupOutcome, InboundEventState, InboundKey, StoreHandle, ThreadAdoptionOutcome,
        ThreadAdoptionState, ThreadOrigin,
    },
};
#[cfg(unix)]
use nix::{
    errno::Errno,
    sys::signal::{Signal, killpg},
    unistd::Pid,
};
use secrecy::SecretString;
use semver::Version;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::{Child, ChildStderr, ChildStdin, ChildStdout, Command},
    sync::{Notify, Semaphore},
    task::JoinHandle,
    time::{Instant, sleep, timeout},
};
use tokio_util::sync::CancellationToken;

const OWNER_ID: &str = "ou_router_handoff_owner_123456";
const CHAT_ID: &str = "oc_router_handoff_chat_123456";
const MODEL_PROVIDER: &str = "thread-adoption-router-smoke";
const MODEL: &str = "gpt-5.4";
const OWNER_A_INPUT: &str = "owner_a_input_4f90f16bcf9541e3935d";
const OWNER_A_OUTPUT: &str = "owner_a_output_5926804b02414639a297";
const OWNER_B_INPUT: &str = "owner_b_input_b47933f05eb344bda9ef";
const OWNER_B_OUTPUT: &str = "owner_b_output_5561039610814b79979d";

const RPC_TIMEOUT: Duration = Duration::from_secs(20);
const TURN_TIMEOUT: Duration = Duration::from_secs(60);
const ROUTER_TIMEOUT: Duration = Duration::from_secs(60);
const PROCESS_GRACE: Duration = Duration::from_secs(5);
const PROCESS_TREE_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_PROTOCOL_LINE_BYTES: usize = 2 * 1024 * 1024;
const MAX_PROTOCOL_STREAM_BYTES: usize = 16 * 1024 * 1024;
const MAX_PROVIDER_REQUEST_BYTES: usize = 2 * 1024 * 1024;
const MAX_NOTIFICATION_BACKLOG: usize = 512;
const MAX_VERSION_OUTPUT_BYTES: usize = 4096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProductionBackend {
    SpawnedStdio,
    ProtocolSidecar,
}

impl ProductionBackend {
    fn required(expected: &Version, configured: &str) -> Result<Self> {
        let backend = match configured {
            "spawned-stdio" => Self::SpawnedStdio,
            "protocol-sidecar" => Self::ProtocolSidecar,
            _ => bail!("production backend gate is invalid"),
        };
        let required = match (expected.major, expected.minor, expected.patch) {
            (0, 149, 0) => Self::SpawnedStdio,
            (0, 151, 0) => Self::ProtocolSidecar,
            _ => bail!("expected version is outside the reviewed handoff matrix"),
        };
        ensure!(
            backend == required,
            "production backend/version gate mismatch"
        );
        Ok(backend)
    }
}

struct ProviderState {
    requests: AtomicUsize,
    current_a_seen: AtomicBool,
    current_b_seen: AtomicBool,
    prior_history_seen: AtomicBool,
    failure: Mutex<Option<&'static str>>,
    changed: Notify,
}

impl ProviderState {
    fn new() -> Self {
        Self {
            requests: AtomicUsize::new(0),
            current_a_seen: AtomicBool::new(false),
            current_b_seen: AtomicBool::new(false),
            prior_history_seen: AtomicBool::new(false),
            failure: Mutex::new(None),
            changed: Notify::new(),
        }
    }

    fn fail(&self, reason: &'static str) {
        if let Ok(mut failure) = self.failure.lock()
            && failure.is_none()
        {
            *failure = Some(reason);
        }
        self.changed.notify_waiters();
    }
}

struct ScriptedResponsesServer {
    base_url: String,
    state: Arc<ProviderState>,
    cancellation: CancellationToken,
    task: Option<JoinHandle<()>>,
}

impl ScriptedResponsesServer {
    async fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("unable to bind local Responses API stub")?;
        let address = listener
            .local_addr()
            .context("unable to inspect local Responses API stub")?;
        let state = Arc::new(ProviderState::new());
        let cancellation = CancellationToken::new();
        let task_state = Arc::clone(&state);
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            loop {
                let accepted = tokio::select! {
                    biased;
                    () = task_cancellation.cancelled() => break,
                    accepted = listener.accept() => accepted,
                };
                let Ok((stream, _)) = accepted else {
                    task_state.fail("provider_accept_failed");
                    break;
                };
                let connection_state = Arc::clone(&task_state);
                let connection_cancellation = task_cancellation.clone();
                tokio::spawn(async move {
                    if serve_provider_request(
                        stream,
                        Arc::clone(&connection_state),
                        connection_cancellation,
                    )
                    .await
                    .is_err()
                    {
                        connection_state.fail("provider_protocol_failed");
                    }
                });
            }
        });
        Ok(Self {
            base_url: format!("http://{address}/v1"),
            state,
            cancellation,
            task: Some(task),
        })
    }

    async fn wait_for_requests(&self, expected: usize) -> Result<()> {
        timeout(ROUTER_TIMEOUT, async {
            loop {
                let changed = self.state.changed.notified();
                if self.state.requests.load(Ordering::SeqCst) >= expected {
                    return;
                }
                changed.await;
            }
        })
        .await
        .context("provider request count did not reach the expected bound")?;
        Ok(())
    }

    fn assert_complete(&self) -> Result<()> {
        ensure!(
            self.state.requests.load(Ordering::SeqCst) == 2,
            "provider request count mismatch"
        );
        ensure!(
            self.state.current_a_seen.load(Ordering::SeqCst)
                && self.state.current_b_seen.load(Ordering::SeqCst),
            "provider did not observe both current inputs"
        );
        ensure!(
            self.state.prior_history_seen.load(Ordering::SeqCst),
            "provider continuation omitted prior history"
        );
        ensure!(
            self.state
                .failure
                .lock()
                .map_err(|_| anyhow::anyhow!("provider state lock failed"))?
                .is_none(),
            "provider recorded a protocol failure"
        );
        Ok(())
    }

    async fn shutdown(mut self) -> Result<()> {
        self.cancellation.cancel();
        let Some(mut task) = self.task.take() else {
            return Ok(());
        };
        if timeout(PROCESS_GRACE, &mut task).await.is_err() {
            task.abort();
            let _ = task.await;
            bail!("provider shutdown timed out");
        }
        Ok(())
    }
}

impl Drop for ScriptedResponsesServer {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

async fn serve_provider_request(
    mut stream: TcpStream,
    state: Arc<ProviderState>,
    cancellation: CancellationToken,
) -> Result<()> {
    let request = read_http_request(&mut stream, &cancellation).await?;
    ensure!(
        request.starts_with(b"POST /v1/responses "),
        "provider received an unexpected request"
    );
    let header_end = request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| index + 4)
        .context("provider request omitted header terminator")?;
    let body = std::str::from_utf8(&request[header_end..])
        .context("provider request body was not UTF-8")?;
    let index = state.requests.fetch_add(1, Ordering::SeqCst);
    state.changed.notify_waiters();
    let output = match index {
        0 => {
            ensure!(
                body.contains(OWNER_A_INPUT),
                "first provider request omitted current input"
            );
            state.current_a_seen.store(true, Ordering::SeqCst);
            OWNER_A_OUTPUT
        }
        1 => {
            ensure!(
                body.contains(OWNER_B_INPUT),
                "second provider request omitted current input"
            );
            state.current_b_seen.store(true, Ordering::SeqCst);
            if body.contains(OWNER_A_INPUT) || body.contains(OWNER_A_OUTPUT) {
                state.prior_history_seen.store(true, Ordering::SeqCst);
            }
            OWNER_B_OUTPUT
        }
        _ => bail!("provider received an unexpected extra request"),
    };
    send_sse(&mut stream, response_sse(index, output)).await
}

async fn read_http_request(
    stream: &mut TcpStream,
    cancellation: &CancellationToken,
) -> Result<Vec<u8>> {
    let mut request = Vec::new();
    let mut chunk = [0_u8; 4096];
    let header_end = loop {
        let read = tokio::select! {
            biased;
            () = cancellation.cancelled() => bail!("provider stopped while reading"),
            read = stream.read(&mut chunk) => read,
        }
        .context("unable to read provider request")?;
        ensure!(read != 0, "provider peer closed before request headers");
        ensure!(
            request.len().saturating_add(read) <= MAX_PROVIDER_REQUEST_BYTES,
            "provider request exceeded its byte bound"
        );
        request.extend_from_slice(&chunk[..read]);
        if let Some(index) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let headers = std::str::from_utf8(&request[..header_end])
        .context("provider request headers were not UTF-8")?;
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .context("provider request omitted content length")?;
    let total = header_end
        .checked_add(content_length)
        .context("provider request length overflowed")?;
    ensure!(
        total <= MAX_PROVIDER_REQUEST_BYTES,
        "provider request body exceeded its byte bound"
    );
    while request.len() < total {
        let read = tokio::select! {
            biased;
            () = cancellation.cancelled() => bail!("provider stopped while reading"),
            read = stream.read(&mut chunk) => read,
        }
        .context("unable to read provider request body")?;
        ensure!(read != 0, "provider peer closed before request body");
        ensure!(
            request.len().saturating_add(read) <= MAX_PROVIDER_REQUEST_BYTES,
            "provider request body exceeded its byte bound"
        );
        request.extend_from_slice(&chunk[..read]);
    }
    ensure!(
        request.len() == total,
        "provider request contained trailing bytes"
    );
    Ok(request)
}

fn response_sse(index: usize, output: &str) -> String {
    let response_id = format!("resp-router-handoff-{}", index + 1);
    let message_id = format!("msg-router-handoff-{}", index + 1);
    let events = [
        json!({"type": "response.created", "response": {"id": response_id}}),
        json!({
            "type": "response.output_item.done",
            "item": {
                "type": "message",
                "role": "assistant",
                "id": message_id,
                "content": [{"type": "output_text", "text": output}]
            }
        }),
        json!({
            "type": "response.completed",
            "response": {
                "id": response_id,
                "usage": {
                    "input_tokens": 0,
                    "input_tokens_details": null,
                    "output_tokens": 0,
                    "output_tokens_details": null,
                    "total_tokens": 0
                }
            }
        }),
    ];
    let mut body = String::new();
    for event in events {
        let kind = event["type"].as_str().unwrap_or("unknown");
        body.push_str("event: ");
        body.push_str(kind);
        body.push('\n');
        body.push_str("data: ");
        body.push_str(&event.to_string());
        body.push_str("\n\n");
    }
    body
}

async fn send_sse(stream: &mut TcpStream, body: String) -> Result<()> {
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(response.as_bytes())
        .await
        .context("unable to send provider response")?;
    stream
        .shutdown()
        .await
        .context("unable to close provider response")?;
    Ok(())
}

struct IndependentOwner {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: ChildStdout,
    stderr_task: Option<JoinHandle<bool>>,
    stdout_buffer: Vec<u8>,
    stdout_bytes: usize,
    notifications: VecDeque<Value>,
    next_id: u64,
    pid: u32,
    stopped: bool,
}

impl IndependentOwner {
    fn spawn(binary: &Path, codex_home: &Path, isolated_home: &Path) -> Result<Self> {
        let mut command = Command::new(binary);
        command
            .args(["app-server", "--listen", "stdio://"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        configure_isolated_environment(&mut command, codex_home, isolated_home);
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt as _;
            command.as_std_mut().process_group(0);
        }
        let mut child = command
            .spawn()
            .context("unable to spawn independent app-server owner")?;
        let pid = child
            .id()
            .context("independent app-server omitted process id")?;
        let stdin = child
            .stdin
            .take()
            .context("independent app-server omitted stdin")?;
        let stdout = child
            .stdout
            .take()
            .context("independent app-server omitted stdout")?;
        let stderr = child
            .stderr
            .take()
            .context("independent app-server omitted stderr")?;
        let stderr_task = tokio::spawn(drain_stderr(stderr));
        Ok(Self {
            child,
            stdin: Some(stdin),
            stdout,
            stderr_task: Some(stderr_task),
            stdout_buffer: Vec::new(),
            stdout_bytes: 0,
            notifications: VecDeque::new(),
            next_id: 1,
            pid,
            stopped: false,
        })
    }

    async fn initialize(&mut self, owner_number: u8) -> Result<()> {
        let result = self
            .request(
                "initialize",
                json!({
                    "clientInfo": {
                        "name": format!("thread-adoption-router-independent-{owner_number}"),
                        "title": "Thread adoption Router smoke",
                        "version": "1.0.0"
                    },
                    "capabilities": {"experimentalApi": false}
                }),
            )
            .await?;
        ensure!(
            result["userAgent"].as_str().is_some(),
            "independent initialize response omitted user agent"
        );
        self.notify("initialized", json!({})).await
    }

    async fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        let id = self.next_id;
        self.next_id = self
            .next_id
            .checked_add(1)
            .context("independent request id overflowed")?;
        self.write_value(&json!({"id": id, "method": method, "params": params}))
            .await?;
        timeout(RPC_TIMEOUT, async {
            loop {
                let value = self.read_protocol_value().await?;
                if value.get("method").is_some() && value.get("id").is_some() {
                    self.reject_reverse_request(&value).await?;
                    continue;
                }
                if value.get("method").is_some() {
                    self.push_notification(value)?;
                    continue;
                }
                ensure!(
                    value.get("id").and_then(Value::as_u64) == Some(id),
                    "independent app-server returned an unknown response"
                );
                ensure!(
                    value.get("error").is_none(),
                    "independent app-server rejected a request"
                );
                return value
                    .get("result")
                    .cloned()
                    .context("independent app-server response omitted result");
            }
        })
        .await
        .context("independent app-server request timed out")?
    }

    async fn notify(&mut self, method: &str, params: Value) -> Result<()> {
        self.write_value(&json!({"method": method, "params": params}))
            .await
    }

    async fn start_turn(&mut self, thread_id: &str, text: &str) -> Result<()> {
        let result = self
            .request(
                "turn/start",
                json!({
                    "threadId": thread_id,
                    "input": [{"type": "text", "text": text, "textElements": []}],
                    "approvalPolicy": "never"
                }),
            )
            .await?;
        let turn_id = result["turn"]["id"]
            .as_str()
            .context("independent turn/start omitted turn id")?
            .to_owned();
        timeout(TURN_TIMEOUT, async {
            loop {
                if let Some(index) = self
                    .notifications
                    .iter()
                    .position(|value| is_turn_completed(value, thread_id, &turn_id))
                {
                    let terminal = self
                        .notifications
                        .remove(index)
                        .context("independent notification disappeared")?;
                    ensure!(
                        terminal["params"]["turn"]["status"] == "completed",
                        "independent turn did not complete"
                    );
                    return Ok(());
                }
                let value = self.read_protocol_value().await?;
                if value.get("method").is_some() && value.get("id").is_some() {
                    self.reject_reverse_request(&value).await?;
                } else if value.get("method").is_some() {
                    self.push_notification(value)?;
                } else {
                    bail!("independent app-server emitted an unexpected response");
                }
            }
        })
        .await
        .context("independent turn completion timed out")?
    }

    async fn write_value(&mut self, value: &Value) -> Result<()> {
        let mut bytes = serde_json::to_vec(value).context("unable to encode independent RPC")?;
        ensure!(
            bytes.len() < MAX_PROTOCOL_LINE_BYTES,
            "independent RPC exceeded line bound"
        );
        bytes.push(b'\n');
        let stdin = self
            .stdin
            .as_mut()
            .context("independent app-server stdin is closed")?;
        stdin
            .write_all(&bytes)
            .await
            .context("unable to write independent RPC")?;
        stdin
            .flush()
            .await
            .context("unable to flush independent RPC")?;
        Ok(())
    }

    async fn read_protocol_value(&mut self) -> Result<Value> {
        loop {
            if let Some(newline) = self.stdout_buffer.iter().position(|byte| *byte == b'\n') {
                let mut line: Vec<u8> = self.stdout_buffer.drain(..=newline).collect();
                line.pop();
                if line.last() == Some(&b'\r') {
                    line.pop();
                }
                if line.is_empty() {
                    continue;
                }
                let value: Value = serde_json::from_slice(&line)
                    .context("independent app-server emitted invalid JSON")?;
                ensure!(
                    value.is_object(),
                    "independent app-server emitted invalid message shape"
                );
                return Ok(value);
            }
            ensure!(
                self.stdout_buffer.len() <= MAX_PROTOCOL_LINE_BYTES,
                "independent app-server line exceeded bound"
            );
            let mut chunk = [0_u8; 8192];
            let read = self
                .stdout
                .read(&mut chunk)
                .await
                .context("unable to read independent app-server stdout")?;
            ensure!(read != 0, "independent app-server stdout closed early");
            self.stdout_bytes = self.stdout_bytes.saturating_add(read);
            ensure!(
                self.stdout_bytes <= MAX_PROTOCOL_STREAM_BYTES,
                "independent app-server stdout exceeded bound"
            );
            ensure!(
                self.stdout_buffer.len().saturating_add(read) <= MAX_PROTOCOL_LINE_BYTES,
                "independent app-server line exceeded bound"
            );
            self.stdout_buffer.extend_from_slice(&chunk[..read]);
        }
    }

    async fn reject_reverse_request(&mut self, value: &Value) -> Result<()> {
        let id = value
            .get("id")
            .cloned()
            .context("independent reverse request omitted id")?;
        self.write_value(&json!({
            "id": id,
            "error": {"code": -32601, "message": "unsupported smoke reverse request"}
        }))
        .await
    }

    fn push_notification(&mut self, value: Value) -> Result<()> {
        ensure!(
            self.notifications.len() < MAX_NOTIFICATION_BACKLOG,
            "independent notification backlog exceeded bound"
        );
        self.notifications.push_back(value);
        Ok(())
    }

    async fn stop(mut self) -> Result<()> {
        drop(self.stdin.take());
        let mut leader_reaped = match timeout(PROCESS_GRACE, self.child.wait()).await {
            Ok(Ok(_)) => true,
            Ok(Err(_)) => bail!("unable to reap independent app-server leader"),
            Err(_) => false,
        };
        if !leader_reaped {
            signal_process_group(self.pid, SignalKind::Terminate)?;
            leader_reaped = match timeout(PROCESS_GRACE, self.child.wait()).await {
                Ok(Ok(_)) => true,
                Ok(Err(_)) => bail!("unable to reap independent app-server leader"),
                Err(_) => false,
            };
        }
        if !leader_reaped {
            signal_process_group(self.pid, SignalKind::Kill)?;
            timeout(PROCESS_GRACE, self.child.wait())
                .await
                .context("independent app-server leader reap timed out")??;
        }
        signal_process_group(self.pid, SignalKind::Kill)?;
        wait_for_process_group_absence(self.pid).await?;
        let stderr_exceeded = match self.stderr_task.take() {
            Some(task) => timeout(PROCESS_GRACE, task)
                .await
                .context("independent stderr drain timed out")?
                .context("independent stderr drain task failed")?,
            None => false,
        };
        ensure!(
            !stderr_exceeded,
            "independent app-server stderr exceeded bound"
        );
        self.stopped = true;
        Ok(())
    }
}

impl Drop for IndependentOwner {
    fn drop(&mut self) {
        if self.stopped {
            return;
        }
        #[cfg(unix)]
        if let Ok(pid) = i32::try_from(self.pid) {
            let _ = killpg(Pid::from_raw(pid), Signal::SIGKILL);
        }
        let _ = self.child.start_kill();
    }
}

async fn drain_stderr(mut stderr: ChildStderr) -> bool {
    let mut bytes = 0_usize;
    let mut exceeded = false;
    let mut chunk = [0_u8; 8192];
    loop {
        match stderr.read(&mut chunk).await {
            Ok(0) | Err(_) => return exceeded,
            Ok(read) => {
                bytes = bytes.saturating_add(read);
                exceeded |= bytes > MAX_PROTOCOL_STREAM_BYTES;
            }
        }
    }
}

#[derive(Clone, Copy)]
enum SignalKind {
    Terminate,
    Kill,
}

#[cfg(unix)]
fn signal_process_group(pid: u32, signal: SignalKind) -> Result<()> {
    let pid = i32::try_from(pid).context("independent process id exceeded platform range")?;
    let signal = match signal {
        SignalKind::Terminate => Signal::SIGTERM,
        SignalKind::Kill => Signal::SIGKILL,
    };
    match killpg(Pid::from_raw(pid), signal) {
        Ok(()) | Err(Errno::ESRCH) => Ok(()),
        Err(_) => bail!("unable to signal independent process group"),
    }
}

#[cfg(not(unix))]
fn signal_process_group(_pid: u32, _signal: SignalKind) -> Result<()> {
    bail!("independent process-group proof requires Unix")
}

#[cfg(unix)]
async fn wait_for_process_group_absence(pid: u32) -> Result<()> {
    let pid = i32::try_from(pid).context("independent process id exceeded platform range")?;
    let group = Pid::from_raw(pid);
    let deadline = Instant::now() + PROCESS_TREE_TIMEOUT;
    loop {
        match killpg(group, None) {
            Err(Errno::ESRCH) => return Ok(()),
            Ok(()) | Err(Errno::EPERM) => {}
            Err(_) => bail!("independent process-group probe failed"),
        }
        ensure!(
            Instant::now() < deadline,
            "independent process group was not reaped"
        );
        sleep(Duration::from_millis(20)).await;
    }
}

#[cfg(not(unix))]
async fn wait_for_process_group_absence(_pid: u32) -> Result<()> {
    bail!("independent process-group proof requires Unix")
}

fn is_turn_completed(value: &Value, thread_id: &str, turn_id: &str) -> bool {
    value["method"] == "turn/completed"
        && value["params"]["threadId"] == thread_id
        && value["params"]["turn"]["id"] == turn_id
}

fn configure_isolated_environment(command: &mut Command, codex_home: &Path, isolated_home: &Path) {
    command
        .env_clear()
        .env("CODEX_HOME", codex_home)
        .env("HOME", isolated_home)
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost")
        .env("RUST_BACKTRACE", "0");
    for name in [
        "PATH",
        "SHELL",
        "LANG",
        "LC_ALL",
        "TMPDIR",
        "TEMP",
        "TMP",
        "SystemRoot",
        "WINDIR",
        "ComSpec",
        "PATHEXT",
    ] {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
}

async fn run_independent_owner_a(
    binary: &Path,
    codex_home: &Path,
    isolated_home: &Path,
    workspace: &Path,
) -> Result<String> {
    let mut owner = IndependentOwner::spawn(binary, codex_home, isolated_home)?;
    let phase = async {
        owner.initialize(1).await?;
        let started = owner
            .request(
                "thread/start",
                json!({
                    "cwd": workspace,
                    "model": MODEL,
                    "modelProvider": MODEL_PROVIDER,
                    "approvalPolicy": "never",
                    "sandbox": "read-only",
                    "ephemeral": false
                }),
            )
            .await?;
        let thread_id = started["thread"]["id"]
            .as_str()
            .context("independent thread/start omitted thread id")?
            .to_owned();
        ensure!(!thread_id.is_empty(), "independent thread id was empty");
        owner.start_turn(&thread_id, OWNER_A_INPUT).await?;
        let history = owner
            .request(
                "thread/read",
                json!({"threadId": thread_id, "includeTurns": true}),
            )
            .await?;
        ensure!(
            contains_marker(&history, OWNER_A_INPUT)
                && contains_marker(&history, OWNER_A_OUTPUT)
                && completed_turns(&history) >= 1,
            "owner A history proof failed"
        );
        Ok(thread_id)
    }
    .await;
    let cleanup = owner.stop().await;
    let thread_id = phase?;
    cleanup?;
    Ok(thread_id)
}

async fn run_independent_owner_c(
    binary: &Path,
    codex_home: &Path,
    isolated_home: &Path,
    workspace: &Path,
    thread_id: &str,
) -> Result<()> {
    let mut owner = IndependentOwner::spawn(binary, codex_home, isolated_home)?;
    let phase = async {
        owner.initialize(3).await?;
        let resumed = owner
            .request(
                "thread/resume",
                json!({
                    "threadId": thread_id,
                    "cwd": workspace,
                    "model": MODEL,
                    "approvalPolicy": "never",
                    "sandbox": "read-only"
                }),
            )
            .await?;
        ensure!(
            resumed["thread"]["id"] == thread_id,
            "owner C resumed a replacement thread"
        );
        let history = owner
            .request(
                "thread/read",
                json!({"threadId": thread_id, "includeTurns": true}),
            )
            .await?;
        ensure!(
            [OWNER_A_INPUT, OWNER_A_OUTPUT, OWNER_B_INPUT, OWNER_B_OUTPUT,]
                .iter()
                .all(|marker| contains_marker(&history, marker))
                && completed_turns(&history) >= 2,
            "owner C history proof failed"
        );
        Ok(())
    }
    .await;
    let cleanup = owner.stop().await;
    phase.and(cleanup)
}

fn contains_marker(value: &Value, marker: &str) -> bool {
    match value {
        Value::String(value) => value.contains(marker),
        Value::Array(values) => values.iter().any(|value| contains_marker(value, marker)),
        Value::Object(values) => values.values().any(|value| contains_marker(value, marker)),
        Value::Null | Value::Bool(_) | Value::Number(_) => false,
    }
}

fn completed_turns(history: &Value) -> usize {
    history["thread"]["turns"].as_array().map_or(0, |turns| {
        turns
            .iter()
            .filter(|turn| turn["status"] == "completed")
            .count()
    })
}

#[tokio::test]
#[ignore = "requires explicit exact Codex binary and production-backend gate"]
#[allow(clippy::too_many_lines)]
async fn real_exact_binary_routes_sequential_adoption_without_replacement() -> Result<()> {
    ensure!(
        required_env("CODEX_THREAD_ADOPTION_ROUTER_E2E")? == "1",
        "CODEX_THREAD_ADOPTION_ROUTER_E2E must equal 1"
    );
    ensure!(
        cfg!(unix),
        "production handoff process-tree proof requires Unix"
    );
    let binary = PathBuf::from(required_env("CODEX_THREAD_ADOPTION_ROUTER_BINARY")?);
    ensure!(
        binary.is_absolute(),
        "CODEX_THREAD_ADOPTION_ROUTER_BINARY must be absolute"
    );
    ensure_native_binary(&binary)?;
    let expected_text = required_env("CODEX_THREAD_ADOPTION_ROUTER_EXPECTED_VERSION")?;
    let expected = Version::parse(&expected_text).context("expected version must be semver")?;
    ensure!(
        expected.pre.is_empty()
            && expected.build.is_empty()
            && expected.to_string() == expected_text,
        "expected version must be canonical exact semver"
    );
    let backend = ProductionBackend::required(
        &expected,
        &required_env("CODEX_THREAD_ADOPTION_ROUTER_BACKEND")?,
    )?;

    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let scratch = tempfile::Builder::new()
        .prefix(".thread-adoption-router-smoke-")
        .tempdir_in(manifest)
        .context("unable to create private smoke scratch")?;
    let codex_home = scratch.path().join("codex-home");
    let isolated_home = scratch.path().join("os-home");
    let workspace = scratch.path().join("workspace");
    for directory in [&codex_home, &isolated_home, &workspace] {
        std::fs::create_dir(directory).context("unable to create smoke directory")?;
    }
    let probed = raw_probe_version(&binary, &codex_home, &isolated_home).await?;
    ensure!(probed == expected, "exact Codex version gate mismatch");

    let provider = ScriptedResponsesServer::start().await?;
    write_model_provider_config(&codex_home, &provider.base_url)?;
    let thread_id =
        run_independent_owner_a(&binary, &codex_home, &isolated_home, &workspace).await?;
    provider.wait_for_requests(1).await?;

    let (backend_config, supervisor) =
        start_production_supervisor(backend, &binary, &codex_home, &expected).await?;
    let credentials = credentials();
    let tenant = TenantNamespace::from_credentials(&credentials);
    let mut config = BridgeConfig {
        owners: vec![OWNER_ID.to_owned()],
        default_workspace: Some(workspace.clone()),
        workspace: WorkspacePolicy {
            allow_roots: vec![workspace.clone()],
            network_access: false,
        },
        codex: CodexSection {
            backend: backend_config,
            model: Some(MODEL.to_owned()),
            effort: None,
            sandbox: SandboxMode::ReadOnly,
            approval_policy: ApprovalPolicy::Named("never".to_owned()),
        },
        ..BridgeConfig::default()
    };
    config
        .validate()
        .context("smoke bridge config was rejected")?;
    let policy = AccessPolicy::from_config(&config).context("unable to build smoke policy")?;
    let settings = RouterSettings::from_config(&config).with_test_timings(
        Duration::from_millis(10),
        Duration::from_secs(60),
        Duration::from_millis(10),
    );
    let store = StoreHandle::open(&scratch.path().join("bridge.sqlite"))
        .await
        .context("unable to open smoke store")?;
    let router = Router::start(
        store.clone(),
        tenant.clone(),
        policy,
        settings,
        supervisor,
        Arc::new(OutboxReplySink::new(store.clone())),
    )
    .await
    .context("unable to start production Router")?;

    if let Err(error) = run_router_owner_b(&router, &store, &tenant, &thread_id).await {
        let _ = router.shutdown().await;
        return Err(error);
    }
    // Owner C reacquires while the production Router and its shared supervisor are still alive.
    // Only the explicitly released dedicated domain may have stopped; Router teardown cannot be
    // the mechanism that made the persisted thread available again.
    let owner_c_phase =
        run_independent_owner_c(&binary, &codex_home, &isolated_home, &workspace, &thread_id).await;
    let router_cleanup = router.shutdown().await.context("Router shutdown failed");
    owner_c_phase?;
    router_cleanup?;
    provider.assert_complete()?;
    provider.shutdown().await?;
    store
        .shutdown()
        .await
        .context("smoke store shutdown failed")?;
    println!("thread-adoption-router-smoke: pass");
    Ok(())
}

async fn start_production_supervisor(
    backend: ProductionBackend,
    binary: &Path,
    codex_home: &Path,
    expected: &Version,
) -> Result<(CodexBackendConfig, SupervisorHandle)> {
    let backend_config = match backend {
        ProductionBackend::SpawnedStdio => CodexBackendConfig::SpawnedStdio {
            binary: binary.to_path_buf(),
            codex_home: Some(codex_home.to_path_buf()),
        },
        ProductionBackend::ProtocolSidecar => {
            let node_binary =
                PathBuf::from(required_env("CODEX_THREAD_ADOPTION_ROUTER_NODE_BINARY")?);
            let sidecar_entrypoint = PathBuf::from(required_env(
                "CODEX_THREAD_ADOPTION_ROUTER_SIDECAR_ENTRYPOINT",
            )?);
            ensure!(
                node_binary.is_absolute() && sidecar_entrypoint.is_absolute(),
                "sidecar paths must be absolute"
            );
            ensure!(
                node_binary.is_file() && sidecar_entrypoint.is_file(),
                "sidecar path gate failed"
            );
            CodexBackendConfig::ProtocolSidecar {
                node_binary,
                sidecar_entrypoint,
                codex_binary: Some(binary.to_path_buf()),
                codex_home: Some(codex_home.to_path_buf()),
                codex_arguments: Vec::new(),
            }
        }
    };
    let mut supervisor = match backend {
        ProductionBackend::SpawnedStdio => {
            AppServerSupervisor::start(
                backend_config
                    .spawned_process_config()
                    .context("spawned backend omitted process config")?,
            )
            .await
        }
        ProductionBackend::ProtocolSidecar => {
            AppServerSupervisor::start_sidecar(
                backend_config
                    .protocol_sidecar_config()
                    .context("sidecar backend omitted sidecar config")?,
            )
            .await
        }
    }
    .context("production Supervisor failed to start")?;
    wait_for_supervisor_ready(&mut supervisor, expected).await?;
    let client = supervisor
        .client()
        .context("ready Supervisor omitted client")?;
    ensure!(
        client.thread_adoption_contract().is_some(),
        "production backend did not negotiate adoption contract"
    );
    Ok((backend_config, supervisor))
}

async fn wait_for_supervisor_ready(
    supervisor: &mut SupervisorHandle,
    expected: &Version,
) -> Result<()> {
    timeout(ROUTER_TIMEOUT, async {
        loop {
            match supervisor.state() {
                SupervisorState::Ready { version, .. } => {
                    ensure!(version == *expected, "Supervisor version gate mismatch");
                    return Ok(());
                }
                SupervisorState::Degraded { .. } | SupervisorState::Stopped => {
                    bail!("production Supervisor did not become ready")
                }
                SupervisorState::Starting { .. } | SupervisorState::Backoff { .. } => {
                    supervisor
                        .changed()
                        .await
                        .context("production Supervisor stopped during startup")?;
                }
            }
        }
    })
    .await
    .context("production Supervisor startup timed out")?
}

async fn run_router_owner_b(
    router: &lark_codex_bridge::runtime::router::RouterHandle,
    store: &StoreHandle,
    tenant: &TenantNamespace,
    thread_id: &str,
) -> Result<()> {
    let scope = ScopeKey::Chat(CHAT_ID.to_owned());
    route_text(router, store, tenant, "evt-router-threads", "/threads").await?;
    let discovery = wait_control_reply(store, tenant, "evt-router-threads").await?;
    let adopt_lines: Vec<&str> = discovery
        .lines()
        .filter(|line| line.starts_with("/adopt "))
        .collect();
    ensure!(
        adopt_lines.len() == 1,
        "discovery did not return one exact candidate command"
    );
    let adopt_command = adopt_lines[0];
    match parse_command(adopt_command).context("rendered adopt command did not parse")? {
        Some(BridgeCommand::Adopt { selector }) => ensure!(
            selector == thread_id,
            "rendered adopt command selected a replacement thread"
        ),
        _ => bail!("rendered candidate was not an adopt command"),
    }

    route_text(router, store, tenant, "evt-router-adopt", adopt_command).await?;
    let adopted_reply = wait_control_reply(store, tenant, "evt-router-adopt").await?;
    ensure!(
        adopted_reply.starts_with("Adoption complete."),
        "production Router adoption did not complete"
    );
    let mapping = store
        .active_thread(&scope)
        .await
        .context("unable to read adopted mapping")?
        .context("adoption did not create a mapping")?;
    ensure!(
        mapping.codex_thread_id == thread_id && mapping.origin == ThreadOrigin::ExternallyAdopted,
        "production Router created a replacement mapping"
    );
    let generation = mapping
        .adoption_generation
        .context("adopted mapping omitted generation")?;
    let owned = store
        .active_thread_adoption(&scope)
        .await
        .context("unable to read active adoption saga")?
        .context("adoption saga was not active")?;
    ensure!(
        owned.codex_thread_id == thread_id
            && owned.generation == generation
            && owned.state == ThreadAdoptionState::Owned,
        "adoption saga did not own the exact mapping"
    );

    route_text(router, store, tenant, "evt-router-marker", OWNER_B_INPUT).await?;
    wait_for_inbound_state(
        store,
        tenant,
        "evt-router-marker",
        InboundEventState::Completed,
    )
    .await?;
    wait_for_outbox_marker(store, OWNER_B_OUTPUT).await?;
    let after_turn = store
        .active_thread(&scope)
        .await
        .context("unable to reread adopted mapping")?
        .context("normal turn removed adopted mapping")?;
    ensure!(
        after_turn.codex_thread_id == thread_id
            && after_turn.origin == ThreadOrigin::ExternallyAdopted
            && after_turn.adoption_generation == Some(generation),
        "normal Router turn fell back to a replacement thread"
    );

    release_router_adoption(router, store, tenant, &scope, thread_id, generation).await
}

async fn release_router_adoption(
    router: &lark_codex_bridge::runtime::router::RouterHandle,
    store: &StoreHandle,
    tenant: &TenantNamespace,
    scope: &ScopeKey,
    thread_id: &str,
    generation: u64,
) -> Result<()> {
    // The mapping is still active immediately before the explicit release. The production
    // coordinator emits the durable success reply only after its dedicated owner shutdown has
    // confirmed process-tree reap and the same store transaction retires this mapping.
    ensure!(
        store
            .active_thread(scope)
            .await
            .context("unable to prove pre-release mapping")?
            .is_some(),
        "mapping disappeared before explicit release"
    );
    route_text(router, store, tenant, "evt-router-release", "/release").await?;
    let release_reply = wait_control_reply(store, tenant, "evt-router-release").await?;
    ensure!(
        release_reply.starts_with("Release complete."),
        "production Router release did not confirm cleanup"
    );
    ensure!(
        store
            .active_thread(scope)
            .await
            .context("unable to read post-release mapping")?
            .is_none(),
        "mapping remained after confirmed release reply"
    );
    let released = store
        .thread_adoption_saga(scope)
        .await
        .context("unable to read terminal adoption saga")?
        .context("terminal adoption saga was missing")?;
    ensure!(
        released.codex_thread_id == thread_id
            && released.generation == generation
            && released.state == ThreadAdoptionState::Terminal
            && released.outcome == Some(ThreadAdoptionOutcome::Released),
        "release did not durably terminate the exact adoption generation"
    );
    Ok(())
}

async fn route_text(
    router: &lark_codex_bridge::runtime::router::RouterHandle,
    store: &StoreHandle,
    tenant: &TenantNamespace,
    event_id: &str,
    text: &str,
) -> Result<()> {
    let event = inbound_event(event_id, text);
    let retained = match store
        .register_inbound(tenant, &event)
        .await
        .context("unable to register Router inbound")?
    {
        DedupOutcome::New(retained) | DedupOutcome::ReplayReceived(retained) => retained,
        DedupOutcome::Duplicate { .. } => bail!("Router inbound was unexpectedly duplicate"),
    };
    let bytes = retained.retained_bytes();
    let permits = u32::try_from(bytes).context("retained inbound exceeded permit range")?;
    let permit = Arc::new(Semaphore::new(bytes))
        .acquire_many_owned(permits)
        .await
        .context("unable to acquire Router inbound permit")?;
    router
        .route(QueuedInboundEvent::new(*retained.into_event(), permit))
        .await
        .context("production Router rejected inbound")
}

async fn wait_control_reply(
    store: &StoreHandle,
    tenant: &TenantNamespace,
    event_id: &str,
) -> Result<String> {
    let key = InboundKey::new(tenant.clone(), event_id.to_owned()).control_outbox_idempotency_key();
    timeout(ROUTER_TIMEOUT, async {
        loop {
            if let Some(row) = store
                .outbox_row_by_key(&key)
                .await
                .context("unable to read durable control reply")?
            {
                return match OutboxOperation::decode(&row.payload_json)
                    .context("durable control reply did not decode")?
                {
                    OutboxOperation::ReplyText { text, .. } => Ok(text),
                    _ => bail!("durable control reply used the wrong operation"),
                };
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("durable control reply timed out")?
}

async fn wait_for_inbound_state(
    store: &StoreHandle,
    tenant: &TenantNamespace,
    event_id: &str,
    expected: InboundEventState,
) -> Result<()> {
    timeout(ROUTER_TIMEOUT, async {
        loop {
            if store
                .inbound_state(tenant, event_id)
                .await
                .context("unable to read Router inbound state")?
                == Some(expected)
            {
                return Ok(());
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("Router inbound did not reach expected state")?
}

async fn wait_for_outbox_marker(store: &StoreHandle, marker: &str) -> Result<()> {
    timeout(ROUTER_TIMEOUT, async {
        loop {
            for row in store
                .claim_outbox_batch(i64::MAX, 64)
                .await
                .context("unable to claim smoke outbox rows")?
            {
                let operation = OutboxOperation::decode(&row.payload_json)
                    .context("smoke outbox row did not decode")?;
                if outbox_operation_contains(&operation, marker) {
                    return Ok(());
                }
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("normal Router reply marker did not reach durable outbox")?
}

fn outbox_operation_contains(operation: &OutboxOperation, marker: &str) -> bool {
    match operation {
        OutboxOperation::ReplyText { text, .. }
        | OutboxOperation::ReplyProgressCard { text, .. }
        | OutboxOperation::UpdateProgressCard { text, .. } => text.contains(marker),
        OutboxOperation::ReplyMarkdownPost { markdown, .. } => markdown.contains(marker),
        OutboxOperation::FinalizeProgressCard {
            text,
            fallback_markdown,
            ..
        } => text.contains(marker) || fallback_markdown.contains(marker),
    }
}

fn inbound_event(event_id: &str, text: &str) -> InboundEvent {
    InboundEvent {
        event_id: event_id.to_owned(),
        message_id: format!("om-{event_id}"),
        chat_id: CHAT_ID.to_owned(),
        sender_id: OWNER_ID.to_owned(),
        chat_type: ChatMode::P2p,
        thread_id: None,
        root_id: None,
        reply_to_message_id: None,
        text: text.to_owned(),
        mentions_bot: false,
        mention_all: false,
        sender_is_human: true,
        mentions: Vec::new(),
        parts: Vec::new(),
        resources: Vec::new(),
        message_type: "text".to_owned(),
        create_time_ms: now_ms(),
        scope: ScopeKey::Chat(CHAT_ID.to_owned()),
    }
}

fn credentials() -> LarkCredentials {
    LarkCredentials::new(
        "cli_thread_adoption_router_smoke".to_owned(),
        SecretString::from("thread-adoption-router-secret".to_owned()),
        TenantBrand::Feishu,
    )
}

fn now_ms() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(i64::MAX)
}

fn required_env(name: &str) -> Result<String> {
    let value = std::env::var(name).with_context(|| format!("required gate {name} is missing"))?;
    ensure!(!value.is_empty(), "required gate {name} is empty");
    Ok(value)
}

fn ensure_native_binary(path: &Path) -> Result<()> {
    ensure!(path.is_file(), "exact Codex binary is not a file");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;

        ensure!(
            path.metadata()
                .context("unable to inspect exact Codex binary")?
                .permissions()
                .mode()
                & 0o111
                != 0,
            "exact Codex binary is not executable"
        );
        let mut magic = [0_u8; 2];
        File::open(path)
            .context("unable to open exact Codex binary")?
            .read_exact(&mut magic)
            .context("unable to inspect exact Codex binary")?;
        ensure!(
            magic != *b"#!",
            "exact Codex binary gate rejected a script wrapper"
        );
    }
    Ok(())
}

async fn raw_probe_version(
    binary: &Path,
    codex_home: &Path,
    isolated_home: &Path,
) -> Result<Version> {
    let mut command = Command::new(binary);
    command
        .arg("--version")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    configure_isolated_environment(&mut command, codex_home, isolated_home);
    let mut child = command
        .spawn()
        .context("unable to start exact Codex version probe")?;
    let stdout = child
        .stdout
        .take()
        .context("version probe omitted stdout")?;
    let stderr = child
        .stderr
        .take()
        .context("version probe omitted stderr")?;
    let stdout_task = tokio::spawn(read_bounded_stream(stdout));
    let stderr_task = tokio::spawn(read_bounded_stream(stderr));
    let Ok(status) = timeout(PROCESS_GRACE, child.wait()).await else {
        let _ = child.start_kill();
        let _ = child.wait().await;
        bail!("exact Codex version probe timed out")
    };
    let status = status.context("unable to reap exact Codex version probe")?;
    ensure!(status.success(), "exact Codex version probe failed");
    let (stdout, stdout_exceeded) = stdout_task
        .await
        .context("version stdout drain task failed")??;
    let (_, stderr_exceeded) = stderr_task
        .await
        .context("version stderr drain task failed")??;
    ensure!(
        !stdout_exceeded && !stderr_exceeded,
        "exact Codex version output exceeded bound"
    );
    let output = std::str::from_utf8(&stdout)
        .context("exact Codex version output was not UTF-8")?
        .strip_suffix("\r\n")
        .or_else(|| {
            std::str::from_utf8(&stdout)
                .ok()
                .and_then(|value| value.strip_suffix('\n'))
        })
        .unwrap_or_else(|| std::str::from_utf8(&stdout).unwrap_or_default());
    ensure!(
        !output.contains(['\r', '\n']),
        "exact Codex version output had extra lines"
    );
    let version = output
        .strip_prefix("codex-cli ")
        .context("exact Codex version output had wrong shape")?;
    let parsed = Version::parse(version).context("exact Codex version was not semver")?;
    ensure!(
        parsed.pre.is_empty() && parsed.build.is_empty() && parsed.to_string() == version,
        "exact Codex version was not canonical"
    );
    Ok(parsed)
}

async fn read_bounded_stream<R>(mut reader: R) -> Result<(Vec<u8>, bool)>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut retained = Vec::new();
    let mut total = 0_usize;
    let mut exceeded = false;
    let mut chunk = [0_u8; 1024];
    loop {
        let read = reader
            .read(&mut chunk)
            .await
            .context("unable to drain version output")?;
        if read == 0 {
            return Ok((retained, exceeded));
        }
        total = total.saturating_add(read);
        exceeded |= total > MAX_VERSION_OUTPUT_BYTES;
        if retained.len() < MAX_VERSION_OUTPUT_BYTES {
            let remaining = MAX_VERSION_OUTPUT_BYTES - retained.len();
            retained.extend_from_slice(&chunk[..read.min(remaining)]);
        }
    }
}

fn write_model_provider_config(codex_home: &Path, base_url: &str) -> Result<()> {
    let config = format!(
        r#"model = "{MODEL}"
model_provider = "{MODEL_PROVIDER}"

[analytics]
enabled = false

[model_providers.{MODEL_PROVIDER}]
name = "Thread adoption Router smoke"
base_url = "{base_url}"
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0
requires_openai_auth = false
"#
    );
    std::fs::write(codex_home.join("config.toml"), config)
        .context("unable to write isolated Codex provider config")
}
