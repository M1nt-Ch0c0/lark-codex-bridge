#![allow(dead_code)]

use std::{
    collections::VecDeque,
    future::Future,
    io,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};

use lark_codex_bridge::codex::{
    process::{CodexProcessConfig, ProcessError, ProcessExit},
    supervisor::{
        AppServerProcess, ProcessFactory, ProcessStdio, SupervisorHandle, SupervisorSettings,
        SupervisorState,
    },
};
use semver::Version;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, duplex},
    sync::{Mutex as AsyncMutex, mpsc, oneshot},
    time::timeout,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(3);
const SCRIPT_CHANNEL_CAPACITY: usize = 64;

type SpawnFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Box<dyn AppServerProcess>, ProcessError>> + Send + 'a>>;

#[derive(Clone)]
pub(crate) struct FakeFactory {
    outcomes: Arc<Mutex<VecDeque<FakeOutcome>>>,
    spawns: Arc<Mutex<usize>>,
}

pub(crate) enum FakeOutcome {
    Ready(FakeControl),
    Error(ProcessError),
}

#[derive(Clone)]
pub(crate) struct FakeControl {
    exit: Arc<Mutex<Option<oneshot::Sender<ProcessExit>>>>,
    hold: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    terminate_calls: Arc<Mutex<Vec<Duration>>>,
    requests_tx: mpsc::Sender<Value>,
    requests_rx: Arc<AsyncMutex<mpsc::Receiver<Value>>>,
    outputs_tx: mpsc::Sender<Value>,
    outputs_rx: Arc<Mutex<Option<mpsc::Receiver<Value>>>>,
}

impl FakeControl {
    /// Closes the fake app-server pipes and then reports the process exit,
    /// mirroring how a real dying child closes stdio before `wait` returns.
    pub(crate) fn signal_exit(&self, exit: ProcessExit) {
        if let Some(hold) = self.hold.lock().expect("hold lock").take() {
            let _ = hold.send(());
        }
        let sender = self
            .exit
            .lock()
            .expect("exit lock")
            .take()
            .expect("process should still be running");
        sender
            .send(exit)
            .expect("supervisor should still wait for process");
    }

    pub(crate) fn unexpected_exit(&self) {
        self.signal_exit(ProcessExit {
            pid: 42,
            success: false,
            code: Some(1),
            signal: None,
        });
    }

    pub(crate) fn terminate_calls(&self) -> Vec<Duration> {
        self.terminate_calls.lock().expect("terminate lock").clone()
    }

    pub(crate) async fn next_request(&self) -> Value {
        self.next_request_within(TEST_TIMEOUT).await
    }

    pub(crate) async fn next_request_within(&self, duration: Duration) -> Value {
        timeout(duration, self.requests_rx.lock().await.recv())
            .await
            .expect("fake request timeout")
            .expect("fake request channel remains open")
    }

    pub(crate) async fn expect_no_request_for(&self, duration: Duration) {
        let result = timeout(duration, self.requests_rx.lock().await.recv()).await;
        assert!(result.is_err(), "fake observed an unexpected request");
    }

    pub(crate) async fn send_json(&self, value: Value) {
        self.outputs_tx
            .send(value)
            .await
            .expect("fake output channel remains open");
    }

    pub(crate) async fn respond(&self, request: &Value, result: Value) {
        self.send_json(json!({
            "id": request.get("id").expect("request contains an id"),
            "result": result
        }))
        .await;
    }
}

impl FakeFactory {
    pub(crate) fn new(outcomes: impl IntoIterator<Item = FakeOutcome>) -> Self {
        Self {
            outcomes: Arc::new(Mutex::new(outcomes.into_iter().collect())),
            spawns: Arc::new(Mutex::new(0)),
        }
    }

    pub(crate) fn ready() -> (FakeOutcome, FakeControl) {
        let (requests_tx, requests_rx) = mpsc::channel(SCRIPT_CHANNEL_CAPACITY);
        let (outputs_tx, outputs_rx) = mpsc::channel(SCRIPT_CHANNEL_CAPACITY);
        let control = FakeControl {
            exit: Arc::new(Mutex::new(None)),
            hold: Arc::new(Mutex::new(None)),
            terminate_calls: Arc::new(Mutex::new(Vec::new())),
            requests_tx,
            requests_rx: Arc::new(AsyncMutex::new(requests_rx)),
            outputs_tx,
            outputs_rx: Arc::new(Mutex::new(Some(outputs_rx))),
        };
        (FakeOutcome::Ready(control.clone()), control)
    }

    pub(crate) fn spawn_count(&self) -> usize {
        *self.spawns.lock().expect("spawn lock")
    }
}

impl ProcessFactory for FakeFactory {
    fn spawn(&self, _config: &CodexProcessConfig) -> SpawnFuture<'_> {
        *self.spawns.lock().expect("spawn lock") += 1;
        let outcome = self
            .outcomes
            .lock()
            .expect("outcomes lock")
            .pop_front()
            .expect("test supplied enough outcomes");
        Box::pin(async move {
            match outcome {
                FakeOutcome::Ready(control) => {
                    Ok(Box::new(FakeProcess::new(control)) as Box<dyn AppServerProcess>)
                }
                FakeOutcome::Error(error) => Err(error),
            }
        })
    }
}

struct FakeProcess {
    control: FakeControl,
    exit_rx: Option<oneshot::Receiver<ProcessExit>>,
    stdout: Option<DuplexStream>,
    stdin: Option<DuplexStream>,
    stderr: Option<DuplexStream>,
}

impl FakeProcess {
    fn new(control: FakeControl) -> Self {
        let (transport_stdout, mut app_stdout) = duplex(8 * 1024);
        let (transport_stdin, app_stdin) = duplex(8 * 1024);
        let (transport_stderr, _app_stderr) = duplex(8 * 1024);
        let (exit_tx, exit_rx) = oneshot::channel();
        let (hold_tx, hold_rx) = oneshot::channel();
        *control.exit.lock().expect("exit lock") = Some(exit_tx);
        *control.hold.lock().expect("hold lock") = Some(hold_tx);
        let requests = control.requests_tx.clone();
        let outputs = control
            .outputs_rx
            .lock()
            .expect("outputs lock")
            .take()
            .expect("fake process takes outputs once");
        tokio::spawn(async move {
            serve_fake(&mut app_stdout, app_stdin, requests, outputs, hold_rx).await;
        });
        Self {
            control,
            exit_rx: Some(exit_rx),
            stdout: Some(transport_stdout),
            stdin: Some(transport_stdin),
            stderr: Some(transport_stderr),
        }
    }
}

impl AppServerProcess for FakeProcess {
    fn version(&self) -> &Version {
        static VERSION: std::sync::LazyLock<Version> =
            std::sync::LazyLock::new(|| Version::new(0, 146, 0));
        &VERSION
    }

    fn take_stdio(&mut self) -> Result<ProcessStdio, ProcessError> {
        Ok(ProcessStdio {
            stdout: Box::new(self.stdout.take().expect("stdout once")),
            stdin: Box::new(self.stdin.take().expect("stdin once")),
            stderr: Box::new(self.stderr.take().expect("stderr once")),
        })
    }

    fn wait(
        &mut self,
    ) -> Pin<Box<dyn Future<Output = Result<ProcessExit, ProcessError>> + Send + '_>> {
        Box::pin(async move {
            self.exit_rx
                .as_mut()
                .expect("wait once")
                .await
                .map_err(|error| {
                    ProcessError::Wait(io::Error::new(io::ErrorKind::BrokenPipe, error))
                })
        })
    }

    fn terminate(
        &mut self,
        grace: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<ProcessExit, ProcessError>> + Send + '_>> {
        self.control
            .terminate_calls
            .lock()
            .expect("terminate lock")
            .push(grace);
        Box::pin(async move {
            self.control.signal_exit(ProcessExit {
                pid: 42,
                success: false,
                code: None,
                signal: Some(9),
            });
            self.wait().await
        })
    }
}

async fn serve_fake(
    stdout: &mut DuplexStream,
    stdin: DuplexStream,
    requests: mpsc::Sender<Value>,
    mut outputs: mpsc::Receiver<Value>,
    mut hold: oneshot::Receiver<()>,
) {
    let mut stdin = BufReader::new(stdin);
    initialize_fake(stdout, &mut stdin).await;
    loop {
        let mut line = String::new();
        tokio::select! {
            _ = &mut hold => break,
            output = outputs.recv() => {
                let Some(output) = output else { break };
                write_line(stdout, output).await;
            }
            result = stdin.read_line(&mut line) => {
                let Ok(bytes) = result else { break };
                if bytes == 0 {
                    break;
                }
                let request = serde_json::from_str(&line).expect("valid scripted request JSON");
                if requests.send(request).await.is_err() {
                    break;
                }
            }
        }
    }
}

async fn initialize_fake(stdout: &mut DuplexStream, stdin: &mut BufReader<DuplexStream>) {
    let request = read_line(stdin).await;
    assert_eq!(request["method"], "initialize");
    write_line(
        stdout,
        json!({
            "id": request["id"],
            "result": {
                "codexHome": absolute_codex_home(),
                "platformFamily": "unix",
                "platformOs": "linux",
                "userAgent": "codex-cli/0.146.0"
            }
        }),
    )
    .await;
    let initialized = read_line(stdin).await;
    assert_eq!(initialized["method"], "initialized");
}

fn absolute_codex_home() -> &'static str {
    if cfg!(windows) {
        r"C:\scrubbed-codex-home"
    } else {
        "/tmp/scrubbed-codex-home"
    }
}

async fn read_line(reader: &mut BufReader<DuplexStream>) -> Value {
    let mut line = String::new();
    timeout(TEST_TIMEOUT, reader.read_line(&mut line))
        .await
        .expect("fake server read timeout")
        .expect("fake server read failure");
    serde_json::from_str(&line).expect("valid wire JSON")
}

async fn write_line(writer: &mut DuplexStream, value: Value) {
    let mut line = serde_json::to_vec(&value).expect("serialize response");
    line.push(b'\n');
    writer.write_all(&line).await.expect("write response");
}

pub(crate) fn test_settings() -> SupervisorSettings {
    SupervisorSettings::new(Duration::ZERO, |_, _| Duration::ZERO)
}

pub(crate) async fn next_state(handle: &mut SupervisorHandle) -> SupervisorState {
    timeout(TEST_TIMEOUT, handle.changed())
        .await
        .expect("state transition timeout")
        .expect("state watch should stay available")
}
