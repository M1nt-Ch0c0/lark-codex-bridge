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
        AppServerProcess, AppServerSupervisor, ProcessFactory, ProcessStdio, SupervisorError,
        SupervisorHandle, SupervisorSettings, SupervisorState,
    },
    types::ThreadStartParams,
};
use semver::Version;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, DuplexStream, duplex},
    sync::oneshot,
    time::timeout,
};

const TEST_TIMEOUT: Duration = Duration::from_secs(1);

type SpawnFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Box<dyn AppServerProcess>, ProcessError>> + Send + 'a>>;

#[derive(Clone)]
struct FakeFactory {
    outcomes: Arc<Mutex<VecDeque<FakeOutcome>>>,
    spawns: Arc<Mutex<usize>>,
}

enum FakeOutcome {
    Ready(FakeControl),
    Error(ProcessError),
}

#[derive(Clone)]
struct FakeControl {
    exit: Arc<Mutex<Option<oneshot::Sender<ProcessExit>>>>,
    hold: Arc<Mutex<Option<oneshot::Sender<()>>>>,
    terminate_calls: Arc<Mutex<Vec<Duration>>>,
}

impl FakeControl {
    /// Closes the fake app-server pipes and then reports the process exit,
    /// mirroring how a real dying child closes stdio before `wait` returns.
    fn signal_exit(&self, exit: ProcessExit) {
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

    fn unexpected_exit(&self) {
        self.signal_exit(ProcessExit {
            pid: 42,
            success: false,
            code: Some(1),
            signal: None,
        });
    }

    fn terminate_calls(&self) -> Vec<Duration> {
        self.terminate_calls.lock().expect("terminate lock").clone()
    }
}

impl FakeFactory {
    fn new(outcomes: impl IntoIterator<Item = FakeOutcome>) -> Self {
        Self {
            outcomes: Arc::new(Mutex::new(outcomes.into_iter().collect())),
            spawns: Arc::new(Mutex::new(0)),
        }
    }

    fn ready() -> (FakeOutcome, FakeControl) {
        let control = FakeControl {
            exit: Arc::new(Mutex::new(None)),
            hold: Arc::new(Mutex::new(None)),
            terminate_calls: Arc::new(Mutex::new(Vec::new())),
        };
        (FakeOutcome::Ready(control.clone()), control)
    }

    fn spawn_count(&self) -> usize {
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
        tokio::spawn(async move {
            initialize_fake(&mut app_stdout, app_stdin).await;
            // Keep the pipes open until the fake process exits, like a live child.
            let _ = hold_rx.await;
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

async fn initialize_fake(stdout: &mut DuplexStream, stdin: DuplexStream) {
    let mut stdin = BufReader::new(stdin);
    let request = read_line(&mut stdin).await;
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
    let initialized = read_line(&mut stdin).await;
    assert_eq!(initialized["method"], "initialized");
}

// The handshake rejects a non-absolute `codexHome`; Windows requires a
// drive-prefixed path, so mirror the helper used by the other test suites.
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

fn test_settings() -> SupervisorSettings {
    SupervisorSettings::new(Duration::ZERO, |_, _| Duration::ZERO)
}

async fn next_state(handle: &mut SupervisorHandle) -> SupervisorState {
    timeout(TEST_TIMEOUT, handle.changed())
        .await
        .expect("state transition timeout")
        .expect("state watch should stay available")
}

#[tokio::test]
async fn restart_increments_epoch_and_invalidates_the_previous_client() {
    let (first, first_control) = FakeFactory::ready();
    let (second, _second_control) = FakeFactory::ready();
    let factory = Arc::new(FakeFactory::new([first, second]));
    let mut handle = AppServerSupervisor::start_with_factory(
        CodexProcessConfig::default(),
        factory.clone(),
        test_settings(),
    )
    .await
    .expect("supervisor starts");

    assert!(matches!(
        next_state(&mut handle).await,
        SupervisorState::Ready { epoch: 1, .. }
    ));
    let stale = handle.client().expect("ready client");
    assert_eq!(stale.epoch().get(), 1);
    first_control.unexpected_exit();

    assert!(matches!(
        next_state(&mut handle).await,
        SupervisorState::Backoff { epoch: 2, attempt: 1, delay } if delay.is_zero()
    ));
    assert!(matches!(
        next_state(&mut handle).await,
        SupervisorState::Starting { epoch: 2 }
    ));
    assert!(matches!(
        next_state(&mut handle).await,
        SupervisorState::Ready { epoch: 2, .. }
    ));
    assert!(
        stale
            .start_thread(ThreadStartParams::default())
            .await
            .is_err(),
        "old client must fail after its epoch exits"
    );
    assert_eq!(
        handle.client().expect("replacement client").epoch().get(),
        2
    );
    assert_eq!(factory.spawn_count(), 2);
    handle.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn permanent_version_failure_degrades_without_retrying() {
    let factory = Arc::new(FakeFactory::new([FakeOutcome::Error(
        ProcessError::UnsupportedVersion {
            found: Version::new(0, 145, 0),
        },
    )]));
    let mut handle = AppServerSupervisor::start_with_factory(
        CodexProcessConfig::default(),
        factory.clone(),
        test_settings(),
    )
    .await
    .expect("supervisor task starts");

    assert!(matches!(
        next_state(&mut handle).await,
        SupervisorState::Degraded { .. }
    ));
    assert_eq!(factory.spawn_count(), 1);
    assert!(matches!(handle.client(), Err(SupervisorError::NotReady)));
    handle.shutdown().await.expect("shutdown");
}

#[test]
fn retry_schedule_is_capped_and_jittered_deterministically() {
    let delays = (1..=8)
        .map(|attempt| AppServerSupervisor::retry_delay(7, attempt))
        .collect::<Vec<_>>();
    assert_eq!(delays.len(), 8);
    assert!(delays[0] >= Duration::from_millis(375));
    assert!(delays[0] <= Duration::from_millis(625));
    assert!(delays[5] <= Duration::from_secs(20));
    assert!(delays[6] <= Duration::from_secs(30));
    assert!(delays[7] <= Duration::from_secs(30));
    assert_ne!(delays[0], AppServerSupervisor::retry_delay(8, 1));
}

#[tokio::test]
async fn shutdown_uses_the_configured_grace_period_before_process_termination() {
    let (outcome, control) = FakeFactory::ready();
    let factory = Arc::new(FakeFactory::new([outcome]));
    let mut handle = AppServerSupervisor::start_with_factory(
        CodexProcessConfig::default(),
        factory,
        SupervisorSettings::new(Duration::from_millis(7), |_, _| Duration::ZERO),
    )
    .await
    .expect("supervisor starts");
    assert!(matches!(
        next_state(&mut handle).await,
        SupervisorState::Ready { .. }
    ));
    handle.shutdown().await.expect("shutdown");
    assert_eq!(control.terminate_calls(), vec![Duration::from_millis(7)]);
}
