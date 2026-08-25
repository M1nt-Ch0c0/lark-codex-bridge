//! Explicit exact-binary smoke for shared external writes and one approval handler.
//!
//! Ordinary suites ignore this test. Its exact invocation fails on missing configuration, wrong
//! binary/version, authentication, race, exact-ID mutation, queue, approval-route, or health proof.

use std::{
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::{fs::File, io::Read};

use anyhow::{Context, Result, bail, ensure};
use futures_util::{SinkExt, StreamExt};
use lark_codex_bridge::{
    codex::{
        external::{
            ExternalAuthentication, ExternalCapabilityProfile, ExternalEndpointConfig,
            ExternalEndpointGate,
        },
        external_write::{
            ExternalApprovalDecision, ExternalWriteCoordinator, ExternalWriteError,
            ExternalWriteSettings,
        },
        process::{CodexProcessConfig, probe_version},
        types::{
            ApprovalPolicy, CommandExecutionApprovalDecision,
            CommandExecutionRequestApprovalResult, SimpleApprovalDecision, ThreadQueueAddParams,
            ThreadQueueStartParams, TurnInterruptParams, TurnStartParams, TurnSteerParams,
            UserInput,
        },
    },
    config::{BridgeConfig, CodexSection, ConcurrencyConfig, PathsSection, WorkspacePolicy},
    lark::{
        api::ChatMode,
        normalize::{InboundEvent, ScopeKey},
    },
    runtime::policy::{AccessPolicy, AuthorizedLarkActor},
    store::{ExternalApprovalState, ExternalEndpointState, ExternalUncertaintyReason, StoreHandle},
};
use semver::Version;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::Command,
    sync::Notify,
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, client::IntoClientRequest, http::HeaderValue},
};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(20);
const READY_TIMEOUT: Duration = Duration::from_secs(30);
const CHILD_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const APPROVAL_REVIEWER: &str = "user";

struct ChildGuard {
    child: tokio::process::Child,
}

struct ScriptedResponsesServer {
    base_url: String,
    count: Arc<AtomicUsize>,
    changed: Arc<Notify>,
    cancellation: CancellationToken,
    task: JoinHandle<()>,
}

impl ScriptedResponsesServer {
    async fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("unable to start the local Responses API stub")?;
        let address = listener
            .local_addr()
            .context("unable to inspect the local Responses API stub")?;
        let count = Arc::new(AtomicUsize::new(0));
        let changed = Arc::new(Notify::new());
        let cancellation = CancellationToken::new();
        let task_count = Arc::clone(&count);
        let task_changed = Arc::clone(&changed);
        let task_cancellation = cancellation.clone();
        let task = tokio::spawn(async move {
            loop {
                let accepted = tokio::select! {
                    () = task_cancellation.cancelled() => break,
                    accepted = listener.accept() => accepted,
                };
                let Ok((stream, _)) = accepted else { break };
                let connection_count = Arc::clone(&task_count);
                let connection_changed = Arc::clone(&task_changed);
                let connection_cancellation = task_cancellation.clone();
                tokio::spawn(async move {
                    serve_response_request(
                        stream,
                        connection_count,
                        connection_changed,
                        connection_cancellation,
                    )
                    .await;
                });
            }
        });
        Ok(Self {
            base_url: format!("http://{address}/v1"),
            count,
            changed,
            cancellation,
            task,
        })
    }

    async fn wait_for_count(&self, expected: usize) -> Result<()> {
        timeout(READY_TIMEOUT, async {
            loop {
                let changed = self.changed.notified();
                if self.count.load(Ordering::SeqCst) >= expected {
                    return;
                }
                changed.await;
            }
        })
        .await
        .with_context(|| format!("Responses API request count did not reach {expected}"))?;
        Ok(())
    }

    fn request_count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}

impl Drop for ScriptedResponsesServer {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.task.abort();
    }
}

async fn serve_response_request(
    mut stream: TcpStream,
    count: Arc<AtomicUsize>,
    changed: Arc<Notify>,
    cancellation: CancellationToken,
) {
    let Ok(request) = read_http_request(&mut stream, &cancellation).await else {
        return;
    };
    if !request.starts_with(b"POST /v1/responses ") {
        return;
    }
    let request_number = count.fetch_add(1, Ordering::SeqCst).saturating_add(1);
    changed.notify_waiters();
    match request_number {
        1 | 3 | 4 | 7 => {
            let mut byte = [0_u8; 1];
            tokio::select! {
                () = cancellation.cancelled() => {}
                _ = stream.read(&mut byte) => {}
            }
        }
        2 => {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let _ = send_sse(&mut stream, final_sse()).await;
        }
        5 => {
            let _ = send_sse(&mut stream, approval_sse()).await;
        }
        _ => {
            let _ = send_sse(&mut stream, final_sse()).await;
        }
    }
}

async fn read_http_request(
    stream: &mut TcpStream,
    cancellation: &CancellationToken,
) -> Result<Vec<u8>> {
    let mut request = Vec::new();
    let mut chunk = [0_u8; 2048];
    let header_end = loop {
        let read = tokio::select! {
            () = cancellation.cancelled() => bail!("Responses API stub stopped"),
            read = stream.read(&mut chunk) => read,
        }
        .context("unable to read Responses API request")?;
        ensure!(read > 0, "Responses API client closed before headers");
        ensure!(
            request.len().saturating_add(read) <= 2 * 1024 * 1024,
            "Responses API request exceeded smoke bound"
        );
        request.extend_from_slice(&chunk[..read]);
        if let Some(index) = request.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let headers = std::str::from_utf8(&request[..header_end])
        .context("Responses API headers were not UTF-8")?;
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    let total = header_end.saturating_add(content_length);
    ensure!(
        total <= 2 * 1024 * 1024,
        "Responses API body exceeded smoke bound"
    );
    while request.len() < total {
        let read = tokio::select! {
            () = cancellation.cancelled() => bail!("Responses API stub stopped"),
            read = stream.read(&mut chunk) => read,
        }
        .context("unable to read Responses API body")?;
        ensure!(read > 0, "Responses API client closed before body");
        request.extend_from_slice(&chunk[..read]);
    }
    Ok(request)
}

async fn send_sse(stream: &mut TcpStream, body: String) -> Result<()> {
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(response.as_bytes())
        .await
        .context("unable to send Responses API SSE")?;
    stream
        .shutdown()
        .await
        .context("unable to close SSE response")?;
    Ok(())
}

fn approval_sse() -> String {
    let arguments = json!({
        "command": "echo approval-route",
        "sandbox_permissions": "require_escalated",
        "justification": "Prove the configured external approval route."
    })
    .to_string();
    sse(vec![
        json!({"type": "response.created", "response": {"id": "resp-approval"}}),
        json!({
            "type": "response.output_item.done",
            "item": {
                "type": "function_call",
                "call_id": "call-approval-route",
                "name": "shell_command",
                "arguments": arguments
            }
        }),
        completed_event("resp-approval"),
    ])
}

fn final_sse() -> String {
    sse(vec![
        json!({"type": "response.created", "response": {"id": "resp-final"}}),
        json!({
            "type": "response.output_item.done",
            "item": {
                "type": "message",
                "role": "assistant",
                "id": "msg-final",
                "content": [{"type": "output_text", "text": "approval route complete"}]
            }
        }),
        completed_event("resp-final"),
    ])
}

fn completed_event(id: &str) -> Value {
    json!({
        "type": "response.completed",
        "response": {
            "id": id,
            "usage": {
                "input_tokens": 0,
                "input_tokens_details": null,
                "output_tokens": 0,
                "output_tokens_details": null,
                "total_tokens": 0
            }
        }
    })
}

fn sse(events: Vec<Value>) -> String {
    let mut output = String::new();
    for event in events {
        let kind = event["type"].as_str().unwrap_or("unknown");
        output.push_str("event: ");
        output.push_str(kind);
        output.push('\n');
        output.push_str("data: ");
        output.push_str(&event.to_string());
        output.push_str("\n\n");
    }
    output
}

impl ChildGuard {
    fn ensure_running(&mut self) -> Result<()> {
        ensure!(
            self.child
                .try_wait()
                .context("unable to inspect the smoke-owned app-server")?
                .is_none(),
            "external app-server exited during write smoke"
        );
        Ok(())
    }

    async fn stop(mut self) -> Result<()> {
        self.child
            .start_kill()
            .context("unable to stop the smoke-owned app-server")?;
        timeout(CHILD_SHUTDOWN_TIMEOUT, self.child.wait())
            .await
            .context("timed out reaping the smoke-owned app-server")??;
        Ok(())
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
    }
}

#[derive(Debug)]
enum OperatorResponse {
    Result(Value),
    Rejected,
}

#[tokio::test]
#[ignore = "requires explicit exact Codex binary gate; missing configuration is a failure"]
#[allow(clippy::too_many_lines)]
async fn real_exact_binary_coordinates_two_clients_queue_exact_ids_and_one_approval_route()
-> Result<()> {
    ensure!(
        required_env("CODEX_EXTERNAL_WRITE_E2E")? == "1",
        "CODEX_EXTERNAL_WRITE_E2E must equal 1"
    );
    let binary = PathBuf::from(required_env("CODEX_EXTERNAL_WRITE_BINARY")?);
    ensure!(
        binary.is_absolute(),
        "CODEX_EXTERNAL_WRITE_BINARY must be an absolute path"
    );
    ensure_native_server_binary(&binary)?;
    let expected_version = required_env("CODEX_EXTERNAL_WRITE_EXPECTED_VERSION")?;
    let expected = Version::parse(&expected_version).context("expected version must be semver")?;
    ensure!(
        expected.pre.is_empty()
            && expected.build.is_empty()
            && expected.to_string() == expected_version,
        "expected version must be canonical exact semver"
    );
    let probed = probe_version(&CodexProcessConfig {
        binary: binary.clone(),
        codex_home: None,
    })
    .await
    .context("exact binary version probe failed")?;
    ensure!(
        probed == expected,
        "configured binary version did not match"
    );

    let scratch = tempfile::tempdir().context("unable to create smoke scratch")?;
    let codex_home = scratch.path().join("codex-home");
    std::fs::create_dir(&codex_home).context("unable to create isolated Codex home")?;
    let model_stub = ScriptedResponsesServer::start().await?;
    write_model_provider_config(&codex_home, &model_stub.base_url)?;
    let workspace = scratch.path().join("workspace");
    std::fs::create_dir(&workspace).context("unable to create smoke workspace")?;
    let token_path = scratch.path().join("write-bearer");
    let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    write_private_token(&token_path, &token)?;
    let store_path = scratch.path().join("bridge.sqlite");

    let probe_listener = std::net::TcpListener::bind("127.0.0.1:0")
        .context("unable to reserve a loopback smoke port")?;
    let port = probe_listener.local_addr().context("smoke port")?.port();
    drop(probe_listener);
    let listen_endpoint = format!("ws://127.0.0.1:{port}");
    let endpoint = format!("{listen_endpoint}/");
    let mut child = spawn_server(&binary, &listen_endpoint, &token_path, &codex_home)?;
    wait_until_listening(port).await?;
    exact_health(port).await?;

    let mut operator = operator_connect(&endpoint, &token).await?;
    let thread = operator_request(
        &mut operator,
        2,
        "thread/start",
        json!({
            "cwd": workspace,
            "ephemeral": false,
            "historyMode": "paginated",
            "model": "gpt-5.4",
            "modelProvider": "external-write-smoke",
            "approvalPolicy": "on-request",
            "approvalsReviewer": APPROVAL_REVIEWER
        }),
    )
    .await?;
    let thread_id = thread["thread"]["id"]
        .as_str()
        .context("thread/start omitted thread id")?
        .to_owned();
    ensure!(
        thread["modelProvider"] == "external-write-smoke",
        "wrong provider"
    );

    // Exact Codex only resumes a persisted thread after it has materialized at least one turn.
    // Seed one interrupted turn, then prove that all later shared mutations create no replay.
    let seed = operator_request(
        &mut operator,
        3,
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [{"type": "text", "text": "seed resumable history"}],
            "clientUserMessageId": "message-seed",
            "approvalPolicy": "never"
        }),
    )
    .await?;
    let seed_turn = seed["turn"]["id"]
        .as_str()
        .context("seed turn/start omitted turn id")?
        .to_owned();
    model_stub.wait_for_count(1).await?;
    operator_request(
        &mut operator,
        4,
        "turn/interrupt",
        json!({"threadId": thread_id, "turnId": seed_turn}),
    )
    .await?;

    // The coordinator must establish the one authoritative resume before the adversarial second
    // client attaches. Codex rejects two simultaneous resume owners for the same live thread.
    close_operator(operator).await;

    let gate = configured_gate(&endpoint, &expected_version, &token_path)?;
    let endpoint_label = gate.endpoint_label().as_str().to_owned();
    let store = StoreHandle::open(&store_path)
        .await
        .context("unable to open write store")?;
    store
        .reserve_external_epoch(&endpoint_label, ExternalUncertaintyReason::BridgeRestart)
        .await
        .context("unable to seed write epoch")?;
    store
        .register_external_thread(&endpoint_label, &thread_id)
        .await
        .context("unable to adopt operator thread")?;
    let (source, recipient) = authorized_actors()?;
    let mut coordinator = ExternalWriteCoordinator::connect(
        gate,
        store.clone(),
        CancellationToken::new(),
        ExternalWriteSettings {
            request_timeout: Duration::from_secs(5),
            approval_timeout: Duration::from_secs(10),
            client_actor: "exact-smoke-client".to_owned(),
            approval_actor: "exact-smoke-approval-handler".to_owned(),
            approval_reviewer: APPROVAL_REVIEWER.to_owned(),
            approval_recipient: recipient.clone(),
        },
    )
    .await
    .context("write coordinator did not reconcile and connect")?;
    let mut operator = operator_connect(&endpoint, &token).await?;

    let active_turn = coordinator
        .start_turn(
            source.clone(),
            "intent-exact-start",
            start_params(&thread_id, "message-exact-start", "exact mutation"),
        )
        .await
        .context("exact bridge start failed")?
        .result_id
        .context("exact bridge start omitted id")?;
    model_stub.wait_for_count(2).await?;
    let steered = coordinator
        .steer_turn(
            source.clone(),
            "intent-exact-steer",
            TurnSteerParams {
                thread_id: thread_id.clone(),
                expected_turn_id: active_turn.clone(),
                input: vec![UserInput::text("exact steer")],
                additional_context: None,
                client_user_message_id: Some("message-exact-steer".to_owned()),
                responsesapi_client_metadata: None,
            },
        )
        .await
        .context("exact-id bridge steer failed")?;
    ensure!(
        steered.result_id.as_deref() == Some(&active_turn),
        "steer id drift"
    );
    let queued_id = coordinator
        .queue_input(
            source.clone(),
            "intent-exact-queue",
            &active_turn,
            ThreadQueueAddParams {
                thread_id: thread_id.clone(),
                client_user_message_id: "message-exact-queue".to_owned(),
                input: vec![UserInput::text("queued exact input")],
            },
        )
        .await
        .context("exact queue add failed")?
        .result_id
        .context("queue add omitted id")?;
    coordinator
        .interrupt_turn(
            source.clone(),
            "intent-exact-interrupt",
            TurnInterruptParams::new(&thread_id, &active_turn),
        )
        .await
        .context("exact-id bridge interrupt failed")?;
    wait_operator_idle(&mut operator, 6, &thread_id).await?;
    let queued_turn = coordinator
        .start_queued(
            source.clone(),
            "intent-exact-queue-start",
            ThreadQueueStartParams {
                thread_id: thread_id.clone(),
                queued_submission_id: Some(queued_id.clone()),
            },
        )
        .await
        .context("exact queue start failed")?
        .result_id
        .context("queue start omitted turn id")?;
    model_stub.wait_for_count(4).await?;
    coordinator
        .interrupt_turn(
            source.clone(),
            "intent-queued-interrupt",
            TurnInterruptParams::new(&thread_id, &queued_turn),
        )
        .await
        .context("queued turn interrupt failed")?;
    wait_operator_idle(&mut operator, 7, &thread_id).await?;

    let approval_turn = coordinator
        .start_turn(
            source.clone(),
            "intent-approval-start",
            start_params(&thread_id, "message-approval-start", "request approval"),
        )
        .await
        .context("approval turn start failed")?
        .result_id
        .context("approval turn omitted id")?;
    model_stub.wait_for_count(5).await?;
    let prompt = timeout(READY_TIMEOUT, coordinator.recv_approval())
        .await
        .context("configured approval route timed out")?
        .context("configured approval route closed")?;
    coordinator
        .resolve_approval(
            recipient.clone(),
            prompt.approval_id.clone(),
            ExternalApprovalDecision::Command(CommandExecutionRequestApprovalResult {
                decision: CommandExecutionApprovalDecision::Simple(SimpleApprovalDecision::Decline),
            }),
        )
        .await
        .context("configured recipient could not answer approval")?;
    model_stub.wait_for_count(6).await?;
    wait_approval_resolved(&store, &endpoint_label, &thread_id, &prompt.approval_id).await?;
    wait_operator_idle(&mut operator, 8, &thread_id).await?;

    let claim = store
        .external_approval_claim(&endpoint_label, &thread_id, &prompt.approval_id)
        .await
        .context("unable to read approval claim")?
        .context("approval claim missing")?;
    ensure!(
        claim.turn_id == approval_turn,
        "approval was routed to wrong turn"
    );
    ensure!(
        claim.source_actor == source.as_str(),
        "approval lost source actor"
    );
    ensure!(
        claim.recipient_actor == recipient.as_str(),
        "approval lost configured recipient"
    );
    ensure!(
        claim.client_actor != claim.approval_actor,
        "client and approval handler actors must remain distinct"
    );
    let queue = operator_request(
        &mut operator,
        9,
        "thread/queue/list",
        json!({"threadId": thread_id, "limit": 100}),
    )
    .await?;
    ensure!(
        queue["data"]
            .as_array()
            .context("queue/list omitted data")?
            .iter()
            .all(|entry| entry["id"] != queued_id),
        "started queued submission remained queued"
    );
    let turns = operator_request(
        &mut operator,
        10,
        "thread/turns/list",
        json!({"threadId": thread_id, "limit": 100, "sortDirection": "asc"}),
    )
    .await?;
    let turn_ids = turns["data"]
        .as_array()
        .context("turns/list omitted data")?
        .iter()
        .filter_map(|turn| turn["id"].as_str())
        .collect::<Vec<_>>();
    ensure!(
        turn_ids.len() == 4,
        "mutation replay created extra turns before the adversarial race"
    );
    ensure!(
        turn_ids.contains(&seed_turn.as_str()),
        "resumable seed turn missing"
    );
    ensure!(
        turn_ids.contains(&active_turn.as_str()),
        "exact mutation turn missing"
    );
    ensure!(
        turn_ids.contains(&queued_turn.as_str()),
        "queued turn missing"
    );
    ensure!(
        turn_ids.contains(&approval_turn.as_str()),
        "approval turn missing"
    );
    ensure!(
        model_stub.request_count() == 6,
        "model work was unexpectedly replayed"
    );

    coordinator
        .shutdown()
        .await
        .context("write coordinator shutdown failed")?;
    let endpoint_state = store
        .external_endpoint_epoch(&endpoint_label)
        .await
        .context("unable to read stopped endpoint")?
        .context("stopped endpoint missing")?;
    ensure!(
        endpoint_state.state == ExternalEndpointState::Stopped,
        "endpoint not stopped"
    );
    close_operator(operator).await;

    // Reconnect after an orderly stop, then race a second client. If the bridge cannot correlate
    // the winning response to its client id, it must fence the thread instead of replaying.
    let mut race_coordinator = Some(
        ExternalWriteCoordinator::connect(
            configured_gate(&endpoint, &expected_version, &token_path)?,
            store.clone(),
            CancellationToken::new(),
            ExternalWriteSettings {
                request_timeout: Duration::from_secs(5),
                approval_timeout: Duration::from_secs(10),
                client_actor: "exact-smoke-client".to_owned(),
                approval_actor: "exact-smoke-approval-handler".to_owned(),
                approval_reviewer: APPROVAL_REVIEWER.to_owned(),
                approval_recipient: recipient,
            },
        )
        .await
        .context("race coordinator did not reconcile and reconnect")?,
    );
    let mut operator = operator_connect(&endpoint, &token).await?;
    let bridge_race = race_coordinator
        .as_ref()
        .expect("coordinator present")
        .start_turn(
            source.clone(),
            "intent-race-bridge",
            start_params(&thread_id, "message-race-bridge", "race"),
        );
    let operator_race = operator_request_raw(
        &mut operator,
        11,
        "turn/start",
        json!({
            "threadId": thread_id,
            "input": [{"type": "text", "text": "operator race"}],
            "clientUserMessageId": "message-race-operator",
            "approvalPolicy": "on-request",
            "approvalsReviewer": APPROVAL_REVIEWER
        }),
    );
    let (bridge_race, operator_race) = tokio::join!(bridge_race, operator_race);
    let (race_turn, race_fenced) = match (bridge_race, operator_race?) {
        (Ok(applied), OperatorResponse::Rejected) => {
            let turn_id = applied.result_id.context("bridge race omitted turn id")?;
            race_coordinator
                .as_ref()
                .expect("coordinator present")
                .interrupt_turn(
                    source.clone(),
                    "intent-race-interrupt",
                    TurnInterruptParams::new(&thread_id, &turn_id),
                )
                .await
                .context("bridge race winner interrupt failed")?;
            (turn_id, false)
        }
        (
            Err(
                error @ (ExternalWriteError::Conflict
                | ExternalWriteError::Uncertain
                | ExternalWriteError::Ambiguous),
            ),
            OperatorResponse::Result(result),
        ) => {
            let turn_id = result["turn"]["id"]
                .as_str()
                .context("operator race omitted turn id")?
                .to_owned();
            operator_request(
                &mut operator,
                12,
                "turn/interrupt",
                json!({"threadId": thread_id, "turnId": turn_id}),
            )
            .await?;
            (turn_id, error != ExternalWriteError::Conflict)
        }
        (bridge, operator) => bail!(
            "two-client start race did not have one correlated or safely fenced winner: bridge={bridge:?} operator={operator:?}"
        ),
    };
    model_stub.wait_for_count(7).await?;
    wait_operator_idle(&mut operator, 13, &thread_id).await?;
    let turns = operator_request(
        &mut operator,
        14,
        "thread/turns/list",
        json!({"threadId": thread_id, "limit": 100, "sortDirection": "asc"}),
    )
    .await?;
    let turn_ids = turns["data"]
        .as_array()
        .context("post-race turns/list omitted data")?
        .iter()
        .filter_map(|turn| turn["id"].as_str())
        .collect::<Vec<_>>();
    ensure!(turn_ids.len() == 5, "two-client race replayed turn/start");
    ensure!(
        turn_ids.iter().filter(|id| **id == race_turn).count() == 1,
        "race winner was missing or duplicated"
    );
    ensure!(
        model_stub.request_count() == 7,
        "two-client race replayed model work"
    );
    if race_fenced {
        wait_endpoint_state(&store, &endpoint_label, ExternalEndpointState::Unavailable).await?;
        drop(race_coordinator.take());
    } else {
        race_coordinator
            .take()
            .expect("coordinator present")
            .shutdown()
            .await
            .context("race coordinator shutdown failed")?;
        wait_endpoint_state(&store, &endpoint_label, ExternalEndpointState::Stopped).await?;
    }
    close_operator(operator).await;
    child.ensure_running()?;
    exact_health(port).await?;
    store.shutdown().await.context("store shutdown failed")?;
    child.stop().await?;
    eprintln!(
        "external_write_exact race={race_turn} active={active_turn} queued={queued_turn} approval={approval_turn} responses={}",
        model_stub.request_count()
    );
    Ok(())
}

fn start_params(thread_id: &str, client_id: &str, text: &str) -> TurnStartParams {
    let mut params = TurnStartParams::new(thread_id, vec![UserInput::text(text)]);
    params.client_user_message_id = Some(client_id.to_owned());
    params.approval_policy = Some(ApprovalPolicy::Named("on-request".to_owned()));
    params.approvals_reviewer = Some(APPROVAL_REVIEWER.to_owned());
    params
}

async fn wait_approval_resolved(
    store: &StoreHandle,
    endpoint_label: &str,
    thread_id: &str,
    approval_id: &str,
) -> Result<()> {
    timeout(READY_TIMEOUT, async {
        loop {
            let claim = store
                .external_approval_claim(endpoint_label, thread_id, approval_id)
                .await
                .context("unable to poll approval claim")?
                .context("approval claim disappeared")?;
            if claim.state == ExternalApprovalState::Resolved {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("approval did not receive serverRequest/resolved")?
}

async fn wait_endpoint_state(
    store: &StoreHandle,
    endpoint_label: &str,
    expected: ExternalEndpointState,
) -> Result<()> {
    timeout(READY_TIMEOUT, async {
        loop {
            if store
                .external_endpoint_epoch(endpoint_label)
                .await
                .context("unable to poll external endpoint")?
                .is_some_and(|epoch| epoch.state == expected)
            {
                return Ok(());
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("external endpoint did not reach the expected state")?
}

async fn wait_operator_idle(
    socket: &mut OperatorSocket,
    request_id: i64,
    thread_id: &str,
) -> Result<()> {
    timeout(READY_TIMEOUT, async {
        loop {
            let thread = operator_request(
                socket,
                request_id,
                "thread/read",
                json!({"threadId": thread_id, "includeTurns": false}),
            )
            .await?;
            if thread["thread"]["status"]["type"] == "idle" {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .context("thread did not become idle")?
}

type OperatorSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn operator_connect(endpoint: &str, token: &str) -> Result<OperatorSocket> {
    let mut request = endpoint
        .into_client_request()
        .context("unable to build operator WebSocket request")?;
    request.headers_mut().insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token}"))
            .context("operator bearer header is invalid")?,
    );
    let (mut socket, _) = timeout(STARTUP_TIMEOUT, connect_async(request))
        .await
        .context("operator WebSocket connect timed out")??;
    let initialized = operator_request(
        &mut socket,
        1,
        "initialize",
        json!({
            "clientInfo": {"name": "external-write-smoke-operator", "version": "1.0.0"},
            "capabilities": {"experimentalApi": true}
        }),
    )
    .await?;
    ensure!(
        initialized["userAgent"].as_str().is_some(),
        "missing user agent"
    );
    socket
        .send(Message::Text(
            json!({"method": "initialized"}).to_string().into(),
        ))
        .await
        .context("unable to send initialized notification")?;
    Ok(socket)
}

async fn operator_request(
    socket: &mut OperatorSocket,
    id: i64,
    method: &str,
    params: Value,
) -> Result<Value> {
    match operator_request_raw(socket, id, method, params).await? {
        OperatorResponse::Result(result) => Ok(result),
        OperatorResponse::Rejected => bail!("operator {method} was rejected"),
    }
}

async fn operator_request_raw(
    socket: &mut OperatorSocket,
    id: i64,
    method: &str,
    params: Value,
) -> Result<OperatorResponse> {
    socket
        .send(Message::Text(
            json!({"id": id, "method": method, "params": params})
                .to_string()
                .into(),
        ))
        .await
        .with_context(|| format!("unable to send operator {method}"))?;
    timeout(STARTUP_TIMEOUT, async {
        loop {
            let message = socket
                .next()
                .await
                .context("operator socket closed before response")??;
            let Message::Text(text) = message else {
                continue;
            };
            let value: Value = serde_json::from_str(&text)
                .with_context(|| format!("operator {method} response was not JSON"))?;
            if value["id"] == id {
                return if value.get("error").is_some() {
                    Ok(OperatorResponse::Rejected)
                } else {
                    Ok(OperatorResponse::Result(
                        value
                            .get("result")
                            .cloned()
                            .context("response omitted result")?,
                    ))
                };
            }
        }
    })
    .await
    .with_context(|| format!("operator {method} timed out"))?
}

async fn close_operator(mut socket: OperatorSocket) {
    let _ = socket.close(None).await;
}

fn authorized_actors() -> Result<(AuthorizedLarkActor, AuthorizedLarkActor)> {
    let config = BridgeConfig {
        owners: vec!["ou_exact_owner_123456".to_owned()],
        allowed_senders: vec!["ou_exact_sender_123456".to_owned()],
        allowed_groups: vec![],
        default_workspace: None,
        workspace: WorkspacePolicy {
            allow_roots: vec![PathBuf::from(env!("CARGO_MANIFEST_DIR"))],
            network_access: false,
        },
        concurrency: ConcurrencyConfig::default(),
        codex: CodexSection::default(),
        paths: PathsSection::default(),
        ..BridgeConfig::default()
    };
    let policy =
        AccessPolicy::from_config(&config).context("unable to build Lark access policy")?;
    let source = policy
        .authorize_external_source(&lark_event("ou_exact_sender_123456"))
        .map_err(|decision| anyhow::anyhow!("source authorization failed: {decision:?}"))?;
    let recipient = policy
        .authorize_external_approval_recipient(&lark_event("ou_exact_owner_123456"))
        .map_err(|decision| anyhow::anyhow!("recipient authorization failed: {decision:?}"))?;
    Ok((source, recipient))
}

fn lark_event(sender_id: &str) -> InboundEvent {
    InboundEvent {
        event_id: "evt-exact-write".to_owned(),
        message_id: "om-exact-write".to_owned(),
        chat_id: "oc-exact-write".to_owned(),
        sender_id: sender_id.to_owned(),
        chat_type: ChatMode::P2p,
        thread_id: None,
        root_id: None,
        reply_to_message_id: None,
        text: "bounded".to_owned(),
        mentions_bot: false,
        mention_all: false,
        sender_is_human: true,
        mentions: Vec::new(),
        parts: Vec::new(),
        resources: Vec::new(),
        message_type: "text".to_owned(),
        create_time_ms: 0,
        scope: ScopeKey::Chat("oc-exact-write".to_owned()),
    }
}

fn spawn_server(
    binary: &Path,
    listen_endpoint: &str,
    token_path: &Path,
    codex_home: &Path,
) -> Result<ChildGuard> {
    let child = Command::new(binary)
        .arg("app-server")
        .arg("--listen")
        .arg(listen_endpoint)
        .arg("--ws-auth")
        .arg("capability-token")
        .arg("--ws-token-file")
        .arg(token_path)
        .env("CODEX_HOME", codex_home)
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .context("unable to start exact external app-server")?;
    Ok(ChildGuard { child })
}

async fn exact_health(port: u16) -> Result<()> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(5))
        .build()
        .context("unable to create health client")?;
    let response = client
        .get(format!("http://127.0.0.1:{port}/healthz"))
        .send()
        .await
        .context("external server health failed")?;
    ensure!(
        response.status() == reqwest::StatusCode::OK,
        "health was not exact HTTP 200"
    );
    Ok(())
}

fn configured_gate(
    endpoint: &str,
    expected_version: &str,
    token_path: &Path,
) -> Result<ExternalEndpointGate> {
    ExternalEndpointGate::new(ExternalEndpointConfig {
        endpoint: endpoint.to_owned(),
        expected_codex_version: expected_version.to_owned(),
        capability_profile: ExternalCapabilityProfile::QueueShared,
        authentication: ExternalAuthentication::BearerTokenFile {
            path: token_path.to_path_buf(),
        },
    })
    .context("explicit external write gate configuration was rejected")
}

fn required_env(name: &str) -> Result<String> {
    let value = std::env::var(name).with_context(|| format!("required gate {name} is missing"))?;
    ensure!(!value.is_empty(), "required gate {name} is empty");
    Ok(value)
}

fn ensure_native_server_binary(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let mut magic = [0_u8; 2];
        File::open(path)
            .context("unable to open exact write binary")?
            .read_exact(&mut magic)
            .context("unable to inspect exact write binary")?;
        ensure!(
            magic != *b"#!",
            "CODEX_EXTERNAL_WRITE_BINARY must name the native Codex executable"
        );
    }
    #[cfg(windows)]
    {
        ensure!(
            path.extension()
                .and_then(std::ffi::OsStr::to_str)
                .is_some_and(|extension| extension.eq_ignore_ascii_case("exe")),
            "CODEX_EXTERNAL_WRITE_BINARY must name the native Codex .exe"
        );
    }
    Ok(())
}

fn write_private_token(path: &Path, token: &str) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .context("unable to create smoke bearer")?;
    file.write_all(format!("{token}\n").as_bytes())
        .context("unable to write smoke bearer")?;
    file.sync_all().context("unable to sync smoke bearer")?;
    Ok(())
}

fn write_model_provider_config(codex_home: &Path, base_url: &str) -> Result<()> {
    let config = format!(
        r#"model = "gpt-5.4"
model_provider = "external-write-smoke"

[model_providers.external-write-smoke]
name = "External write smoke"
base_url = "{base_url}"
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0
requires_openai_auth = false
"#
    );
    std::fs::write(codex_home.join("config.toml"), config)
        .context("unable to write isolated smoke provider")
}

async fn wait_until_listening(port: u16) -> Result<()> {
    let deadline = Instant::now() + STARTUP_TIMEOUT;
    loop {
        if TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("exact external app-server did not start before deadline");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
