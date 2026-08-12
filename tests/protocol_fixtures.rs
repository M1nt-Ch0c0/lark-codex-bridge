use lark_codex_bridge::codex::protocol::{
    InboundMessage, OutboundMessage, ProtocolError, RequestId, decode_line, encode_line,
};
use lark_codex_bridge::codex::types::{
    AgentMessageDeltaNotification, InitializeResult, ItemCompletedNotification, MessagePhase,
    SandboxMode, ThreadItem, ThreadStartResult, TurnCompletedNotification, TurnSandboxPolicy,
    TurnStartedNotification, TurnStatus, UserInput,
};
use lark_codex_bridge::limits::MAX_JSONL_LINE_BYTES;
use serde_json::{Value, json};

const INITIALIZE_RESPONSE: &[u8] = include_bytes!("fixtures/codex/initialize_response.json");
const THREAD_START_RESPONSE: &[u8] = include_bytes!("fixtures/codex/thread_start_response.json");
const TURN_STARTED: &[u8] = include_bytes!("fixtures/codex/turn_started.json");
const AGENT_DELTA: &[u8] = include_bytes!("fixtures/codex/agent_delta.json");
const ITEM_COMPLETED: &[u8] = include_bytes!("fixtures/codex/item_completed.json");
const TURN_COMPLETED: &[u8] = include_bytes!("fixtures/codex/turn_completed.json");

#[test]
fn decodes_string_and_integer_response_ids() {
    let initialized = decode_line(INITIALIZE_RESPONSE).expect("initialize fixture should decode");
    match initialized {
        InboundMessage::Response { id, result } => {
            assert_eq!(id, RequestId::String("initialize-1".to_owned()));
            assert_eq!(result["platformOs"], "linux");
        }
        message => panic!("expected initialize response, got {message:?}"),
    }

    let started = decode_line(THREAD_START_RESPONSE).expect("thread fixture should decode");
    match started {
        InboundMessage::Response { id, result } => {
            assert_eq!(id, RequestId::Integer(7));
            assert_eq!(
                result["thread"]["id"],
                "0198f100-0000-7000-8000-000000000001"
            );
        }
        message => panic!("expected thread response, got {message:?}"),
    }
}

#[test]
fn schema_fixtures_decode_into_the_stable_typed_subset() {
    let initialized: InitializeResult =
        serde_json::from_value(response_result(INITIALIZE_RESPONSE))
            .expect("initialize result should match the 0.146.0 schema");
    assert_eq!(initialized.platform_os, "linux");

    let started: ThreadStartResult = serde_json::from_value(response_result(THREAD_START_RESPONSE))
        .expect("thread start result should match the 0.146.0 schema");
    assert_eq!(started.thread.cli_version, "0.146.0");
    assert!(matches!(
        started.sandbox,
        TurnSandboxPolicy::WorkspaceWrite { .. }
    ));

    let turn_started: TurnStartedNotification =
        serde_json::from_value(notification_params(TURN_STARTED, "turn/started"))
            .expect("turn started params should decode");
    assert_eq!(turn_started.turn.status, TurnStatus::InProgress);

    let delta: AgentMessageDeltaNotification =
        serde_json::from_value(notification_params(AGENT_DELTA, "item/agentMessage/delta"))
            .expect("agent delta params should decode");
    assert_eq!(delta.delta, "Working");

    let item: ItemCompletedNotification =
        serde_json::from_value(notification_params(ITEM_COMPLETED, "item/completed"))
            .expect("completed item params should decode");
    assert!(matches!(item.item, ThreadItem::AgentMessage { .. }));

    let completed: TurnCompletedNotification =
        serde_json::from_value(notification_params(TURN_COMPLETED, "turn/completed"))
            .expect("completed turn params should decode");
    assert_eq!(completed.turn.status, TurnStatus::Completed);
}

#[test]
fn classifies_interleaved_notifications_independently_from_responses() {
    let wire_messages = [TURN_STARTED, INITIALIZE_RESPONSE, AGENT_DELTA];
    let decoded = wire_messages.map(|line| decode_line(line).expect("fixture should decode"));

    assert!(matches!(
        &decoded[0],
        InboundMessage::Notification { method, params: Some(params) }
            if method == "turn/started" && params["turn"]["status"] == "inProgress"
    ));
    assert!(matches!(
        &decoded[1],
        InboundMessage::Response {
            id: RequestId::String(id),
            ..
        } if id == "initialize-1"
    ));
    assert!(matches!(
        &decoded[2],
        InboundMessage::Notification { method, params: Some(params) }
            if method == "item/agentMessage/delta" && params["delta"] == "Working"
    ));
}

#[test]
fn decodes_error_responses_without_losing_error_data() {
    let line = br#"{
        "id": "turn-9",
        "error": {
            "code": -32602,
            "message": "invalid turn parameters",
            "data": {"field": "threadId"}
        },
        "traceId": "additive-field"
    }"#;

    let message = decode_line(line).expect("error response should decode");
    match message {
        InboundMessage::ErrorResponse { id, error } => {
            assert_eq!(id, RequestId::String("turn-9".to_owned()));
            assert_eq!(error.code, -32602);
            assert_eq!(error.message, "invalid turn parameters");
            assert_eq!(error.data, Some(json!({"field": "threadId"})));
        }
        other => panic!("expected error response, got {other:?}"),
    }
}

#[test]
fn rejects_missing_and_ambiguous_envelopes() {
    assert_protocol_error(
        decode_line(br#"{"params": {}}"#).expect_err("missing method/id must be rejected"),
    );
    assert_protocol_error(
        decode_line(br#"{"id": 1, "method": "thread/start", "result": {}}"#)
            .expect_err("request/response hybrid must be rejected"),
    );
    assert_protocol_error(
        decode_line(br#"{"id": 1, "result": {}, "error": {"code": -1, "message": "x"}}"#)
            .expect_err("result/error hybrid must be rejected"),
    );
    assert_protocol_error(
        decode_line(br#"{"id": 1, "id": 2, "result": {}}"#)
            .expect_err("duplicate discriminator must be rejected"),
    );
    assert_protocol_error(
        decode_line(br#"{"id": 1, "error": null}"#)
            .expect_err("a null error object must be rejected"),
    );
}

#[test]
fn preserves_explicit_null_and_accepts_additive_fields() {
    let result = decode_line(br#"{"id":1,"result":null,"traceId":"safe-addition"}"#)
        .expect("null is a valid response result");
    assert!(matches!(
        result,
        InboundMessage::Response {
            result: Value::Null,
            ..
        }
    ));

    let notification = decode_line(br#"{"method":"initialized","params":null}"#)
        .expect("explicit null params should decode");
    assert!(matches!(
        notification,
        InboundMessage::Notification {
            params: Some(Value::Null),
            ..
        }
    ));
}

#[test]
fn request_ids_are_strings_or_signed_64_bit_integers_only() {
    for line in [
        br#"{"id":-9223372036854775808,"result":null}"#.as_slice(),
        br#"{"id":9223372036854775807,"result":null}"#.as_slice(),
        br#"{"id":"opaque-id","result":null}"#.as_slice(),
    ] {
        decode_line(line).expect("valid request id should decode");
    }

    for line in [
        br#"{"id":9223372036854775808,"result":null}"#.as_slice(),
        br#"{"id":1.0,"result":null}"#.as_slice(),
        br#"{"id":true,"result":null}"#.as_slice(),
        br#"{"id":null,"result":null}"#.as_slice(),
    ] {
        assert_protocol_error(decode_line(line).expect_err("invalid request id must be rejected"));
    }
}

#[test]
fn outbound_encoding_appends_one_newline_and_omits_jsonrpc() {
    let request = OutboundMessage::Request {
        id: RequestId::Integer(42),
        method: "thread/read".to_owned(),
        params: Some(json!({"threadId": "thread-1", "includeTurns": true})),
    };
    let encoded = encode_line(&request).expect("request should encode");

    assert!(encoded.ends_with(b"\n"));
    assert!(!encoded.ends_with(b"\n\n"));
    let value: Value = serde_json::from_slice(&encoded[..encoded.len() - 1])
        .expect("encoded bytes before newline should be JSON");
    assert_eq!(value["id"], 42);
    assert_eq!(value["method"], "thread/read");
    assert_eq!(value["params"]["includeTurns"], true);
    assert!(value.get("jsonrpc").is_none());

    let initialized = OutboundMessage::Notification {
        method: "initialized".to_owned(),
        params: None,
    };
    let encoded = encode_line(&initialized).expect("notification should encode");
    let value: Value = serde_json::from_slice(&encoded).expect("newline is JSON whitespace");
    assert_eq!(value, json!({"method": "initialized"}));
    assert!(!encoded[..encoded.len() - 1].contains(&b'\n'));
}

#[test]
fn oversized_lines_are_rejected_before_parsing() {
    let oversized = vec![b' '; MAX_JSONL_LINE_BYTES + 1];
    assert_protocol_error(decode_line(&oversized).expect_err("oversized line must be rejected"));

    let oversized_outbound = OutboundMessage::Notification {
        method: "future/event".to_owned(),
        params: Some(Value::String("x".repeat(MAX_JSONL_LINE_BYTES))),
    };
    assert_protocol_error(
        encode_line(&oversized_outbound).expect_err("oversized output must be rejected"),
    );
}

#[test]
fn thread_sandbox_modes_use_kebab_case() {
    for wire in ["read-only", "workspace-write", "danger-full-access"] {
        let mode: SandboxMode =
            serde_json::from_value(json!(wire)).expect("sandbox mode should decode");
        assert_eq!(
            serde_json::to_value(mode).expect("mode should encode"),
            wire
        );
    }
}

#[test]
fn turn_sandbox_policy_types_use_camel_case() {
    let policies = [
        json!({"type": "readOnly", "networkAccess": false}),
        json!({
            "type": "workspaceWrite",
            "writableRoots": ["/workspace"],
            "networkAccess": false,
            "excludeTmpdirEnvVar": false,
            "excludeSlashTmp": false
        }),
        json!({"type": "dangerFullAccess"}),
    ];

    for policy in policies {
        let expected_type = policy["type"].clone();
        let typed: TurnSandboxPolicy =
            serde_json::from_value(policy).expect("turn sandbox policy should decode");
        assert_eq!(
            serde_json::to_value(typed).expect("turn sandbox policy should encode")["type"],
            expected_type
        );
    }
}

#[test]
fn unknown_thread_items_preserve_the_complete_raw_payload() {
    let raw = json!({
        "id": "item-future-1",
        "type": "futureWidget",
        "payload": {"nested": [1, 2, 3]},
        "newFlag": true
    });
    let item: ThreadItem =
        serde_json::from_value(raw.clone()).expect("unknown item should be accepted");

    match &item {
        ThreadItem::Unknown {
            item_type,
            raw: preserved,
        } => {
            assert_eq!(item_type, "futureWidget");
            assert_eq!(preserved, &raw);
        }
        other => panic!("expected unknown item, got {other:?}"),
    }
    assert_eq!(
        serde_json::to_value(item).expect("unknown item should re-encode"),
        raw
    );

    let without_id = json!({"type": "futureWithoutId", "payload": 1});
    let item: ThreadItem = serde_json::from_value(without_id.clone())
        .expect("unknown future items should not assume an id field");
    assert_eq!(serde_json::to_value(item).unwrap(), without_id);
}

#[test]
fn malformed_known_items_are_rejected_and_open_values_round_trip() {
    serde_json::from_value::<ThreadItem>(json!({"type": "agentMessage", "id": "item-1"}))
        .expect_err("known items must retain their required-field validation");

    let status: TurnStatus = serde_json::from_value(json!("futureTerminal"))
        .expect("unknown status should remain forwards compatible");
    assert_eq!(status, TurnStatus::Unknown("futureTerminal".to_owned()));
    assert_eq!(serde_json::to_value(status).unwrap(), "futureTerminal");

    let input: UserInput = serde_json::from_value(json!({
        "type": "text",
        "text": "hello",
        "text_elements": [{"start": 0, "end": 5}]
    }))
    .expect("text_elements uses the schema's snake_case spelling");
    assert!(
        serde_json::to_value(input)
            .unwrap()
            .get("text_elements")
            .is_some()
    );

    let phase: MessagePhase = serde_json::from_value(json!("final_answer")).unwrap();
    assert_eq!(phase, MessagePhase::FinalAnswer);
}

#[test]
fn debug_and_errors_do_not_expose_wire_payloads() {
    let sentinel = "DO_NOT_LOG_THIS_PROMPT_OR_TOKEN";
    let response =
        decode_line(format!(r#"{{"id":1,"result":{{"secret":"{sentinel}"}}}}"#).as_bytes())
            .unwrap();
    assert!(!format!("{response:?}").contains(sentinel));

    let error = decode_line(
        format!(r#"{{"id":1,"error":{{"code":-1,"message":"{sentinel}","data":"{sentinel}"}}}}"#)
            .as_bytes(),
    )
    .unwrap();
    assert!(!format!("{error:?}").contains(sentinel));

    let item: ThreadItem = serde_json::from_value(json!({
        "type": "futureSecretItem",
        "id": "item-1",
        "payload": sentinel
    }))
    .unwrap();
    assert!(!format!("{item:?}").contains(sentinel));
}

#[test]
fn completed_item_and_turn_fixtures_are_authoritative_terminal_dtos() {
    let item_params = notification_params(ITEM_COMPLETED, "item/completed");
    let item: ThreadItem =
        serde_json::from_value(item_params["item"].clone()).expect("completed item should decode");
    match item {
        ThreadItem::AgentMessage { id, text, .. } => {
            assert_eq!(id, "item-agent-1");
            assert_eq!(text, "Hello from Codex.");
        }
        other => panic!("expected completed agent message, got {other:?}"),
    }

    let started_params = notification_params(TURN_STARTED, "turn/started");
    let started: TurnStatus = serde_json::from_value(started_params["turn"]["status"].clone())
        .expect("started turn status should decode");
    assert_eq!(started, TurnStatus::InProgress);

    let completed_params = notification_params(TURN_COMPLETED, "turn/completed");
    let completed: TurnStatus = serde_json::from_value(completed_params["turn"]["status"].clone())
        .expect("completed turn status should decode");
    assert_eq!(completed, TurnStatus::Completed);
    assert_eq!(
        completed_params["turn"]["items"][0]["text"],
        "Hello from Codex."
    );
}

fn notification_params(line: &[u8], expected_method: &str) -> Value {
    match decode_line(line).expect("notification fixture should decode") {
        InboundMessage::Notification {
            method,
            params: Some(params),
        } => {
            assert_eq!(method, expected_method);
            params
        }
        message => panic!("expected notification with params, got {message:?}"),
    }
}

fn response_result(line: &[u8]) -> Value {
    match decode_line(line).expect("response fixture should decode") {
        InboundMessage::Response { result, .. } => result,
        message => panic!("expected response, got {message:?}"),
    }
}

fn assert_protocol_error(_error: ProtocolError) {}
