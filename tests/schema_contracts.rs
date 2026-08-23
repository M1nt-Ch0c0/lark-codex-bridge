use lark_codex_bridge::codex::{
    client::{CONSUMED_NOTIFICATION_METHODS, ClientError, NORMAL_NOTIFICATION_ORDER},
    compat::{self, WireAdapter},
    protocol::{InboundMessage, OutboundMessage, RequestId, decode_line, encode_line},
    rpc::{ConnectionEpoch, RpcError},
    types, wire,
};
use serde_json::{Value, json};

fn contract(version: &str) -> Value {
    let source = match version {
        "0.146.0" => include_str!("../protocol/codex/contracts/0.146.0.json"),
        "0.149.0" => include_str!("../protocol/codex/contracts/0.149.0.json"),
        _ => panic!("test requested an unknown contract version"),
    };
    serde_json::from_str(source).expect("committed contract fixture should be JSON")
}

fn exchange<'a>(contract: &'a Value, method: &str) -> &'a Value {
    contract["exchanges"]
        .as_array()
        .expect("exchanges should be an array")
        .iter()
        .find(|exchange| exchange["method"] == method)
        .expect("selected method should have a contract exchange")
}

fn notification<'a>(contract: &'a Value, method: &str) -> &'a Value {
    contract["notifications"]
        .as_array()
        .expect("notifications should be an array")
        .iter()
        .find(|notification| notification["method"] == method)
        .expect("consumed notification should have a contract fixture")
}

fn request_params_through_protocol(method: &str, params: Value) -> Value {
    let encoded = encode_line(&OutboundMessage::Request {
        id: RequestId::Integer(1),
        method: method.to_owned(),
        params: Some(params),
    })
    .expect("contract request should fit the production JSONL limits");
    assert_eq!(encoded.last(), Some(&b'\n'));

    match decode_line(&encoded).expect("encoded contract request should decode") {
        InboundMessage::Request {
            id,
            method: decoded_method,
            params: Some(decoded_params),
        } => {
            assert_eq!(id, RequestId::Integer(1));
            assert_eq!(decoded_method, method);
            decoded_params
        }
        _ => panic!("contract request should retain its protocol envelope kind"),
    }
}

fn notification_params_through_protocol(method: &str, params: Value) -> Value {
    let encoded = encode_line(&OutboundMessage::Notification {
        method: method.to_owned(),
        params: Some(params),
    })
    .expect("contract notification should fit the production JSONL limits");

    match decode_line(&encoded).expect("encoded contract notification should decode") {
        InboundMessage::Notification {
            method: decoded_method,
            params: Some(decoded_params),
        } => {
            assert_eq!(decoded_method, method);
            decoded_params
        }
        _ => panic!("contract notification should retain its protocol envelope kind"),
    }
}

fn response_result_through_protocol(result: Value) -> Value {
    let encoded = encode_line(&OutboundMessage::Response {
        id: RequestId::String("reverse-contract-1".to_owned()),
        result,
    })
    .expect("contract response should fit the production JSONL limits");

    match decode_line(&encoded).expect("encoded contract response should decode") {
        InboundMessage::Response { id, result } => {
            assert_eq!(id, RequestId::String("reverse-contract-1".to_owned()));
            result
        }
        _ => panic!("contract response should retain its protocol envelope kind"),
    }
}

macro_rules! assert_outgoing_adapter_params {
    ($fixture:expr, $adapter:expr, $method:literal, $stable:ty, $mapper:ident) => {{
        let exchange = exchange($fixture, $method);
        let stable: $stable = serde_json::from_value(exchange["params"].clone())
            .expect("contract params should decode into the stable request type");
        let mapped = $adapter
            .$mapper(&stable)
            .expect("supported adapter should encode stable request params");
        assert_eq!(mapped, exchange["params"], "adapter drift for {}", $method);
        assert_eq!(
            request_params_through_protocol($method, mapped),
            exchange["params"],
            "protocol round trip drift for {}",
            $method
        );
    }};
}

macro_rules! assert_contract {
    ($version:literal, $wire:ident, $compat:ident) => {{
        let fixture = contract($version);

        let initialize = exchange(&fixture, "initialize");
        serde_json::from_value::<wire::$wire::InitializeParams>(initialize["params"].clone())
            .expect("initialize params should decode as generated wire");
        let response =
            serde_json::from_value::<wire::$wire::InitializeResponse>(initialize["result"].clone())
                .expect("initialize response should decode as generated wire");
        assert!(compat::$compat::initialize_response(response).is_ok());

        let start = exchange(&fixture, "thread/start");
        serde_json::from_value::<wire::$wire::ThreadStartParams>(start["params"].clone())
            .expect("thread/start params should decode as generated wire");
        let response =
            serde_json::from_value::<wire::$wire::ThreadStartResponse>(start["result"].clone())
                .expect("thread/start response should decode as generated wire");
        let stable = compat::$compat::thread_start_response(response)
            .expect("thread/start response should map to stable domain");
        assert_eq!(stable.thread.cli_version, $version);

        let list = exchange(&fixture, "thread/list");
        serde_json::from_value::<wire::$wire::ThreadListParams>(list["params"].clone())
            .expect("thread/list params should decode as generated wire");
        let response =
            serde_json::from_value::<wire::$wire::ThreadListResponse>(list["result"].clone())
                .expect("thread/list response should decode as generated wire");
        let stable = compat::$compat::thread_list_response(response)
            .expect("thread/list response should map to stable domain");
        assert_eq!(stable.data.len(), 1);

        let read = exchange(&fixture, "thread/read");
        serde_json::from_value::<wire::$wire::ThreadReadParams>(read["params"].clone())
            .expect("thread/read params should decode as generated wire");
        let response =
            serde_json::from_value::<wire::$wire::ThreadReadResponse>(read["result"].clone())
                .expect("thread/read response should decode as generated wire");
        let stable = compat::$compat::thread_read_response(response)
            .expect("thread/read response should map to stable domain");
        assert_eq!(stable.thread.id, "thread-contract-1");

        let resume = exchange(&fixture, "thread/resume");
        serde_json::from_value::<wire::$wire::ThreadResumeParams>(resume["params"].clone())
            .expect("thread/resume params should decode as generated wire");
        let response =
            serde_json::from_value::<wire::$wire::ThreadResumeResponse>(resume["result"].clone())
                .expect("thread/resume response should decode as generated wire");
        assert!(compat::$compat::thread_resume_response(response).is_ok());

        let turn_start = exchange(&fixture, "turn/start");
        serde_json::from_value::<wire::$wire::TurnStartParams>(turn_start["params"].clone())
            .expect("turn/start params should decode as generated wire");
        let response =
            serde_json::from_value::<wire::$wire::TurnStartResponse>(turn_start["result"].clone())
                .expect("turn/start response should decode as generated wire");
        assert!(compat::$compat::turn_start_response(response).is_ok());

        let interrupt = exchange(&fixture, "turn/interrupt");
        serde_json::from_value::<wire::$wire::TurnInterruptParams>(interrupt["params"].clone())
            .expect("turn/interrupt params should decode as generated wire");
        let response = serde_json::from_value::<wire::$wire::TurnInterruptResponse>(
            interrupt["result"].clone(),
        )
        .expect("turn/interrupt response should decode as generated wire");
        assert!(compat::$compat::turn_interrupt_response(response).is_ok());

        let started = notification(&fixture, "thread/started");
        let params = serde_json::from_value::<wire::$wire::ThreadStartedNotification>(
            started["params"].clone(),
        )
        .expect("thread/started should decode as generated wire");
        assert!(compat::$compat::thread_started_notification(params).is_ok());

        let turn_started = notification(&fixture, "turn/started");
        let params = serde_json::from_value::<wire::$wire::TurnStartedNotification>(
            turn_started["params"].clone(),
        )
        .expect("turn/started should decode as generated wire");
        assert!(compat::$compat::turn_started_notification(params).is_ok());

        let item_started = notification(&fixture, "item/started");
        let params = serde_json::from_value::<wire::$wire::ItemStartedNotification>(
            item_started["params"].clone(),
        )
        .expect("item/started should decode as generated wire");
        assert!(compat::$compat::item_started_notification(params).is_ok());

        let delta = notification(&fixture, "item/agentMessage/delta");
        let params = serde_json::from_value::<wire::$wire::AgentMessageDeltaNotification>(
            delta["params"].clone(),
        )
        .expect("agent delta should decode as generated wire");
        assert!(compat::$compat::agent_message_delta_notification(params).is_ok());

        let output = notification(&fixture, "item/commandExecution/outputDelta");
        let params =
            serde_json::from_value::<wire::$wire::CommandExecutionOutputDeltaNotification>(
                output["params"].clone(),
            )
            .expect("command output should decode as generated wire");
        assert!(compat::$compat::command_output_delta_notification(params).is_ok());

        let completed = notification(&fixture, "item/completed");
        let params = serde_json::from_value::<wire::$wire::ItemCompletedNotification>(
            completed["params"].clone(),
        )
        .expect("item/completed should decode as generated wire");
        assert!(compat::$compat::item_completed_notification(params).is_ok());

        let usage = notification(&fixture, "thread/tokenUsage/updated");
        let params = serde_json::from_value::<wire::$wire::ThreadTokenUsageUpdatedNotification>(
            usage["params"].clone(),
        )
        .expect("token usage should decode as generated wire");
        assert!(compat::$compat::token_usage_updated_notification(params).is_ok());

        let error = notification(&fixture, "error");
        let params =
            serde_json::from_value::<wire::$wire::ErrorNotification>(error["params"].clone())
                .expect("error should decode as generated wire");
        assert!(compat::$compat::error_notification(params).is_ok());

        let completed = notification(&fixture, "turn/completed");
        let params = serde_json::from_value::<wire::$wire::TurnCompletedNotification>(
            completed["params"].clone(),
        )
        .expect("turn/completed should decode as generated wire");
        assert!(compat::$compat::turn_completed_notification(params).is_ok());

        let reverse = &fixture["reverseRequests"][0];
        let params =
            serde_json::from_value::<wire::$wire::DynamicToolCallParams>(reverse["params"].clone())
                .expect("dynamic tool call should decode as generated wire");
        assert!(compat::$compat::dynamic_tool_call_params(params).is_ok());
        let stable: types::DynamicToolCallResponse =
            serde_json::from_value(reverse["result"].clone()).expect("stable tool response");
        assert!(compat::$compat::dynamic_tool_call_response(&stable).is_ok());
    }};
}

#[test]
fn supported_and_candidate_contracts_map_through_their_versioned_namespaces() {
    assert_contract!("0.146.0", v0_146_0, v0_146_0);
    assert_contract!("0.149.0", v0_149_0, v0_149_0);
}

#[test]
fn supported_adapter_serializes_every_outgoing_contract_exactly() {
    let fixture = contract("0.146.0");
    let adapter = WireAdapter::V0_146_0;

    assert_outgoing_adapter_params!(
        &fixture,
        adapter,
        "initialize",
        types::InitializeParams,
        initialize_params
    );
    assert_outgoing_adapter_params!(
        &fixture,
        adapter,
        "thread/start",
        types::ThreadStartParams,
        thread_start_params
    );
    assert_outgoing_adapter_params!(
        &fixture,
        adapter,
        "thread/list",
        types::ThreadListParams,
        thread_list_params
    );
    assert_outgoing_adapter_params!(
        &fixture,
        adapter,
        "thread/read",
        types::ThreadReadParams,
        thread_read_params
    );
    assert_outgoing_adapter_params!(
        &fixture,
        adapter,
        "thread/resume",
        types::ThreadResumeParams,
        thread_resume_params
    );
    assert_outgoing_adapter_params!(
        &fixture,
        adapter,
        "turn/start",
        types::TurnStartParams,
        turn_start_params
    );
    assert_outgoing_adapter_params!(
        &fixture,
        adapter,
        "turn/interrupt",
        types::TurnInterruptParams,
        turn_interrupt_params
    );

    let reverse = &fixture["reverseRequests"][0];
    let stable: types::DynamicToolCallResponse = serde_json::from_value(reverse["result"].clone())
        .expect("contract result should decode into the stable tool response");
    let mapped = adapter
        .dynamic_tool_call_response(&stable)
        .expect("supported adapter should encode the dynamic-tool response");
    assert_eq!(mapped, reverse["result"]);
    assert_eq!(response_result_through_protocol(mapped), reverse["result"]);
}

#[test]
fn supported_adapter_maps_every_incoming_notification_and_reverse_request() {
    let fixture = contract("0.146.0");
    let adapter = WireAdapter::V0_146_0;

    for notification in fixture["notifications"]
        .as_array()
        .expect("notifications should be an array")
    {
        let method = notification["method"]
            .as_str()
            .expect("notification method should be a string");
        let params = notification_params_through_protocol(method, notification["params"].clone());
        match method {
            "thread/started" => {
                adapter
                    .thread_started_notification(params)
                    .expect("thread/started should map through the supported adapter");
            }
            "turn/started" => {
                adapter
                    .turn_started_notification(params)
                    .expect("turn/started should map through the supported adapter");
            }
            "item/started" => {
                adapter
                    .item_started_notification(params)
                    .expect("item/started should map through the supported adapter");
            }
            "item/agentMessage/delta" => {
                adapter
                    .agent_message_delta_notification(params)
                    .expect("agent delta should map through the supported adapter");
            }
            "item/commandExecution/outputDelta" => {
                adapter
                    .command_output_delta_notification(params)
                    .expect("command output should map through the supported adapter");
            }
            "item/completed" => {
                adapter
                    .item_completed_notification(params)
                    .expect("item/completed should map through the supported adapter");
            }
            "thread/tokenUsage/updated" => {
                adapter
                    .token_usage_updated_notification(params)
                    .expect("token usage should map through the supported adapter");
            }
            "error" => {
                adapter
                    .error_notification(params)
                    .expect("error should map through the supported adapter");
            }
            "turn/completed" => {
                adapter
                    .turn_completed_notification(params)
                    .expect("turn/completed should map through the supported adapter");
            }
            other => panic!("fixture notification lacks an adapter assertion: {other}"),
        }
    }

    let reverse = &fixture["reverseRequests"][0];
    let method = reverse["method"]
        .as_str()
        .expect("reverse-request method should be a string");
    let params = request_params_through_protocol(method, reverse["params"].clone());
    let stable = adapter
        .dynamic_tool_call_params(params)
        .expect("dynamic-tool request should map through the supported adapter");
    assert_eq!(stable.thread_id, "thread-contract-1");
    assert_eq!(stable.turn_id, "turn-contract-1");
    assert_eq!(stable.call_id, "call-contract-1");
    assert_eq!(stable.namespace, None);
    assert_eq!(stable.tool, "contract_tool");
    assert_eq!(stable.arguments, json!({"key": "value"}));
}

#[test]
fn fixture_notification_order_is_the_production_router_order() {
    let fixture = contract("0.146.0");
    let fixture_order: Vec<&str> = fixture["normalNotificationOrder"]
        .as_array()
        .expect("normalNotificationOrder should be an array")
        .iter()
        .map(|value| {
            value
                .as_str()
                .expect("notification method should be a string")
        })
        .collect();
    assert_eq!(fixture_order, NORMAL_NOTIFICATION_ORDER);
    let fixture_methods: std::collections::BTreeSet<&str> = fixture["notifications"]
        .as_array()
        .expect("notifications should be an array")
        .iter()
        .map(|entry| entry["method"].as_str().expect("method should be a string"))
        .collect();
    assert_eq!(
        fixture_methods,
        CONSUMED_NOTIFICATION_METHODS.iter().copied().collect()
    );
}

#[test]
fn turn_start_failure_contract_drives_client_retry_classification() {
    let fixture = contract("0.146.0");
    let confirmed_turn = WireAdapter::V0_146_0
        .turn_start_response(exchange(&fixture, "turn/start")["result"].clone())
        .expect("turn/start result should map through the supported adapter")
        .turn;

    for failure in fixture["failureCases"]
        .as_array()
        .expect("failureCases should be an array")
    {
        let source = failure["source"]
            .as_str()
            .expect("failure source should be a string");
        let error = match source {
            "serialize" => ClientError::Rpc(RpcError::Serialize {
                method: "turn/start",
            }),
            "payload_too_large" => ClientError::Rpc(RpcError::PayloadTooLarge {
                method: "turn/start",
            }),
            "request_id_exhausted" => ClientError::Rpc(RpcError::RequestIdExhausted),
            "server_error" => ClientError::Rpc(RpcError::Server {
                method: "turn/start",
                code: -32602,
            }),
            "timeout" => ClientError::Rpc(RpcError::Timeout {
                method: "turn/start",
            }),
            "connection_lost" => {
                ClientError::Rpc(RpcError::ConnectionLost(ConnectionEpoch::new(7)))
            }
            "confirmed_untracked" => ClientError::ConfirmedTurnUntracked {
                turn: Box::new(confirmed_turn.clone()),
            },
            other => panic!("fixture failure source lacks a ClientError assertion: {other}"),
        };
        let expected = match failure["expected"].as_str() {
            Some("definitely_not_applied") => true,
            Some("uncertain") => false,
            other => panic!("unknown turn/start failure expectation: {other:?}"),
        };
        assert_eq!(
            error.turn_start_definitely_not_applied(),
            expected,
            "turn/start classification drift for {source}"
        );
    }
}

#[test]
fn candidate_only_section_position_remains_unknown_in_the_stable_domain() {
    let supported_schema: Value = serde_json::from_str(include_str!(
        "../protocol/codex/schemas/0.146.0/selected.schema.json"
    ))
    .expect("supported schema should be JSON");
    let supported_sort_keys =
        supported_schema["roots"]["thread.list.params"]["definitions"]["ThreadSortKey"]["enum"]
            .as_array()
            .expect("supported ThreadSortKey should be an enum");
    assert!(!supported_sort_keys.contains(&json!("section_position")));

    let stable: types::ThreadSortKey = serde_json::from_value(json!("section_position"))
        .expect("stable open enum should retain candidate-only values");
    assert_eq!(
        stable,
        types::ThreadSortKey::Unknown("section_position".to_owned())
    );
    assert_eq!(
        serde_json::to_value(stable).expect("unknown sort key should remain serializable"),
        json!("section_position")
    );
    let mut params = types::ThreadListParams::default();
    params.sort_key = Some(types::ThreadSortKey::Unknown("section_position".to_owned()));
    assert!(
        WireAdapter::V0_146_0.thread_list_params(&params).is_err(),
        "candidate-only sort keys must not leak onto the supported 0.146 wire"
    );
}

#[test]
fn supported_adapter_rejects_outgoing_values_outside_the_0_146_schema() {
    let adapter = WireAdapter::V0_146_0;
    let mut start = types::ThreadStartParams::default();
    start.approval_policy = Some(types::ApprovalPolicy::Named("future-policy".to_owned()));
    assert!(adapter.thread_start_params(&start).is_err());

    let mut turn = types::TurnStartParams::new("thread-contract-1", vec![]);
    turn.summary = Some("future-summary".to_owned());
    assert!(adapter.turn_start_params(&turn).is_err());
}

#[test]
fn unknown_generated_enum_values_fail_soft_at_the_stable_boundary() {
    let generated: wire::v0_146_0::TurnStartedNotification = serde_json::from_value(json!({
        "threadId": "thread-contract-1",
        "turn": {
            "id": "turn-contract-1",
            "items": [],
            "status": "futureTerminalState"
        }
    }))
    .expect("open generated enum should accept an unknown value");
    let stable = compat::v0_146_0::turn_started_notification(generated)
        .expect("unknown status should map without failing");
    assert_eq!(
        stable.turn.status,
        types::TurnStatus::Unknown("futureTerminalState".to_owned())
    );
}

#[test]
fn compatibility_errors_never_echo_wire_payloads() {
    let sentinel = "DO_NOT_LOG_CONTRACT_PAYLOAD";
    let generated: wire::v0_146_0::ItemCompletedNotification = serde_json::from_value(json!({
        "threadId": "thread-contract-1",
        "turnId": "turn-contract-1",
        "completedAtMs": 1,
        "item": {
            "id": "item-contract-1",
            "type": "agentMessage",
            "text": {"secret": sentinel}
        }
    }))
    .expect("generated wire keeps the selected item payload opaque");
    let Err(error) = compat::v0_146_0::item_completed_notification(generated) else {
        panic!("malformed known item should fail stable mapping");
    };
    assert!(!format!("{error:?}").contains(sentinel));
    assert!(!error.to_string().contains(sentinel));
}

#[test]
fn only_reviewed_versions_are_enabled_at_runtime() {
    assert_eq!(wire::SUPPORTED_CODEX_VERSIONS, ["0.146.0"]);
    let supported = semver::Version::parse("0.146.0").unwrap();
    let candidate = semver::Version::parse("0.149.0").unwrap();
    assert!(wire::is_supported_codex_version(&supported));
    assert!(!wire::is_supported_codex_version(&candidate));
    assert_eq!(
        WireAdapter::for_version(&supported),
        Some(WireAdapter::V0_146_0)
    );
    assert_eq!(WireAdapter::V0_146_0.codex_version(), "0.146.0");
    assert_eq!(WireAdapter::for_version(&candidate), None);
}
