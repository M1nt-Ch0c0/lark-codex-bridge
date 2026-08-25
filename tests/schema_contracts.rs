use lark_codex_bridge::codex::{
    client::{CONSUMED_NOTIFICATION_METHODS, ClientError, NORMAL_NOTIFICATION_ORDER},
    compat::{self, SharedWireProfile, WireAdapter},
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
        let initialize_params =
            serde_json::from_value::<wire::$wire::InitializeParams>(initialize["params"].clone())
            .expect("initialize params should decode as generated wire");
        assert_eq!(
            initialize["params"]["clientInfo"]["version"].as_str(),
            Some(env!("CARGO_PKG_VERSION")),
            "contract client version must track the package version"
        );
        assert_eq!(
            serde_json::to_value(initialize_params).expect("initialize params should re-encode"),
            initialize["params"]
        );
        let response =
            serde_json::from_value::<wire::$wire::InitializeResponse>(initialize["result"].clone())
                .expect("initialize response should decode as generated wire");
        let stable = compat::$compat::initialize_response(response)
            .expect("initialize response should map to stable domain");
        assert_eq!(stable.platform_family, "unix");
        assert_eq!(stable.platform_os, "linux");

        let start = exchange(&fixture, "thread/start");
        serde_json::from_value::<wire::$wire::ThreadStartParams>(start["params"].clone())
            .expect("thread/start params should decode as generated wire");
        let response =
            serde_json::from_value::<wire::$wire::ThreadStartResponse>(start["result"].clone())
                .expect("thread/start response should decode as generated wire");
        let stable = compat::$compat::thread_start_response(response)
            .expect("thread/start response should map to stable domain");
        assert_eq!(stable.thread.id, "thread-contract-1");
        assert_eq!(stable.thread.cli_version, $version);
        assert_eq!(stable.approvals_reviewer, "user");

        let list = exchange(&fixture, "thread/list");
        serde_json::from_value::<wire::$wire::ThreadListParams>(list["params"].clone())
            .expect("thread/list params should decode as generated wire");
        let response =
            serde_json::from_value::<wire::$wire::ThreadListResponse>(list["result"].clone())
                .expect("thread/list response should decode as generated wire");
        let stable = compat::$compat::thread_list_response(response)
            .expect("thread/list response should map to stable domain");
        assert_eq!(stable.data.len(), 1);
        assert_eq!(stable.data[0].id, "thread-contract-1");
        assert_eq!(stable.next_cursor, None);

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
        let stable = compat::$compat::thread_resume_response(response)
            .expect("thread/resume response should map to stable domain");
        assert_eq!(stable.thread.id, "thread-contract-1");
        assert_eq!(stable.model, "gpt-5.6");

        let turn_start = exchange(&fixture, "turn/start");
        serde_json::from_value::<wire::$wire::TurnStartParams>(turn_start["params"].clone())
            .expect("turn/start params should decode as generated wire");
        let response =
            serde_json::from_value::<wire::$wire::TurnStartResponse>(turn_start["result"].clone())
                .expect("turn/start response should decode as generated wire");
        let stable = compat::$compat::turn_start_response(response)
            .expect("turn/start response should map to stable domain");
        assert_eq!(stable.turn.id, "turn-contract-1");
        assert_eq!(stable.turn.status, types::TurnStatus::InProgress);

        let interrupt = exchange(&fixture, "turn/interrupt");
        serde_json::from_value::<wire::$wire::TurnInterruptParams>(interrupt["params"].clone())
            .expect("turn/interrupt params should decode as generated wire");
        let response = serde_json::from_value::<wire::$wire::TurnInterruptResponse>(
            interrupt["result"].clone(),
        )
        .expect("turn/interrupt response should decode as generated wire");
        let stable = compat::$compat::turn_interrupt_response(response)
            .expect("turn/interrupt response should map to stable domain");
        assert_eq!(
            serde_json::to_value(stable).expect("interrupt result should re-encode"),
            json!({})
        );

        let started = notification(&fixture, "thread/started");
        let params = serde_json::from_value::<wire::$wire::ThreadStartedNotification>(
            started["params"].clone(),
        )
        .expect("thread/started should decode as generated wire");
        let stable = compat::$compat::thread_started_notification(params)
            .expect("thread/started should map to stable domain");
        assert_eq!(stable.thread.id, "thread-contract-1");
        assert_eq!(stable.thread.cli_version, $version);

        let turn_started = notification(&fixture, "turn/started");
        let params = serde_json::from_value::<wire::$wire::TurnStartedNotification>(
            turn_started["params"].clone(),
        )
        .expect("turn/started should decode as generated wire");
        let stable = compat::$compat::turn_started_notification(params)
            .expect("turn/started should map to stable domain");
        assert_eq!(stable.thread_id, "thread-contract-1");
        assert_eq!(stable.turn.id, "turn-contract-1");

        let item_started = notification(&fixture, "item/started");
        let params = serde_json::from_value::<wire::$wire::ItemStartedNotification>(
            item_started["params"].clone(),
        )
        .expect("item/started should decode as generated wire");
        let stable = compat::$compat::item_started_notification(params)
            .expect("item/started should map to stable domain");
        assert_eq!(stable.turn_id, "turn-contract-1");
        assert_eq!(stable.started_at_ms, 1_786_478_401_000);
        assert_eq!(stable.item.kind(), "agentMessage");

        let delta = notification(&fixture, "item/agentMessage/delta");
        let params = serde_json::from_value::<wire::$wire::AgentMessageDeltaNotification>(
            delta["params"].clone(),
        )
        .expect("agent delta should decode as generated wire");
        let stable = compat::$compat::agent_message_delta_notification(params)
            .expect("agent delta should map to stable domain");
        assert_eq!(stable.item_id, "item-contract-1");
        assert_eq!(stable.delta, "contract delta");

        let output = notification(&fixture, "item/commandExecution/outputDelta");
        let params =
            serde_json::from_value::<wire::$wire::CommandExecutionOutputDeltaNotification>(
                output["params"].clone(),
            )
            .expect("command output should decode as generated wire");
        let stable = compat::$compat::command_output_delta_notification(params)
            .expect("command output should map to stable domain");
        assert_eq!(stable.item_id, "command-contract-1");
        assert_eq!(stable.delta, "bounded output");

        let completed = notification(&fixture, "item/completed");
        let params = serde_json::from_value::<wire::$wire::ItemCompletedNotification>(
            completed["params"].clone(),
        )
        .expect("item/completed should decode as generated wire");
        let stable = compat::$compat::item_completed_notification(params)
            .expect("item/completed should map to stable domain");
        assert_eq!(stable.completed_at_ms, 1_786_478_402_000);
        assert_eq!(stable.item.kind(), "agentMessage");
        let types::ThreadItem::AgentMessage { text, phase, .. } = stable.item else {
            panic!("fixture item should stay an agent message")
        };
        assert_eq!(text, "contract output");
        assert_eq!(phase, Some(types::MessagePhase::FinalAnswer));

        let usage = notification(&fixture, "thread/tokenUsage/updated");
        let params = serde_json::from_value::<wire::$wire::ThreadTokenUsageUpdatedNotification>(
            usage["params"].clone(),
        )
        .expect("token usage should decode as generated wire");
        let stable = compat::$compat::token_usage_updated_notification(params)
            .expect("token usage should map to stable domain");
        assert_eq!(stable.turn_id, "turn-contract-1");
        assert_eq!(stable.token_usage.total.total_tokens, 16);
        assert_eq!(stable.token_usage.model_context_window, Some(100_000));

        let error = notification(&fixture, "error");
        let params =
            serde_json::from_value::<wire::$wire::ErrorNotification>(error["params"].clone())
                .expect("error should decode as generated wire");
        let stable = compat::$compat::error_notification(params)
            .expect("error notification should map to stable domain");
        assert_eq!(stable.turn_id, "turn-contract-1");
        assert!(!stable.will_retry);

        let completed = notification(&fixture, "turn/completed");
        let params = serde_json::from_value::<wire::$wire::TurnCompletedNotification>(
            completed["params"].clone(),
        )
        .expect("turn/completed should decode as generated wire");
        let stable = compat::$compat::turn_completed_notification(params)
            .expect("turn/completed should map to stable domain");
        assert_eq!(stable.thread_id, "thread-contract-1");
        assert_eq!(stable.turn.id, "turn-contract-1");
        assert_eq!(stable.turn.status, types::TurnStatus::Completed);

        let reverse = &fixture["reverseRequests"][0];
        let params =
            serde_json::from_value::<wire::$wire::DynamicToolCallParams>(reverse["params"].clone())
                .expect("dynamic tool call should decode as generated wire");
        let stable = compat::$compat::dynamic_tool_call_params(params)
            .expect("dynamic tool call should map to stable domain");
        assert_eq!(stable.call_id, "call-contract-1");
        assert_eq!(stable.namespace, None);
        assert_eq!(stable.arguments, json!({"key": "value"}));
        let stable: types::DynamicToolCallResponse =
            serde_json::from_value(reverse["result"].clone()).expect("stable tool response");
        let mapped = compat::$compat::dynamic_tool_call_response(&stable)
            .expect("dynamic tool response should map to generated wire");
        assert_eq!(
            serde_json::to_value(mapped).expect("dynamic tool response should re-encode"),
            reverse["result"]
        );
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
fn promoted_adapter_serializes_every_base_outgoing_contract_exactly() {
    let fixture = contract("0.149.0");
    let adapter = WireAdapter::V0_149_0;

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

    let reverse = fixture["reverseRequests"]
        .as_array()
        .expect("reverse requests")
        .iter()
        .find(|entry| entry["method"] == "item/tool/call")
        .expect("dynamic tool fixture");
    let stable: types::DynamicToolCallResponse = serde_json::from_value(reverse["result"].clone())
        .expect("contract result should decode into the stable tool response");
    let mapped = adapter
        .dynamic_tool_call_response(&stable)
        .expect("promoted adapter should encode the dynamic-tool response");
    assert_eq!(mapped, reverse["result"]);
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
    for version in ["0.146.0", "0.149.0"] {
        let fixture = contract(version);
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
        assert_eq!(fixture_order, NORMAL_NOTIFICATION_ORDER, "{version}");
        let fixture_methods: std::collections::BTreeSet<&str> = fixture["notifications"]
            .as_array()
            .expect("notifications should be an array")
            .iter()
            .map(|entry| entry["method"].as_str().expect("method should be a string"))
            .collect();
        let mut expected_methods: std::collections::BTreeSet<&str> =
            CONSUMED_NOTIFICATION_METHODS.iter().copied().collect();
        if version == "0.149.0" {
            expected_methods.extend(
                SharedWireProfile::QueueShared
                    .required_notifications()
                    .iter()
                    .copied(),
            );
        }
        assert_eq!(fixture_methods, expected_methods, "{version}");
    }
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
fn promoted_section_position_remains_rejected_by_the_older_adapter() {
    let supported_schema: Value = serde_json::from_str(include_str!(
        "../protocol/codex/schemas/0.146.0/selected.schema.json"
    ))
    .expect("supported schema should be JSON");
    let supported_sort_keys =
        supported_schema["roots"]["thread.list.params"]["definitions"]["ThreadSortKey"]["enum"]
            .as_array()
            .expect("supported ThreadSortKey should be an enum");
    assert!(!supported_sort_keys.contains(&json!("section_position")));

    let stable: types::ThreadSortKey =
        serde_json::from_value(json!("section_position")).expect("promoted sort key should decode");
    assert_eq!(stable, types::ThreadSortKey::SectionPosition);
    assert_eq!(
        serde_json::to_value(stable).expect("unknown sort key should remain serializable"),
        json!("section_position")
    );
    let params = types::ThreadListParams {
        sort_key: Some(types::ThreadSortKey::SectionPosition),
        ..types::ThreadListParams::default()
    };
    assert!(
        WireAdapter::V0_146_0.thread_list_params(&params).is_err(),
        "candidate-only sort keys must not leak onto the supported 0.146 wire"
    );
    assert!(WireAdapter::V0_149_0.thread_list_params(&params).is_ok());
}

#[test]
fn supported_adapter_rejects_outgoing_values_outside_the_0_146_schema() {
    let adapter = WireAdapter::V0_146_0;
    let start = types::ThreadStartParams {
        approval_policy: Some(types::ApprovalPolicy::Named("future-policy".to_owned())),
        ..types::ThreadStartParams::default()
    };
    assert!(adapter.thread_start_params(&start).is_err());

    let start = types::ThreadStartParams {
        project_id: Some("project-contract-1".to_owned()),
        ..types::ThreadStartParams::default()
    };
    assert!(adapter.thread_start_params(&start).is_err());
    assert_eq!(
        WireAdapter::V0_149_0
            .thread_start_params(&start)
            .expect("0.149 should retain its promoted project id")["projectId"],
        "project-contract-1"
    );

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

    let generated: wire::v0_146_0::ItemCompletedNotification = serde_json::from_value(json!({
        "threadId": "thread-contract-1",
        "turnId": "turn-contract-1",
        "completedAtMs": 1,
        "item": {
            "id": "item-contract-1",
            "type": "agentMessage",
            "text": "future phase",
            "phase": "future_phase"
        }
    }))
    .expect("generated item should retain an unknown message phase");
    let stable = compat::v0_146_0::item_completed_notification(generated)
        .expect("unknown message phase should map without failing");
    let types::ThreadItem::AgentMessage { phase, .. } = stable.item else {
        panic!("unknown-phase fixture should remain an agent message")
    };
    assert_eq!(
        phase,
        Some(types::MessagePhase::Unknown("future_phase".to_owned()))
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
    assert_eq!(wire::SUPPORTED_CODEX_VERSIONS, ["0.146.0", "0.149.0"]);
    let baseline = semver::Version::parse("0.146.0").unwrap();
    let promoted = semver::Version::parse("0.149.0").unwrap();
    let unreviewed = semver::Version::parse("0.150.0").unwrap();
    assert!(wire::is_supported_codex_version(&baseline));
    assert!(wire::is_supported_codex_version(&promoted));
    assert!(!wire::is_supported_codex_version(&unreviewed));
    assert_eq!(
        WireAdapter::for_version(&baseline),
        Some(WireAdapter::V0_146_0)
    );
    assert_eq!(WireAdapter::V0_146_0.codex_version(), "0.146.0");
    assert_eq!(
        WireAdapter::for_version(&promoted),
        Some(WireAdapter::V0_149_0)
    );
    assert_eq!(WireAdapter::V0_149_0.codex_version(), "0.149.0");
    assert_eq!(WireAdapter::for_version(&unreviewed), None);
    for profile in [
        SharedWireProfile::ObserveShared,
        SharedWireProfile::ResumeShared,
        SharedWireProfile::MutateShared,
        SharedWireProfile::QueueShared,
    ] {
        assert!(WireAdapter::V0_149_0.supports_shared_profile(profile));
        assert!(!WireAdapter::V0_146_0.supports_shared_profile(profile));
        assert!(!profile.required_methods().is_empty());
    }
}

#[test]
fn promoted_shared_surface_maps_every_selected_exchange() {
    let fixture = contract("0.149.0");
    let adapter = WireAdapter::V0_149_0;

    macro_rules! outgoing {
        ($method:literal, $stable:ty, $mapper:ident) => {{
            let exchange = exchange(&fixture, $method);
            let stable: $stable = serde_json::from_value(exchange["params"].clone())
                .expect("shared contract params should decode into the stable type");
            assert_eq!(
                adapter.$mapper(&stable).expect("promoted shared mapper"),
                exchange["params"],
                "shared outgoing drift for {}",
                $method
            );
        }};
    }

    macro_rules! incoming {
        ($method:literal, $mapper:ident) => {{
            let exchange = exchange(&fixture, $method);
            let stable = adapter
                .$mapper(exchange["result"].clone())
                .expect("promoted shared response mapper");
            serde_json::to_value(&stable).expect("stable shared response should encode");
            stable
        }};
    }

    outgoing!(
        "thread/unsubscribe",
        types::ThreadUnsubscribeParams,
        thread_unsubscribe_params
    );
    incoming!("thread/unsubscribe", thread_unsubscribe_response);
    outgoing!("turn/steer", types::TurnSteerParams, turn_steer_params);
    incoming!("turn/steer", turn_steer_response);
    outgoing!(
        "thread/queue/add",
        types::ThreadQueueAddParams,
        thread_queue_add_params
    );
    incoming!("thread/queue/add", thread_queue_add_response);
    outgoing!(
        "thread/queue/list",
        types::ThreadQueueListParams,
        thread_queue_list_params
    );
    incoming!("thread/queue/list", thread_queue_list_response);
    outgoing!(
        "thread/queue/start",
        types::ThreadQueueStartParams,
        thread_queue_start_params
    );
    incoming!("thread/queue/start", thread_queue_start_response);
    outgoing!(
        "thread/turns/list",
        types::ThreadTurnsListParams,
        thread_turns_list_params
    );
    incoming!("thread/turns/list", thread_turns_list_response);
    outgoing!(
        "thread/items/list",
        types::ThreadItemsListParams,
        thread_items_list_params
    );
    let items = incoming!("thread/items/list", thread_items_list_response);
    assert_eq!(items.data.len(), 1);
    assert_eq!(items.data[0].turn_id, "turn-contract-1");
    assert_eq!(items.data[0].item.kind(), "agentMessage");
}

#[test]
fn promoted_shared_notifications_and_approvals_cross_the_stable_boundary() {
    let fixture = contract("0.149.0");
    let adapter = WireAdapter::V0_149_0;

    let status = notification(&fixture, "thread/status/changed");
    assert_eq!(
        adapter
            .thread_status_changed_notification(status["params"].clone())
            .expect("status notification")
            .thread_id,
        "thread-contract-1"
    );
    let queue = notification(&fixture, "thread/queue/changed");
    assert_eq!(
        adapter
            .thread_queue_changed_notification(queue["params"].clone())
            .expect("queue notification")
            .thread_id,
        "thread-contract-1"
    );
    let resolved = notification(&fixture, "serverRequest/resolved");
    assert_eq!(
        adapter
            .server_request_resolved_notification(resolved["params"].clone())
            .expect("resolved notification")
            .request_id,
        json!("approval-contract-1")
    );

    let reverse = fixture["reverseRequests"]
        .as_array()
        .expect("reverse requests");
    let command = reverse
        .iter()
        .find(|entry| entry["method"] == "item/commandExecution/requestApproval")
        .expect("command approval fixture");
    assert_eq!(
        adapter
            .command_execution_request_approval_params(command["params"].clone())
            .expect("command approval params")
            .item_id,
        "command-contract-1"
    );
    let command_result: types::CommandExecutionRequestApprovalResult =
        serde_json::from_value(command["result"].clone()).expect("command decision");
    assert_eq!(
        adapter
            .command_execution_request_approval_response(&command_result)
            .expect("command approval response"),
        command["result"]
    );

    let file = reverse
        .iter()
        .find(|entry| entry["method"] == "item/fileChange/requestApproval")
        .expect("file approval fixture");
    assert_eq!(
        adapter
            .file_change_request_approval_params(file["params"].clone())
            .expect("file approval params")
            .item_id,
        "file-change-contract-1"
    );
    let file_result: types::FileChangeRequestApprovalResult =
        serde_json::from_value(file["result"].clone()).expect("file decision");
    assert_eq!(
        adapter
            .file_change_request_approval_response(&file_result)
            .expect("file approval response"),
        file["result"]
    );

    let permissions = reverse
        .iter()
        .find(|entry| entry["method"] == "item/permissions/requestApproval")
        .expect("permissions approval fixture");
    assert_eq!(
        adapter
            .permissions_request_approval_params(permissions["params"].clone())
            .expect("permissions approval params")
            .item_id,
        "permissions-contract-1"
    );
    let permissions_result: types::PermissionsRequestApprovalResult =
        serde_json::from_value(permissions["result"].clone()).expect("permission grant");
    assert_eq!(
        adapter
            .permissions_request_approval_response(&permissions_result)
            .expect("permissions approval response"),
        permissions["result"]
    );
}

#[test]
fn promoted_approval_variants_encode_with_the_exact_schema_keys() {
    let adapter = WireAdapter::V0_149_0;
    let execpolicy = types::CommandExecutionRequestApprovalResult {
        decision: types::CommandExecutionApprovalDecision::AcceptWithExecpolicyAmendment {
            amendment: types::ExecpolicyAmendment {
                execpolicy_amendment: vec!["allow prefix".to_owned()],
            },
        },
    };
    assert_eq!(
        adapter
            .command_execution_request_approval_response(&execpolicy)
            .expect("execpolicy amendment should encode"),
        json!({
            "decision": {
                "acceptWithExecpolicyAmendment": {
                    "execpolicy_amendment": ["allow prefix"]
                }
            }
        })
    );

    let network = types::CommandExecutionRequestApprovalResult {
        decision: types::CommandExecutionApprovalDecision::ApplyNetworkPolicyAmendment {
            amendment: types::NetworkPolicyAmendmentEnvelope {
                network_policy_amendment: types::NetworkPolicyAmendment {
                    action: types::NetworkPolicyAction::Allow,
                    host: "example.invalid".to_owned(),
                },
            },
        },
    };
    assert_eq!(
        adapter
            .command_execution_request_approval_response(&network)
            .expect("network amendment should encode"),
        json!({
            "decision": {
                "applyNetworkPolicyAmendment": {
                    "network_policy_amendment": {
                        "action": "allow",
                        "host": "example.invalid"
                    }
                }
            }
        })
    );
}

#[test]
fn promoted_shared_unknown_values_are_preserved_or_rejected_by_policy() {
    let adapter = WireAdapter::V0_149_0;
    let unsubscribe = adapter
        .thread_unsubscribe_response(json!({"status": "futureUnsubscribeState"}))
        .expect("open unsubscribe state should map");
    assert_eq!(
        unsubscribe.status,
        types::ThreadUnsubscribeStatus::Unknown("futureUnsubscribeState".to_owned())
    );

    let status = adapter
        .thread_status_changed_notification(json!({
            "threadId": "thread-contract-1",
            "status": {"type": "futureActiveState", "opaque": true}
        }))
        .expect("unknown thread state should remain opaque");
    assert_eq!(status.status["type"], "futureActiveState");

    assert!(
        serde_json::from_value::<types::CommandExecutionRequestApprovalResult>(
            json!({"decision": "futureDecision"})
        )
        .is_err()
    );
    assert!(
        serde_json::from_value::<types::FileChangeRequestApprovalResult>(
            json!({"decision": "futureDecision"})
        )
        .is_err()
    );
    assert!(
        serde_json::from_value::<types::PermissionsRequestApprovalResult>(json!({
            "permissions": {},
            "scope": "futureScope"
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<types::PermissionsRequestApprovalResult>(json!({
            "permissions": "not-an-object"
        }))
        .is_err()
    );

    for params in [
        json!({"threadId": "thread-contract-1", "sortDirection": "sideways"}),
        json!({
            "threadId": "thread-contract-1",
            "turnId": "turn-contract-1",
            "sortDirection": "sideways"
        }),
    ] {
        if params.get("turnId").is_some() {
            let stable: types::ThreadItemsListParams =
                serde_json::from_value(params).expect("stable open sort direction");
            assert!(adapter.thread_items_list_params(&stable).is_err());
        } else {
            let stable: types::ThreadTurnsListParams =
                serde_json::from_value(params).expect("stable open sort direction");
            assert!(adapter.thread_turns_list_params(&stable).is_err());
        }
    }

    assert!(
        serde_json::from_value::<types::TurnSteerParams>(json!({
            "threadId": "thread-contract-1",
            "expectedTurnId": "turn-contract-1",
            "input": [],
            "additionalContext": {
                "source": {"kind": "futureKind", "value": "opaque"}
            }
        }))
        .is_err()
    );

    assert!(
        serde_json::from_value::<wire::v0_149_0::TurnSteerParams>(json!({
            "threadId": "thread-contract-1",
            "input": []
        }))
        .is_err(),
        "the exact expectedTurnId precondition is required"
    );
    assert!(
        WireAdapter::V0_146_0
            .thread_unsubscribe_response(json!({"status": "unsubscribed"}))
            .is_err(),
        "the baseline adapter must reject a shape it never promoted"
    );
}

#[test]
fn shared_profile_catalogs_are_covered_by_the_promoted_contract() {
    let fixture = contract("0.149.0");
    let methods: std::collections::BTreeSet<&str> = fixture["exchanges"]
        .as_array()
        .expect("exchanges")
        .iter()
        .filter_map(|entry| entry["method"].as_str())
        .collect();
    let notifications: std::collections::BTreeSet<&str> = fixture["notifications"]
        .as_array()
        .expect("notifications")
        .iter()
        .filter_map(|entry| entry["method"].as_str())
        .collect();
    let reverse: std::collections::BTreeSet<&str> = fixture["reverseRequests"]
        .as_array()
        .expect("reverse requests")
        .iter()
        .filter_map(|entry| entry["method"].as_str())
        .collect();

    for profile in [
        SharedWireProfile::ObserveShared,
        SharedWireProfile::ResumeShared,
        SharedWireProfile::MutateShared,
        SharedWireProfile::QueueShared,
    ] {
        assert!(
            profile
                .required_methods()
                .iter()
                .all(|method| methods.contains(method))
        );
        assert!(
            profile
                .required_notifications()
                .iter()
                .all(|method| notifications.contains(method))
        );
        assert!(
            profile
                .required_reverse_requests()
                .iter()
                .all(|method| reverse.contains(method))
        );
    }
}
