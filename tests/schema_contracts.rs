use lark_codex_bridge::codex::{compat, types, wire};
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
    assert!(wire::is_supported_codex_version(
        &semver::Version::parse("0.146.0").unwrap()
    ));
    assert!(!wire::is_supported_codex_version(
        &semver::Version::parse("0.149.0").unwrap()
    ));
}
