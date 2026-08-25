use lark_codex_bridge::codex::types::{
    DynamicToolCallOutputContentItem, DynamicToolCallParams, DynamicToolCallResponse,
    DynamicToolFunctionSpec, DynamicToolNamespaceSpec, DynamicToolNamespaceTool, DynamicToolSpec,
    InitializeCapabilities, ThreadStartParams,
};
use serde_json::json;

fn function(name: &str, defer_loading: bool) -> DynamicToolFunctionSpec {
    DynamicToolFunctionSpec {
        name: name.to_owned(),
        description: format!("Call {name}"),
        input_schema: json!({
            "type": "object",
            "properties": {"ctx_id": {"type": "string"}},
            "required": ["ctx_id"]
        }),
        defer_loading,
    }
}

#[test]
fn thread_start_serializes_function_and_namespace_specs_like_codex_0_146_and_0_147() {
    let params = ThreadStartParams {
        dynamic_tools: Some(vec![
            DynamicToolSpec::Function(function("resolve_context", false)),
            DynamicToolSpec::Namespace(DynamicToolNamespaceSpec {
                name: "lark".to_owned(),
                description: "Lark bridge context".to_owned(),
                tools: vec![DynamicToolNamespaceTool::Function(function(
                    "fetch_resource",
                    true,
                ))],
            }),
        ]),
        ..ThreadStartParams::default()
    };

    assert_eq!(
        serde_json::to_value(&params).expect("thread params should serialize"),
        json!({
            "dynamicTools": [
                {
                    "type": "function",
                    "name": "resolve_context",
                    "description": "Call resolve_context",
                    "inputSchema": {
                        "type": "object",
                        "properties": {"ctx_id": {"type": "string"}},
                        "required": ["ctx_id"]
                    }
                },
                {
                    "type": "namespace",
                    "name": "lark",
                    "description": "Lark bridge context",
                    "tools": [{
                        "type": "function",
                        "name": "fetch_resource",
                        "description": "Call fetch_resource",
                        "inputSchema": {
                            "type": "object",
                            "properties": {"ctx_id": {"type": "string"}},
                            "required": ["ctx_id"]
                        },
                        "deferLoading": true
                    }]
                }
            ]
        })
    );
}

#[test]
fn dynamic_tool_specs_round_trip_from_wire_shape() {
    let wire = json!([
        {
            "type": "function",
            "name": "resolve_context",
            "description": "Resolve context",
            "inputSchema": true
        },
        {
            "type": "namespace",
            "name": "lark",
            "description": "Lark context",
            "tools": [{
                "type": "function",
                "name": "fetch_resource",
                "description": "Fetch resource",
                "inputSchema": {"type": "object"}
            }]
        }
    ]);

    let specs: Vec<DynamicToolSpec> =
        serde_json::from_value(wire.clone()).expect("0.147 tool specs should decode");
    assert_eq!(
        serde_json::to_value(specs).expect("tool specs should re-encode"),
        wire
    );
}

#[test]
fn item_tool_call_params_use_required_nullable_namespace() {
    let wire = json!({
        "threadId": "thread-1",
        "turnId": "turn-1",
        "callId": "call-1",
        "namespace": null,
        "tool": "resolve_context",
        "arguments": {"ctx_id": "ctx-1", "include": ["sender", "quote"]}
    });

    let params: DynamicToolCallParams =
        serde_json::from_value(wire.clone()).expect("item/tool/call params should decode");
    assert_eq!(params.thread_id, "thread-1");
    assert_eq!(params.namespace, None);
    assert_eq!(params.arguments["ctx_id"], "ctx-1");
    assert_eq!(
        serde_json::to_value(params).expect("item/tool/call params should re-encode"),
        wire
    );

    let missing_namespace = json!({
        "threadId": "thread-1",
        "turnId": "turn-1",
        "callId": "call-1",
        "tool": "resolve_context",
        "arguments": {}
    });
    assert!(
        serde_json::from_value::<DynamicToolCallParams>(missing_namespace).is_err(),
        "namespace is required even though its value may be null"
    );
}

#[test]
fn dynamic_tool_response_supports_all_0_147_content_items() {
    let response = DynamicToolCallResponse {
        content_items: vec![
            DynamicToolCallOutputContentItem::InputText {
                text: "resolved".to_owned(),
            },
            DynamicToolCallOutputContentItem::InputImage {
                image_url: "data:image/png;base64,AA==".to_owned(),
            },
            DynamicToolCallOutputContentItem::InputAudio {
                audio_url: "data:audio/ogg;base64,AA==".to_owned(),
            },
        ],
        success: true,
    };

    let wire = json!({
        "contentItems": [
            {"type": "inputText", "text": "resolved"},
            {"type": "inputImage", "imageUrl": "data:image/png;base64,AA=="},
            {"type": "inputAudio", "audioUrl": "data:audio/ogg;base64,AA=="}
        ],
        "success": true
    });
    assert_eq!(
        serde_json::to_value(&response).expect("dynamic tool response should serialize"),
        wire
    );

    let decoded: DynamicToolCallResponse =
        serde_json::from_value(wire).expect("dynamic tool response should decode");
    assert!(decoded == response);
}

#[test]
fn default_thread_start_does_not_emit_experimental_dynamic_tools_field() {
    let wire = serde_json::to_value(ThreadStartParams::default())
        .expect("default thread params should serialize");
    assert_eq!(wire, json!({}));
}

#[test]
fn experimental_api_capability_is_explicit_and_omitted_by_default() {
    assert_eq!(
        serde_json::to_value(InitializeCapabilities::default())
            .expect("default capabilities should serialize"),
        json!({})
    );

    let capabilities = InitializeCapabilities {
        experimental_api: Some(true),
        ..InitializeCapabilities::default()
    };
    assert_eq!(
        serde_json::to_value(capabilities).expect("experimental capability should serialize"),
        json!({"experimentalApi": true})
    );
}
