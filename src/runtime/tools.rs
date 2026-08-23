//! Codex dynamic tools exposing turn-scoped Lark context and media.

use serde::Deserialize;
use serde_json::{Value, json};

use crate::codex::{
    client::AppServerClient,
    rpc::ServerRequest,
    types::{
        DynamicToolCallOutputContentItem, DynamicToolCallParams, DynamicToolCallResponse,
        DynamicToolFunctionSpec, DynamicToolNamespaceSpec, DynamicToolNamespaceTool,
        DynamicToolSpec,
    },
};
use crate::lark::api::ResourceKind;
use crate::runtime::{
    attachments::{AttachError, AttachmentCache, DownloadKind},
    context::{ContextError, ContextErrorCode, ContextId, ContextRegistry, MediaHandle},
};

/// Version persisted with Codex threads that were created with these tools.
pub const CONTEXT_TOOLS_VERSION: u32 = 1;

/// Tool declarations installed on every context-enabled Codex thread.
#[must_use]
pub fn bridge_dynamic_tools() -> Vec<DynamicToolSpec> {
    vec![
        DynamicToolSpec::Namespace(DynamicToolNamespaceSpec {
            name: "bridge_context".to_owned(),
            description: "Resolve metadata and typed content for the current Lark message."
                .to_owned(),
            tools: vec![DynamicToolNamespaceTool::Function(
                DynamicToolFunctionSpec {
                    name: "resolve".to_owned(),
                    description: "Resolve one opaque bridge_context id for this turn.".to_owned(),
                    input_schema: json!({
                        "type": "object",
                        "properties": {"id": {"type": "string"}},
                        "required": ["id"],
                        "additionalProperties": false
                    }),
                    defer_loading: false,
                },
            )],
        }),
        DynamicToolSpec::Namespace(DynamicToolNamespaceSpec {
            name: "bridge_media".to_owned(),
            description: "Fetch media authorized by a resolved bridge context.".to_owned(),
            tools: vec![DynamicToolNamespaceTool::Function(
                DynamicToolFunctionSpec {
                    name: "read".to_owned(),
                    description: "Download one opaque media handle into the bounded cache."
                        .to_owned(),
                    input_schema: json!({
                        "type": "object",
                        "properties": {
                            "context_id": {"type": "string"},
                            "handle": {"type": "string"}
                        },
                        "required": ["context_id", "handle"],
                        "additionalProperties": false
                    }),
                    defer_loading: false,
                },
            )],
        }),
    ]
}

#[derive(Deserialize)]
struct ResolveArguments {
    id: String,
}

#[derive(Deserialize)]
struct MediaArguments {
    context_id: String,
    handle: String,
}

/// Answers one app-server reverse request owned by the context-tool stream.
/// Unknown methods are rejected explicitly so no request lease can hang.
pub async fn handle_server_request(
    client: &AppServerClient,
    mut request: ServerRequest,
    contexts: &ContextRegistry,
    attachments: &AttachmentCache,
) {
    if request.method != "item/tool/call" {
        let _ = client
            .respond_request_error(&mut request, -32_601, "unsupported server request")
            .await;
        return;
    }

    let Ok(params) =
        request.params.clone().ok_or(()).and_then(|value| {
            serde_json::from_value::<DynamicToolCallParams>(value).map_err(|_| ())
        })
    else {
        respond_tool_error(
            client,
            &mut request,
            &tool_error(
                "invalid_request",
                "dynamic tool parameters are invalid",
                false,
            ),
        )
        .await;
        return;
    };

    let result = match (params.namespace.as_deref(), params.tool.as_str()) {
        (Some("bridge_context"), "resolve") => resolve_context(contexts, &params),
        (Some("bridge_media"), "read") => read_media(contexts, attachments, &params).await,
        _ => Err(tool_error(
            "unsupported",
            "dynamic tool is not registered by this bridge",
            false,
        )),
    };

    let response = match result {
        Ok(value) => tool_response(&value, true),
        Err(value) => tool_response(&value, false),
    };
    let _ = client.respond_request(&mut request, &response).await;
}

fn resolve_context(
    contexts: &ContextRegistry,
    params: &DynamicToolCallParams,
) -> Result<Value, Value> {
    let arguments =
        serde_json::from_value::<ResolveArguments>(params.arguments.clone()).map_err(|_| {
            tool_error(
                "invalid_request",
                "bridge_context.resolve arguments are invalid",
                false,
            )
        })?;
    let context_id = ContextId::from_external(arguments.id);
    contexts
        .resolve_for_tool(&context_id, &params.thread_id, &params.turn_id)
        .and_then(|snapshot| {
            serde_json::to_value(snapshot).map_err(|_| ContextError {
                code: ContextErrorCode::InvalidRequest,
                message: "context response serialization failed",
                retryable: false,
            })
        })
        .map_err(|error| context_error(&error))
}

async fn read_media(
    contexts: &ContextRegistry,
    attachments: &AttachmentCache,
    params: &DynamicToolCallParams,
) -> Result<Value, Value> {
    let arguments =
        serde_json::from_value::<MediaArguments>(params.arguments.clone()).map_err(|_| {
            tool_error(
                "invalid_request",
                "bridge_media.read arguments are invalid",
                false,
            )
        })?;
    let context_id = ContextId::from_external(arguments.context_id);
    let handle = MediaHandle::from_external(arguments.handle);
    let authorized = contexts
        .authorize_media_for_tool(&context_id, &handle, &params.thread_id, &params.turn_id)
        .map_err(|error| context_error(&error))?;
    let cached = attachments
        .fetch(
            &authorized.message_id,
            &authorized.resource,
            authorized.local_turn_row_id,
        )
        .await
        .map_err(|error| attachment_error(&error))?;
    let path = cached.path.to_str().ok_or_else(|| {
        tool_error(
            "media_unavailable",
            "cached media path is not representable",
            false,
        )
    })?;
    Ok(json!({
        "media": {
            "kind": resource_kind(cached.kind),
            "semanticKind": authorized.media_kind,
            "path": path,
            "sha256": cached.sha256,
            "bytes": cached.bytes,
        }
    }))
}

fn resource_kind(kind: ResourceKind) -> &'static str {
    match kind {
        ResourceKind::Image => "image",
        ResourceKind::File => "file",
    }
}

fn context_error(error: &ContextError) -> Value {
    json!({"error": error})
}

fn attachment_error(error: &AttachError) -> Value {
    let retryable = matches!(
        error,
        AttachError::Cancelled { .. }
            | AttachError::Download {
                kind: DownloadKind::Retryable
            }
    );
    tool_error(
        "media_fetch_failed",
        "media could not be materialized in the bounded cache",
        retryable,
    )
}

fn tool_error(code: &'static str, message: &'static str, retryable: bool) -> Value {
    json!({
        "error": {
            "code": code,
            "message": message,
            "retryable": retryable,
        }
    })
}

fn tool_response(value: &Value, success: bool) -> DynamicToolCallResponse {
    DynamicToolCallResponse {
        content_items: vec![DynamicToolCallOutputContentItem::InputText {
            text: serde_json::to_string(&value).unwrap_or_else(|_| {
                "{\"error\":{\"code\":\"serialization_failed\",\"message\":\"tool response serialization failed\",\"retryable\":false}}".to_owned()
            }),
        }],
        success,
    }
}

async fn respond_tool_error(client: &AppServerClient, request: &mut ServerRequest, error: &Value) {
    let response = tool_response(error, false);
    let _ = client.respond_request(request, &response).await;
}
