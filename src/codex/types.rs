use std::{collections::BTreeMap, fmt, path::PathBuf};

use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};
use serde_json::{Map, Value};

macro_rules! open_string_enum {
    (
        pub enum $name:ident {
            $($variant:ident => $wire:literal,)+
        }
    ) => {
        #[derive(Clone, Debug, Eq, PartialEq)]
        pub enum $name {
            $($variant,)+
            Unknown(String),
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                let value = match self {
                    $(Self::$variant => $wire,)+
                    Self::Unknown(value) => value,
                };
                serializer.serialize_str(value)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Ok(match value.as_str() {
                    $($wire => Self::$variant,)+
                    _ => Self::Unknown(value),
                })
            }
        }
    };
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClientInfo {
    pub name: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

impl ClientInfo {
    #[must_use]
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            title: None,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InitializeCapabilities {
    /// Enables app-server experimental APIs for this connection.
    ///
    /// Codex 0.146 and 0.147 require this capability to accept
    /// `thread/start.dynamicTools` and emit `item/tool/call` requests. It is
    /// optional so the default handshake remains on the stable API surface.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub experimental_api: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mcp_server_openai_form_elicitation: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub opt_out_notification_methods: Option<Vec<String>>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InitializeParams {
    pub client_info: ClientInfo,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<InitializeCapabilities>,
}

impl InitializeParams {
    #[must_use]
    pub const fn new(client_info: ClientInfo) -> Self {
        Self {
            client_info,
            capabilities: None,
        }
    }
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InitializeResult {
    pub codex_home: PathBuf,
    pub platform_family: String,
    pub platform_os: String,
    pub user_agent: String,
}

pub type InitializeResponse = InitializeResult;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum SandboxMode {
    ReadOnly,
    WorkspaceWrite,
    DangerFullAccess,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(untagged)]
pub enum ApprovalPolicy {
    Named(String),
    Granular { granular: GranularApprovalPolicy },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[allow(clippy::struct_excessive_bools)] // Mirrors the app-server wire schema exactly.
pub struct GranularApprovalPolicy {
    pub mcp_elicitations: bool,
    pub rules: bool,
    pub sandbox_approval: bool,
    #[serde(default)]
    pub request_permissions: bool,
    #[serde(default)]
    pub skill_approval: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Personality {
    None,
    Friendly,
    Pragmatic,
}

#[derive(Clone, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadStartParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox: Option<SandboxMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_policy: Option<ApprovalPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approvals_reviewer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_instructions: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<Map<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub developer_instructions: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dynamic_tools: Option<Vec<DynamicToolSpec>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ephemeral: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub personality: Option<Personality>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_start_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thread_source: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
}

/// A client-provided function which Codex may call during a turn.
///
/// `input_schema` is the JSON Schema for the function arguments. Codex 0.146
/// and 0.147 expose this type through the experimental
/// `thread/start.dynamicTools` field and invoke it through the experimental
/// `item/tool/call` reverse-request method.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DynamicToolFunctionSpec {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub defer_loading: bool,
}

/// A named group of dynamic functions exposed as one namespace.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DynamicToolNamespaceSpec {
    pub name: String,
    pub description: String,
    pub tools: Vec<DynamicToolNamespaceTool>,
}

/// A tool nested inside a [`DynamicToolNamespaceSpec`].
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type")]
pub enum DynamicToolNamespaceTool {
    #[serde(rename = "function")]
    Function(DynamicToolFunctionSpec),
}

/// A top-level dynamic function or namespace registered when a thread starts.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type")]
pub enum DynamicToolSpec {
    #[serde(rename = "function")]
    Function(DynamicToolFunctionSpec),
    #[serde(rename = "namespace")]
    Namespace(DynamicToolNamespaceSpec),
}

/// Parameters sent by Codex in an `item/tool/call` reverse request.
#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DynamicToolCallParams {
    pub thread_id: String,
    pub turn_id: String,
    pub call_id: String,
    /// `None` is serialized as JSON `null`; the field remains required on wire.
    #[serde(deserialize_with = "deserialize_nullable_dynamic_tool_namespace")]
    pub namespace: Option<String>,
    pub tool: String,
    pub arguments: Value,
}

/// One content item returned from an `item/tool/call` reverse request.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "type")]
pub enum DynamicToolCallOutputContentItem {
    #[serde(rename = "inputText")]
    InputText { text: String },
    #[serde(rename = "inputImage")]
    InputImage {
        #[serde(rename = "imageUrl")]
        image_url: String,
    },
    #[serde(rename = "inputAudio")]
    InputAudio {
        #[serde(rename = "audioUrl")]
        audio_url: String,
    },
}

/// Result returned to Codex for an `item/tool/call` reverse request.
#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DynamicToolCallResponse {
    pub content_items: Vec<DynamicToolCallOutputContentItem>,
    pub success: bool,
}

fn deserialize_nullable_dynamic_tool_namespace<'de, D>(
    deserializer: D,
) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)
}

open_string_enum! {
    pub enum ThreadSortKey {
        CreatedAt => "created_at",
        UpdatedAt => "updated_at",
        RecencyAt => "recency_at",
        SectionPosition => "section_position",
    }
}

open_string_enum! {
    pub enum SortDirection {
        Ascending => "asc",
        Descending => "desc",
    }
}

open_string_enum! {
    pub enum ThreadSourceKind {
        Cli => "cli",
        Vscode => "vscode",
        Exec => "exec",
        AppServer => "appServer",
        SubAgent => "subAgent",
        SubAgentReview => "subAgentReview",
        SubAgentCompact => "subAgentCompact",
        SubAgentThreadSpawn => "subAgentThreadSpawn",
        SubAgentOther => "subAgentOther",
        UnknownSource => "unknown",
    }
}

/// One or several exact working-directory filters for `thread/list`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub enum ThreadListCwdFilter {
    One(PathBuf),
    Many(Vec<PathBuf>),
}

/// Stable request subset for the selected 0.146.0 `thread/list` contract.
#[derive(Clone, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadListParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort_key: Option<ThreadSortKey>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort_direction: Option<SortDirection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_providers: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_kinds: Option<Vec<ThreadSourceKind>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<ThreadListCwdFilter>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_pinned: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub section_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub search_term: Option<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub use_state_db_only: bool,
}

/// Stable response for `thread/list`.
#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadListResult {
    pub data: Vec<Thread>,
    #[serde(default)]
    pub next_cursor: Option<String>,
    #[serde(default)]
    pub backwards_cursor: Option<String>,
}

pub type ThreadListResponse = ThreadListResult;

/// Stable request for `thread/read`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadReadParams {
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include_turns: Option<bool>,
}

impl ThreadReadParams {
    #[must_use]
    pub fn new(thread_id: impl Into<String>) -> Self {
        Self {
            thread_id: thread_id.into(),
            include_turns: None,
        }
    }
}

/// Stable response for `thread/read`.
#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub struct ThreadReadResult {
    pub thread: Thread,
}

pub type ThreadReadResponse = ThreadReadResult;

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadResumeParams {
    pub thread_id: String,
    #[serde(flatten)]
    pub overrides: ThreadResumeOverrides,
}

impl ThreadResumeParams {
    #[must_use]
    pub fn new(thread_id: impl Into<String>) -> Self {
        Self {
            thread_id: thread_id.into(),
            overrides: ThreadResumeOverrides::default(),
        }
    }
}

#[derive(Clone, Default, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadResumeOverrides {
    /// Return metadata/live state only so bounded turn/item APIs can hydrate authoritative history.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exclude_turns: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_policy: Option<ApprovalPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approvals_reviewer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_instructions: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<Map<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub developer_instructions: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox: Option<SandboxMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub personality: Option<Personality>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<String>,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadStartResult {
    pub thread: Thread,
    pub approval_policy: ApprovalPolicy,
    pub approvals_reviewer: String,
    pub cwd: PathBuf,
    pub model: String,
    pub model_provider: String,
    pub sandbox: TurnSandboxPolicy,
    #[serde(default)]
    pub instruction_sources: Vec<PathBuf>,
    #[serde(default)]
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    pub service_tier: Option<String>,
}

pub type ThreadStartResponse = ThreadStartResult;
pub type ThreadResumeResult = ThreadStartResult;
pub type ThreadResumeResponse = ThreadStartResult;

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type")]
pub enum TurnSandboxPolicy {
    #[serde(rename = "readOnly")]
    ReadOnly {
        #[serde(default, rename = "networkAccess")]
        network_access: bool,
    },
    #[serde(rename = "workspaceWrite")]
    WorkspaceWrite {
        #[serde(default, rename = "writableRoots")]
        writable_roots: Vec<PathBuf>,
        #[serde(default, rename = "networkAccess")]
        network_access: bool,
        #[serde(default, rename = "excludeSlashTmp")]
        exclude_slash_tmp: bool,
        #[serde(default, rename = "excludeTmpdirEnvVar")]
        exclude_tmpdir_env_var: bool,
    },
    #[serde(rename = "dangerFullAccess")]
    DangerFullAccess,
    #[serde(rename = "externalSandbox")]
    ExternalSandbox {
        #[serde(default, rename = "networkAccess")]
        network_access: ExternalNetworkAccess,
    },
}

impl fmt::Debug for TurnSandboxPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadOnly { network_access } => formatter
                .debug_struct("ReadOnly")
                .field("network_access", network_access)
                .finish(),
            Self::WorkspaceWrite {
                writable_roots,
                network_access,
                exclude_slash_tmp,
                exclude_tmpdir_env_var,
            } => formatter
                .debug_struct("WorkspaceWrite")
                .field("writable_root_count", &writable_roots.len())
                .field("network_access", network_access)
                .field("exclude_slash_tmp", exclude_slash_tmp)
                .field("exclude_tmpdir_env_var", exclude_tmpdir_env_var)
                .finish(),
            Self::DangerFullAccess => formatter.write_str("DangerFullAccess"),
            Self::ExternalSandbox { network_access } => formatter
                .debug_struct("ExternalSandbox")
                .field("network_access", network_access)
                .finish(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ExternalNetworkAccess {
    #[default]
    Restricted,
    Enabled,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnStartParams {
    pub thread_id: String,
    pub input: Vec<UserInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sandbox_policy: Option<TurnSandboxPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval_policy: Option<ApprovalPolicy>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approvals_reviewer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_user_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<PathBuf>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub personality: Option<Personality>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,
}

impl TurnStartParams {
    #[must_use]
    pub fn new(thread_id: impl Into<String>, input: Vec<UserInput>) -> Self {
        Self {
            thread_id: thread_id.into(),
            input,
            sandbox_policy: None,
            approval_policy: None,
            approvals_reviewer: None,
            client_user_message_id: None,
            summary: None,
            cwd: None,
            effort: None,
            personality: None,
            model: None,
            service_tier: None,
            output_schema: None,
        }
    }
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub struct TurnStartResult {
    pub turn: Turn,
}

pub type TurnStartResponse = TurnStartResult;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnInterruptParams {
    pub thread_id: String,
    pub turn_id: String,
}

impl TurnInterruptParams {
    #[must_use]
    pub fn new(thread_id: impl Into<String>, turn_id: impl Into<String>) -> Self {
        Self {
            thread_id: thread_id.into(),
            turn_id: turn_id.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct TurnInterruptResult {}

pub type TurnInterruptResponse = TurnInterruptResult;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadUnsubscribeParams {
    pub thread_id: String,
}

open_string_enum! {
    pub enum ThreadUnsubscribeStatus {
        NotLoaded => "notLoaded",
        NotSubscribed => "notSubscribed",
        Unsubscribed => "unsubscribed",
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ThreadUnsubscribeResult {
    pub status: ThreadUnsubscribeStatus,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnSteerParams {
    pub thread_id: String,
    pub expected_turn_id: String,
    pub input: Vec<UserInput>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub additional_context: Option<Map<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_user_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub responsesapi_client_metadata: Option<BTreeMap<String, String>>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnSteerResult {
    pub turn_id: String,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadQueueAddParams {
    pub thread_id: String,
    pub client_user_message_id: String,
    pub input: Vec<UserInput>,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueuedSubmission {
    pub id: String,
    pub client_user_message_id: String,
    pub input: Vec<UserInput>,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadQueueAddResult {
    pub queued_submission: QueuedSubmission,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadQueueListParams {
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadQueueListResult {
    pub data: Vec<QueuedSubmission>,
    #[serde(default)]
    pub next_cursor: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadQueueStartParams {
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued_submission_id: Option<String>,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub struct ThreadQueueStartResult {
    pub turn: Turn,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadTurnsListParams {
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort_direction: Option<SortDirection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub items_view: Option<String>,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadTurnsListResult {
    pub data: Vec<Turn>,
    #[serde(default)]
    pub next_cursor: Option<String>,
    #[serde(default)]
    pub backwards_cursor: Option<String>,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadItemsListParams {
    pub thread_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort_direction: Option<SortDirection>,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadItemsListResult {
    pub data: Vec<ThreadItemEntry>,
    #[serde(default)]
    pub next_cursor: Option<String>,
    #[serde(default)]
    pub backwards_cursor: Option<String>,
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadItemEntry {
    pub item: ThreadItem,
    pub turn_id: String,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadStatusChangedNotification {
    pub thread_id: String,
    pub status: Value,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ThreadGoalClearedNotification {
    pub thread_id: String,
}

/// Exact 0.149 operational status emitted to every initialized WebSocket. It carries no thread
/// lifecycle data and is validated then ignored by external reconciliation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RemoteControlStatusChangedNotification {
    pub environment_id: Option<String>,
    pub installation_id: String,
    pub server_name: String,
    pub status: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadQueueChangedNotification {
    pub thread_id: String,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerRequestResolvedNotification {
    pub thread_id: String,
    pub request_id: Value,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandExecutionRequestApprovalParams {
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub started_at_ms: i64,
    #[serde(flatten)]
    pub details: Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecpolicyAmendment {
    pub execpolicy_amendment: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkPolicyAmendment {
    pub action: NetworkPolicyAction,
    pub host: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum NetworkPolicyAction {
    Allow,
    Deny,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NetworkPolicyAmendmentEnvelope {
    pub network_policy_amendment: NetworkPolicyAmendment,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum SimpleApprovalDecision {
    #[serde(rename = "accept")]
    Accept,
    #[serde(rename = "acceptForSession")]
    AcceptForSession,
    #[serde(rename = "decline")]
    Decline,
    #[serde(rename = "cancel")]
    Cancel,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub enum CommandExecutionApprovalDecision {
    Simple(SimpleApprovalDecision),
    AcceptWithExecpolicyAmendment {
        #[serde(rename = "acceptWithExecpolicyAmendment")]
        amendment: ExecpolicyAmendment,
    },
    ApplyNetworkPolicyAmendment {
        #[serde(rename = "applyNetworkPolicyAmendment")]
        amendment: NetworkPolicyAmendmentEnvelope,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CommandExecutionRequestApprovalResult {
    pub decision: CommandExecutionApprovalDecision,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileChangeRequestApprovalParams {
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub started_at_ms: i64,
    #[serde(flatten)]
    pub details: Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct FileChangeRequestApprovalResult {
    pub decision: SimpleApprovalDecision,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PermissionsRequestApprovalParams {
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub started_at_ms: i64,
    pub cwd: PathBuf,
    pub permissions: Value,
    #[serde(flatten)]
    pub details: Map<String, Value>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PermissionGrantScope {
    Turn,
    Session,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub struct PermissionsRequestApprovalResult {
    pub permissions: Value,
    #[serde(default)]
    pub scope: Option<PermissionGrantScope>,
    #[serde(default, rename = "strictAutoReview")]
    pub strict_auto_review: Option<bool>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ImageDetail {
    Auto,
    Low,
    High,
    Original,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type")]
pub enum UserInput {
    #[serde(rename = "text")]
    Text {
        text: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        text_elements: Vec<Value>,
    },
    #[serde(rename = "image")]
    Image {
        url: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<ImageDetail>,
    },
    #[serde(rename = "localImage")]
    LocalImage {
        path: PathBuf,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail: Option<ImageDetail>,
    },
    #[serde(rename = "audio")]
    Audio { url: String },
    #[serde(rename = "localAudio")]
    LocalAudio { path: PathBuf },
    #[serde(rename = "skill")]
    Skill { name: String, path: PathBuf },
    #[serde(rename = "mention")]
    Mention { name: String, path: PathBuf },
}

impl UserInput {
    #[must_use]
    pub fn text(text: impl Into<String>) -> Self {
        Self::Text {
            text: text.into(),
            text_elements: Vec::new(),
        }
    }
}

open_string_enum! {
    pub enum TurnStatus {
        Completed => "completed",
        Interrupted => "interrupted",
        Failed => "failed",
        InProgress => "inProgress",
    }
}

open_string_enum! {
    pub enum MessagePhase {
        Commentary => "commentary",
        FinalAnswer => "final_answer",
    }
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnError {
    pub message: String,
    #[serde(default)]
    pub additional_details: Option<String>,
    #[serde(default)]
    pub codex_error_info: Option<Value>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Turn {
    pub id: String,
    pub items: Vec<ThreadItem>,
    pub status: TurnStatus,
    #[serde(default)]
    pub started_at: Option<i64>,
    #[serde(default)]
    pub completed_at: Option<i64>,
    #[serde(default)]
    pub duration_ms: Option<i64>,
    #[serde(default)]
    pub error: Option<TurnError>,
    #[serde(default)]
    pub items_view: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Thread {
    pub id: String,
    pub session_id: String,
    pub preview: String,
    pub model_provider: String,
    pub created_at: i64,
    pub updated_at: i64,
    pub status: Value,
    pub ephemeral: bool,
    pub turns: Vec<Turn>,
    pub source: Value,
    pub cli_version: String,
    pub cwd: PathBuf,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub path: Option<PathBuf>,
    #[serde(default)]
    pub forked_from_id: Option<String>,
    #[serde(default)]
    pub parent_thread_id: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, PartialEq)]
pub enum ThreadItem {
    UserMessage {
        id: String,
        content: Vec<UserInput>,
        client_id: Option<String>,
        extra: Map<String, Value>,
    },
    AgentMessage {
        id: String,
        text: String,
        phase: Option<MessagePhase>,
        memory_citation: Option<Value>,
        extra: Map<String, Value>,
    },
    Plan {
        id: String,
        text: String,
        extra: Map<String, Value>,
    },
    Reasoning {
        id: String,
        summary: Vec<String>,
        content: Vec<String>,
        extra: Map<String, Value>,
    },
    HookPrompt {
        id: String,
        raw: Value,
    },
    CommandExecution {
        id: String,
        raw: Value,
    },
    FileChange {
        id: String,
        raw: Value,
    },
    McpToolCall {
        id: String,
        raw: Value,
    },
    DynamicToolCall {
        id: String,
        raw: Value,
    },
    CollabAgentToolCall {
        id: String,
        raw: Value,
    },
    SubAgentActivity {
        id: String,
        raw: Value,
    },
    WebSearch {
        id: String,
        raw: Value,
    },
    ImageView {
        id: String,
        raw: Value,
    },
    Sleep {
        id: String,
        raw: Value,
    },
    ImageGeneration {
        id: String,
        raw: Value,
    },
    EnteredReviewMode {
        id: String,
        raw: Value,
    },
    ExitedReviewMode {
        id: String,
        raw: Value,
    },
    ContextCompaction {
        id: String,
        raw: Value,
    },
    Unknown {
        item_type: String,
        raw: Value,
    },
}

impl fmt::Debug for ThreadItem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct(self.kind())
            .field("id", &self.id())
            .finish_non_exhaustive()
    }
}

impl ThreadItem {
    #[must_use]
    pub fn id(&self) -> Option<&str> {
        match self {
            Self::UserMessage { id, .. }
            | Self::AgentMessage { id, .. }
            | Self::Plan { id, .. }
            | Self::Reasoning { id, .. }
            | Self::HookPrompt { id, .. }
            | Self::CommandExecution { id, .. }
            | Self::FileChange { id, .. }
            | Self::McpToolCall { id, .. }
            | Self::DynamicToolCall { id, .. }
            | Self::CollabAgentToolCall { id, .. }
            | Self::SubAgentActivity { id, .. }
            | Self::WebSearch { id, .. }
            | Self::ImageView { id, .. }
            | Self::Sleep { id, .. }
            | Self::ImageGeneration { id, .. }
            | Self::EnteredReviewMode { id, .. }
            | Self::ExitedReviewMode { id, .. }
            | Self::ContextCompaction { id, .. } => Some(id),
            Self::Unknown { raw, .. } => raw.get("id").and_then(Value::as_str),
        }
    }

    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::UserMessage { .. } => "userMessage",
            Self::AgentMessage { .. } => "agentMessage",
            Self::Plan { .. } => "plan",
            Self::Reasoning { .. } => "reasoning",
            Self::HookPrompt { .. } => "hookPrompt",
            Self::CommandExecution { .. } => "commandExecution",
            Self::FileChange { .. } => "fileChange",
            Self::McpToolCall { .. } => "mcpToolCall",
            Self::DynamicToolCall { .. } => "dynamicToolCall",
            Self::CollabAgentToolCall { .. } => "collabAgentToolCall",
            Self::SubAgentActivity { .. } => "subAgentActivity",
            Self::WebSearch { .. } => "webSearch",
            Self::ImageView { .. } => "imageView",
            Self::Sleep { .. } => "sleep",
            Self::ImageGeneration { .. } => "imageGeneration",
            Self::EnteredReviewMode { .. } => "enteredReviewMode",
            Self::ExitedReviewMode { .. } => "exitedReviewMode",
            Self::ContextCompaction { .. } => "contextCompaction",
            Self::Unknown { .. } => "unknown",
        }
    }
}

impl<'de> Deserialize<'de> for ThreadItem {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = Value::deserialize(deserializer)?;
        let object = raw
            .as_object()
            .ok_or_else(|| D::Error::custom("thread item must be an object"))?;
        let item_type = object
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| D::Error::custom("thread item type must be a string"))?
            .to_owned();

        match item_type.as_str() {
            "userMessage" => serde_json::from_value::<UserMessageWire>(raw)
                .map(Into::into)
                .map_err(D::Error::custom),
            "agentMessage" => serde_json::from_value::<AgentMessageWire>(raw)
                .map(Into::into)
                .map_err(D::Error::custom),
            "plan" => serde_json::from_value::<PlanWire>(raw)
                .map(Into::into)
                .map_err(D::Error::custom),
            "reasoning" => serde_json::from_value::<ReasoningWire>(raw)
                .map(Into::into)
                .map_err(D::Error::custom),
            known => opaque_item(known, raw).map_err(D::Error::custom),
        }
    }
}

impl Serialize for ThreadItem {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::UserMessage {
                id,
                content,
                client_id,
                extra,
            } => UserMessageWire {
                item_type: "userMessage".to_owned(),
                id: id.clone(),
                content: content.clone(),
                client_id: client_id.clone(),
                extra: extra.clone(),
            }
            .serialize(serializer),
            Self::AgentMessage {
                id,
                text,
                phase,
                memory_citation,
                extra,
            } => AgentMessageWire {
                item_type: "agentMessage".to_owned(),
                id: id.clone(),
                text: text.clone(),
                phase: phase.clone(),
                memory_citation: memory_citation.clone(),
                extra: extra.clone(),
            }
            .serialize(serializer),
            Self::Plan { id, text, extra } => PlanWire {
                item_type: "plan".to_owned(),
                id: id.clone(),
                text: text.clone(),
                extra: extra.clone(),
            }
            .serialize(serializer),
            Self::Reasoning {
                id,
                summary,
                content,
                extra,
            } => ReasoningWire {
                item_type: "reasoning".to_owned(),
                id: id.clone(),
                summary: summary.clone(),
                content: content.clone(),
                extra: extra.clone(),
            }
            .serialize(serializer),
            Self::HookPrompt { raw, .. }
            | Self::CommandExecution { raw, .. }
            | Self::FileChange { raw, .. }
            | Self::McpToolCall { raw, .. }
            | Self::DynamicToolCall { raw, .. }
            | Self::CollabAgentToolCall { raw, .. }
            | Self::SubAgentActivity { raw, .. }
            | Self::WebSearch { raw, .. }
            | Self::ImageView { raw, .. }
            | Self::Sleep { raw, .. }
            | Self::ImageGeneration { raw, .. }
            | Self::EnteredReviewMode { raw, .. }
            | Self::ExitedReviewMode { raw, .. }
            | Self::ContextCompaction { raw, .. }
            | Self::Unknown { raw, .. } => raw.serialize(serializer),
        }
    }
}

#[derive(Deserialize, Serialize)]
struct UserMessageWire {
    #[serde(rename = "type")]
    item_type: String,
    id: String,
    content: Vec<UserInput>,
    #[serde(default, rename = "clientId", skip_serializing_if = "Option::is_none")]
    client_id: Option<String>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

impl From<UserMessageWire> for ThreadItem {
    fn from(value: UserMessageWire) -> Self {
        Self::UserMessage {
            id: value.id,
            content: value.content,
            client_id: value.client_id,
            extra: value.extra,
        }
    }
}

#[derive(Deserialize, Serialize)]
struct AgentMessageWire {
    #[serde(rename = "type")]
    item_type: String,
    id: String,
    text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    phase: Option<MessagePhase>,
    #[serde(
        default,
        rename = "memoryCitation",
        skip_serializing_if = "Option::is_none"
    )]
    memory_citation: Option<Value>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

impl From<AgentMessageWire> for ThreadItem {
    fn from(value: AgentMessageWire) -> Self {
        Self::AgentMessage {
            id: value.id,
            text: value.text,
            phase: value.phase,
            memory_citation: value.memory_citation,
            extra: value.extra,
        }
    }
}

#[derive(Deserialize, Serialize)]
struct PlanWire {
    #[serde(rename = "type")]
    item_type: String,
    id: String,
    text: String,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

impl From<PlanWire> for ThreadItem {
    fn from(value: PlanWire) -> Self {
        Self::Plan {
            id: value.id,
            text: value.text,
            extra: value.extra,
        }
    }
}

#[derive(Deserialize, Serialize)]
struct ReasoningWire {
    #[serde(rename = "type")]
    item_type: String,
    id: String,
    #[serde(default)]
    summary: Vec<String>,
    #[serde(default)]
    content: Vec<String>,
    #[serde(flatten)]
    extra: Map<String, Value>,
}

impl From<ReasoningWire> for ThreadItem {
    fn from(value: ReasoningWire) -> Self {
        Self::Reasoning {
            id: value.id,
            summary: value.summary,
            content: value.content,
            extra: value.extra,
        }
    }
}

fn opaque_item(item_type: &str, raw: Value) -> Result<ThreadItem, &'static str> {
    if !matches!(
        item_type,
        "hookPrompt"
            | "commandExecution"
            | "fileChange"
            | "mcpToolCall"
            | "dynamicToolCall"
            | "collabAgentToolCall"
            | "subAgentActivity"
            | "webSearch"
            | "imageView"
            | "sleep"
            | "imageGeneration"
            | "enteredReviewMode"
            | "exitedReviewMode"
            | "contextCompaction"
    ) {
        return Ok(ThreadItem::Unknown {
            item_type: item_type.to_owned(),
            raw,
        });
    }

    let id = raw
        .get("id")
        .and_then(Value::as_str)
        .ok_or("known thread item is missing a string id")?
        .to_owned();
    Ok(match item_type {
        "hookPrompt" => ThreadItem::HookPrompt { id, raw },
        "commandExecution" => ThreadItem::CommandExecution { id, raw },
        "fileChange" => ThreadItem::FileChange { id, raw },
        "mcpToolCall" => ThreadItem::McpToolCall { id, raw },
        "dynamicToolCall" => ThreadItem::DynamicToolCall { id, raw },
        "collabAgentToolCall" => ThreadItem::CollabAgentToolCall { id, raw },
        "subAgentActivity" => ThreadItem::SubAgentActivity { id, raw },
        "webSearch" => ThreadItem::WebSearch { id, raw },
        "imageView" => ThreadItem::ImageView { id, raw },
        "sleep" => ThreadItem::Sleep { id, raw },
        "imageGeneration" => ThreadItem::ImageGeneration { id, raw },
        "enteredReviewMode" => ThreadItem::EnteredReviewMode { id, raw },
        "exitedReviewMode" => ThreadItem::ExitedReviewMode { id, raw },
        "contextCompaction" => ThreadItem::ContextCompaction { id, raw },
        _ => unreachable!("known item type checked above"),
    })
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadStartedNotification {
    pub thread: Thread,
}

/// Exact required projection of the 0.149.0 shared-thread settings notification.
#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadSettingsUpdatedNotification {
    pub thread_id: String,
    pub thread_settings: ThreadSettingsSnapshot,
}

/// Security-relevant required fields retained from a shared thread settings update.
#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadSettingsSnapshot {
    pub approval_policy: ApprovalPolicy,
    pub approvals_reviewer: String,
    pub collaboration_mode: Value,
    pub cwd: PathBuf,
    pub model: String,
    pub model_provider: String,
    pub sandbox_policy: Value,
}

/// Bounded endpoint-level rate-limit notification; values are not used for write admission.
#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountRateLimitsUpdatedNotification {
    pub rate_limits: Map<String, Value>,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnStartedNotification {
    pub thread_id: String,
    pub turn: Turn,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentMessageDeltaNotification {
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub delta: String,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ItemStartedNotification {
    pub thread_id: String,
    pub turn_id: String,
    pub started_at_ms: i64,
    pub item: ThreadItem,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandExecutionOutputDeltaNotification {
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub delta: String,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ItemCompletedNotification {
    pub thread_id: String,
    pub turn_id: String,
    pub completed_at_ms: i64,
    pub item: ThreadItem,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TurnCompletedNotification {
    pub thread_id: String,
    pub turn: Turn,
}

#[derive(Clone, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorNotification {
    pub thread_id: String,
    pub turn_id: String,
    pub error: TurnError,
    pub will_retry: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenUsageBreakdown {
    pub input_tokens: i64,
    pub cached_input_tokens: i64,
    #[serde(default)]
    pub cache_write_input_tokens: i64,
    pub output_tokens: i64,
    pub reasoning_output_tokens: i64,
    pub total_tokens: i64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadTokenUsage {
    pub total: TokenUsageBreakdown,
    pub last: TokenUsageBreakdown,
    #[serde(default)]
    pub model_context_window: Option<i64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadTokenUsageUpdatedNotification {
    pub thread_id: String,
    pub turn_id: String,
    pub token_usage: ThreadTokenUsage,
}
