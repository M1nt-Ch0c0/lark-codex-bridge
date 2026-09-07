//! Original-style structured prompt injection for Codex turns.
//!
//! The previous capability-ID + `bridge_context.resolve` tool path required
//! the model to fetch its own conversation metadata. This module instead
//! injects the same XML sections the reference `lark-channel-bridge` puts in
//! every user message, so the agent can answer immediately.

use serde::Serialize;
use serde_json::Value;

use crate::lark::api::ChatMode;
use crate::lark::normalize::{InboundEvent, MentionIdentity};
use crate::runtime::context::{DraftPart, QuoteDraft, QuoteStatus};

/// Compact mention identity copied into `<bridge_context>`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptMention {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub open_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_bot: Option<bool>,
}

/// Conversation metadata injected at the top of every user turn.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptContext {
    pub chat_id: String,
    pub chat_type: String,
    pub sender_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender_name: Option<String>,
    pub sender_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bot_open_id: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub mentions: Vec<PromptMention>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub message_ids: Vec<String>,
    pub source: &'static str,
}

/// One directly quoted parent, already resolved by the one-hop quote path.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptQuotedMessage {
    pub message_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender_name: Option<String>,
    pub raw_content_type: String,
    pub content: String,
    pub status: String,
}

/// A local attachment already downloaded into the bounded cache.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PromptAttachment {
    pub path: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
}

/// Inputs required to render one original-style agent prompt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuildAgentPromptInput {
    pub context: PromptContext,
    pub user_input: String,
    pub quoted_messages: Vec<PromptQuotedMessage>,
    pub topic_context: Vec<PromptQuotedMessage>,
    pub attachments: Vec<PromptAttachment>,
}

/// System prompt prefixed onto every Codex stdin turn, matching the
/// reference Codex adapter's `prefixBridgeSystemPrompt`.
pub const BRIDGE_SYSTEM_PROMPT: &str = r#"# lark-codex-bridge 运行约定

你正在 lark-codex-bridge 里跑：把飞书/Lark 用户消息桥到本地 Codex。

## bridge_context

每条 user message 顶部会带一个 `<bridge_context>` 块：

```
<bridge_context>
{"chatId":"oc_xxx","chatType":"p2p","senderId":"ou_xxx","senderType":"user|bot","botOpenId":"ou_xxx","mentions":[{"openId":"ou_xxx","name":"...","isBot":true}], ...}
</bridge_context>
```

里面是当前对话的 chat_id、chat 类型（p2p / group / topic）、发送者。关键字段：

- `senderType`：发送者是人（`user`）还是另一个 bot（`bot`）
- `botOpenId`：**你自己**的 open_id
- `mentions`：这条消息 @ 到的账号列表

这些都是 bridge 注入的元数据，**不要照抄、不要在你的回复里渲染**——它对用户不可见。

## quoted_messages

如果用户用「引用回复」指向某条消息，bridge 会注入 `<quoted_messages>`。这是用户**指向的对象**；用户的实际问题在 `<user_input>`。回答时围绕这段内容展开，不要照抄 XML 标签。合并转发会展开为 `<forwarded_messages>`。

## topic_context

话题群首次介入时，bridge 可能注入 `<topic_context>`：当前话题里最近若干条上游消息。这是背景，不是用户这一句。之后同一话题的会话会自己记住上文，不会重复注入。

## user_input

`<user_input>` 里的 `text` 才是用户这句话。`attachments` 是已经下载到本地的文件路径，可以直接读取。图片也会作为独立的本地图片输入发给你。

## 与其他 bot 协作

- `bridge_context.botOpenId` 是你自己。消息内容或 mentions 里出现这个 id 就是指你自己。
- bot 只有被真实 @ 才能收到群消息。需要某个 bot 接着处理时，必须真实 @ 它。
- 默认不要 @ 其他 bot，避免互相 @ 形成死循环。
"#;

/// Builds the original-style XML user prompt.
#[must_use]
pub fn build_agent_prompt(input: &BuildAgentPromptInput) -> String {
    let mut sections = vec![prompt_section("bridge_context", &input.context)];
    if !input.quoted_messages.is_empty() {
        sections.push(prompt_section("quoted_messages", &input.quoted_messages));
    }
    if !input.topic_context.is_empty() {
        sections.push(prompt_section("topic_context", &input.topic_context));
    }
    let user_input = if input.attachments.is_empty() {
        serde_json::json!({ "text": input.user_input })
    } else {
        serde_json::json!({
            "text": input.user_input,
            "attachments": input.attachments,
        })
    };
    sections.push(prompt_section("user_input", &user_input));
    sections.join("\n\n")
}

/// Prefixes the bridge system prompt, matching the reference Codex adapter.
#[must_use]
pub fn prefix_bridge_system_prompt(prompt: &str, bot_open_id: Option<&str>) -> String {
    let mut system = BRIDGE_SYSTEM_PROMPT.to_owned();
    if let Some(open_id) = bot_open_id.filter(|value| !value.is_empty()) {
        system.push_str("\n## 你的身份\n\n你的 open_id 是 `");
        system.push_str(open_id);
        system.push_str("`。消息内容或 mentions 里出现这个 open_id 都是指你自己。\n");
    }
    format!("{system}\n\n## user_message\n\n{prompt}")
}

/// Builds a prompt context from one normalized inbound event.
#[must_use]
pub fn prompt_context_from_event(
    event: &InboundEvent,
    bot_open_id: Option<&str>,
    source: &'static str,
) -> PromptContext {
    PromptContext {
        chat_id: event.chat_id.clone(),
        chat_type: chat_type_label(event.chat_type).to_owned(),
        sender_id: event.sender_id.clone(),
        sender_name: None,
        sender_type: if event.sender_is_human {
            "user".to_owned()
        } else {
            "bot".to_owned()
        },
        bot_open_id: bot_open_id
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
        mentions: event.mentions.iter().map(prompt_mention).collect(),
        thread_id: event.thread_id.clone(),
        message_ids: vec![event.message_id.clone()],
        source,
    }
}

/// Flattens a resolved quote draft into one prompt section entry.
#[must_use]
pub fn quoted_message_from_draft(draft: &QuoteDraft) -> PromptQuotedMessage {
    PromptQuotedMessage {
        message_id: draft.message_id.clone(),
        sender_id: draft.sender_id.clone(),
        sender_name: draft.sender_name.clone(),
        raw_content_type: draft
            .message_type
            .clone()
            .unwrap_or_else(|| "unknown".to_owned()),
        content: quote_content(draft),
        status: quote_status_label(draft.status).to_owned(),
    }
}

fn prompt_mention(mention: &MentionIdentity) -> PromptMention {
    PromptMention {
        open_id: mention.open_id.clone(),
        name: mention.name.clone(),
        is_bot: None,
    }
}

fn quote_content(draft: &QuoteDraft) -> String {
    let mut texts = Vec::new();
    for part in &draft.parts {
        match part {
            DraftPart::Text(text) if !text.is_empty() => texts.push(text.clone()),
            DraftPart::Media { kind, metadata, .. } => {
                let name = metadata.name.as_deref().unwrap_or("attachment");
                texts.push(format!("[{kind:?}: {name}]"));
            }
            DraftPart::Card(_) => {
                if texts.is_empty() {
                    texts.push("[interactive card]".to_owned());
                }
            }
            DraftPart::Forward { .. } => {
                if texts.is_empty() {
                    texts.push("[forwarded messages]".to_owned());
                }
            }
            DraftPart::Unsupported { message_type, .. } => {
                texts.push(format!("[unsupported:{message_type}]"));
            }
            DraftPart::Text(_) => {}
        }
    }
    if texts.is_empty() {
        format!("({} quoted message)", quote_status_label(draft.status))
    } else {
        texts.join("\n")
    }
}

fn quote_status_label(status: QuoteStatus) -> &'static str {
    match status {
        QuoteStatus::Available => "available",
        QuoteStatus::Deleted => "deleted",
        QuoteStatus::Unauthorized => "unauthorized",
        QuoteStatus::Oversize => "oversize",
        QuoteStatus::Unsupported => "unsupported",
        QuoteStatus::Unavailable => "unavailable",
    }
}

fn chat_type_label(mode: ChatMode) -> &'static str {
    match mode {
        ChatMode::P2p => "p2p",
        ChatMode::Group => "group",
        ChatMode::Topic => "topic",
    }
}

fn prompt_section(tag: &str, value: &impl Serialize) -> String {
    format!("<{tag}>\n{}\n</{tag}>", safe_json_stringify(value))
}

fn safe_json_stringify(value: &impl Serialize) -> String {
    let encoded = serde_json::to_string(value).unwrap_or_else(|_| "null".to_owned());
    encoded
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026")
}

/// Escapes a JSON value the same way prompt sections do. Exposed for tests.
#[must_use]
pub fn escape_prompt_json(value: &Value) -> String {
    let encoded = serde_json::to_string(value).unwrap_or_else(|_| "null".to_owned());
    encoded
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lark::normalize::ScopeKey;

    fn event(text: &str) -> InboundEvent {
        InboundEvent {
            event_id: "evt_1".to_owned(),
            message_id: "om_1".to_owned(),
            chat_id: "oc_1".to_owned(),
            sender_id: "ou_alice".to_owned(),
            chat_type: ChatMode::Group,
            thread_id: None,
            root_id: None,
            reply_to_message_id: Some("om_parent".to_owned()),
            text: text.to_owned(),
            mentions_bot: true,
            mention_all: false,
            sender_is_human: true,
            mentions: vec![MentionIdentity {
                key: Some("@_user_1".to_owned()),
                open_id: Some("ou_bob".to_owned()),
                user_id: None,
                union_id: None,
                name: Some("Bob".to_owned()),
            }],
            parts: Vec::new(),
            resources: Vec::new(),
            message_type: "text".to_owned(),
            create_time_ms: 1,
            scope: ScopeKey::Chat("oc_1".to_owned()),
        }
    }

    #[test]
    fn injects_full_bridge_context_instead_of_opaque_capability() {
        let event = event("看看这个");
        let prompt = build_agent_prompt(&BuildAgentPromptInput {
            context: prompt_context_from_event(&event, Some("ou_bot"), "im"),
            user_input: event.text.clone(),
            quoted_messages: vec![PromptQuotedMessage {
                message_id: "om_parent".to_owned(),
                sender_id: Some("ou_carol".to_owned()),
                sender_name: Some("Carol".to_owned()),
                raw_content_type: "text".to_owned(),
                content: "原始问题".to_owned(),
                status: "available".to_owned(),
            }],
            topic_context: Vec::new(),
            attachments: Vec::new(),
        });
        assert!(prompt.contains("Carol"));
        assert!(prompt.contains("<bridge_context>"));
        assert!(prompt.contains("\"chatId\":\"oc_1\""));
        assert!(prompt.contains("\"chatType\":\"group\""));
        assert!(prompt.contains("\"botOpenId\":\"ou_bot\""));
        assert!(prompt.contains("\"senderId\":\"ou_alice\""));
        assert!(prompt.contains("<quoted_messages>"));
        assert!(prompt.contains("原始问题"));
        assert!(prompt.contains("<user_input>"));
        assert!(prompt.contains("看看这个"));
        assert!(!prompt.contains("bridge_context.resolve"));
        assert!(!prompt.contains("\"id\":\"ctx_"));
    }

    #[test]
    fn injects_topic_context_separately_from_quotes() {
        let event = event("继续");
        let prompt = build_agent_prompt(&BuildAgentPromptInput {
            context: prompt_context_from_event(&event, Some("ou_bot"), "im"),
            user_input: event.text.clone(),
            quoted_messages: Vec::new(),
            topic_context: vec![PromptQuotedMessage {
                message_id: "om_topic".to_owned(),
                sender_id: Some("ou_dana".to_owned()),
                sender_name: Some("Dana".to_owned()),
                raw_content_type: "text".to_owned(),
                content: "先说背景".to_owned(),
                status: "available".to_owned(),
            }],
            attachments: Vec::new(),
        });
        assert!(prompt.contains("<topic_context>"));
        assert!(prompt.contains("先说背景"));
        assert!(prompt.contains("Dana"));
        assert!(!prompt.contains("<quoted_messages>"));
    }

    #[test]
    fn prefixes_system_prompt_and_escapes_html_in_json() {
        let prefixed = prefix_bridge_system_prompt(
            "<user_input>\n{\"text\":\"hi\"}\n</user_input>",
            Some("ou_self"),
        );
        assert!(prefixed.contains("lark-codex-bridge 运行约定"));
        assert!(prefixed.contains("你的 open_id 是 `ou_self`"));
        assert!(prefixed.contains("## user_message"));
        let escaped = escape_prompt_json(&serde_json::json!({"x": "<script>&"}));
        assert!(escaped.contains("\\u003cscript\\u003e\\u0026"));
        assert!(!escaped.contains("<script>"));
    }
}
