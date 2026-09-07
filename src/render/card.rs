//! Card 2.0 builders for live run updates and slash-command replies.
//!
//! The reference bridge ships functional but visually flat cards. These
//! templates keep the same interaction model (in-place updates, command
//! buttons, stop) while adding a clearer header, status strip, and grouped
//! actions.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::stabilize_streaming_markdown;

/// Visual accent used by command and run cards.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CardAccent {
    Blue,
    Green,
    Orange,
    Red,
    Grey,
    Purple,
}

impl CardAccent {
    const fn template(self) -> &'static str {
        match self {
            Self::Blue => "blue",
            Self::Green => "green",
            Self::Orange => "orange",
            Self::Red => "red",
            Self::Grey => "grey",
            Self::Purple => "purple",
        }
    }
}

/// One callback button encoded into a command card.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CardButton {
    pub label: String,
    pub cmd: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arg: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub style: Option<String>,
}

/// Durable, versioned spec stored in an outbox `reply_interactive_card` row.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InteractiveCardSpec {
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subtitle: Option<String>,
    pub body: String,
    pub accent: CardAccent,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub actions: Vec<CardButton>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub form: Option<ConfigFormSpec>,
}

/// Fields rendered into the `/config` Card 2.0 form.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ConfigFormSpec {
    pub model: String,
    pub effort: String,
    pub sandbox: String,
    pub approval: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_groups: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_senders: Vec<String>,
}

/// Whether a live run card is still streaming.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunCardPhase {
    Running,
    #[default]
    Done,
    Interrupted,
    Failed,
}

/// Fields gathered for the `/status` troubleshooting card.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatusSnapshot {
    pub chat_mode: String,
    pub cwd: Option<String>,
    pub session_id: Option<String>,
    pub active_run: bool,
    pub scope_state: String,
    pub model: Option<String>,
    pub effort: Option<String>,
    pub sandbox: String,
    pub approval: String,
    pub pending_media: usize,
    pub outbox_pending: u64,
    pub outbox_failed: u64,
    pub outbox_uncertain: u64,
    pub group_allowed: Option<bool>,
    pub show_config: bool,
}

/// Fields gathered for the `/info` environment card.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InfoSnapshot {
    pub workspace: Option<String>,
    pub mcp: Vec<String>,
    pub skills: Vec<(String, Option<String>)>,
    pub sessions: Vec<(String, String, bool)>,
}

/// Renders a command or status card.
#[must_use]
pub fn render_interactive_card(spec: &InteractiveCardSpec) -> Value {
    let mut elements = vec![markdown_block(&spec.body)];
    if let Some(form) = &spec.form {
        elements.push(json!({ "tag": "hr" }));
        elements.push(config_form(form));
    }
    if !spec.actions.is_empty() {
        elements.push(json!({
            "tag": "hr",
        }));
        elements.push(action_row(&spec.actions));
    }
    let mut header = serde_json::Map::new();
    header.insert(
        "title".to_owned(),
        json!({ "tag": "plain_text", "content": spec.title }),
    );
    header.insert("template".to_owned(), json!(spec.accent.template()));
    if let Some(subtitle) = &spec.subtitle {
        header.insert(
            "subtitle".to_owned(),
            json!({ "tag": "plain_text", "content": subtitle }),
        );
    }
    json!({
        "schema": "2.0",
        "config": {
            "update_multi": true,
            "wide_screen_mode": true,
        },
        "header": header,
        "body": {
            "elements": elements,
        },
    })
}

/// Renders the live Codex run card shown while a turn streams.
#[must_use]
pub fn render_run_card(text: &str, phase: RunCardPhase) -> Value {
    let body = stabilize_streaming_markdown(text);
    let (title, subtitle, template, streaming) = match phase {
        RunCardPhase::Running => ("Codex 正在回答", "实时更新中", "blue", true),
        RunCardPhase::Done => ("Codex 已完成", "本轮回答", "green", false),
        RunCardPhase::Interrupted => ("已中断", "本轮被停止", "orange", false),
        RunCardPhase::Failed => ("回答失败", "本轮未完成", "red", false),
    };
    let mut elements = Vec::new();
    elements.push(json!({
        "tag": "markdown",
        "content": format!("_{subtitle}_"),
        "text_size": "notation",
    }));
    if body.trim().is_empty() {
        elements.push(json!({
            "tag": "markdown",
            "content": match phase {
                RunCardPhase::Running => "正在组织回复…",
                RunCardPhase::Done => "_（未返回内容）_",
                RunCardPhase::Interrupted => "_⏹ 已被中断_",
                RunCardPhase::Failed => "_回答失败_",
            },
        }));
    } else {
        elements.push(markdown_block(&body));
    }
    if phase == RunCardPhase::Running {
        elements.push(json!({
            "tag": "button",
            "text": { "tag": "plain_text", "content": "⏹ 停止" },
            "type": "danger",
            "width": "fill",
            "behaviors": [{
                "type": "callback",
                "value": { "cmd": "stop" },
            }],
        }));
    }
    json!({
        "schema": "2.0",
        "config": {
            "streaming_mode": streaming,
            "update_multi": true,
            "wide_screen_mode": true,
            "summary": { "content": title },
        },
        "header": {
            "title": { "tag": "plain_text", "content": title },
            "template": template,
        },
        "body": { "elements": elements },
    })
}

/// `/help` card with one-tap command buttons.
#[must_use]
pub fn help_card() -> InteractiveCardSpec {
    InteractiveCardSpec {
        title: "使用帮助".to_owned(),
        subtitle: Some("常用命令".to_owned()),
        accent: CardAccent::Purple,
        body: [
            "**会话**",
            "- `/new` — 开始新会话，保留当前工作目录",
            "- `/resume` — 查看并恢复历史会话",
            "- `/stop` — 停止当前正在跑的任务",
            "",
            "**工作目录**",
            "- `/cd <绝对路径>` — 切换目录并重置会话",
            "",
            "**状态**",
            "- `/status` — 查看模型、队列和运行状态",
            "- `/info` — 查看 MCP、技能、工作区和会话",
            "- `/help` — 打开这张帮助卡",
            "",
            "**设置（仅私聊）**",
            "- `/config` — 改模型、沙箱和白名单，立即生效",
            "",
            "群聊和话题需要 @机器人。卡片按钮与输入命令效果相同。",
        ]
        .join("\n"),
        actions: vec![
            button("🆕 新会话", "new", None, "primary"),
            button("📊 状态", "status", None, "default"),
            button("🧩 环境", "info", None, "default"),
        ],
        form: None,
    }
}

/// `/config` form shown only in private chats.
#[must_use]
pub fn config_card(
    model: Option<&str>,
    effort: Option<&str>,
    sandbox: &str,
    approval: &str,
    allowed_groups: &[String],
    allowed_senders: &[String],
) -> InteractiveCardSpec {
    let groups = if allowed_groups.is_empty() {
        "_（无）_".to_owned()
    } else {
        allowed_groups
            .iter()
            .map(|id| format!("- `{id}`"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let senders = if allowed_senders.is_empty() {
        "_（无）_".to_owned()
    } else {
        allowed_senders
            .iter()
            .map(|id| format!("- `{id}`"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    InteractiveCardSpec {
        title: "运行设置".to_owned(),
        subtitle: Some("仅私聊，保存后立即生效".to_owned()),
        accent: CardAccent::Blue,
        body: format!(
            "改模型、推理强度、沙箱和审批策略。白名单用命令加减：\n`/config group add <chat_id>`\n`/config sender add <open_id>`\n\n**允许的群**\n{groups}\n\n**允许私聊的用户**\n{senders}"
        ),
        actions: Vec::new(),
        form: Some(ConfigFormSpec {
            model: model.unwrap_or_default().to_owned(),
            effort: effort.unwrap_or("medium").to_owned(),
            sandbox: sandbox.to_owned(),
            approval: approval.to_owned(),
            allowed_groups: allowed_groups.to_vec(),
            allowed_senders: allowed_senders.to_vec(),
        }),
    }
}

/// Confirmation after `/config` saved.
#[must_use]
pub fn config_saved_card(view: &ConfigFormSpec) -> InteractiveCardSpec {
    InteractiveCardSpec {
        title: "设置已保存".to_owned(),
        subtitle: Some("下一条消息开始生效".to_owned()),
        accent: CardAccent::Green,
        body: format!(
            "**模型** `{}`\n**effort** `{}`\n**sandbox** `{}`\n**approval** `{}`\n**群** {} 个\n**私聊用户** {} 个",
            empty_as_default(&view.model),
            empty_as_default(&view.effort),
            view.sandbox,
            view.approval,
            view.allowed_groups.len(),
            view.allowed_senders.len()
        ),
        actions: vec![button("再改一次", "config", None, "default")],
        form: None,
    }
}

/// Shown when the operator cancels the config card.
#[must_use]
pub fn config_cancelled_card() -> InteractiveCardSpec {
    InteractiveCardSpec {
        title: "已取消".to_owned(),
        subtitle: None,
        accent: CardAccent::Grey,
        body: "没有改任何设置。".to_owned(),
        actions: vec![button("打开设置", "config", None, "default")],
        form: None,
    }
}

/// `/status` troubleshooting card.
#[must_use]
pub fn status_card(snapshot: &StatusSnapshot) -> InteractiveCardSpec {
    let session = snapshot
        .session_id
        .as_deref()
        .map(|id| format!("`{}`", short_id(id)))
        .unwrap_or_else(|| "（未建立）".to_owned());
    let cwd = snapshot
        .cwd
        .as_deref()
        .map(|path| format!("`{path}`"))
        .unwrap_or_else(|| "（未设置）".to_owned());
    let model = snapshot.model.as_deref().unwrap_or("（默认）");
    let effort = snapshot.effort.as_deref().unwrap_or("（默认）");
    let group_line = match snapshot.group_allowed {
        Some(true) => "\n🛡 **当前群** 已在白名单",
        Some(false) => "\n🛡 **当前群** 未在白名单",
        None => "",
    };
    let topic_note = if snapshot.chat_mode == "topic" {
        " · 话题独立会话"
    } else {
        ""
    };
    let mut actions = vec![
        button("🆕 新会话", "new", None, "primary"),
        button("🔁 恢复会话", "resume", None, "default"),
        button("🧩 环境", "info", None, "default"),
    ];
    if snapshot.show_config {
        actions.push(button("⚙️ 改设置", "config", None, "default"));
    }
    InteractiveCardSpec {
        title: "运行状态".to_owned(),
        subtitle: Some(format!("{}{topic_note}", snapshot.chat_mode)),
        accent: if snapshot.scope_state.starts_with("failed") {
            CardAccent::Red
        } else if snapshot.active_run {
            CardAccent::Orange
        } else {
            CardAccent::Blue
        },
        body: format!(
            "**模型** `{model}` · **effort** `{effort}`\n**sandbox** `{}` · **approval** `{}`\n\n**范围状态** `{}`\n**工作目录** {cwd}\n**会话** {session}\n**进行中** {}\n**待消费附件** {}\n**出箱** 排队 {} · 失败 {} · 未确认 {}{group_line}",
            snapshot.sandbox,
            snapshot.approval,
            snapshot.scope_state,
            if snapshot.active_run { "是" } else { "否" },
            snapshot.pending_media,
            snapshot.outbox_pending,
            snapshot.outbox_failed,
            snapshot.outbox_uncertain,
        ),
        actions,
        form: None,
    }
}

/// `/info` environment card for MCP, skills, workspace, and sessions.
#[must_use]
pub fn info_card(snapshot: &InfoSnapshot) -> InteractiveCardSpec {
    let workspace = snapshot
        .workspace
        .as_deref()
        .map(|path| format!("`{path}`"))
        .unwrap_or_else(|| "（未设置）".to_owned());
    let mcp = if snapshot.mcp.is_empty() {
        "- （未发现或无法读取）".to_owned()
    } else {
        snapshot
            .mcp
            .iter()
            .map(|name| format!("- `{name}`"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let skills = if snapshot.skills.is_empty() {
        "- （未发现）".to_owned()
    } else {
        snapshot
            .skills
            .iter()
            .map(|(name, summary)| match summary {
                Some(summary) => format!("- **{name}** — {summary}"),
                None => format!("- **{name}**"),
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    let sessions = if snapshot.sessions.is_empty() {
        "- （还没有会话）".to_owned()
    } else {
        snapshot
            .sessions
            .iter()
            .map(|(id, status, current)| {
                if *current {
                    format!("- `{id}` · {status} · **当前**")
                } else {
                    format!("- `{id}` · {status}")
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    InteractiveCardSpec {
        title: "机器人环境".to_owned(),
        subtitle: Some("MCP · 技能 · 工作区 · 会话".to_owned()),
        accent: CardAccent::Purple,
        body: format!(
            "**工作区**\n{workspace}\n\n**MCP**\n{mcp}\n\n**Skills**\n{skills}\n\n**Sessions**\n{sessions}"
        ),
        actions: vec![
            button("📊 状态", "status", None, "default"),
            button("🔁 恢复会话", "resume", None, "default"),
            button("💡 帮助", "help", None, "default"),
        ],
        form: None,
    }
}

/// `/new` confirmation card.
#[must_use]
pub fn new_session_card(interrupted: bool, cwd: Option<&str>) -> InteractiveCardSpec {
    let cwd = cwd
        .map(|path| format!("当前目录：`{path}`"))
        .unwrap_or_else(|| "当前目录未设置。".to_owned());
    InteractiveCardSpec {
        title: "已开始新会话".to_owned(),
        subtitle: None,
        accent: CardAccent::Green,
        body: format!(
            "{}\n{cwd}\n\n下一条消息会在当前目录开启全新对话。",
            if interrupted {
                "已中断正在运行的任务，并归档当前会话。"
            } else {
                "已归档当前会话。"
            }
        ),
        actions: vec![
            button("🔁 恢复刚才的会话", "resume", None, "default"),
            button("📊 状态", "status", None, "default"),
        ],
        form: None,
    }
}

/// `/cd` result or usage card.
#[must_use]
pub fn cd_card(cwd: &str, reset: bool) -> InteractiveCardSpec {
    InteractiveCardSpec {
        title: if reset {
            "已切换工作目录".to_owned()
        } else {
            "当前工作目录".to_owned()
        },
        subtitle: None,
        accent: if reset {
            CardAccent::Green
        } else {
            CardAccent::Blue
        },
        body: if reset {
            format!("✓ 已切换到 `{cwd}`\n会话已重置，下一条消息会开启新对话。")
        } else {
            format!("当前目录：`{cwd}`\n用法：`/cd <绝对路径>` 或 `/cd ~/项目`")
        },
        actions: vec![
            button("🆕 新会话", "new", None, "default"),
            button("🔁 恢复会话", "resume", None, "default"),
        ],
        form: None,
    }
}

/// One `/resume` candidate shown as a button.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResumeEntry {
    pub selector: String,
    pub preview: String,
    pub detail: String,
    pub current: bool,
}

/// `/resume` picker card.
#[must_use]
pub fn resume_card(cwd: &str, entries: &[ResumeEntry]) -> InteractiveCardSpec {
    if entries.is_empty() {
        return InteractiveCardSpec {
            title: "恢复历史会话".to_owned(),
            subtitle: Some(cwd.to_owned()),
            accent: CardAccent::Grey,
            body: format!(
                "当前目录：`{cwd}`\n\n这里还没有可恢复的历史会话。先聊一轮，再用 `/resume` 回来。"
            ),
            actions: Vec::new(),
            form: None,
        };
    }
    let mut lines = vec![format!("当前目录：`{cwd}`"), String::new()];
    let mut actions = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        let marker = if entry.current { "  ← 当前" } else { "" };
        lines.push(format!(
            "**{}. {}**{marker}\n{}",
            index + 1,
            entry.preview,
            entry.detail
        ));
        if !entry.current {
            actions.push(button(
                &format!("▸ 恢复 #{}", index + 1),
                "resume.use",
                Some(entry.selector.as_str()),
                "primary",
            ));
        }
    }
    InteractiveCardSpec {
        title: "恢复历史会话".to_owned(),
        subtitle: Some(cwd.to_owned()),
        accent: CardAccent::Blue,
        body: lines.join("\n\n"),
        actions,
        form: None,
    }
}

/// `/resume` applied confirmation.
#[must_use]
pub fn resume_applied_card() -> InteractiveCardSpec {
    InteractiveCardSpec {
        title: "已恢复会话".to_owned(),
        subtitle: None,
        accent: CardAccent::Green,
        body: "已完成，请继续发送下一条消息。".to_owned(),
        actions: vec![button("📊 状态", "status", None, "default")],
        form: None,
    }
}

fn empty_as_default(value: &str) -> &str {
    if value.is_empty() {
        "（默认）"
    } else {
        value
    }
}

fn select_option(label: &str, value: &str) -> Value {
    json!({
        "text": { "tag": "plain_text", "content": label },
        "value": value,
    })
}

fn config_form(form: &ConfigFormSpec) -> Value {
    let effort = if form.effort.is_empty() {
        "medium"
    } else {
        form.effort.as_str()
    };
    json!({
        "tag": "form",
        "name": "config_form",
        "elements": [
            {
                "tag": "markdown",
                "content": "**模型**\n_留空 = 跟随 Codex 默认_",
            },
            {
                "tag": "input",
                "name": "model",
                "default_value": form.model,
                "placeholder": { "tag": "plain_text", "content": "例如 gpt-6-astra" },
            },
            {
                "tag": "markdown",
                "content": "**推理强度**",
            },
            {
                "tag": "select_static",
                "name": "effort",
                "initial_option": effort,
                "options": [
                    select_option("low", "low"),
                    select_option("medium", "medium"),
                    select_option("high", "high"),
                    select_option("xhigh", "xhigh"),
                ],
            },
            {
                "tag": "markdown",
                "content": "**沙箱**",
            },
            {
                "tag": "select_static",
                "name": "sandbox",
                "initial_option": form.sandbox,
                "options": [
                    select_option("只读", "read-only"),
                    select_option("工作区可写", "workspace-write"),
                    select_option("完全访问", "danger-full-access"),
                ],
            },
            {
                "tag": "markdown",
                "content": "**审批策略**",
            },
            {
                "tag": "select_static",
                "name": "approval",
                "initial_option": form.approval,
                "options": [
                    select_option("never", "never"),
                    select_option("on-request", "on-request"),
                    select_option("on-failure", "on-failure"),
                    select_option("untrusted", "untrusted"),
                ],
            },
            {
                "tag": "column_set",
                "flex_mode": "flow",
                "horizontal_spacing": "small",
                "columns": [
                    {
                        "tag": "column",
                        "width": "weighted",
                        "weight": 1,
                        "elements": [{
                            "tag": "button",
                            "name": "submit_btn",
                            "text": { "tag": "plain_text", "content": "保存" },
                            "type": "primary",
                            "form_action_type": "submit",
                            "behaviors": [{ "type": "callback", "value": { "cmd": "config.submit" } }],
                        }],
                    },
                    {
                        "tag": "column",
                        "width": "weighted",
                        "weight": 1,
                        "elements": [{
                            "tag": "button",
                            "name": "cancel_btn",
                            "text": { "tag": "plain_text", "content": "取消" },
                            "behaviors": [{ "type": "callback", "value": { "cmd": "config.cancel" } }],
                        }],
                    },
                ],
            },
        ],
    })
}

fn button(label: &str, cmd: &str, arg: Option<&str>, style: &str) -> CardButton {
    CardButton {
        label: label.to_owned(),
        cmd: cmd.to_owned(),
        arg: arg.map(ToOwned::to_owned),
        style: Some(style.to_owned()),
    }
}

fn markdown_block(content: &str) -> Value {
    json!({ "tag": "markdown", "content": content })
}

fn action_row(buttons: &[CardButton]) -> Value {
    json!({
        "tag": "column_set",
        "flex_mode": "flow",
        "background_style": "default",
        "columns": buttons.iter().map(|button| {
            json!({
                "tag": "column",
                "width": "weighted",
                "weight": 1,
                "elements": [{
                    "tag": "button",
                    "text": { "tag": "plain_text", "content": button.label },
                    "type": button.style.as_deref().unwrap_or("default"),
                    "width": "fill",
                    "behaviors": [{
                        "type": "callback",
                        "value": {
                            "cmd": button.cmd,
                            "arg": button.arg,
                        },
                    }],
                }],
            })
        }).collect::<Vec<_>>(),
    })
}

fn short_id(id: &str) -> String {
    let take = id.chars().take(8).collect::<String>();
    if id.chars().count() > 8 {
        format!("{take}…")
    } else {
        take
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_card_streaming_has_stop_button_and_header() {
        let card = render_run_card("正在改代码", RunCardPhase::Running);
        assert_eq!(card["schema"], "2.0");
        assert_eq!(card["config"]["streaming_mode"], true);
        assert_eq!(card["header"]["template"], "blue");
        let encoded = card.to_string();
        assert!(encoded.contains("⏹ 停止"));
        assert!(encoded.contains("正在改代码"));
        assert!(!encoded.contains("bridge_context"));
    }

    #[test]
    fn finished_run_card_drops_stop_and_turns_green() {
        let card = render_run_card("最终答案", RunCardPhase::Done);
        assert_eq!(card["config"]["streaming_mode"], false);
        assert_eq!(card["header"]["template"], "green");
        assert!(!card.to_string().contains("⏹ 停止"));
    }

    #[test]
    fn failed_run_card_is_red_and_keeps_reason_text() {
        let card = render_run_card("任务执行失败（附件未能处理）", RunCardPhase::Failed);
        assert_eq!(card["header"]["template"], "red");
        assert!(card.to_string().contains("任务执行失败（附件未能处理）"));
        assert!(!card.to_string().contains("⏹ 停止"));
    }

    #[test]
    fn status_card_shows_live_settings_and_hides_config_in_groups() {
        let card = render_interactive_card(&status_card(&StatusSnapshot {
            chat_mode: "group".to_owned(),
            cwd: Some("~/src".to_owned()),
            session_id: Some("thread-abcdefgh".to_owned()),
            active_run: false,
            scope_state: "idle".to_owned(),
            model: Some("gpt-6".to_owned()),
            effort: Some("high".to_owned()),
            sandbox: "workspace-write".to_owned(),
            approval: "never".to_owned(),
            pending_media: 2,
            outbox_pending: 1,
            outbox_failed: 0,
            outbox_uncertain: 3,
            group_allowed: Some(true),
            show_config: false,
        }));
        let encoded = card.to_string();
        assert!(encoded.contains("gpt-6"));
        assert!(encoded.contains("待消费附件"));
        assert!(encoded.contains("未确认"));
        assert!(encoded.contains("已在白名单"));
        assert!(encoded.contains("\"cmd\":\"info\""));
        assert!(!encoded.contains("\"cmd\":\"config\""));
    }

    #[test]
    fn info_card_lists_mcp_skills_workspace_and_sessions() {
        let card = render_interactive_card(&info_card(&InfoSnapshot {
            workspace: Some("~/proj".to_owned()),
            mcp: vec!["github".to_owned()],
            skills: vec![("review".to_owned(), Some("Code review".to_owned()))],
            sessions: vec![("abcd1234".to_owned(), "进行中".to_owned(), true)],
        }));
        let encoded = card.to_string();
        assert!(encoded.contains("~/proj"));
        assert!(encoded.contains("github"));
        assert!(encoded.contains("review"));
        assert!(encoded.contains("Code review"));
        assert!(encoded.contains("abcd1234"));
        assert!(encoded.contains("当前"));
    }

    #[test]
    fn omitted_subtitle_is_absent_not_null() {
        let card = render_interactive_card(&config_cancelled_card());
        assert!(card["header"].get("subtitle").is_none());
        assert!(!card["header"].to_string().contains("null"));
    }

    #[test]
    fn help_and_resume_cards_expose_callback_commands() {
        let help = render_interactive_card(&help_card());
        let encoded = help.to_string();
        assert!(encoded.contains("/new"));
        assert!(encoded.contains("\"cmd\":\"new\""));
        let resume = render_interactive_card(&resume_card(
            "/tmp/project",
            &[ResumeEntry {
                selector: "thread-abc".to_owned(),
                preview: "上次的修复".to_owned(),
                detail: "2 小时前 · 已归档".to_owned(),
                current: false,
            }],
        ));
        let encoded = resume.to_string();
        assert!(encoded.contains("resume.use"));
        assert!(encoded.contains("thread-abc"));
        assert!(encoded.contains("上次的修复"));
    }
}
