# 飞书命令与卡片

bridge 在进入 Codex 之前截获已识别的 slash command。卡片按钮会合成同一条命令，效果与手打相同。群聊和话题必须直接 @机器人；`@all` 不算。

完整解析表见 `src/runtime/commands.rs`。这里只写当前生产入口已经接线的行为。

## 常用命令

| 命令 | 谁能用 | 作用 |
| --- | --- | --- |
| `/help` | 已授权用户 | 打开帮助卡，带新会话 / 状态 / 环境按钮 |
| `/status` | 已授权用户 | 排障卡：热更新设置、范围状态、工作目录、会话短 ID、是否在跑、待消费附件、出箱排队/失败/未确认；群里显示当前群是否在白名单 |
| `/info` | 已授权用户 | 环境卡：工作区、本机 Codex MCP 名称、skill 名称与一句话摘要、当前 scope 的 sessions |
| `/new` | 已授权用户 | 归档当前会话并保留工作目录；进行中的 turn 会先中断 |
| `/stop` | 已授权用户 | 中断当前 turn |
| `/resume` | 已授权用户 | 列出或恢复同 scope 历史会话 |
| `/cd [path]` | owner | 查看或切换工作目录并重置会话 |
| `/config` | owner，仅私聊 | 改模型、effort、sandbox、approval；群/私聊白名单用命令加减 |
| `/threads` `/adopt` `/release` | owner | 显式顺序交接 persisted thread，见 [thread-adoption](../thread-adoption.md) |

`/reset` 是 `/new` 的别名。未知的 `/foo` 会当作普通用户输入发给 Codex，不会被静默丢弃。

## `/status`

排障用，不回显用户正文、完整 thread id 或本机绝对 home。工作目录里的 `$HOME` 前缀会收成 `~`。会话只显示前 8 个字符。

群聊不放「改设置」按钮。私聊可以点进 `/config`。两张卡都能进 `/info`。

出箱三列：

- **排队**：还没发出或正在重试；
- **失败**：已记为确定性失败；
- **未确认**：请求可能已到达飞书，但不能自动重发。

## `/info`

只展示名称，不展示 MCP command、参数、URL、token 或完整 home 路径。

读取来源：

- MCP：`CODEX_HOME/config.toml` 或 `~/.codex/config.toml` 的 `[mcp_servers.*]` 键名；
- Skills：Codex home 的 `skills/`，以及当前工作区的 `.codex/skills`、`.agents/skills`、`skills/`；
- Sessions：当前 scope 已持久化的 thread，最多 8 条。

本机没有 MCP 或 skill 时会写「未发现」，这是实话。skill 目录名可以是中文；摘要来自 `SKILL.md` 的 `description` 或一级标题，最长约 80 字。

## `/config`

只在私聊生效。保存后立即热更新，不必重启进程。卡片表单和下面的命令等价：

```text
/config model gpt-6-astra
/config effort high
/config sandbox workspace-write
/config approval never
/config group add oc_chat_id
/config sender add ou_open_id
```

白名单加减只改内存和（若已加载配置文件）原子写回 `config.toml`。群聊里使用 `/config` 会收到一句拒绝说明。

飞书审批卡仍未接线；当前建议 `approval = "never"`，避免 turn 卡在 Codex 本地审批。

## 失败通知

turn 失败不再只回「任务执行失败」。稳定分类会写进飞书通知和 `-v` 日志的 `failure=` 字段：

| 分类 | 用户可见文案 |
| --- | --- |
| `attachment` | 任务执行失败（附件未能处理） |
| `workspace` | 任务执行失败（工作目录无效） |
| `turn_start_rejected` | 任务未能启动（模型或请求被拒绝） |
| `turn_failed` | 任务执行失败（Codex 返回失败） |
| `connection_lost` | 任务执行结果未知，请重新发起 |
| `interrupted` | 任务已中断 |

已有进度卡时，终态会把同一张卡改成红/橙，并带上分类；没有进度卡就发短文本。正文、路径、完整 thread id 不会回显。

## 群聊上下文

引用只解析直接父消息一跳，不顺着 `parent_id` 递归。

- 引用会尽量补发送者显示名（需要通讯录读权限；失败则只留 open_id）；
- 合并转发会展开子消息，嵌套转发最多再展开一层，不再只写 `[forwarded messages]`；
- 交互卡尽量抽出可见文本；
- 话题群**第一次**介入（该 scope 还没有活跃会话）会按创建时间倒序拉最近最多 40 条上游消息，再按时间正序注入 `<topic_context>`；当前句和直接引用不重复注入。之后同一话题靠 Codex 会话自己记上文。

## 卡片约定

命令卡和运行卡都是 Card 2.0。没有副标题时不会写出 `subtitle: null`，避免飞书拒收。按钮 `value.cmd` 只接受白名单命令；无法识别的卡片回调会被 ACK 后忽略，不会当成普通聊天。
