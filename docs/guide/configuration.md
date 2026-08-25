# 配置手册

bridge 使用严格 TOML 配置。未知字段、危险路径、空 owner 和不安全工作区会 fail closed。

## 配置位置

使用 `--config` 时读取指定文件；否则读取平台默认位置：

- Linux/macOS：`$XDG_CONFIG_HOME/lark-codex-bridge/config.toml`，未设置时使用
  `$HOME/.config/lark-codex-bridge/config.toml`；
- Windows：`%APPDATA%\lark-codex-bridge\config.toml`。

相对的数据库和附件目录以配置文件所在目录为基准解析。

## 完整示例

```toml
owners = ["ou_owner_open_id"]
allowed_senders = ["ou_member_open_id"]   # optional; default deny
allowed_groups = ["oc_chat_id"]           # optional; default deny
default_workspace = "/absolute/path/to/workspace"

[workspace]
allow_roots = ["/absolute/path/to/workspace"]
network_access = false

[concurrency]
active_turn_permits = 4
max_scope_actors = 256

[codex]
binary = "codex"
# codex_home = "/absolute/path/to/codex-home"
# model = "model-name"
sandbox = "workspace-write"
approval_policy = "never"

[paths]
database = "state/bridge.sqlite3"
attachment_cache = "state/attachments"
```

## 顶层字段

### owners

允许使用 bridge 的 owner `open_id` 列表。至少一个，重复值会去重。不要使用 bot 自身
`open_id`。Owner 可发起普通 turn，并独占 owner-only 控制命令。

### allowed_senders

按用户身份授权的普通调用者 `open_id` 列表。可选，默认拒绝。只授予普通 turn，不授予
owner-only 控制命令。

### allowed_groups

群 `chat_id` 白名单。可选，默认拒绝。仅该群内普通人类成员可发起普通 turn；群/话题仍
要求真实直接 @机器人（`@all` 不算）。白名单不授予控制命令。

非人类 sender（应用、机器人等）一律拒绝，任何 allowlist 都不例外。列表各有 256 条 /
32 KiB 上限，畸形 ID 拒绝加载；移除条目即撤销授权。不支持通配符、群名匹配或成员自动
同步。

### default_workspace

没有 scope 专属记录时使用的默认工作区。必须存在、为绝对路径、通过 allow root 检查，并且
不能是：

- 文件系统根；
- HOME 根；
- 系统目录；
- 临时目录；
- allow root 之外的路径。

## workspace

- `allow_roots`：允许选择的工作区根列表。子目录可以作为实际 cwd。
- `network_access`：传给 Codex workspace sandbox 的网络策略。

allow root 只是 bridge 的 cwd 准入边界，不替代 Codex sandbox。

## concurrency

- `active_turn_permits`：跨 scope 同时运行的 Codex turn 数。
- `max_scope_actors`：进程内最多保留的 scope actor 数。

两个值都受编译时硬上限保护。调大前应验证 app-server、内存和飞书 API 配额。

## codex

- `binary`：Codex CLI 可执行文件路径或命令名。
- `codex_home`：可选的独立 `CODEX_HOME`。
- `model`：可选模型覆盖。
- `sandbox`：`read-only`、`workspace-write` 或 `danger-full-access`。
- `approval_policy`：传给 app-server 的策略。当前建议 `never`，因为飞书审批卡尚未接线。

默认值是 `workspace-write` + `never`。

## paths

- `database`：SQLite 主数据库。不要放在网络文件系统或多个实例共享的位置。
- `attachment_cache`：专用内容缓存目录。首次打开会写入 marker、收紧权限并获取实例锁。

不要让两个 bridge 实例共用同一个数据库或附件缓存目录。

## 凭证来源

优先级：

1. 同时设置 `LARK_APP_ID`、`LARK_APP_SECRET`、`LARK_TENANT`；
2. `LARK_CREDENTIALS_FILE` 指定的文件；
3. 平台默认 `credentials.toml`。

`LARK_TENANT` 只能是 `feishu` 或 `lark`。三个环境变量必须同时设置；部分设置会直接报错。

推荐通过以下命令生成权限受限的凭证文件：

```bash
lark-codex-bridge lark auth register
```

## 配置变更

当前没有热更新。修改配置、凭证或工作区边界后应：

1. 停止 bridge；
2. 备份数据库和配置；
3. 执行 `lark auth check`、`codex probe` 和 `lark probe`；
4. 重新启动。
