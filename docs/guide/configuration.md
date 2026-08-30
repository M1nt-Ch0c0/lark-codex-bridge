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
# model = "model-name"
# effort = "high"
sandbox = "workspace-write"
approval_policy = "never"

[codex.backend]
mode = "spawned_stdio"
binary = "codex"
# codex_home = "/absolute/path/to/codex-home"

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

- `model`：可选模型覆盖。
- `effort`：可选的非空字符串，原样传给每次 `turn/start`。省略时不发送该字段，继续使用
  Codex 自身默认；bridge 不限制枚举，以保持对未来档位的兼容。
- `sandbox`：`read-only`、`workspace-write` 或 `danger-full-access`。
- `approval_policy`：传给 app-server 的策略。当前建议 `never`，因为飞书审批卡尚未接线。

默认值是 `workspace-write` + `never`。

`[codex.backend]` 是带 `mode` 的严格表。默认/生成配置使用：

```toml
[codex.backend]
mode = "spawned_stdio"
binary = "codex"
# codex_home = "/absolute/private/codex-home"
```

可选的本地协议 sidecar 使用：

```toml
[codex.backend]
mode = "protocol_sidecar"
node_binary = "node"
sidecar_entrypoint = "/opt/lark-codex-bridge/codex-sidecar/index.cjs"
# codex_binary = "/absolute/path/to/exact/codex" # optional override
# codex_home = "/absolute/private/codex-home"
# codex_arguments = []
```

`protocol_sidecar` 只接受精确 Codex 0.149.0/0.151.0，要求七个 v1 capability 完全匹配，
没有运行中 fallback。省略 `codex_binary` 时使用 sidecar package-lock 精确固定的 Codex
0.151.0；显式
字段是用于另一份已审核 0.149.0/0.151.0 binary 的 override。`node_binary` 和显式
`codex_binary` 可使用 `PATH` 中的命令名；带路径分隔符的相对 `node_binary` / `codex_binary`
以及相对 `sidecar_entrypoint` / `codex_home` 都按配置文件目录解析。`codex_home` 必须是
已存在目录。`codex_arguments` 最多 8 个非空值，每个最多 1,024 字节，只用于
受审查 wrapper 的非 secret 前置参数。

配置文件不开放 frame、pending、握手或 shutdown 调参：当前固定为 33,554,432-byte frame、
448 pending、15 秒 bootstrap 和 5 秒 process grace。启动前用与配置相同的路径检查：

```bash
lark-codex-bridge codex sidecar-probe \
  --entrypoint /opt/lark-codex-bridge/codex-sidecar/index.cjs
```

`external_endpoint` 是另一个显式模式，但普通 mutation-driven `run` 仍对它 fail closed；配置与
边界见 [External Codex endpoint admission gate](../external-codex-endpoint-gate.md)。不同 mode
的字段不能混用，未知字段会拒绝加载。

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
3. 执行 `lark auth check`、`lark probe`，并按 backend 执行 `codex probe` 或
   `codex sidecar-probe`；
4. 重新启动。
