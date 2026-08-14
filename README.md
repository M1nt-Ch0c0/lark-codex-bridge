# lark-codex-bridge

一个面向飞书 / Lark 的 Codex 本地桥接器。项目使用 Rust 重写，直接连接
`codex app-server`，专注于低资源占用、可靠会话、稳定流式回复和可恢复运行。

## 项目状态

当前版本是早期 alpha，已经可以启动常驻 `run` 运行时做最小试用，但不应视为可投入
生产的飞书机器人。当前可运行链路包括：

- Codex app-server 的有界 stdio transport、RPC broker、typed thread/turn client、
  长驻 supervisor、thread 复用、`codex probe` 和门控的真实 Codex smoke；
- Rust 原生飞书/Lark 凭证登记、OpenAPI、WebSocket transport、事件归一化、
  `lark probe` 和门控的真实 Lark smoke；
- SQLite WAL 单写者 store、持久 inbox/outbox、去重、owner gate、安全工作区策略、
  scope actor、同 scope 串行 turn 和不同 scope 的有界并发；
- 延迟进度卡、独立最终回复、重试/receipt/uncertain delivery，以及终态先持久化再收口；
- 图片 `localImage` 和普通文件结构化路径输入、内容寻址缓存、turn lease、GC 与启动校验；
- 完整应用装配和 `run --config`：飞书消息 → Codex turn → 飞书进度/终答。

尚未接线的是 slash command handler、Codex 审批卡、服务管理和完整故障注入/恢复。
`/stop`、`/status` 按当前最小试用范围明确暂缓；`/new`、`/cd`、`/help` 目前也只有
解析与 help 元数据，还未进入运行时。启动时会预装有界的 `Received` 行，但尚无周期性
重扫。真实 Lark smoke 是显式门控验收项；未运行或只看到 skip 都不算通过。

## 最小试用

前提：本机已安装并登录受支持的 `codex-cli 0.146.x`，飞书/Lark 应用机器人已创建并
加入目标会话，并准备好 owner 的 `open_id` 与一个允许 Codex 操作的安全工作区。

先登记凭证：

```bash
cargo run --locked -- lark auth register
# 或已有应用；secret 建议只放环境变量，避免进入 shell history
LARK_APP_SECRET=… cargo run --locked -- lark auth register --app-id cli_… --tenant feishu
```

创建配置文件，例如 `config.toml`。相对的数据库和缓存路径以配置文件所在目录为基准：

```toml
owners = ["ou_owner_open_id"]
default_workspace = "/absolute/path/to/workspace"

[workspace]
allow_roots = ["/absolute/path/to/workspace"]
network_access = false

[codex]
binary = "codex"
sandbox = "workspace-write"
approval_policy = "never"

[paths]
database = "state/bridge.sqlite3"
attachment_cache = "state/attachments"
```

配置会拒绝相对工作区、文件系统根、HOME 根、系统目录、临时目录以及 allow root 外的
路径。首次试用前建议分别检查两侧连接：

```bash
cargo run --locked -- codex probe
cargo run --locked -- lark auth check
cargo run --locked -- lark probe
```

启动常驻桥接器：

```bash
cargo run --locked -- run --config /absolute/path/to/config.toml
```

私聊可直接发消息；群聊和话题需要直接 @机器人。按 `Ctrl-C` 结束。当前真实飞书的
“发消息 → Codex 回答 → 飞书收到回复”验收由操作者手动执行。

## Codex 环境检查

```bash
cargo run --locked -- codex probe
```

`codex probe` 会真实启动 `codex app-server --listen stdio://` 并完成 initialize
握手，输出单个 JSON 对象，只包含 supported version、initialize user agent、
platform family/OS 和 epoch；不包含 Codex home、账户身份、token 或环境变量。

真实端到端 smoke 需要已认证的 Codex 账户，并按环境变量门控：

```bash
CODEX_E2E=1 cargo test --test codex_smoke --locked -- --ignored --nocapture
```

## 飞书 / Lark 接入与检查

登记应用凭证（扫码注册新 PersonalAgent 应用，或登记已有 App ID/Secret）：

```bash
cargo run --locked -- lark auth register
cargo run --locked -- lark auth register --app-id <id> --tenant <feishu|lark>   # secret 从 LARK_APP_SECRET 读取
cargo run --locked -- lark auth check
```

凭证也可用环境变量提供（优先级高于凭证文件）：`LARK_APP_ID`、`LARK_APP_SECRET`、
`LARK_TENANT`（`feishu|lark`）。`lark auth check` 只输出 tenant、bot 名称和 bot open_id。

`lark probe` 用已存凭证换取 tenant token、查询 bot 信息、拉取 WebSocket endpoint
并完成一次真实 ping/pong 往返，输出单个脱敏 JSON 对象（tenant、botName、botOpenId、
endpointHost、pingIntervalSecs、elapsedMs）；绝不输出 secret、token 或完整 endpoint URL。
缺凭证、永久认证失败或超时均以非零退出并给出可操作的诊断。

真实飞书/Lark 端到端 smoke 需要应用凭证和一个机器人已加入的会话，并按环境变量门控
（未设置时测试打印 skip 原因并成功退出，skip 不算证据）：

```bash
LARK_E2E=1 LARK_E2E_APP_ID=… LARK_E2E_APP_SECRET=… LARK_E2E_TENANT=feishu LARK_E2E_CHAT_ID=oc_… \
  cargo test --test lark_smoke --locked -- --ignored --nocapture
```

设计规格见
[docs/superpowers/specs/2026-08-12-lark-codex-bridge-design.md](docs/superpowers/specs/2026-08-12-lark-codex-bridge-design.md)。

仓库只跟踪稳定的产品说明和架构规格。实施计划、实时进度、Agent 接管记录和临时
测试证据属于本地开发材料，不发布到 Git。

## 目标

- 长期托管一个 `codex app-server`，避免每轮启动 `codex exec`。
- 使用 Rust 原生实现飞书长连接、OpenAPI、事件归一化和消息发送。
- 保留原项目的核心聊天、会话、工作区、附件、卡片和命令能力。
- 用有界队列、持久 outbox、幂等处理和显式恢复状态提高稳定性。
- 不支持 Claude、Web UI 和会议功能。

## 来源与许可证

本项目参考
[lark-coding-agent-bridge](https://github.com/zarazhangrui/lark-coding-agent-bridge)
的用户可见行为，但使用独立仓库、独立 Git 历史和全新 Rust 实现，并非 fork。

本项目采用 [MIT License](LICENSE)。
