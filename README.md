# lark-codex-bridge

一个面向飞书 / Lark 的 Codex 本地桥接器。项目使用 Rust 重写，直接连接
`codex app-server`，专注于低资源占用、可靠会话、稳定流式回复和可恢复运行。

## 项目状态

当前版本是早期 alpha，尚未提供完整的常驻 `run` 运行时，不应视为可投入生产的
飞书机器人。已经实现的基础能力包括：

- Codex app-server 的有界 stdio transport、RPC broker、typed thread/turn client、
  supervisor、`codex probe` 和门控的真实 Codex smoke；
- Rust 原生飞书/Lark 凭证登记、OpenAPI、WebSocket transport、事件归一化、
  `lark probe` 和门控的真实 Lark smoke；
- SQLite WAL 单写者 store、持久入站收件箱、去重、访问策略，以及初步的
  scope router/actor 和连续 turn 排队。

仍在实现的 alpha 能力包括命令、回复投影与持久 outbox、附件缓存、完整应用装配、
崩溃恢复和端到端运行时验证。真实 Lark smoke 是显式门控的验收项；未提供凭证、
未运行或仅观察到 skip 都不算通过。

## Codex 环境检查

```bash
cargo run -- codex probe
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
cargo run -- lark auth register
cargo run -- lark auth register --app-id <id> --tenant <feishu|lark>   # secret 从 LARK_APP_SECRET 读取
cargo run -- lark auth check
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
