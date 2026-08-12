# lark-codex-bridge

一个面向飞书 / Lark 的 Codex 本地桥接器。项目使用 Rust 重写，直接连接
`codex app-server`，专注于低资源占用、可靠会话、稳定流式回复和可恢复运行。

## 项目状态

当前处于早期开发阶段。已完成架构设计与 Codex app-server 基础层：有界 stdio
transport、RPC 握手与并发、typed thread/turn client、supervisor（重启退避、
永久错误 Degraded、优雅关闭）、结构化 `codex probe`，以及门控的真实 Codex
smoke 测试。正在继续实现飞书侧的业务能力。

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

设计规格见
[docs/superpowers/specs/2026-08-12-lark-codex-bridge-design.md](docs/superpowers/specs/2026-08-12-lark-codex-bridge-design.md)。

当前开发状态、未完成工作和 Agent 接管方式见
[docs/DEVELOPMENT_HANDOFF.md](docs/DEVELOPMENT_HANDOFF.md)。

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
