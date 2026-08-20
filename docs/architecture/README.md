# 开发架构手册

本目录面向维护者和代码贡献者。阅读顺序：

1. [总体架构](overview.md)
2. [CLI、配置与应用装配](app-cli-config.md)
3. [Codex 子系统](codex.md)
4. [Lark 子系统](lark.md)
5. [Runtime、路由与策略](runtime.md)
6. [Store 与恢复](store.md)
7. [Render 与 outbox](outbox-render.md)
8. [Attachment cache](attachments.md)
9. [测试与变更验收](testing.md)

## 共同设计原则

- 非幂等外部写不盲目重试；
- 先持久化事实，再执行不可逆副作用；
- 每个队列、批次、缓存和 payload 同时有 count/byte 上限；
- 同 scope 单写者，跨 scope 有界并发；
- 错误只携带静态分类、计数和长度；
- 生产装配、fake transport 和纯状态机分层；
- 不引入第二个 SQLite writer 或第二个 Codex stdin writer。

## 代码所有权

| 路径 | 责任 |
| --- | --- |
| `src/main.rs` / `src/cli.rs` / `src/config.rs` | 进程入口和配置 |
| `src/app.rs` | 生产组件装配和退出顺序 |
| `src/codex/` | app-server RPC 和生命周期 |
| `src/lark/` | 飞书协议、API 和 transport |
| `src/runtime/` | intake、策略、actor 和附件 |
| `src/store/` | durable state 和事务 |
| `src/render/` | 纯回复投影 |
| `src/outbox/` | 持久副作用投递 |
| `src/limits.rs` | 全局硬上限 |
