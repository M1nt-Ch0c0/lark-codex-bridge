# 模块功能手册

本目录描述当前生产装配中的七个功能模块。每份手册包含能力、入口、可观察结果、限制和
关联代码。

| 模块 | 主要职责 |
| --- | --- |
| [CLI 与配置](cli-and-configuration.md) | 命令入口、配置、凭证和启动参数 |
| [Codex](codex.md) | app-server 生命周期、thread/turn、事件和中断 |
| [Lark](lark.md) | 凭证、OpenAPI、WebSocket、事件归一化 |
| [Runtime](runtime.md) | durable intake、策略、scope actor、路由与并发 |
| [Store 与恢复](store-and-recovery.md) | SQLite 状态机、去重、claim/resolve 和恢复 |
| [回复与 outbox](replies-and-outbox.md) | 进度/终答投影、持久发送、重试与 receipt |
| [附件](attachments.md) | 下载、内容寻址缓存、lease、GC 和 reconcile |

“底层 API 已存在”不等于“用户入口已完成”。例如命令 parser 和 interrupt seam 已存在，
但 slash command handler 尚未进入生产 runtime。
