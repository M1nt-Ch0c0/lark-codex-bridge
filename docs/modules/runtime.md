# Runtime 与路由模块功能手册

## 模块职责

Runtime 把已持久化的 Lark 事件变成有权限、有工作区、有顺序保证的 Codex turn，并将 Codex
事件交给 durable reply sink。

关联代码位于 `src/runtime/`。

## Durable intake

入站事件先进入 SQLite，再向内存队列暴露：

- message_id + tenant namespace 去重；
- payload 和资源描述受字节上限约束；
- enqueue 成功后才向 Lark transport 返回已接收；
- 内存队列满时事件仍保留在 store，不伪装成已处理。

## Access policy

当前策略：

- sender 必须是配置 owner；
- p2p 不要求 mention；
- group/topic 必须直接 @机器人；
- `@all` 不算直接 mention；
- 工作区必须通过 allow roots 和系统危险路径检查；
- policy fingerprint 随关键配置变化。

## Router

Router 按 scope 管理 actor：

- actor 数量和总内存有界；
- 新 scope 超过上限时拒绝，不驱逐正在运行的 actor；
- 不同 scope 共享全局 active-turn semaphore；
- shutdown 时先停止接收，再等待 actor 有限收口。

## Scope actor

每个 scope 内严格串行：

1. debounce 一小批消息；
2. claim durable inbound；
3. 解析工作区和 thread；
4. 创建 turn row；
5. 获取全局执行许可；
6. start/resume thread 并 start turn；
7. 消费事件并持久化 progress/final；
8. terminal 后 resolve inbound、turn 和附件 lease。

运行中的后续普通消息留给下一 turn，不 steer 当前 turn。

## 中断与状态

底层已有：

- 高优先级 interrupt seam；
- redacted `ScopeSnapshot`；
- outbox depth 查询；
- thread archive 和 turn interrupted 状态。

但 `/stop`、`/status`、`/new`、`/cd`、`/help` 尚未作为生产命令路由。

## 故障语义

- 明确未发送的 `turn/start` 可以安全失败；
- 结果未知的非幂等请求进入 uncertain；
- supervisor epoch 结束后才能收口依赖旧连接的 uncertain 资源；
- final 持久化成功后才把 inbound 标记完成。
