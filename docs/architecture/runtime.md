# Runtime、路由与策略架构

## 文件分层

| 文件 | 职责 |
| --- | --- |
| `intake.rs` | tenant namespace、durable register 和 retained event |
| `policy.rs` | owner/sender/group allowlist、mention、quoted-parent 和 workspace 准入 |
| `router.rs` | scope actor 生命周期、容量和全局并发 |
| `scope.rs` | 单 scope 状态机和 Codex turn |
| `commands.rs` | 第一阶段纯 parser/metadata |
| `context.rs` | turn-scoped opaque context/media capability |
| `quote.rs` | 单跳父消息解析和 fail-closed 状态 |
| `attachments.rs` | attachment cache 和 downloader |

## Scope actor 模型

每个 scope 只有一个 mailbox consumer。核心状态：

```text
Idle
  → Debouncing
  → WaitingPermit
  → StartingTurn
  → Running
  → Finalizing
  → Idle

任意阶段的确定性错误 → Failed/terminal cleanup
未知 turn/start 结果      → Uncertain，等待 epoch recovery
```

Actor 不允许两个 turn 同时写同一 Codex thread。

## Router 容量

容量分三层：

- actor 个数；
- 每 actor mailbox 条目；
- 每 mailbox 字节 semaphore。

全局 `active_turn_permits` 只限制实际执行，不限制 durable intake。拥塞时 fail closed 或保留
durable backlog，不能无界缓存。

## Policy fingerprint

scope row 保存 policy fingerprint。它代表影响执行安全的配置快照，例如 owner/sender/group
allowlist、allow roots、sandbox/network。恢复或复用 thread 时必须检查 fingerprint，不能在
策略变更后静默沿用旧安全上下文。

## Context registry

Resource key 不进入 prompt 或 durable DTO。Turn-scoped registry 发 opaque handle；工具读取
时再走附件缓存。群聊未触发媒体在 durable settlement 后丢弃描述符。引用解析失败使用稳定
Unavailable/Unauthorized/Deleted/Oversize/Unsupported 状态，不暴露父消息内容。

## Turn 事务顺序

1. 选择并 claim inbound；
2. 校验 policy/workspace；
3. 创建 Starting turn；
4. 绑定/创建 thread；
5. 获取附件和 lease；
6. `turn/start`；
7. 标记 Running；
8. 投影事件；
9. durable finalization；
10. terminal resolve 和 lease release。

任何调整都要检查 crash window：在第 N 步崩溃后，重启能否区分“未执行”“已执行”
和“结果未知”。

## Command 扩展

当前 `BridgeCommand` 是纯 parser，`ScopeCommand` 尚未包含 control variant。接入命令时：

- command 必须在进入 Codex 前截获；
- 状态变更命令与普通 inbound 在同 scope 串行；
- `/stop` 使用独立高优先级控制通道，不能排在长普通队列尾部；
- 所有回复进入 durable outbox；
- owner/admin gate 在模型外执行。

## 推荐测试

- `tests/runtime_intake.rs`；
- `tests/runtime_policy.rs`；
- `tests/runtime_scope.rs`；
- `tests/store.rs`；
- fake Codex 和 Lark stub 联合的应用装配测试。
