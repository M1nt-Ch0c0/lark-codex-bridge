# Render 与 outbox 架构

## 分层

`ReplyProjector` 是纯状态机，不执行网络或数据库 I/O。

`OutboxReplySink` 把 projector 输出转换为确定性 `NewOutboxRow`。

`OutboxPump` claim row、调用 `LarkApi`、分类结果并写 receipt。

```text
AppServerEvent
    ↓
ReplyProjector
    ↓ ProjectedReply / Progress
OutboxReplySink
    ↓ durable row
SQLite
    ↓ claim
OutboxPump
    ↓
LarkApi
```

## Projector 状态

有界状态包括：

- 当前 item delta buffer；
- progress buffer；
- 已显示内容；
- 最近完成 item ID；
- 最近一次 progress checkpoint。

delta 没有 phase，因此不能直接显示。只有 item completed 后才能把 commentary 作为 progress，
或把 final 保留到 terminal。

## Durable operation

Outbox payload 有显式版本和操作类型。修改 wire schema 时：

- 旧版本必须可读或明确拒绝；
- 未知 operation 不得执行；
- idempotency key 由业务事实确定，不能使用重试次数；
- payload 和所有字符串受大小上限保护。

## 顺序

同一逻辑回复的 progress/create/update/final 必须保持 store ID 顺序。前驱 deferred 时，
后继不能被其他 poll cycle claim。新增批量优化不能破坏这一条件。

## Delivery 分类

发送结果分为：

- success + authoritative message_id；
- retryable definitive failure；
- permanent definitive failure；
- uncertain delivery。

HTTP 200 但缺少可用 message_id 不是成功。transport disconnect 前尚未发出的 row 可以 re-park；
已经发出但没收到权威响应的写不能自动重试。

## Shutdown

pump 停止时：

- 当前原子 store 操作完成；
- 尚未发送的 claimed tail 返回 pending；
- 不会增加无实际发送的 attempt；
- store 在 pump join 后关闭。

## 推荐测试

- `tests/reply_projector.rs`：纯投影契约；
- `tests/outbox.rs`：ordering/retry/uncertain；
- `tests/store.rs`：outbox transaction；
- `tests/lark_api.rs`：receipt shape。
