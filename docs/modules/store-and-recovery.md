# Durable store 与恢复模块功能手册

## 模块职责

Store 是 SQLite WAL 单写者，保存 intake、scope/thread/turn、outbox、receipt、attachment 和
lease 状态。业务模块通过有界 request channel 访问，不能直接持有第二个写连接。

关联代码位于 `src/store/`。

## 主要状态

### Inbound

- `Received`：已持久化、尚未 claim；
- `Claimed`：已归属某个 turn；
- terminal resolution：完成或带静态原因拒绝。

控制回复和拒绝通知会把确定性的 outbox key 与 terminal inbound 状态原子记录。当前去重
窗口由 `DEDUP_TTL` 限定（默认 7 天）：窗口内只要 inbound marker 或同 key 的 outbox 行
仍存在，重复 webhook 都不会产生第二次发送。terminal inbound 与已发送/失败 outbox 都超过
各自 retention、且两份证据都被清理后，极晚到达的同一 webhook 会按新事件处理；这里不承诺
无限期 tombstone。`uncertain_delivery` 不参与自动清理，必须由运维人员显式处置。

### Thread

- `Active`：当前 scope 使用；
- `Archived`：由未来 `/new` 或 `/cd` 退休，保留历史。

### Turn

- `Starting`；
- `Running`；
- `Completed`；
- `Failed`；
- `Interrupted`；
- `Uncertain`。

### Outbox

保存 pending/claimed/retry/terminal/uncertain 等发送生命周期，以及有序 watermark 和 receipt。

### Attachment

保存内容 hash、大小、最后使用时间和 turn lease；磁盘文件不是唯一事实来源。

## 写入模型

- 单独 writer task 串行事务；
- request channel 同时有条目数和字节预算；
- migration 使用 SQLite `user_version`；
- schema 版本过新时拒绝打开；
- Debug/error 不携带正文、secret 或敏感路径。

## 原子边界

关键事务包括：

- 注册 inbound 与去重；
- claim 消息并创建 turn；
- terminal final/outbox 与 turn/inbound resolve；
- outbox 失败、attempt 增加和后继 defer；
- attachment row 与 lease；
- rejection notice 与 inbound rejection。

## 恢复

启动时会：

- 打开 WAL store 并执行受控 migration；
- 预装有界数量的 `Received`；
- 校验/回收 terminal turn 和 attachment lease；
- 启动先执行一个 attachment reconcile 批次，runtime actor 接续到 EOF 并周期复扫。

outbox pump 同时拥有一个与传输连接独立的 retention task。它每次只提交一个有界批次；满批
会让出执行权后继续，writer 短暂满载会按短周期重试。因此断连或单次网络发送阻塞不会阻止
terminal inbound/outbox 释放容量。

当前限制：没有后台周期性 `Received` 全量重扫；极端积压可能需要后续批次或重启推进。

## 运维约束

- 不要手工编辑业务表；
- 备份时同时保留数据库、`-wal` 和 `-shm`，最好先有序退出；
- 不要让多个实例共享同一个 store；
- uncertain 行不能通过删除数据库“解决”。
