# Store 与恢复架构

## 单写者

`StoreHandle` 把所有写请求发送给一个 writer task。这样保证：

- 事务顺序可推理；
- 不出现业务模块之间的 SQLite 写锁竞争；
- request channel 可以统一实施 count/byte backpressure；
- shutdown 可以显式 flush 和 join。

禁止新增第二个长期写连接绕过 `StoreHandle`。

## Schema 域

| 域 | 事实 |
| --- | --- |
| inbound/dedup | 外部消息是否登记、claim、resolve |
| scopes | cwd、policy fingerprint、更新时间 |
| threads | scope 到 Codex thread 的 active/archive 映射 |
| turns | start/run/terminal/uncertain 和外部 ID |
| outbox/receipt | 外部副作用、顺序、attempt 和权威回执 |
| attachments/leases | hash 内容与 turn 生命周期 |

schema migration 由 `schema.rs` 和 `PRAGMA user_version` 控制。数据库版本高于代码时拒绝打开。

## 请求预算

请求大小在进入 writer channel 前估算。大型 payload 不允许依靠 channel 条目数“假装有界”。
新增 store API 时必须：

1. 定义 request byte 估算；
2. 在 clone 大对象前申请预算；
3. 使用静态错误分类；
4. 为 channel 满、writer 关闭和事务失败写测试。

## 事务设计

优先把互相依赖的状态变化放入一个事务。例如 outbox 重试必须同时：

- 更新当前 attempt/state；
- 设置 retry time；
- defer 所有后继；
- 更新有序 watermark。

拆成多个事务会产生后继越过失败行的 crash window。

## Recovery 边界

启动恢复应始终是有界批次，并允许重复调用收敛：

- `Received` 预装；
- Starting/Running/Uncertain turn 分类；
- terminal lease 清理；
- outbox claimed row re-park；
- attachment reconcile。

不要在启动线程中扫描无界数据库或目录。需要全量收敛时，增加游标/批次和周期调度。

## 数据隐私

数据库确实保存业务必需的消息和路径，因此文件权限和备份同样敏感。日志/Debug 层不得把
这些列值复制出去；测试 fixture 也不要包含真实用户数据。

## 推荐测试

`tests/store.rs` 应覆盖：

- migration；
- request capacity；
- atomic claim/resolve；
- duplicate event；
- uncertain；
- outbox ordering；
- attachment lease；
- shutdown 和 writer disappearance。
