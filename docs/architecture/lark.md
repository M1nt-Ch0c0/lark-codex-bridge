# Lark 子系统架构

## 文件分层

| 文件 | 职责 |
| --- | --- |
| `config.rs` | tenant endpoint 和 brand |
| `credentials.rs` | env/file credential store |
| `http.rs` | 受控 HTTP client、响应大小和错误分类 |
| `token.rs` | tenant token 缓存/刷新 |
| `api.rs` | typed OpenAPI boundary |
| `frame.rs` / `fragments.rs` | protobuf frame 和分片 |
| `transport.rs` | WebSocket actor、ping/pong、重连 |
| `normalize.rs` | raw event → stable `InboundEvent` |
| `bridge.rs` | transport + normalizer + bounded intake channel |
| `register.rs` | PersonalAgent 设备授权 |

## Inbound pipeline

```text
WS bytes
  → Frame decode
  → Fragment assembly
  → payload JSON
  → Normalizer
  → DurableIntake handler
  → RetainedInbound + ACK
```

ACK 必须反映 durable intake 结果。事件尚未进入 SQLite 时，不能向上游报告业务成功。

## Normalizer 边界

Normalizer 做：

- 必填字段和消息类型解析；
- bot 自身 mention 判定；
- chat mode 查询和有界缓存；
- thread_id 单次回填；
- scope key；
- resource descriptor。

Normalizer 不做：

- owner/sender 授权；
- workspace 选择；
- Codex prompt 策略；
- 附件磁盘写入；
- outbox 投递。

## Transport 状态

Transport 广播连接状态供 outbox 判断是否可发送。断线时：

- 停止取新的外部副作用；
- 释放尚未发送的 claimed outbox tail；
- 保留 durable inbound/outbox；
- 按有界 backoff 重连。

## 错误分类

`LarkError` 区分：

- permanent auth；
- retryable network/service；
- protocol；
- capacity；
- shutdown。

分类必须无 secret、token、正文或完整 endpoint。

## Card ingress

目前 `MessageType::Card` 被 ACK 为 unsupported。加入 callback 时必须先建立：

- payload shape 和大小上限；
- HMAC/nonce/expiry；
- sender/scope/policy fingerprint 绑定；
- 重放拒绝；
- 到 runtime control queue 的单一入口。

不能直接把任意 card value 当成 slash command。

## 推荐测试

- `tests/lark_frame.rs` / `lark_transport.rs`；
- `tests/lark_api.rs`；
- `tests/lark_normalize.rs`；
- `tests/lark_bridge.rs`；
- `tests/lark_register.rs`；
- `tests/lark_smoke.rs`（真实门控，skip 不算证据）。
