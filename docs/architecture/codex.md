# Codex 子系统架构

## 文件分层

| 文件 | 职责 |
| --- | --- |
| `process.rs` | 版本探测、子进程 spawn、stdio 和退出 |
| `transport.rs` | 有界 JSONL framing |
| `rpc.rs` | request ID、pending map、双向 request/response |
| `protocol.rs` / `types.rs` | wire DTO 和兼容解析 |
| `client.rs` | typed thread/turn API、subscription router |
| `supervisor.rs` | epoch、重启、状态广播和 child ownership |

## 依赖方向

```text
supervisor
   └─ process ─ transport ─ rpc ─ client ─ typed events
                         └──── protocol/types
```

高层不能直接写 child stdin；所有 RPC 都通过唯一 `RpcPeer`/client 路由。

## RPC 语义

### 幂等性

`thread/start`、`thread/resume`、`turn/start` 是非幂等或所有权敏感操作：

- 客户端不做透明重试；
- pending 发送前失败可以分类为 definite-not-applied；
- 写入后响应丢失视为 uncertain；
- 上层必须等待 epoch 或权威状态恢复。

### Subscription

每个 thread route 有有界 mailbox，并记录：

- live subscription；
- active/pending turn；
- deferred notification buffer；
- retained projection；
- invalidation reason。

route 只有在无 live subscription、无 active turn、无 pending start 时才能释放。

### 双向 request

app-server 可向客户端发 server request。`ControlEventReceiver` 是唯一 owner，使用 request token
回答、拒绝或释放。当前上层审批 UI 未接线，不能静默批准。

## Supervisor 状态

核心状态：

- Starting；
- Ready { epoch, version, client }；
- Backoff/Degraded；
- Stopped。

epoch 是跨重启隔离 token。所有 subscription、turn 和 approval 请求都必须绑定 epoch。

## 兼容策略

- 版本输出严格；
- 当前支持窗口有限；
- open string enum 保留 unknown 值；
- 未知 notification 可投影为 Unknown；
- 关键字段缺失、顺序冲突或容量溢出时 fail closed。

未来 Schema 生成代码必须位于 wire namespace，通过 mapper 转为稳定 domain type，不能直接覆盖
`types.rs` 的核心模型。

## 推荐测试

- `tests/transport.rs`：framing/进程；
- `tests/rpc_duplex.rs`：RPC 双向语义；
- `tests/client_flow.rs`：thread/turn/subscription；
- `tests/supervisor.rs`：epoch/restart；
- `tests/protocol_fixtures.rs`：wire compatibility；
- `tests/codex_smoke.rs`：真实 app-server 门控。
