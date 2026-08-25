# 总体架构

## 目标

bridge 不是一次消息启动一次 Codex 命令，而是一个长期运行、可恢复的本地 actor 系统。
它把 Lark delivery、Codex execution 和 SQLite durability 分成独立故障域。

## 数据流

```text
Lark WebSocket
      │ frame / normalize
      ▼
DurableIntake ───────────────► SQLite inbox
      │ retained event
      ▼
Router ─► ScopeActor ─► Codex AppServerClient
               │             │ thread/turn events
               │             ▼
               └──────► ReplyProjector
                              │ durable operations
                              ▼
                         SQLite outbox
                              │
                              ▼
                         OutboxPump
                              │
                              ▼
                         Lark OpenAPI
```

附件旁路：

```text
Inbound resources ─► LarkResourceDownloader ─► AttachmentCache
                                              │
                                              ├─ hash file
                                              └─ SQLite row + turn lease
```

## 控制面与数据面

控制面：

- CLI 参数和 `BridgeConfig`；
- credentials；
- supervisor state/epoch；
- transport connection state；
- cancellation token 和 shutdown。

数据面：

- inbound event；
- scope mailbox；
- thread/turn event；
- projected reply；
- outbox row；
- attachment content/lease。

两者不能混入用户正文。控制状态可以记录分类和计数，数据内容只能在明确业务路径中出现。

## 并发域

- 一个 SQLite writer task；
- 一个 Codex RPC transport/router；
- 一个 Lark transport actor；
- 一个 outbox pump；
- 每个 scope 一个 actor；
- 全局 semaphore 限制 active turn；
- attachment cache 内部 mutex + OS 实例锁。

## 一致性原则

### Inbound

事件先写 SQLite，再对内存 runtime 可见。内存队列丢失不会抹掉 durable 事实。

### Codex

turn row 先进入 Starting，再发 `turn/start`。连接在响应前丢失时进入 Uncertain，不能创建
替代 turn 掩盖结果。

### Outbound

final/progress 先成为 outbox row，再由 pump 发送。只有权威 Lark receipt 才推进发送状态。

### Attachment

文件先完整安装，再创建 row/lease；GC 先删除 row，再删除文件。崩溃偏向可清理 orphan，
不产生有效 row 指向缺失文件。

## 当前架构边界

- 一个进程只服务一个 profile；
- app-server 由 bridge 独占启动；
- card callback/approval 尚未进入控制面；
- service manager、Web UI、meeting 和 Claude/provider adapter 不在当前架构中。
