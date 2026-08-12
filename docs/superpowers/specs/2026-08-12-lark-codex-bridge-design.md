# lark-codex-bridge 设计规格

- 日期：2026-08-12
- 状态：已批准
- 参考基线：`zarazhangrui/lark-coding-agent-bridge@e5d3ce5`
- 首个支持的 Codex 版本：`codex-cli 0.146.x`

## 1. 目的

`lark-codex-bridge` 是一个新的 Rust 项目，用于把飞书 / Lark 消息连接到本机
Codex。它回收参考项目的核心用户体验，同时解决参考实现中每轮启动
`codex exec --json`、事件形态漂移、无界等待、回复竞态和恢复状态不明确等问题。

项目必须满足以下顶层要求：

1. 使用一个长期运行的 `codex app-server`，通过 stdio JSONL 上的双向 JSON-RPC
   管理 thread、turn、流式事件、取消和审批。
2. 使用 Rust 原生实现飞书长连接和 OpenAPI，不依赖 Node sidecar。
3. 同一 scope 内严格串行，不同 scope 可在有界全局并发下并行。
4. 对飞书重复投递、桥接器重启、Codex 子进程退出和消息发送失败有明确且可观测的恢复语义。
5. 不支持 Claude、Web UI 和会议功能。
6. 使用独立 public 仓库和全新 Git 历史，不 fork 参考项目。

## 2. 设计原则

- **终态优先**：Codex 的 `item/completed` 和 `turn/completed` 是权威状态，delta
  只用于低延迟展示。
- **有界资源**：所有输入队列、RPC 表、事件邮箱、待审批集合和出站队列都有上限。
- **不伪造确定性**：断线时未确认的写请求标为 `uncertain`，不会盲目重试并声称成功。
- **单一所有者**：每个可变资源由一个 actor 或单写者任务拥有，减少锁和交叉竞态。
- **安全默认值**：个人模式、owner-only、群聊需 @、`workspace-write`、审批 fail-closed。
- **渐进兼容**：协议核心字段强类型，新增字段宽松保留，未知通知计数后忽略。
- **可测量优化**：低负载和高效率通过与参考实现的基准对比证明，不靠主观判断。

## 3. 范围

### 3.1 第一阶段：可用 alpha

- 扫码注册 PersonalAgent，或使用已有 App ID / App Secret。
- 飞书和 Lark 国际版长连接。
- 私聊、群聊 @ 和话题消息；话题使用独立 scope。
- 消息 ID 去重、600ms 短时合批、运行期间排队到下一 turn。
- Codex thread 创建、持久映射、恢复和 `/new`。
- `turn/start`、`turn/interrupt`、流式 agent 消息、命令摘要和最终状态。
- 文本、图片和普通文件输入。
- 延迟创建的进度卡和独立最终回复。
- `/new`、`/stop`、`/status`、`/cd`、`/help`。
- owner-only 默认访问控制和安全工作区校验。
- 单 profile 前台运行、结构化脱敏日志和优雅退出。

### 3.2 第二阶段：核心功能完整度

- `/ws list|save|use|remove`。
- `/resume`、`/timeout`、`/doctor`、`/reconnect`。
- `/invite`、`/remove` 和管理员管理。
- 引用消息、首次话题历史和合并转发三态处理。
- Codex 命令执行、文件修改和权限请求审批卡片。
- profile 隔离及 `lark-cli` profile 环境绑定。
- systemd、launchd 和 Windows Task Scheduler 服务管理。

### 3.3 第三阶段：受控扩展

- 云文档评论入口。
- personal/team 部署模式。

这两项必须在入口权限、工作区隔离、审批和崩溃恢复通过安全审查后启用；在此之前保持关闭，不能以兼容旧行为为理由绕过访问控制。

### 3.4 明确不做

- Claude 或通用多 agent adapter。
- Web UI、HTTP 管理后台和 React 前端。
- 飞书会议机器人、会议转录和会议总结。
- 跨 profile 的远程 `/ps`、`/exit` 主机进程管理。
- 在首版中使用 app-server experimental API。

## 4. 系统架构

单个 Rust binary 承载控制面和数据面，并托管一个 Codex 子进程：

```text
Feishu/Lark WebSocket
        │
        ▼
LarkTransport ──► InboundRouter ──► ScopeActor ──► CodexClient
        │               │                │              │
        │               │                │              ▼
        │               │                │       codex app-server
        │               │                │
        ▼               ▼                ▼
    LarkApi         SQLite Store      ReplyProjector
        ▲                                │
        └──────────── Reliable Outbox ◄──┘
```

### 4.1 crate 与模块边界

首版采用单 crate，等公共接口稳定后再决定是否拆 workspace，避免过早模块化。

- `app`：启动顺序、配置装配、信号处理和优雅关停。
- `config`：配置 schema、默认值、验证、秘密引用和原子更新。
- `lark::transport`：WebSocket bootstrap、protobuf frame、心跳、重连和分片合并。
- `lark::api`：tenant token 缓存、消息/卡片、资源下载和应用身份 API。
- `lark::normalize`：将原始事件转换为稳定的内部 `InboundEvent`。
- `codex::supervisor`：子进程、版本门禁、连接 epoch、重启和关停。
- `codex::transport`：stdout reader、stdin writer 和限长 JSONL framing。
- `codex::rpc`：请求 ID、pending RPC、server request 和超时。
- `codex::protocol`：稳定 RPC 子集、开放枚举和原始扩展字段。
- `runtime::router`：scope 解析、actor 生命周期和全局并发许可。
- `runtime::scope`：单 scope 状态机、合批、turn 和命令串行化。
- `runtime::approval`：审批状态、签名回调和一次性决策。
- `render`：进度视图、终答、错误、邮箱审计掩码和卡片投影。
- `store`：SQLite migration、事务和专用写任务。
- `outbox`：可靠发送、重试、收据和幂等键。
- `cli`：`run`、`init`、`status`、`doctor` 与后续 service 命令。

## 5. 飞书传输

### 5.1 长连接

实现与官方 SDK 一致的稳定路径：

1. `POST /callback/ws/endpoint`，使用 App ID / Secret 获取临时 WebSocket URL
   和服务端连接参数。
2. 建立 binary WebSocket。
3. 使用 protobuf `Frame` 解码 control/data frame。
4. 对 `sum > 1` 的 frame 按 `message_id` 和 `seq` 有界重组。
5. 对 event/card payload 完成处理后，在原 frame 上返回状态响应。
6. 按服务端参数发送 ping，接收 pong 并更新连接配置。
7. 网络失败使用指数退避和 jitter 重连；认证类永久错误进入 degraded 状态，不无限重试。

分片缓存有总字节、单消息字节、分片数量和 5 秒 TTL 限制。重复或越界分片被拒绝并记录协议异常。

### 5.2 入站归一化

内部消息至少包含：

- `event_id`、`message_id`、`chat_id`、`sender_id`；
- `chat_type`、`thread_id`、`root_id`、`reply_to_message_id`；
- 文本内容、mention、资源描述和创建时间；
- 原始消息类型以及必要的受限原始字段。

scope 规则：

- 私聊/普通群消息：`im:<chat_id>`；
- 话题消息：`im:<chat_id>:thread:<thread_id>`；
- 后续评论功能：`doc:<file_token>`。

`thread_id` 缺失但群已知为话题模式时，通过消息查询回填；无法回填则按普通群回复，记录降级原因。

### 5.3 去重与确认

先在 SQLite 事务中登记 `(tenant, event_id/message_id)`，再进入业务队列。状态包括
`received`、`accepted`、`completed` 和 `rejected`。相同事件在 TTL 内不会再次启动 Codex。

WebSocket 回执只代表 bridge 已持久接收，不代表 Codex 已完成。业务失败通过飞书回复和日志呈现，不要求平台重投。

## 6. Codex app-server 客户端

### 6.1 连接和版本

启动时执行 `codex --version`，首版接受经过 CI 验证的 `0.146.x`。随后启动
`codex app-server --listen stdio://`，固定继承用户选择的 `CODEX_HOME`。

每个连接执行：

1. 发送唯一的 `initialize` 请求，`clientInfo.name = "lark_codex_bridge"`；
2. 等待对应 response；
3. 发送 `initialized` notification；
4. 完成健康检查后进入 `Ready`。

不设置 `experimentalApi`。初始化前到达的通知可以缓存少量全局状态，但不会路由给 scope。

### 6.2 稳定 RPC 子集

- thread：`thread/start`、`thread/resume`、`thread/read`、`thread/unsubscribe`；
- turn：`turn/start`、`turn/steer`、`turn/interrupt`；
- events：`thread/*`、`turn/*`、`item/*`、`error`、`warning`、token usage；
- approvals：command execution、file change、permissions；
- account：只读登录状态检查，登录流程由 CLI 明确触发。

`thread/start.sandbox` 使用当前 schema 的 kebab-case 值，
`turn/start.sandboxPolicy.type` 使用 camelCase。该差异必须由真实 binary smoke test 覆盖。

### 6.3 transport 与 RPC broker

- stdout reader 只负责限长读取、解析和分类，不等待飞书网络 I/O。
- stderr 单独进入结构化日志，永不混入协议流。
- stdin 只有一个 writer，高优先队列处理审批响应、中断和认证刷新。
- client request ID 包含连接 epoch，重启后不复用。
- pending RPC 保存 method、deadline、epoch 和 oneshot sender。
- server request ID 是 opaque string/integer，原样回传。
- 未知、重复和迟到响应只增加 drift 指标，不导致进程崩溃。

### 6.4 事件路由

事件先按 `threadId` 进入有界 mailbox，再由 scope actor 消费。文本和命令 output delta
可以按 `(thread, turn, item, method)` 合并；response、server request、item terminal 和
turn terminal 不可丢弃。

`turn/start` response 和 `turn/started` notification 可能任意顺序到达，使用 turn ID
幂等合并。`item/completed` 覆盖同 item 的增量状态。

## 7. scope actor 与并发

每个 scope 最多有一个 `ScopeActor`，它拥有：

- 当前工作区和策略指纹；
- 当前 Codex thread ID；
- active turn ID 和状态；
- 600ms 合批缓冲；
- 运行期间的下一批消息；
- 进度投影和待审批集合。

状态为：

```text
Idle → Debouncing → WaitingPermit → StartingTurn → Running → Finalizing → Idle
                                      │              │
                                      └── Failed ◄───┘
```

全局 semaphore 限制 active turns。拿到许可后必须重新检查消息年龄、访问权限、工作区和策略指纹，防止排队期间授权变化。

运行中的普通消息进入下一批，不默认使用 `turn/steer`，以保持清晰的用户轮次边界。
`turn/steer` 只留给显式“追加当前任务”能力。中断后必须等到 `turn/completed` 或超时恢复完成，旧 turn 仍活跃时不能启动同 scope 新 turn。

## 8. 会话和持久化

SQLite 使用 WAL、foreign keys 和单写者任务。首版表包含：

- `inbound_events`：去重与处理状态；
- `scopes`：scope、cwd、策略指纹和更新时间；
- `threads`：scope 到 Codex thread 的 active/archived 映射；
- `turns`：client message ID、Codex turn ID、状态和 uncertain 标志；
- `outbox`：可靠出站消息、幂等键、尝试次数、下次重试时间和飞书 receipt；
- `approvals`：请求 epoch、ID、scope、turn、期限和决策；
- `workspace_aliases`：第二阶段工作区别名；
- `callback_nonces`：签名回调一次性 nonce 和过期时间。

App Secret 和 Codex 凭据不写入业务数据库。App Secret 由环境变量、权限为 `0600`
的配置文件或系统 keyring 提供；日志只显示引用来源。

会话键由 `scope + canonical cwd + policy fingerprint` 构成。工作区或安全策略改变时不自动复用旧 thread。

## 9. 回复状态机

参考项目最近修复的以下行为被视为硬性契约：

1. 最后一条 agent message 作为独立终答，不混入过程流。
2. final-only 回合不创建进度卡。
3. clean empty 回合不创建、发送或撤回占位卡。
4. 进度流失败不能吞掉终答。
5. 已流式展示的文字在没有独立终答时不能再次发送。
6. 只有获得非空飞书 `messageId` 才能把最终回复标记为 delivered。

`ReplyProjector` 汇总 reasoning、命令、文件修改、普通 agent message 和 final answer。
第一次出现终局后仍可见的过程内容时才向 outbox 创建进度消息。更新频率和字符阈值均有限制。

最终回复首先写入 outbox，再异步发送。相同 `turn_id + final` 使用固定幂等键，重启后继续重试而不创建第二条业务记录。若飞书 API 缺少幂等能力且第一次请求结果未知，则记录 `uncertain_delivery`，不自动无界重发。

所有 agent 生成的字符串在出站边界执行邮箱审计掩码，例如
`user@example.com` → `user[at]example.com`，避免飞书租户审计拒绝整条回复；包名、版本和普通 mention 不应被误伤。

## 10. 附件

- 在下载前校验声明类型、数量和平台提供的大小。
- 流式下载时同时计算 SHA-256，并强制单件和批次总字节上限。
- 使用随机临时名、`fsync` 和原子 rename 写入内容寻址缓存。
- 每次 turn 持有附件 lease；GC 不删除仍被使用的对象。
- 文件名仅作为展示元数据，Codex 接收 canonical cache path。
- 图片使用 `localImage` input；普通文件路径写入结构化用户文本上下文。

失败路径必须删除临时文件。并发相同 hash 使用同一内容记录和引用计数，不允许一个 turn 的清理删除另一 turn 的附件。

## 11. 权限和审批

首版默认：

- 只有应用 owner 可以使用；
- 群聊必须直接 @bot；
- `workspace-write`，网络设置沿用明确配置；
- 工作区拒绝 `/`、HOME 根、系统目录、桌面/下载根和临时目录根；
- `/cd`、访问控制和配置命令仅 owner/admin；
- Codex 审批 fail-closed。

审批保存 `{epoch, request_id, thread_id, turn_id, item_id, expires_at}`，卡片回调使用 HMAC、operator ID、策略指纹和一次性 nonce 绑定。只允许一次决策；连接 epoch 变化、turn 终结或 `serverRequest/resolved` 到达时立即撤销。

第三阶段 team mode 启用时，团队成员只能触发允许的 scope，不能继承 owner 的个人
`lark-cli` 身份；默认 sandbox 不因 team mode 自动放宽。

## 12. 命令语义

第一阶段命令：

- `/new`：归档当前 session identity 的 active thread，保留工作区，不保留旧 turn。
- `/stop`：中断 active turn；没有 active turn 时返回被动状态。
- `/status`：显示连接、scope、cwd、thread、turn、队列和权限摘要，不泄露 secret。
- `/cd <absolute-path>`：校验并切换工作区，同时归档当前 thread。
- `/help`：显示当前可用命令。

命令不能静默删除此前排队的普通消息。改变 scope 状态的命令与普通消息通过同一个 actor
邮箱排序；命令明确决定是保留、延后还是拒绝队列，并向用户反馈。

## 13. 故障恢复

### 13.1 Codex 子进程退出

1. supervisor 原子切换到 `NotReady`，epoch 增加；
2. 所有 pending RPC 以 `ConnectionLost` 失败；
3. active turn 标为 `uncertain`，审批撤销；
4. 暂停新 turn；
5. 使用 0.5、1、2、4 秒至 30 秒上限的退避和 jitter 重启；
6. initialize 与账户健康检查成功后恢复 `Ready`；
7. scope 按需 `thread/resume`，不在启动时加载全部历史。

App-server 没有已承诺的事件 cursor、exactly-once turn 或进行中 turn 跨进程恢复。
因此断线后的 `turn/start` 不盲目重发；通过 `thread/read` 做有限核对，仍不确定时明确提示用户重新发起。

### 13.2 飞书断线

入站暂停，Codex active turn 可以继续。Codex 事件仍投影到 SQLite/outbox；飞书连接恢复后按顺序发送。若凭据永久失败，桥接器保持 degraded 并在终端给出可操作诊断。

### 13.3 慢消费者和过载

协议 reader 永远不等待飞书 I/O。非终态 delta 可合并；关键队列持续满时拒绝新入口并回复 busy，必要时中断受影响 turn。绝不通过无限内存维持表面可用。

## 14. 可观测性

使用 `tracing` 输出 JSON 日志，至少包含 `profile`、`scope_hash`、`thread_id`、
`turn_id`、`message_id`、`connection_epoch`、阶段、耗时和错误分类。

以下内容必须脱敏或禁止记录：App Secret、access token、Authorization header、完整用户文本、完整工具输出、审批签名和本地敏感路径。诊断模式只增加结构元数据，不解除秘密脱敏。

指标覆盖：连接重试、协议 drift、队列深度、active turns、RPC 延迟、首事件延迟、终答发送、outbox 重试、重复事件和进程 RSS/CPU。

## 15. 依赖选择

建议的主要依赖：

- async/runtime：`tokio`、`tokio-util`、`futures-util`、`bytes`；
- protocol：`serde`、`serde_json`、`prost`、`tokio-tungstenite`；
- HTTP：`reqwest`，使用 rustls；
- persistence：`rusqlite` 加专用 blocking writer，首版不引入 ORM；
- CLI/config：`clap`、`figment` 或直接 `toml` + `serde`；
- errors/logging：`thiserror`、`anyhow`、`tracing`、`tracing-subscriber`；
- security：`secrecy`、`zeroize`、`hmac`、`sha2`、`subtle`；
- utilities：`uuid`、`semver`、`rand`、`url`。

不把完整 app-server schema 生成成僵硬的大型 Rust enum；JSON Schema 用于 CI 兼容检查和 fixture 验证。

## 16. 验证与基准

开发以功能切片和集中验证为主，不要求每个私有函数先写测试。每个里程碑至少执行：

- `cargo fmt --check`；
- `cargo clippy --all-targets --all-features -- -D warnings`；
- 受影响模块测试和关键集成测试；
- 使用真实 `codex app-server` 的握手/thread/turn smoke test。

发布候选额外执行：

- 全量测试；
- 飞书测试应用端到端收发、话题、附件、取消和审批；
- app-server `kill -9`、飞书断网、慢出站和重复事件注入；
- 24 小时以上 soak test；
- 与参考 Node/`codex exec` 实现的基准比较。

基准统一使用相同机器、Codex 版本、模型、账户和输入，采集：

- idle RSS 和 CPU；
- cold/warm 首事件延迟；
- 连续 20 turns 的进程启动次数和延迟分布；
- 1、4、8 个并发 scope 的吞吐、p50/p95 和峰值 RSS；
- 子进程退出后的恢复时间。

“更低负载”和“更高调用效率”只有在这些数据优于参考实现且没有降低正确性时才宣称达成。

## 17. 提交与发布策略

仓库从 `main` 开始，以可运行里程碑小步提交并立即推送：

1. 设计、许可证和项目说明；
2. Rust 骨架与 CI；
3. app-server transport/RPC；
4. 飞书 transport/OpenAPI；
5. SQLite、router 与 scope actor；
6. 回复状态机、outbox 和附件；
7. 核心命令与权限；
8. 服务管理、parity、基准和发布。

不使用 fork，不把参考仓库历史合并进来。参考行为通过文档、测试用例名称和独立实现追踪。

## 18. 验收标准

项目完成需要同时满足：

1. 第一、第二阶段列出的核心能力均有可运行实现；第三阶段列出的评论和 team mode 已按安全约束实现。
2. Claude、Web UI、会议相关代码和依赖不存在。
3. 连续对话复用同一 app-server 进程和正确 thread，不为每轮启动 `codex exec`。
4. 重复飞书事件不会产生重复 turn；同 scope 不会并发两个 turn。
5. final-only、empty、流失败和迟到流场景符合第 9 节契约。
6. Codex/飞书断线恢复没有静默丢终答或盲目重复未知写请求。
7. 配置、日志、数据库和卡片不泄露 secret。
8. 支持平台的安装、初始化、前台运行和后台服务文档可复现。
9. 全量检查、真实 Codex smoke、飞书端到端与故障注入通过。
10. 基准报告证明相对参考实现的资源和调用效率改进，或如实记录未达到的指标并继续优化。
11. GitHub 仓库为 public、非 fork，并保留清晰的阶段性提交记录。

## 19. 依据

- [Codex App Server 文档](https://learn.chatgpt.com/docs/app-server.md)
- [Codex 开源 app-server](https://github.com/openai/codex/tree/main/codex-rs/app-server)
- [飞书官方 Go SDK](https://github.com/larksuite/oapi-sdk-go)
- [参考项目](https://github.com/zarazhangrui/lark-coding-agent-bridge)

实现时以本机支持版本生成的 JSON Schema 和真实 binary 行为为 Codex wire shape 的最终依据；文档与 schema 冲突时，先通过 smoke test确认，再将兼容决策记录在代码和 CI 中。
