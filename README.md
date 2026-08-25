# lark-codex-bridge

[![CI](https://github.com/M1nt-Ch0c0/lark-codex-bridge/actions/workflows/ci.yml/badge.svg)](https://github.com/M1nt-Ch0c0/lark-codex-bridge/actions/workflows/ci.yml)
[![Codecov](https://codecov.io/gh/M1nt-Ch0c0/lark-codex-bridge/branch/main/graph/badge.svg)](https://codecov.io/gh/M1nt-Ch0c0/lark-codex-bridge)

一个面向飞书 / Lark 的 Codex 本地桥接器。项目使用 Rust 重写，直接连接
`codex app-server`，专注于低资源占用、可靠会话、稳定流式回复和可恢复运行。

## 项目状态

当前版本是早期 alpha，已经可以启动常驻 `run` 运行时做最小试用，但不应视为可投入
生产的飞书机器人。当前可运行链路包括：

- Codex app-server 的有界 stdio transport、RPC broker、typed thread/turn client、
  长驻 supervisor、thread 复用、`codex probe` 和门控的真实 Codex smoke；另含显式
  spawned/external backend 配置、外部端点认证/精确版本/只读能力准入门禁，以及不拥有
  服务端进程的有界只读 WebSocket transport、持久化跨 epoch 恢复，以及尚未接入普通
  `run` 链路的共享写入/queue/单审批处理者协调器；
- Rust 原生飞书/Lark 凭证登记、OpenAPI、WebSocket transport、事件归一化，
  以及可灰度启用、受 Rust 监督的官方 Node SDK 入站 sidecar；
  `lark probe` 和门控的真实 Lark smoke；
- SQLite WAL 单写者 store、持久 inbox/outbox、去重、owner/指定 sender/指定群组 allowlist 授权、安全工作区策略、
  scope actor、同 scope 串行 turn 和不同 scope 的有界并发；
- 延迟进度卡、独立最终回复、重试/receipt/uncertain delivery，以及终态先持久化再收口；
- 图片 `localImage` 和普通文件结构化路径输入、内容寻址缓存、turn lease、GC 与启动校验；
- 完整应用装配和 `run --config`：飞书消息 → Codex turn → 飞书进度/终答。

尚未接线的是 slash command handler、Codex 审批卡、服务管理和完整故障注入/恢复。
外部端点已有 fail-closed 准入、只读长连接 transport、持久 epoch fence 和有界
resume/read reconciliation，并已具备显式 `mutate_shared` / `queue_shared` 的持久写入与
单审批处理者策略；这些写入能力仍未接入普通 `run` 链路。选择 external mode 不会回退
为新起一个 stdio child。配置与验收说明见
[`docs/external-codex-endpoint-gate.md`](docs/external-codex-endpoint-gate.md) 和
[`docs/external-codex-transport.md`](docs/external-codex-transport.md)，恢复语义与验收见
[`docs/external-codex-reconciliation.md`](docs/external-codex-reconciliation.md)，共享写入、
审批与不重放语义见
[`docs/external-codex-write-coordination.md`](docs/external-codex-write-coordination.md)。另已完成
Codex 0.149.0 Unix-socket listener 的原始 WebSocket 握手 RFC/双 Unix 平台门禁；当前运行时仍
明确拒绝 `unix://`，不会把它别名为 JSONL、stdio 或 TCP，结论与复现见
[`docs/codex-unix-websocket-contract.md`](docs/codex-unix-websocket-contract.md)。
`/stop`、`/status` 按当前最小试用范围明确暂缓；`/new`、`/cd`、`/help` 目前也只有
解析与 help 元数据，还未进入运行时。启动时会预装有界的 `Received` 行，但尚无周期性
重扫。首次启动 onboarding 已恢复参考实现的一命令体验：扫码注册后自动携带创建者身份、
生成安全默认工作区和运行配置并直接启动，见
[GitHub Issue #2](https://github.com/M1nt-Ch0c0/lark-codex-bridge/issues/2)。
真实 Lark smoke 是显式门控验收项；未运行或只看到 skip 都不算通过。

## 最小试用

前提：本机已安装并登录当前精确支持的 `codex-cli 0.146.0` 或 `0.149.0`，飞书/Lark 应用机器人已创建并
加入目标会话。首次运行不再需要手写 TOML、手动查询 `open_id` 或预先创建工作区。

直接启动常驻桥接器，按提示完成扫码授权即可：

```bash
cargo run --locked -- run
```

首次运行会自动完成以下步骤，然后直接进入前台桥接器：

1. 通过二维码/设备流登记一个 `PersonalAgent` 应用（或复用已存的凭证）；
2. 把授权者（应用创建者）的 `open_id` 作为 owner 写入访问控制；
3. 在平台数据目录下创建受管工作区，并派生本地数据库与附件缓存路径；
4. 以私有权限、原子替换的方式写入凭证、owner 提示和 `config.toml`。

生成的工作区位于 `~/.local/share/lark-codex-bridge/workspace`（Windows 为
`%LOCALAPPDATA%\lark-codex-bridge\workspace`），配置文件位于平台配置目录
（Linux/macOS 为 `~/.config/lark-codex-bridge/config.toml`，Windows 为
`%APPDATA%\lark-codex-bridge\config.toml`）。已有配置文件或显式 `--config` 绝不会被
静默覆盖；重复运行与并发首次运行均幂等。

私聊可直接发消息；群聊和话题需要直接 @机器人。按 `Ctrl-C` 结束。当前真实飞书的
“发消息 → Codex 回答 → 飞书收到回复”验收由操作者手动执行。

### 终端日志与排障

日志默认只显示错误和必要告警；全局 `-v` 打开 info 级连接、turn、恢复与 outbox
生命周期，`-vv` 再打开队列深度、批次和重试等 debug 状态。全局参数可写在子命令前后：

```bash
cargo run --locked -- run -v
cargo run --locked -- -vv run
```

设置 `RUST_LOG` 时会覆盖上述 bridge 默认过滤器，并使用
[`tracing-subscriber` EnvFilter](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html)
语法；无效值会在启动前报错并给出示例，不会被静默忽略：

```bash
RUST_LOG='warn,lark_codex_bridge=debug' cargo run --locked -- run
cargo run --locked -- --log-format json run -v
```

为维持日志脱敏边界，终端 subscriber 只接收 `lark_codex_bridge` 自身经过审计的事件；
即使设置 `RUST_LOG=trace` 或指定第三方 crate，也不会输出 HTTP/WebSocket 依赖库可能包含
完整 endpoint、header 或 frame payload 的诊断日志。

human 与 JSON 日志都只写入 stderr；`codex probe`、`lark probe` 和认证检查等命令的
stdout JSON 契约保持独立。因此 launchd/systemd 可分别重定向 stdout 与 stderr，后者
即可用于基本故障诊断。日志字段限于静态分类、计数、耗时、重试次数和 supervisor epoch；
不会记录 App Secret、tenant token、完整 WebSocket endpoint、消息/prompt/模型/工具正文、
媒体内容、完整本地路径、用户身份或原始事件 payload。

已登记应用与诊断场景保持不变；如需分别检查两侧连接：

```bash
cargo run --locked -- codex probe
cargo run --locked -- lark auth check
cargo run --locked -- lark probe
```

## Codex 环境检查

Codex app-server 协议采用精确版本的 Schema/契约门控；候选版本不会因“版本更高”而自动进入
支持范围。同步、兼容性报告和升级流程见
[`docs/codex-schema-maintenance.md`](docs/codex-schema-maintenance.md)。普通构建不会安装或运行
Codex，也不依赖本机存在 Codex binary。

连接显式共享 app-server endpoint 的研究结论、所有权边界和失败关闭规则见
[`docs/shared-codex-app-server-rfc.md`](docs/shared-codex-app-server-rfc.md)。该路径目前只是 RFC；
WebSocket 仍是上游实验接口，生产运行时不会自动发现或连接 Desktop/CLI 私有 endpoint。

```bash
cargo run --locked -- codex probe
```

`codex probe` 会真实启动 `codex app-server --listen stdio://` 并完成 initialize
握手，输出单个 JSON 对象，只包含 supported version、initialize user agent、
platform family/OS 和 epoch；不包含 Codex home、账户身份、token 或环境变量。

真实端到端 smoke 需要已认证的 Codex 账户，并按环境变量门控：

```bash
CODEX_E2E=1 cargo test --test codex_smoke --locked -- --ignored --nocapture
```

外部端点门禁另有精确 0.149 binary + bearer 的真实 smoke；它要求显式环境配置，缺项会
直接失败，完整命令见
[`docs/external-codex-endpoint-gate.md`](docs/external-codex-endpoint-gate.md#verification)。
外部只读 transport 另有双客户端、shutdown/abort、health 和新连接复用的真实生命周期
smoke，见 [`docs/external-codex-transport.md`](docs/external-codex-transport.md#verification)。
跨 socket epoch 与操作者重启服务端的 resume/read reconciliation 真实 smoke 见
[`docs/external-codex-reconciliation.md`](docs/external-codex-reconciliation.md#verification)。
两客户端写入竞争、queue、单审批路由和不重放的真实 smoke 见
[`docs/external-codex-write-coordination.md`](docs/external-codex-write-coordination.md#verification)。
Unix-socket 上的原始 HTTP/WebSocket Upgrade、peer credential、路径碰撞、stale socket 与
清理策略的精确 binary smoke 见
[`docs/codex-unix-websocket-contract.md`](docs/codex-unix-websocket-contract.md#committed-reproduction)。

### Persisted thread adoption（当前禁用）

顺序接管既有 Codex thread 当前明确 fail closed。受支持的 app-server 契约可以用
`thread/resume` 取得 writer，但没有经验证的、在 app-server 继续运行时释放该 writer 的操作；
本地退订或丢弃 bridge route 不等于释放远端 ownership。因此 bridge 不列出候选、不调用
`thread/resume`、不写 scope mapping，也不会以 idle 状态猜测 thread 已无人持有。

`/threads`、`/adopt <selector> --handoff-complete` 和 `/release` 已保留为显式命令语法；当前
slash handler 尚未接线，且任何未来 handler 都必须先通过零状态 capability gate。可用下面的
只读诊断查看稳定分类（不读取 `CODEX_HOME`，也不启动 Codex）：

```bash
cargo run --locked -- codex adoption-status
```

完整的负向互操作证据、生命周期规则和未来启用条件见
[`docs/thread-adoption.md`](docs/thread-adoption.md)。实时多客户端共享不属于该顺序交接方案，
由 [Issue #8](https://github.com/M1nt-Ch0c0/lark-codex-bridge/issues/8) 单独研究。

## 会话富媒体与单跳引用

- 私聊图片、视频和文件只暂存有界描述符，不下载、不启动 Codex；下一条普通文字把当前
  pending media 合并进同一个 turn。队列受 10 分钟 TTL、16 条和 256 KiB 元数据上限约束，
  消费、显式引用、`/cancel`、`/new`、`/stop`、中断、scope 回收和超时都会清理。
- 私聊语音是独立的完整输入，会直接触发 turn，但不会消费之前暂存的图片/文件。语音字节和
  ASR 仍只在 `bridge_media.read` 时发生。
- 群聊/话题里未触发的图片、视频、文件和语音直接忽略并做无 turn 的 durable settlement；
  不创建 scope actor，不进入 pending/context/附件缓存，也不运行 ASR。
- 群聊/话题用“直接 @机器人并回复媒体消息”触发。Bridge 在当前触发消息通过 sender/group/
  mention 策略后只拉取直接父消息一跳，并对父消息 sender 再独立执行 human/owner/sender/group
  授权；资源 key 留在 turn-scoped capability registry，
  `bridge_context.resolve` 只返回 opaque handle。删除、无权限、超限、不支持和暂时不可用均有
  稳定状态，不递归读取引用链或聊天历史。

真实移动端引用 smoke 是人工操作、显式门控的测试。运行后按终端提示，在指定群里先发送一条
不 `@bot` 的图片/视频/文件/语音，再用飞书移动端直接回复该消息并 `@bot` 附带给出的 marker。
启用路径会验证 standalone 群媒体完成 no-turn settlement 且未创建 actor/context/cache 工作，随后
验证触发消息策略、父消息发送者授权、单跳引用解析、opaque handle（序列化结果不含 resource key）
以及通过有界附件缓存的真实按需读取；全过程不打印 resource key 或媒体内容。未设置 gate 时会
明确报告 skip，而 skip 不算验收证据。

```bash
LARK_MEDIA_E2E=1 LARK_E2E_APP_ID=… LARK_E2E_APP_SECRET=… LARK_E2E_TENANT=feishu \
LARK_MEDIA_E2E_GROUP_CHAT_ID=oc_… \
  cargo test --test lark_smoke --locked real_mobile_group_quote_resolves_direct_media_parent \
  -- --ignored --nocapture
```

## 本地语音转写（ASR sidecar）

飞书语音气泡默认**不会**把 `localAudio` 交给 Codex。`bridge_media.read` 对音频按需转写后只回传文本：

1. 入站 payload 已带客户端识别文本时，原文只通过一次性的、与 event/message/part/resource 精确绑定的内存 handoff 进入 turn-scoped media grant；durable DTO、SQLite/WAL、outbox、checkpoint、`ContextSnapshot` / `TypedPart`、prompt 和 `Debug` 都不含原文。只有 `bridge_media.read` 会按 `max_transcript_bytes` 校验并返回它。进程在读取前重启时返回无内容的 `transcript_unavailable`，不会把已接受文本写盘或误降级到 sidecar；畸形或超限文本同样只保留 `invalid_transcript` / `transcript_too_large` 分类；
2. 否则 `ffmpeg` 只向受监督的 pipe 输出 16 kHz mono PCM；Bridge 自己在专属私有根目录中、每次写入前检查硬字节上限并构造完整 canonical WAV，再跑本地 sidecar。子进程从不获得输出文件路径，不能用单次大写入或 sparse 文件绕过上限。Unix 目录/文件会在创建时显式设为并复核 `0700` / `0600`（不依赖 umask）；Windows 会在写入内容前设置并复核仅当前用户与 `SYSTEM` 的 protected DACL；
3. `ffmpeg` 和 sidecar 都在完整的进程组（Windows 为 Job Object）中运行。正常完成、turn 中断、Bridge shutdown、超时或 future drop 都会终止残留子孙并等待回收；中断响应屏障保证 transcript/media 内容不会出现在成功的中断确认之后。每次媒体读取持有独立 lease token，同一 turn/hash 的重叠读取不会相互释放 GC 保护；
4. `max_duration_ms` 可下调但绝不能超过 10 分钟；Bridge 在解码期间实施固定 PCM 投影的绝对硬上限，并在交给 sidecar 前验证 RIFF 声明长度、所有 chunk/padding、PCM 格式、唯一 data chunk、精确时长和完整文件边界，防止小型压缩输入膨胀或畸形 WAV；
5. 异常退出残留目录会在启动时和运行期间定时做有界清理：即使随后禁用 sidecar，只要私有 ASR 根目录仍存在就会继续清理；禁用且根不存在时不会仅为 sweep 创建目录。进程内保留的目录迭代器跨 tick 续扫，每轮实际目录读取、metadata 与清理尝试都有硬上限，并能越过大量 hostile/fresh/symlink 项最终走到本轮目录末尾；重启只会安全地重置遍历进度。Bridge workspace 先原子隔离并用目录身份 claim 证明所有权，已知 `decoded.wav` 以 no-follow 方式擦除；未知文件绝不删除，失败状态保留供后续重试；
6. 缺 sidecar、解码失败、空/畸形/超限/恢复后不可用的转写、过长音频、取消或私有目录失败都会返回稳定错误码（`sidecar_missing` / `unsupported_codec` / `empty_transcript` / `invalid_transcript` / `transcript_too_large` / `transcript_unavailable` / `too_long` / `oversize` / `sidecar_failed` / `cancelled` / `temporary_storage_failed`），不会静默丢 part。

推荐 sidecar 是 [sherpa-onnx](https://github.com/k2-fsa/sherpa-onnx) 上的 SenseVoice Small。仓库不内置模型权重；未配置 sidecar 时图片/文件读取不受影响。

```toml
[asr]
command = "/Users/YOU/lark-codex-bridge-asr/bin/sensevoice"
args = []
ffmpeg = "ffmpeg"
max_duration_ms = 600000
max_transcript_bytes = 32768
```

`command` 可省略。`args` 按声明顺序传给 sidecar，解码后的 WAV 路径始终作为最后一个参数。`max_duration_ms` 必须在 `1..=600000` 内，`max_transcript_bytes` 必须在 `1..=32768` 内。
相对路径相对配置文件目录解析；单个程序名（如 `ffmpeg`）走 `PATH`。

sherpa-onnx-offline 的 stdout 可含配置转储；Bridge 会在有界输出中提取首个 JSON
`text`。仓库提供的包装脚本 [`scripts/sensevoice-sidecar.sh`](scripts/sensevoice-sidecar.sh)
用 `exec` 直接启动识别器以减少一层 shell；安全性不依赖这一点，Bridge 的进程组/Job Object 也会覆盖未 `exec` 的子孙进程。本机真实模型冒烟默认 `#[ignore]`，只在显式提供模型、样本和 `LARK_ASR_SMOKE=1` 时运行：

```bash
export SENSEVOICE_BIN=$HOME/lark-codex-bridge-asr/sherpa-onnx-v1.13.6-osx-arm64-static-no-tts/bin/sherpa-onnx-offline
export SENSEVOICE_MODEL=$HOME/lark-codex-bridge-asr/sherpa-onnx-sense-voice-zh-en-ja-ko-yue-int8-2025-09-09/model.int8.onnx
export SENSEVOICE_TOKENS=$HOME/lark-codex-bridge-asr/sherpa-onnx-sense-voice-zh-en-ja-ko-yue-int8-2025-09-09/tokens.txt
export LARK_ASR_SMOKE=1
export LARK_ASR_SIDECAR=$PWD/scripts/sensevoice-sidecar.sh
export LARK_ASR_FFMPEG=$(command -v ffmpeg)
export LARK_ASR_SAMPLE_OGG=$HOME/lark-codex-bridge-asr/samples/zh.ogg
cargo test --locked --lib runtime::asr::tests::sensevoice_transcribes_real_feishu_like_ogg -- --ignored --nocapture
```

该测试只有在上述环境变量齐全、命令实际成功且得到非空结果时才构成真实模型证据；`#[ignore]`、跳过或缺少任一变量都明确表示 **NO EVIDENCE**。测试和错误输出不会打印转写正文。

## 授权角色（owner / sender / group）

除 owner 外，`config.toml` 还支持两类低权限授权（均为可选、默认拒绝）：

```toml
owners = ["ou_owner_open_id"]
allowed_senders = ["ou_member_open_id"]   # 按用户身份授权的普通调用者
allowed_groups = ["oc_chat_id"]           # 仅该群内普通人类成员可发起普通 turn
```

语义要点：

- 群/话题中的普通 turn 仍要求真实直接 @机器人，`@all` 不算数；私聊不受影响。
- 群白名单只授予普通消息；owner-only 控制命令仅 owner 可执行。
- 非人类 sender（应用、机器人等）一律拒绝，任何 allowlist 都不例外。
- 列表有数量与字节上限（各 256 条 / 32 KiB），重复条目幂等去重，畸形 ID 拒绝加载。
- 不匹配的通配符、群名称匹配、成员自动同步均不支持；移除条目即撤销授权。

## 飞书 / Lark 接入与检查

登记应用凭证（扫码注册新 PersonalAgent 应用，或登记已有 App ID/Secret）：

```bash
cargo run --locked -- lark auth register
cargo run --locked -- lark auth register --app-id <id> --tenant <feishu|lark>   # secret 从 LARK_APP_SECRET 读取
cargo run --locked -- lark auth check
```

凭证也可用环境变量提供（优先级高于凭证文件）：`LARK_APP_ID`、`LARK_APP_SECRET`、
`LARK_TENANT`（`feishu|lark`）。`lark auth check` 只输出 tenant、bot 名称和 bot open_id。

`lark probe` 用已存凭证换取 tenant token、查询 bot 信息、拉取 WebSocket endpoint
并完成一次真实 ping/pong 往返，输出单个脱敏 JSON 对象（tenant、botName、botOpenId、
endpointHost、pingIntervalSecs、elapsedMs）；绝不输出 secret、token 或完整 endpoint URL。
缺凭证、永久认证失败或超时均以非零退出并给出可操作的诊断。

真实飞书/Lark 端到端 smoke 需要应用凭证和一个机器人已加入的会话，并按环境变量门控
（未设置时测试打印 skip 原因并成功退出，skip 不算证据）：

```bash
LARK_E2E=1 LARK_E2E_APP_ID=… LARK_E2E_APP_SECRET=… LARK_E2E_TENANT=feishu LARK_E2E_CHAT_ID=oc_… \
  cargo test --test lark_smoke --locked -- --ignored --nocapture
```

### 官方 Node SDK 入站 sidecar（可选）

默认 `native` 行为不变。要只把入站 WebSocket 灰度切到固定版本的官方 Node SDK，先在
构建/部署阶段安装 lockfile 依赖（运行时不会执行 npm）：

```bash
npm ci --ignore-scripts --prefix sidecar
npm run check --prefix sidecar
```

然后在 `config.toml` 显式选择；相对 `sidecar_entrypoint` 按配置文件目录解析，部署时建议
使用绝对路径：

```toml
[channel]
transport = "node-sidecar"       # 默认 "native"
node_binary = "node"
sidecar_entrypoint = "/opt/lark-codex-bridge/sidecar/index.cjs"
fallback_to_native = true
```

sidecar 只接收入站事件与连接状态；查询、媒体下载和出站仍走 Rust OpenAPI。Rust 在完成
SQLite durable intake 和有界队列预留后才回送正 ack；失败、超时或背压都会使 SDK handler
失败，让上游保留重投语义。协议、脱敏与容量细节见
[`docs/channel-wire-v1.md`](docs/channel-wire-v1.md)。

`fallback_to_native` 只用于首次启动：只有在 sidecar 完成协议配置并由 SDK 报告真实
`connected` 后启动才算成功；在此之前失败会稳定返回给组装层，由该开关决定是否启用原生
transport。首次连接成功后的崩溃不会在运行中切换来源，而是按有界退避重启；连续健康连接
30 秒后才重置退避。Rust 在 POSIX 上拥有整个 sidecar 进程组、在 Windows 上拥有 Job
object，协议错误、stdout EOF、超时、关闭和 handle drop 都会终止其全部后代进程。

真实 sidecar smoke 还要求操作者在连接后发送一条新私聊；未运行或 skip 不算证据：

```bash
LARK_SIDECAR_E2E=1 LARK_SIDECAR_E2E_APP_ID=… LARK_SIDECAR_E2E_APP_SECRET=… \
  LARK_SIDECAR_E2E_TENANT=feishu \
  cargo test --test lark_sidecar_smoke --locked -- --ignored --nocapture
```
## 回复显示与 Markdown 验收

输出层显式保留语义载体，不会根据字符串中是否出现 Markdown 符号来猜消息类型：

- 无进度卡的独立终答使用飞书/Lark `msg_type=post`，内容固定包装成
  `zh_cn.content -> [[{tag: "md", text: …}]]`；
- 流式中间态和终态始终是同一条 Card 2.0 `interactive` 消息，正文元素保持
  `tag=markdown`。每次发送的快照会临时补齐未闭合代码围栏，后续增量仍基于未修改的原文；
- 拒绝、过载、失败、中断等短通知继续使用 `msg_type=text`。

`post/tag=md` 与 Card 2.0 `tag=markdown` 分别经过载体专用的净化入口。两者支持并保留
段落、无序/有序列表、引用、行内代码、fenced code、粗体、斜体、删除线和行内链接；
行内代码与链接字面量不会被 HTML/脚注规则误写。标题稳定降级为粗体段落，表格固定降级为
`text` fenced code（不依赖客户端不一致的表格支持），任务列表变成 Unicode 复选框，脚注
变成带标签的普通文本且绝不在代码 span 内替换。复杂嵌套会展开成单层可读结构，连续空行会
压缩。引用/列表容器内的 fence 与表格会先剥离容器前缀再识别，输出时保留外层引用语义，
因此嵌套的开闭 fence 不会被当作普通正文切断。

不支持的图片会降级为 `Image: alt (target)`，reference link/image 会降级为带 reference 标签的
可读文本，reference definition 会显示为普通文本。`~~~` fence 通常转成反引号 fence；若正文
与 delimiter 冲突则选择更短的安全 marker 或做显式可读降级；
畸形 info string 整体降级为 `text`，未闭合 fence 在每个发送快照中补齐。原始 HTML 标签/注释
只在代码 span 外移除，畸形标签使用惰性的 Unicode 尖括号显示且不会跨行吞掉引用内容。
可能触发真实提及的 `<at …>` 控制（包括 `&lt;at …&gt;`、十进制/十六进制实体编码）、
双向/零宽 Unicode format controls（包括数字/命名实体）会在两个载体中净化；代码 span 内的
对应字面量原样保留。代码外由实体解码得到的 ASCII 与空白会显示为惰性的全角字符或可见
控制符号，不能在二次结构解析或客户端中重建粗体、fence、换行、列表、引用、链接或图片。
链接目标按载体白名单处理：`post` 只保留 `https`、`http`、`mailto`，
Card 2.0 只保留 `https`；`javascript`、`data`、`file`、带控制字符/实体的目标稳定降级为文本。
畸形 link/image 只消费已确认的语法前缀，目标及其后的普通文字不会被吞掉。

转换先于分片。每个 `post` 分片同时检查 4,000 个 Unicode 标量上限和最终 Lark
reply JSON 的精确序列化字节数（包括内层 JSON 转义与话题回复标记），最多 8 片；在
代码块内切分时会闭合当前片并在下一片重新打开相同围栏，超出总预算则显式附加
`…[truncated]`。Card 2.0 的创建、更新和终态（包括 v1/v2 durable replay）也在净化后按
单个 `{tag:"markdown",content:…}` 元素的精确 JSON wire 大小执行 `30 * 1024` 字节硬上限；
JSON 转义、Unicode 与闭合 fence 开销均计入，超限时 fence-safe 截断。病态超长 fence delimiter
会整体规范化/降级，不会在 delimiter 中间切分。

持久 outbox 从本功能起只写 payload v2；升级后的 reader 同时严格读取 v1/v2。历史 v1
`reply_text` 始终保持纯文本载体，不会因内容像 Markdown 而改型；历史终态 Card 的备用正文
会先经过当前净化器再成为 `post`。数据库 `PRAGMA user_version=6` 是显式降级栅栏：升级前应
停掉旧进程并备份数据库，升级后不得再用只认识 payload v1/schema v5 的旧二进制打开同一库。
确定未生效的终态 Card PATCH 在永久拒绝（或同样确定未生效的限流重试耗尽）后，会把同一个
确定性 outbox 行原子转换成 standalone Markdown post；响应丢失、畸形或其他
`uncertain_delivery` 绝不会触发备用发送。非幂等 POST 收到 HTTP 5xx 时，即使服务器返回了
响应，也可能已经写入，因此立即记为 uncertain 且绝不盲重放。Card PATCH 可按幂等操作对
HTTP 429/5xx 和平台明确记录的限频码做有界重试；5xx 重试耗尽仍为 uncertain，绝不授权
fallback POST。校验、卡片格式、机器人不在会话、消息已撤回等明确业务拒绝直接视为永久失败。

真实桌面端/移动端 Markdown 验收是单独的显式门控测试。先在目标会话发送一条可供
机器人回复的消息并取得其 `message_id`，准备三个不纳入 Git 的本地路径，然后运行：

```bash
LARK_MARKDOWN_E2E=1 \
LARK_E2E_APP_ID=… LARK_E2E_APP_SECRET=… LARK_E2E_TENANT=feishu \
LARK_MARKDOWN_E2E_PARENT_MESSAGE_ID=om_… \
LARK_MARKDOWN_E2E_DESKTOP_SCREENSHOT=/tmp/lark-markdown-desktop.png \
LARK_MARKDOWN_E2E_MOBILE_SCREENSHOT=/tmp/lark-markdown-mobile.png \
LARK_MARKDOWN_E2E_ATTESTATION=/tmp/lark-markdown-attestation.json \
LARK_MARKDOWN_E2E_REVIEWER='独立审核人标识' \
  cargo test --test lark_markdown_smoke --locked -- --ignored --nocapture
```

审核信任锚不是运行时输入。`LARK_MARKDOWN_E2E_REVIEWER` 只能选择
`tests/lark_markdown_smoke.rs` 中编译进测试二进制的审核人标识/Ed25519 公钥，环境变量不能新增
或替换公钥。当前仓库的可信 allowlist 有意为空，因此真实桌面端/移动端证据仍然**缺失**；即使
提供全部环境变量，smoke 也会在读取凭证和发送消息前以
`no trusted review anchor configured` 失败关闭。只有通过独立渠道取得审核公钥，并通过受审查的
仓库提交固定该身份/公钥后，才可执行并产生可采信的真实验收证据；私钥不得进入仓库。

测试发出覆盖全部子集及表格降级的真实 `post`；正文末尾会显示本次唯一 `nonce` 与基础正文
SHA-256，测试同时打印回复 `message_id`、marker/body/最终正文 hash，默认等待 5 分钟。在飞书
桌面端和移动端分别打开该回复，确认排版可读、表格显示为 fenced text，且两张截图都完整显示
本次 marker。保存两张新截图后，测试会打印文件 hash、按 `width || height || RGBA8` 计算的
canonical pixel hash、尺寸与最终 `evidence_sha256`，把这些值写入新生成的强类型验收文件：

```json
{
  "version": 3,
  "nonce": "测试打印的一次性nonce",
  "message_id": "om_测试打印的回复ID",
  "body_sha256": "测试打印的基础正文hash",
  "markdown_sha256": "测试打印的正文hash",
  "marker": {"verdict": "visible_in_both", "sha256": "测试打印的marker hash"},
  "desktop": {
    "verdict": "pass", "file_sha256": "桌面文件hash",
    "pixel_sha256": "桌面canonical pixel hash", "width": 1234, "height": 800
  },
  "mobile": {
    "verdict": "pass", "file_sha256": "移动文件hash",
    "pixel_sha256": "移动canonical pixel hash", "width": 800, "height": 1234
  },
  "review": {
    "reviewer": "与环境变量完全一致的独立审核人标识",
    "public_key_sha256": "测试打印的审核公钥hash",
    "signature_ed25519": "审核人生成的128位Ed25519签名hex"
  },
  "evidence_sha256": "测试打印的全字段绑定hash",
  "table": "fenced"
}
```

测试在发送前记录三个证据文件的旧 hash，只接受发送完成后变更且能真实解码、像素数有界的
PNG/JPEG/WebP；桌面与移动截图按解码后的 canonical RGBA 像素比较，因而“相同像素重新编码”
不会伪装成两份证据。`evidence_sha256` 以无歧义长度前缀格式绑定本次 nonce、message ID、
marker/body/最终正文 hash、两张文件及 pixel hash/尺寸、所有视觉 verdict、审核人和审核公钥。

程序只验证文件、像素和字段绑定，**不会声称能从像素中识别 marker 或判断排版**。这两个视觉
结论必须由独立审核人检查桌面端和移动端截图后签署。审核公钥必须先经独立渠道核验，再由受审查
的仓库提交固定；对应私钥不得提供给 smoke 进程或截图操作者。审核人用 Ed25519 签署 UTF-8 字节串
`lark-markdown-independent-review-signature-v3\n<evidence_sha256>\n`。因此任意两张不同图片加
自填 verdict/attestation、错误审核密钥、陈旧、同像素或字段错配均不能通过；有效签名表示外部
审核人对绑定图片作了人工确认，而不是程序完成了 OCR。显式运行 ignored smoke 却没有
`LARK_MARKDOWN_E2E=1` 或任一配置会直接失败，不会以 skip 冒充证据。截图可能包含会话信息，
因此只作为操作者保存的外部验收证据，不应提交仓库。

仓库只跟踪稳定的产品说明；缺陷和遗留项通过 GitHub Issue 与对应 PR 跟踪。实施计划、
实时进度、Agent 接管记录和临时测试证据属于本地开发材料，不发布到 Git。

## CI 与质量保障

PR 与 main 推送触发 [ci.yml](.github/workflows/ci.yml)，检查并行执行，每周一凌晨自动
全量重跑以刷新漏洞库数据：

- 格式与静态检查：`cargo fmt --check`、Clippy（`-D warnings`，含 pedantic）、
  rustdoc（`-D warnings`，含私有项链接检查）、typos 拼写检查、actionlint 工作流自检；
- 构建与测试：nextest 全目标测试（失败自动重试一次、慢测试超时告警、JUnit 报告）+
  doctest；Linux / macOS / Windows 三平台；release 构建；MSRV（Rust 1.85）`--locked` 检查；
- 依赖健康：cargo-audit 漏洞库（`--deny warnings`）、cargo-deny（漏洞 / 许可证 /
  重复版本 / 通配符）、cargo-machete 未用依赖、Dependency Review（PR 依赖对比，
  高危阻断）、Dependabot 每周自动提依赖更新 PR；
- 覆盖率：`cargo llvm-cov` 生成 LCOV 并上传 [Codecov](https://codecov.io/gh/M1nt-Ch0c0/lark-codex-bridge)。
  公开仓库无需 token；若仓库转私有，需配置 `CODECOV_TOKEN` secret。

AI Review 使用 [CodeRabbit](https://www.coderabbit.ai/)（公开开源仓库免费，中文评论、
PR 摘要与逐行审查），配置见 [.coderabbit.yaml](.coderabbit.yaml)。接入步骤：用 GitHub
账号登录 [app.coderabbit.ai](https://app.coderabbit.ai/) 并选择本仓库完成 GitHub App
安装（或直接在仓库 Settings → GitHub Apps 搜索安装 CodeRabbit），选择 Free 计划；
之后每个 PR 会自动触发审查。觉得反馈太多时，把配置里的 `profile` 从 `assertive`
改为 `chill` 或 `quiet`。

## 目标

- 长期托管一个 `codex app-server`，避免每轮启动 `codex exec`。
- 使用 Rust 原生实现飞书长连接、OpenAPI、事件归一化和消息发送。
- 保留原项目的核心聊天、会话、工作区、附件、卡片和命令能力。
- 用有界队列、持久 outbox、幂等处理和显式恢复状态提高稳定性。
- 不支持 Claude、Web UI 和会议功能。

## 来源与许可证

本项目参考
[lark-coding-agent-bridge](https://github.com/zarazhangrui/lark-coding-agent-bridge)
的用户可见行为，但使用独立仓库、独立 Git 历史和全新 Rust 实现，并非 fork。

本项目采用 [MIT License](LICENSE)。
