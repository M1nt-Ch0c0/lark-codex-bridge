# lark-codex-bridge

[![CI](https://github.com/M1nt-Ch0c0/lark-codex-bridge/actions/workflows/ci.yml/badge.svg)](https://github.com/M1nt-Ch0c0/lark-codex-bridge/actions/workflows/ci.yml)
[![Codecov](https://codecov.io/gh/M1nt-Ch0c0/lark-codex-bridge/branch/main/graph/badge.svg)](https://codecov.io/gh/M1nt-Ch0c0/lark-codex-bridge)

一个面向飞书 / Lark 的 Codex 本地桥接器。项目使用 Rust 重写，直接连接
`codex app-server`，专注于低资源占用、可靠会话、稳定流式回复和可恢复运行。

## 项目状态

当前版本是早期 alpha，已经可以启动常驻 `run` 运行时做最小试用，但不应视为可投入
生产的飞书机器人。当前可运行链路包括：

- Codex app-server 的有界 stdio transport、RPC broker、typed thread/turn client、
  长驻 supervisor、thread 复用、`codex probe` 和门控的真实 Codex smoke；
- Rust 原生飞书/Lark 凭证登记、OpenAPI、WebSocket transport、事件归一化、
  `lark probe` 和门控的真实 Lark smoke；
- SQLite WAL 单写者 store、持久 inbox/outbox、去重、owner/指定 sender/指定群组 allowlist 授权、安全工作区策略、
  scope actor、同 scope 串行 turn 和不同 scope 的有界并发；
- 延迟进度卡、独立最终回复、重试/receipt/uncertain delivery，以及终态先持久化再收口；
- 图片 `localImage` 和普通文件结构化路径输入、内容寻址缓存、turn lease、GC 与启动校验；
- 完整应用装配和 `run --config`：飞书消息 → Codex turn → 飞书进度/终答。

尚未接线的是 slash command handler、Codex 审批卡、服务管理和完整故障注入/恢复。
`/stop`、`/status` 按当前最小试用范围明确暂缓；`/new`、`/cd`、`/help` 目前也只有
解析与 help 元数据，还未进入运行时。启动时会预装有界的 `Received` 行，但尚无周期性
重扫。首次启动 onboarding 已恢复参考实现的一命令体验：扫码注册后自动携带创建者身份、
生成安全默认工作区和运行配置并直接启动，见
[GitHub Issue #2](https://github.com/M1nt-Ch0c0/lark-codex-bridge/issues/2)。
真实 Lark smoke 是显式门控验收项；未运行或只看到 skip 都不算通过。

## 最小试用

前提：本机已安装并登录受支持的 `codex-cli 0.146.0` 或更高版本，飞书/Lark 应用机器人已创建并
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
