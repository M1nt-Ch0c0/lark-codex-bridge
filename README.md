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
压缩。

不支持的图片会降级为 `Image: alt (target)`，reference link/image 会降级为带 reference 标签的
可读文本，reference definition 会显示为普通文本。`~~~` fence 统一转成长度安全的反引号 fence；
畸形 info string 整体降级为 `text`，未闭合 fence 在每个发送快照中补齐。原始 HTML 标签/注释
只在代码 span 外移除，畸形标签使用惰性的 Unicode 尖括号显示且不会跨行吞掉引用内容。
可能触发真实提及的 `<at …>` 控制、双向/零宽 Unicode format controls 会在两个载体中净化；
代码 span 内的 `<at …>` 仅作为代码字面量保留，不能触发提及。

转换先于分片。每个 `post` 分片同时检查 4,000 个 Unicode 标量上限和最终 Lark
reply JSON 的精确序列化字节数（包括内层 JSON 转义与话题回复标记），最多 8 片；在
代码块内切分时会闭合当前片并在下一片重新打开相同围栏，超出总预算则显式附加
`…[truncated]`。

持久 outbox 从本功能起只写 payload v2；升级后的 reader 同时严格读取 v1/v2。历史 v1
`reply_text` 始终保持纯文本载体，不会因内容像 Markdown 而改型；历史终态 Card 的备用正文
会先经过当前净化器再成为 `post`。数据库 `PRAGMA user_version=6` 是显式降级栅栏：升级前应
停掉旧进程并备份数据库，升级后不得再用只认识 payload v1/schema v5 的旧二进制打开同一库。
确定未生效的终态 Card PATCH 在永久拒绝或瞬态重试耗尽后，会把同一个确定性 outbox 行原子
转换成 standalone Markdown post；响应丢失、畸形或其他 `uncertain_delivery` 绝不会触发备用
发送。仅 HTTP 429/5xx 和平台明确记录的限频码会重试；校验、卡片格式、机器人不在会话、消息
已撤回等明确业务拒绝直接视为永久失败。

真实桌面端/移动端 Markdown 验收是单独的显式门控测试。先在目标会话发送一条可供
机器人回复的消息并取得其 `message_id`，准备三个不纳入 Git 的本地路径，然后运行：

```bash
LARK_MARKDOWN_E2E=1 \
LARK_E2E_APP_ID=… LARK_E2E_APP_SECRET=… LARK_E2E_TENANT=feishu \
LARK_MARKDOWN_E2E_PARENT_MESSAGE_ID=om_… \
LARK_MARKDOWN_E2E_DESKTOP_SCREENSHOT=/tmp/lark-markdown-desktop.png \
LARK_MARKDOWN_E2E_MOBILE_SCREENSHOT=/tmp/lark-markdown-mobile.png \
LARK_MARKDOWN_E2E_ATTESTATION=/tmp/lark-markdown-attestation.json \
  cargo test --test lark_markdown_smoke --locked -- --ignored --nocapture
```

测试发出覆盖全部子集及表格降级的真实 `post` 后会打印回复 `message_id`、一次性 `nonce`
和发送正文 SHA-256，默认等待 5 分钟。在飞书桌面端和移动端分别打开该回复、确认排版可读
且表格显示为 fenced text，再把两张不同的新截图保存到上述路径，计算各自 SHA-256，并写入
同样是新生成的强类型验收文件：

```json
{
  "version": 1,
  "nonce": "测试打印的一次性nonce",
  "message_id": "om_测试打印的回复ID",
  "markdown_sha256": "测试打印的正文hash",
  "desktop": {"verdict": "pass", "sha256": "桌面截图hash"},
  "mobile": {"verdict": "pass", "sha256": "移动截图hash"},
  "table": "fenced"
}
```

测试在发送前记录三个证据文件的旧 hash，只接受发送完成后发生变化且能真实解码、像素数有界、
内容 hash 不同的 PNG/JPEG/WebP；验收文件还必须绑定本次 nonce、message ID、正文 hash 和两张
截图 hash。显式运行 ignored smoke 却没有 `LARK_MARKDOWN_E2E=1` 或任一配置会直接失败，不会
以 skip 冒充证据。截图可能包含会话信息，因此只作为操作者保存的外部验收证据，不应提交仓库。

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
