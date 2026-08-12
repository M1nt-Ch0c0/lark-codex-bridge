# lark-codex-bridge 开发接管手册

> 最后核对：2026-08-13（Asia/Shanghai）
> 已验证代码基线：`f804c36`
> 公开仓库：<https://github.com/M1nt-Ch0c0/lark-codex-bridge>
> 当前工作区：`/home/wcy/.lark-channel-workspaces/codex/default/lark-codex-bridge`

本文用于让新的主 Agent 在不丢失已有设计、代码、审查结论和半成品的前提下直接接管开发。
请把当前文件系统和 Git 状态视为最终事实；本文是导航和约束，不替代接管时的只读核对。

## 1. 接管后的第一组命令

新主 Agent 应先执行：

```bash
cd /home/wcy/.lark-channel-workspaces/codex/default/lark-codex-bridge
git status --short
git log --oneline --decorate -12
git rev-list --left-right --count origin/main...main
gh run list --branch main --limit 5
codex --version
```

预期状态：

- `main` 与 `origin/main` 无分叉，且 `f804c36` 是当前 `HEAD` 的祖先；交接文档的同步提交会位于它之后；
- 代码基线 `f804c36` 对应的 GitHub Actions run `31635523167` 成功；若已有更新 run，另行核对它的结论；
- 工作区干净，没有未跟踪文件；
- 本机 Codex 是 `codex-cli 0.146.0`；
- 当前没有本项目启动的残留 `codex app-server --listen stdio://` 子进程。

不要在接管时运行 `git clean`、`git reset --hard`、`git checkout --`。若出现用户新增的文件，先确认所有权再处理。

## 2. 原始目标和不可缩小的范围

本项目基于参考仓库重新实现一个 Rust 桥接器：长期托管 Codex app-server，保留参考项目的
核心飞书/Lark 使用体验，同时提高稳定性、降低常驻负载，并减少每轮调用 Codex 的启动成本。

必须遵守：

1. 新项目是独立 public 仓库和全新 Git 历史，不是 fork。
2. 只支持 Codex app-server；不支持 Claude、多 provider adapter 或 `codex exec` 每轮子进程。
3. 不实现 Web UI、HTTP 管理后台、React 前端或会议机器人。
4. 使用 Rust 原生实现飞书/Lark WebSocket 与 OpenAPI，不依赖 Node sidecar。
5. 所有长期队列、缓存、pending map、mailbox、附件和 outbox 都必须同时有数量和字节上限。
6. 不确定的非幂等写入必须显式 `uncertain` 或 fail-closed，不得盲目重试并声称成功。
7. 以高效切片开发为主：每个阶段一次集中实现、一次集中门禁、一次高风险复审，避免把工作拆得过碎。
8. 经常做可运行的小提交并推送公开 `main`，让用户能看到进展。
9. 低负载和更高效率最终必须由同机基准证明，不能只凭实现语言或主观判断宣称达成。

权威设计和计划：

- `docs/superpowers/specs/2026-08-12-lark-codex-bridge-design.md`
- `docs/superpowers/plans/2026-08-12-foundation-app-server.md`

上面目录名中的 `superpowers` 只是早期开发时留下的文件路径；两个文件都是普通 Markdown。
接管者直接读取即可，不需要安装 Superpowers 或任何其他插件，也不需要遵循其中与具体 Agent 工具相关的流程。

参考仓库本地路径：

```text
/home/wcy/.lark-channel-workspaces/codex/default/feishu-claude-code-bridge
```

其已拉取的参考基线为 `zarazhangrui/lark-coding-agent-bridge@e5d3ce5`。只参考用户可见行为和
必要协议语义，不复制其 Git 历史，也不要回收 Claude、Web UI 或会议功能。

## 3. Git、仓库和发布约定

新仓库：

```text
git@github.com:M1nt-Ch0c0/lark-codex-bridge.git
```

用户已明确授权：

- 不 fork；
- 直接在新仓库 `main` 上开发、提交和推送；
- 经常推送阶段性进展。

因此无需为正常阶段提交再次询问是否可以 push。仍需遵守：

- 不覆盖用户已有 dirty 文件；
- 子 Agent 默认只提交、不 push，由主 Agent统一复核和 push；
- 每次 push 后检查 GitHub Actions；
- 失败时读取具体 job 日志，做最小修复并单独提交；
- 不重写远端历史，不 force-push。

可靠的 SSH push 命令：

```bash
git -c core.sshCommand='ssh -o BatchMode=yes -o ConnectTimeout=15 -o ServerAliveInterval=10 -o ServerAliveCountMax=2' push origin main
```

## 4. 已完成并公开发布的工作

| 提交 | 内容 | 状态 |
| --- | --- | --- |
| `e427a4b` | Rust 重写架构设计 | 已发布 |
| `4f5e4eb` | app-server 基础里程碑计划 | 已发布 |
| `e2d631c` | Rust crate、CLI shell、CI、许可证 | 已发布 |
| `dc6ca2f` | Codex 0.146.x 稳定协议 DTO 与 fixtures | 已发布 |
| `feefbbf` | 有界 stdio JSONL transport、版本探测、进程适配 | 已发布 |
| `b1ac583` | RPC actor、请求关联、初始化握手 | 已发布 |
| `cdb7413` | Task 4 文档同步 | 已发布 |
| `1c90bb1` | typed thread/turn/event client | 已发布 |
| `72d66fd` | RPC 并发、可靠事件与 lease 边界加固 | 已发布 |
| `6eca3fa` | 覆盖 normal+high 全部 RPC cancellation 容量 | 已发布 |
| `a9b41be` | Task 5 计划勾选同步 | 已发布 |
| `fe04651` | 兼容当前 GitHub stable Clippy | 已发布、CI 全绿 |
| `baa5da9` | supervisor 拥有 child/transport/RPC/client epoch | 已发布 |
| `f2bdb2c` | 结构化 `codex probe`（真实 initialize 握手） | 已发布 |
| `40f0118` | opt-in 真实 Codex smoke（`CODEX_E2E=1`） | 已发布 |
| `49cfd60` | Task 6 文档与计划勾选 | 已发布 |
| `0f15127` | 审查加固：client shutdown 限时、wait 失败强杀 | 已发布 |
| `f804c36` | 修复 Windows fake 的绝对 codexHome | 已发布、CI 全绿 |

### 4.1 当前可用的 Codex 基础能力

- 严格门禁 `codex-cli >=0.146.0,<0.147.0`；
- 直接启动 `codex app-server --listen stdio://`；
- 限长、分 lane、有字节预算的 stdin/stdout/stderr transport；
- 不带 `jsonrpc` 字段的官方 JSONL envelope；
- `initialize` → response → `initialized {}` 精确握手；
- epoch 化请求 ID：`c:<epoch>:<monotonic-u64>`；
- 并发 RPC、超时清理、EOF fanout、迟到/未知 response drift；
- opaque string/integer server request ID 原样响应；
- `thread/start`、`thread/resume`、`turn/start`、`turn/interrupt`；
- thread-scoped bounded mailbox、delta 合并和 wire 因果顺序；
- authoritative `item/completed` / `turn/completed` 投影；
- typed `TurnOutcome`，包含 status、error、completed items 和最后 token usage；
- approval/server request 的高优先级响应和 fail-closed lease；
- 显式 `release_thread`，防止长连接历史 route 永久耗尽；
- supervisor 单一持有 child/transport/RPC/client epoch，`Starting → Ready`、异常退出 `Backoff → Starting → Ready`、永久错误 `Degraded`；
- 退避基数 0.5/1/2/4/8/16/30 秒封顶 30 秒，bounded jitter；shutdown 先关 stdin、5 秒 grace、再强杀并 wait；
- `codex probe` 经真实 initialize 握手输出单 JSON 对象（version、user agent、platform、epoch），不泄露 codexHome/身份/token/env；
- `tests/codex_smoke.rs` 真实 smoke（`#[ignore]` + `CODEX_E2E=1` 双门控）已在本机认证账户下通过。

### 4.2 已经证明的重要并发和资源不变量

后续代码不得回退这些不变量：

1. high/normal RPC 分别有独立 inflight、command byte budget 和 sender pump。
2. normal inflight 饱和时，interrupt/approval 仍能进入 high lane。
3. cancellation channel 容量覆盖 `normal 320 + high 64 = 384` 的全部 pending 上限。
4. server request、`item/completed`、`turn/completed`、`error` 使用独立的 reliable count+byte lane；普通 progress 不能占用它。
5. normal/reliable 两条事件 lane 使用 actor sequence 合并，保持原始 wire 顺序。
6. server-response lease 在命令进入 actor 后由后台持有；成功 flush 才 resolve，任何 queue、deadline、transport、pump 或 actor failure 都 fail-close epoch。
7. completion send 观察 cancellation，actor shutdown 在 join pump 前关闭并 drop completion receiver，不形成互等。
8. inbound JSON 在构造 `serde_json::Value` 前进行保守的 depth/token preflight；密集数组和深嵌套会前置拒绝。
9. transport/RPC/client mailbox 都有 byte permit，permit 随对象跨层传递，不能在排队前提前释放。
10. `thread/start`、`thread/resume`、`turn/start` 在可能已发送且调用方取消时 fail-close；明确的本地 pre-send 错误不应杀健康 epoch。
11. terminal overflow 必须显式使订阅失效，不能假装流仍完整。
12. `Debug`/错误不得输出 prompt、tool output、server error message/data、stderr 内容或敏感路径。

相关回归测试集中在：

- `tests/protocol_fixtures.rs`
- `tests/transport.rs`
- `tests/rpc_duplex.rs`
- `tests/client_flow.rs`

Task 5 最终本地门禁为 90 tests；随后公开 `fe04651` 的 GitHub Actions run
`31621324810` 在 Linux quality、Rust 1.85、macOS、Windows 全部成功。

## 5. Task 6 已完成（2026-08-13）

Task 6（supervisor、`codex probe`、真实 Codex smoke）已实现、审查、真实验证并发布，
`docs/superpowers/plans/2026-08-12-foundation-app-server.md` 中 Task 6 的 Step 1–5 全部勾选，
app-server 基础里程碑完成。

### 5.1 直接证据

- 本地全量门禁全绿：fmt、clippy `-D warnings`、`cargo test --all-targets --locked`（94 passed + 1 ignored smoke）、`cargo +1.85.0 check`、release build、`git diff --check`；
- 真实 smoke：`CODEX_E2E=1 cargo test --test codex_smoke --locked -- --ignored --nocapture` → 1 passed in 6.11s（initialize → read-only ephemeral thread → turn → authoritative `TurnCompleted`，agent message 含 `pong`）；
- 真实 probe：`cargo run --locked -- codex probe` 输出单 JSON 对象，仅含 `supportedVersion`、`initializeUserAgent`、`platformFamily`、`platformOs`、`epoch`；
- smoke/probe 后 `ps` 检查无本项目残留的 `codex app-server --listen stdio` 子进程；
- GitHub Actions run `31635523167`（`f804c36`）在 Linux quality、Rust 1.85、macOS、Windows 全部成功（Windows 曾因 fake `codexHome: "/scrubbed"` 非绝对路径失败，已由 `f804c36` 修复）；
- 只读并发/资源审查（`4a2be26..0f15127`）结论无 P0；P2 加固（client shutdown 限时、wait 失败强杀）已随 `0f15127` 落地。

### 5.2 已知限制与审查遗留 P2（不阻塞，后续里程碑处理）

- supervisor 仅以 child 退出触发重启；transport 协议违例但 child 仍存活时 epoch 会 fail-close 但不重启（RPC 连接已被 client 消耗，无死亡观测接口）；
- fake 测试未覆盖"grace 内不退出→超时强杀"与握手期 `RpcError::Server` → `Degraded` 的分类路径；
- `terminate` 结果被忽略不记录；`start_with_factory` 依赖注入 factory 有界；watch 合并下瞬时中间态可能被跳过（测试靠 yield 规避）。

## 6. Task 6 之后仍未完成的工作

整个用户目标远未完成。Task 6 只是 Codex 基础层里程碑，后续至少还有三大阶段。

### 6.1 原生飞书/Lark transport 与 OpenAPI

需要新建计划 `docs/superpowers/plans/2026-08-12-native-lark-transport.md`，实现：

- PersonalAgent 扫码注册或已有 App ID/App Secret；
- 飞书和 Lark 国际版域名；
- `/callback/ws/endpoint` bootstrap；
- protobuf binary frame、ping/pong、分片有界重组；
- WebSocket 重连、认证永久错误 degraded；
- tenant token 缓存；
- 消息、卡片、图片、文件下载/发送；
- 私聊、群聊 @、话题、引用、必要的历史回填；
- 稳定 `InboundEvent` 归一化；
- 处理成功后才返回 frame receipt。

目前 `Cargo.toml` 还没有 `prost`、`tokio-tungstenite`、`reqwest`、`url` 等飞书依赖，
也没有任何 `src/lark` 模块。

### 6.2 SQLite、scope runtime、outbox 和回复

需要新建计划 `docs/superpowers/plans/2026-08-12-reliable-bridge-runtime.md`，实现：

- SQLite WAL、foreign keys、migration、单写者任务；
- inbound event/message 去重及 `received/accepted/completed/rejected`；
- scope key、workspace 和 policy fingerprint；
- 每 scope 一个 actor、600ms 合批、运行期间消息进入下一 turn；
- 全局 active-turn semaphore；
- thread 映射、resume、`/new`、uncertain turn 恢复；
- durable outbox、幂等键、receipt 和 uncertain delivery；
- progress/final reply projector；
- final-only、clean-empty、流失败、迟到流和重复文字硬契约；
- 图片和文件的有界下载、内容寻址缓存、lease 与 GC；
- 第一阶段命令 `/new`、`/stop`、`/status`、`/cd`、`/help`；
- owner-only、群 @、安全工作区默认值。

目前没有 `src/store`、`src/runtime`、`src/outbox`、`src/render`、`src/config` 或 `src/app`。

### 6.3 核心 parity、平台集成和发布

需要新建计划 `docs/superpowers/plans/2026-08-12-core-parity-platform.md`，实现：

- `/ws`、`/resume`、`/timeout`、`/doctor`、`/reconnect`；
- `/invite`、`/remove` 与管理员；
- command/file/permission approval 卡片和一次性 nonce；
- profile 与 `lark-cli` 身份隔离；
- systemd、launchd、Windows Task Scheduler；
- 云文档评论和 team mode（安全审查后才开启）；
- kill -9、断网、慢消费者、重复投递、发送不确定等故障注入；
- 24h soak；
- 与参考 Node/`codex exec` 实现的同机 RSS/CPU、cold/warm latency、20 turns、1/4/8 scope 基准；
- 安装、配置、运维、迁移和发布文档。

只有设计规格第 18 节的所有验收项都有直接证据时，才能把持久 goal 标为 complete。

## 7. 验证和发布门禁

### 7.1 每个实现切片

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-targets --locked
cargo +1.85.0 check --all-targets --all-features --locked
cargo build --release --locked
git diff --check
```

Task 6 额外执行：

```bash
cargo test --test supervisor --locked -- --nocapture
CODEX_E2E=1 cargo test --test codex_smoke --locked -- --ignored --nocapture
cargo run --locked -- codex probe
```

真实 smoke 后用精确命令行和 PID/parent 关系确认没有本测试残留的 stdio app-server。桌面 Codex 自身的
`app-server --listen unix://` 和 `app-server proxy` 是宿主进程，不要误杀。

### 7.2 推送后

```bash
gh run list --branch main --limit 5 --json databaseId,headSha,status,conclusion,url
gh run view <run-id> --json status,conclusion,jobs,url
gh run view <run-id> --job <job-id> --log-failed
```

GitHub stable 可能比本机 stable 更新。`a9b41be` 曾因 Clippy 1.97 的 `question_mark` lint 失败，
随后由 `fe04651` 做语义等价修复并获得全绿。不要把本机 lint 通过当作远端全绿的替代。

## 8. Kimi 接管与协作方式

本项目不要求 Kimi 安装任何特定 Agent 插件或扩展。只要 Kimi 能读取工作目录、
执行 shell、编辑文件和使用 Git，就可以继续开发。若某个工具名在 Kimi 环境中不存在，直接使用其已有的
等价能力；仓库事实和验收要求不依赖某个 Agent 产品。

### 8.1 启动 Kimi 主会话

1. 让 Kimi 使用当前本地目录，而不是只读取 GitHub 网页：
   `/home/wcy/.lark-channel-workspaces/codex/default/lark-codex-bridge`。
2. 直接粘贴第 9 节的接管提示词。
3. 要求第一轮只读核对 Git、文档、CI 和工作区状态，不要立即批量改代码。
4. 核对完成后从第 6 节的下一个里程碑继续，不需要恢复或知道此前聊天记录。

Task 6 起所有代码与测试均已提交并推送；只从 `origin/main` 新克隆的 Kimi 也能看到全部成果。

### 8.2 如果 Kimi 支持子 Agent

子 Agent 不是必需条件。支持时按以下边界使用；不支持时由主会话顺序执行同样工作。

1. 同一个工作目录同时只允许一个 Agent 写代码。
2. 实现 Agent 负责一个明确切片，可以修改和提交，但不 push。
3. 审查 Agent 默认只读，只检查固定 commit 或 diff，不和实现 Agent 同时改文件。
4. 主 Agent 复核 diff、运行门禁、处理审查意见，然后统一 push 并观察 CI。
5. 调研类 Agent 不修改代码，只返回来源、结论和对主线的具体建议。
6. 每个子任务都必须写清工作目录、允许修改的范围、禁止事项、验收命令和预期输出。
7. 不要同时派多个实现 Agent 修改 supervisor、RPC、client 等共享模块。

### 8.3 实现子任务提示词（模板）

Task 6 已用下列提示词完成。后续切片（见第 6 节）套用同一模板，替换目标与验收命令：

```text
在下面仓库继续 <切片名称>：
/home/wcy/.lark-channel-workspaces/codex/default/lark-codex-bridge

先完整阅读 docs/DEVELOPMENT_HANDOFF.md，以及
docs/superpowers/plans/<对应计划文件> 中的 <切片范围>。

<切片的具体要求、必须复用的现有模块、禁止事项>。

不要删除或覆盖无关 dirty 文件。先跑 focused tests，再集中运行文档第 7 节门禁。完成后提交但不要 push，
返回 commit、完整测试结果、尚存风险和需要主 Agent 决定的问题。
```

### 8.4 并发与资源审查子任务提示词

```text
只读审查 lark-codex-bridge 当前切片的固定 commit/diff，不修改、不提交。
先读 docs/DEVELOPMENT_HANDOFF.md 第 4、5、7 节。

重点检查：child/transport/RPC/client 单一所有权、epoch 失效、重启状态顺序、退避和永久错误分类、
shutdown/kill/wait、调用方取消、watch/client race、有界 queue/byte、敏感信息脱敏、probe 输出、
真实 smoke 是否遗留 orphan。只报告会影响正确性、资源安全、机密性或可恢复性的具体问题。

每项问题给出 file:line、复现场景、影响和最小修复建议；若没有阻塞问题，明确写出审查过的边界和
未覆盖风险。把结果返回主 Agent，由主 Agent 决定修复和发布。
```

### 8.5 Kimi 没有子 Agent 时

由同一会话按顺序完成：只读核对 → 实现 → focused tests → 全量门禁 → 重新阅读 diff 做审查 → 修复 →
提交 → push → 检查 CI。审查阶段应暂时停止写代码，以固定的 `git diff` 或 commit 为对象逐项核对第 8.4 节，
避免一边修改一边宣布审查通过。

## 9. 可直接复制给 Kimi 的接管提示词

```text
请接管并持续完成 lark-codex-bridge 的持久开发目标。

工作目录：
/home/wcy/.lark-channel-workspaces/codex/default/lark-codex-bridge

第一步必须完整阅读：
docs/DEVELOPMENT_HANDOFF.md

然后以当前文件系统、Git、GitHub Actions 为事实源核对 handoff。不要 git clean/reset。已验证代码基线是
f804c36，Task 1–6（app-server 基础里程碑）已完成并 CI 全绿；supervisor/probe/smoke 已实现并有真实证据。

继续完整目标，不要把成功标准缩小到已完成的里程碑：按设计依次完成原生飞书/Lark transport、
SQLite/scope/outbox/reply/attachments、命令/审批/权限/服务管理、故障注入、parity 和性能基准。
不要支持 Claude、Web UI 或会议功能。

不需要安装任何特定 Agent 插件。若使用子 Agent，共享工作区同一时间只允许一个 writer，
审查 Agent 只读；若不支持子 Agent，就在当前会话中按实现、测试、审查的顺序完成。采用高效切片开发，
集中测试和审查，避免把任务拆得过碎。

用户已批准在独立 public 仓库 main 上小步提交并经常 push。每次 push 后必须观察 CI。只有设计规格
第 18 节所有验收项都有直接证据时才可宣称整个项目完成；否则继续推进并明确剩余工作。
```

## 10. 接管完成前的自检

新主 Agent应能明确回答：

- 当前稳定远端 commit 和 CI run 是什么？
- 当前是否还有未提交的半成品？（Task 6 完成后应为无）
- Task 6 的直接证据是什么（第 5.1 节）？
- 哪些 Codex 并发/内存不变量已经由 Task 5 建立，不能回退？
- Task 6 后还剩哪三大里程碑？
- 如果使用多个 Agent，如何保证同一时间只有一个 writer、审查者只读？
- 什么情况下可以 push，什么情况下可以宣称完整目标完成？

若这些问题还不能从当前状态得到确定答案，应继续只读核对，不要开始大规模改动。
