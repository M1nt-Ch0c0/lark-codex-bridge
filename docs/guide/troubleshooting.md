# 故障排查

先判断故障发生在哪一段：

```text
配置/凭证 → Lark 连接 → durable intake → scope/Codex → outbox/Lark 回复
```

## 无法读取配置

检查：

- `--config` 是否为正确文件；
- TOML 是否包含未知字段；
- owner 是否为空；
- 工作区是否为绝对路径且位于 allow root；
- 数据库和缓存相对路径是否能以配置目录为基准解析。

配置错误不会回退到宽松默认值。

## Codex probe 失败

```bash
lark-codex-bridge codex probe
codex --version
```

精确支持列表以源码中的 `SUPPORTED_CODEX_VERSIONS` 为准；当前是 `codex-cli 0.146.0` 和
`0.149.0`。更高的 patch/minor 版本不会自动视为兼容。还应确认 Codex 已登录、
`codex app-server` 可以启动、`CODEX_HOME` 可访问。

probe 超时或 app-server 退出时，先单独修复 Codex 环境，不要同时排查 Lark。

## Codex protocol sidecar probe 失败

选择 `[codex.backend].mode = "protocol_sidecar"` 时，不要用 native `codex probe` 代替实际路径：

```bash
lark-codex-bridge codex sidecar-probe \
  --entrypoint /opt/lark-codex-bridge/codex-sidecar/index.cjs
```

部署覆盖 pinned Codex 或使用非默认 Node、Codex home、wrapper 参数时，同步传入
`--codex-binary`、`--node-binary`、`--codex-home` 和可重复的 `--codex-argument`。检查
Node 20+、entrypoint 是否存在、`codex_home` 是否为已有
目录，以及版本输出是否精确为 `codex-cli 0.149.0` 或 `codex-cli 0.151.0`。sidecar bootstrap
在 15 秒内必须完成 hello/configure、七个 capability 精确匹配、版本 probe 和 Codex child
启动；缺失/额外 capability、其他版本和畸形帧都会 fail closed。

sidecar stderr 只给出 `codex_sidecar_failure code=<静态分类>`。不要为排障打印 configure frame、
Codex stderr、用户正文或完整路径。`protocol_sidecar` 失败不会自动切换到 native；修复后重新
运行 probe，再完整重启 bridge。版本 probe 超时、probe I/O 和进程资源压力会进入有界退避；
无效配置、缺失的 pinned artifact、确定性启动/版本/协议错误会永久 fail closed。若显示进程树
cleanup 失败，supervisor 会禁止创建替代 epoch，必须先确认残留进程已清理再重启 bridge。

## run 报告 Codex supervisor degraded

`Codex supervisor degraded` 表示 Codex supervisor 已进入本次进程内不可恢复的 terminal 状态。
该 tracing 行故意不包含具体 reason，避免把本机路径或 secret 写进结构化日志。处理方式取决于
它发生在 `bridge runtime ready` 之前还是之后：

- **启动期降级**：`run` 会在打印 ready 之前清理已启动组件，以非零状态退出；紧随 tracing
  行的 CLI 错误会给出可操作原因，Lark 入站不会启动。
- **运行期降级**：进程保持运行，让 Router 对仍在等待或尚未 claim 的 `received` 入站做持久化
  拒绝并写入静态内部提示，避免消息无限等待。运行时 terminal 通知不会携带具体 reason，也不会
  把它写入 tracing 或用户提示；运维应停止当前进程、用下面的 probe 复核并修复后再启动。

按当前 backend 执行对应命令复核同一原因：

```bash
# spawned_stdio
lark-codex-bridge codex probe

# protocol_sidecar（参数必须与配置一致）
lark-codex-bridge codex sidecar-probe \
  --entrypoint /opt/lark-codex-bridge/codex-sidecar/index.cjs
```

`external_endpoint` 不使用上述两个本地 probe，也不会进入普通 `run` 的 supervisor
恢复流程；选择它时普通 mutation-driven `run` 会 fail closed。要独立复核 admission
gate，在仓库 checkout 中运行 `cargo test --locked --test external_endpoint_gate`，然后按
[External Codex endpoint admission gate](../external-codex-endpoint-gate.md#verification) 以精确测试名
显式运行真实 binary smoke。不要尝试不存在的 external CLI probe，也不要把 admission
测试通过视为普通 `run` 已支持该模式。

如果原因是 unsupported version，native backend 只安装 `SUPPORTED_CODEX_VERSIONS` 中列出的
0.146.0/0.149.0；protocol sidecar 只安装其独立精确列表 0.149.0/0.151.0。其他版本必须先按
Schema/adapter 契约升级流程评审，不能仅因本机版本号更高就绕过门禁。若旧版 bridge 在运行期
degraded 后没有拒绝新消息，应先停止并升级，避免消息停留在 `received`。

## Lark auth 失败

```bash
lark-codex-bridge lark auth check
```

确认：

- App ID、App Secret 和 tenant 成套匹配；
- `feishu` 与 `lark` 没有选错；
- PersonalAgent 应用仍有效；
- 环境变量没有只设置一部分并覆盖正确的文件凭证。

不要在命令输出、Issue 或日志中粘贴 secret。

## Lark probe 失败

```bash
lark-codex-bridge lark probe
```

auth check 成功但 probe 失败，通常是 endpoint 获取、DNS、TLS、代理、防火墙或
WebSocket ping/pong 问题。probe 只报告 endpoint host，不会输出完整带参数 URL。

## Bot 收不到消息

- 确认机器人已经加入会话；
- 群聊/话题必须直接 @机器人；
- sender 必须是人类，且满足其一：在 `owners` 中、在 `allowed_senders` 中，或所在群聊命中
  `allowed_groups`（P2P 不适用群白名单）；
- `@all` 不算直接 mention；
- 检查应用事件权限和长连接订阅；
- 使用 `RUST_LOG=debug` 查看脱敏的忽略原因。

## Codex 有结果但飞书没有终答

检查 outbox 日志分类和 Lark transport 状态。不要手工重放 uncertain 写：

- definitive failure 可以按 backoff 重试；
- 连接中断导致的发送结果未知必须保留 uncertain；
- final 只有拿到非空 Lark `message_id` 才算成功。

## 附件失败

- 只支持当前 normalize 路径识别的图片和普通文件；
- 单文件、单消息数量、单 turn 总字节都有限制；
- attachment cache 必须是专用目录；
- 第二个实例使用同一缓存会被实例锁拒绝；
- marker 损坏、symlink、权限无法收紧都会 fail closed。

不要删除正在被 lease 使用的内容文件，也不要手工改 SQLite attachment 表。

## 退出卡住或进程异常退出

正常情况下使用 `Ctrl-C`。若操作系统强制终止：

1. 确认没有残留 bridge 或 app-server 进程；
2. 保留数据库、WAL 和缓存目录；
3. 重新执行两个 probe；
4. 启动并观察 startup reconcile；
5. 不要通过删除数据库来掩盖 uncertain turn。

如果问题可复现，Issue 应包含版本、平台、静态错误分类和复现步骤；不要附带用户正文、
secret、token 或完整本地路径。
