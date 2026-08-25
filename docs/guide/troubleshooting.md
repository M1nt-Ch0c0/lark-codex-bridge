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

当前精确支持 `codex-cli 0.146.0` 和 `0.149.0`。还应确认 Codex 已登录、`codex app-server` 可以启动、
`CODEX_HOME` 可访问。

probe 超时或 app-server 退出时，先单独修复 Codex 环境，不要同时排查 Lark。

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
