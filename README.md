# lark-codex-bridge

[![CI](https://github.com/M1nt-Ch0c0/lark-codex-bridge/actions/workflows/ci.yml/badge.svg)](https://github.com/M1nt-Ch0c0/lark-codex-bridge/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/M1nt-Ch0c0/lark-codex-bridge?include_prereleases)](https://github.com/M1nt-Ch0c0/lark-codex-bridge/releases)

一个使用 Rust 编写的本地飞书 / Lark ↔ Codex 桥接器。它常驻连接
`codex app-server`，将私聊、群聊和话题中的消息交给 Codex，并通过持久化
inbox/outbox、会话复用和有界附件缓存提供可恢复的流式回复。

> 当前版本是 early alpha。基础消息链路可试用，但 slash command handler、Codex
> 审批卡、后台服务管理和完整故障恢复仍未完成。

## 安装

1. 打开 [GitHub Releases](https://github.com/M1nt-Ch0c0/lark-codex-bridge/releases)。
2. 下载与操作系统和 CPU 架构匹配的压缩包。
3. 解压后把 `lark-codex-bridge`（Windows 为
   `lark-codex-bridge.exe`）放入 `PATH`。

Linux / macOS 示例：

```bash
install -d "$HOME/.local/bin"
install -m 0755 ./lark-codex-bridge "$HOME/.local/bin/lark-codex-bridge"
lark-codex-bridge --version
```

Windows 可将 `lark-codex-bridge.exe` 放入一个已加入 `PATH` 的目录，再执行：

```powershell
lark-codex-bridge.exe --version
```

完整的下载、校验、升级和卸载说明见
[Release 安装手册](docs/guide/installation.md)。

## 前置条件

- 已安装并登录 Codex CLI；当前代码支持 `codex-cli 0.146.x`。
- 已创建飞书 / Lark PersonalAgent 应用，并把机器人加入目标会话。
- 已准备 owner 的 `open_id` 和一个允许 Codex 操作的绝对工作区路径。

## 快速开始

登记飞书应用。无参数时进入设备授权流程：

```bash
lark-codex-bridge lark auth register
```

也可以登记已有应用；secret 建议通过环境变量提供：

```bash
LARK_APP_SECRET='***' \
  lark-codex-bridge lark auth register --app-id cli_xxx --tenant feishu
```

创建 `config.toml`：

```toml
owners = ["ou_owner_open_id"]
default_workspace = "/absolute/path/to/workspace"

[workspace]
allow_roots = ["/absolute/path/to/workspace"]
network_access = false

[codex]
binary = "codex"
sandbox = "workspace-write"
approval_policy = "never"

[paths]
database = "state/bridge.sqlite3"
attachment_cache = "state/attachments"
```

先检查两侧连接：

```bash
lark-codex-bridge codex probe
lark-codex-bridge lark auth check
lark-codex-bridge lark probe
```

启动前台 bridge：

```bash
lark-codex-bridge run --config /absolute/path/to/config.toml
```

私聊可以直接发送消息；群聊和话题需要直接 @机器人。按 `Ctrl-C` 执行有序退出。

## 已实现能力

- 常驻 Codex app-server supervisor、thread 复用、turn 串行和中断底层能力；
- 飞书/Lark 凭证登记、OpenAPI、WebSocket、事件归一化与重连；
- SQLite WAL 单写者、持久 inbox/outbox、去重和 uncertain 状态；
- owner gate、安全工作区、同 scope 串行和跨 scope 有界并发；
- 延迟进度卡、独立最终回复、有序重试和 delivery receipt；
- 图片和普通文件输入、内容寻址缓存、lease、GC 与启动校验。

当前尚未接线：

- `/new`、`/cd`、`/stop`、`/status`、`/help` 的运行时 handler；
- Codex approve/deny 交互卡；
- 后台服务安装与管理；
- 周期性 inbox 重扫和完整 fault-injection/soak 门禁。

## 文档

- [文档首页](docs/README.md)
- [Release 安装](docs/guide/installation.md)
- [配置手册](docs/guide/configuration.md)
- [运行与维护](docs/guide/operations.md)
- [故障排查](docs/guide/troubleshooting.md)
- [模块功能手册](docs/modules/README.md)
- [开发架构手册](docs/architecture/README.md)

## 安全说明

- 默认 `workspace-write`、网络关闭、owner-only。
- 不要把 App Secret、tenant token、Authorization header 或用户正文写入日志。
- 不要把 `/`、HOME 根目录、系统目录或临时目录配置为工作区。
- `approval_policy` 当前建议保持 `never`；交互审批链路尚未实现。
- 附件缓存必须使用专用目录，不能复用 HOME 或其他业务目录。

本项目参考
[lark-coding-agent-bridge](https://github.com/zarazhangrui/lark-coding-agent-bridge)
的用户可见行为，但采用独立 Git 历史和全新 Rust 实现，并非 fork。

许可证：[MIT](LICENSE)。
