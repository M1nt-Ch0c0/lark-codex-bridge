# lark-codex-bridge

[![CI](https://github.com/M1nt-Ch0c0/lark-codex-bridge/actions/workflows/ci.yml/badge.svg)](https://github.com/M1nt-Ch0c0/lark-codex-bridge/actions/workflows/ci.yml)

飞书 / Lark 和本机 Codex app-server 之间的桥。Rust 写的常驻进程：消息进 Codex，回复打回飞书。还是 alpha，能日常用，别当生产机器人。

## 环境

- Rust 1.85+（见 `rust-toolchain.toml`）
- 已登录的 Codex CLI。默认 `spawned_stdio` 只要精确 `codex-cli 0.146.0` 或 `0.149.0`
- 一个飞书 / Lark 应用机器人，并已加入目标会话

更高版本的 Codex（目前是 `0.149.0` / `0.151.0`）走可选的 `protocol_sidecar`，需要 Node 20+。

## 跑起来

```bash
cargo run --locked -- run
```

第一次会扫码登记 PersonalAgent（或复用已有凭证），把你写成 owner，生成工作区和 `config.toml`，然后前台跑。`Ctrl-C` 停。已有配置或显式 `--config` 不会被覆盖。

路径：

| | Linux / macOS | Windows |
| --- | --- | --- |
| 配置 | `~/.config/lark-codex-bridge/config.toml` | `%APPDATA%\lark-codex-bridge\config.toml` |
| 工作区 | `~/.local/share/lark-codex-bridge/workspace` | `%LOCALAPPDATA%\lark-codex-bridge\workspace` |

私聊直接发。群聊和话题要 @机器人。起来后可以先发 `/help`、`/status`、`/info`。

日志默认只打错误。`-v` 看连接和 turn，`-vv` 再看队列。`RUST_LOG` 会盖掉这套默认过滤。

```bash
cargo run --locked -- run -v
cargo run --locked -- --log-format json run
```

## 检查两边

```bash
cargo run --locked -- lark auth check
cargo run --locked -- lark probe
cargo run --locked -- codex probe
```

已有 App ID / Secret 时：

```bash
cargo run --locked -- lark auth register --app-id <id> --tenant feishu
# secret 从 LARK_APP_SECRET 读
```

## Codex sidecar（可选）

默认不用。要用 lockfile 里钉死的 0.151.0（或已审核的 0.149.0 binary）：

```bash
npm ci --ignore-scripts --prefix codex-sidecar
cargo run --locked -- codex sidecar-probe --entrypoint "$PWD/codex-sidecar/index.cjs"
```

配置里显式打开，不要指望运行中回退到 stdio：

```toml
[codex.backend]
mode = "protocol_sidecar"
node_binary = "node"
sidecar_entrypoint = "/absolute/path/to/codex-sidecar/index.cjs"
```

协议说明：[docs/codex-sidecar-wire-v1.md](docs/codex-sidecar-wire-v1.md)。

## 配置要点

完整字段见 [docs/guide/configuration.md](docs/guide/configuration.md)。最少要有 owner 和工作区。可选白名单：

```toml
owners = ["ou_owner_open_id"]
allowed_senders = ["ou_member_open_id"]
allowed_groups = ["oc_chat_id"]
```

群白名单只放开普通消息，控制命令仍只有 owner 能用。群里还是要直接 @机器人。

飞书命令见 [docs/guide/commands.md](docs/guide/commands.md)。排障见 [docs/guide/troubleshooting.md](docs/guide/troubleshooting.md)。

## 文档

[docs/README.md](docs/README.md) 是目录。使用说明在 `docs/guide/`，模块说明在 `docs/modules/`，实现细节在 `docs/architecture/`。

行为参考过 [lark-coding-agent-bridge](https://github.com/zarazhangrui/lark-coding-agent-bridge)，仓库和实现都是独立的。[MIT](LICENSE)。
