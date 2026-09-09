# 安装

目前没有 GitHub Release。从源码编译：

```bash
git clone https://github.com/M1nt-Ch0c0/lark-codex-bridge.git
cd lark-codex-bridge
cargo build --release --locked
```

二进制在 `target/release/lark-codex-bridge`。日常开发直接 `cargo run --locked -- run` 即可。

## 运行依赖

- 默认 `spawned_stdio`：本机 `codex` 已登录，版本必须是 `codex-cli 0.146.0` 或 `0.149.0`
- 可选 `protocol_sidecar`：Node 20+，以及 `codex-sidecar/`（先 `npm ci --ignore-scripts --prefix codex-sidecar`）。只接受精确 `0.149.0` / `0.151.0`
- 能访问对应租户的飞书或 Lark OpenAPI 和 WebSocket

```bash
cargo run --locked -- lark auth check
cargo run --locked -- lark probe
cargo run --locked -- codex probe
```

sidecar 模式把最后一条换成：

```bash
cargo run --locked -- codex sidecar-probe --entrypoint "$PWD/codex-sidecar/index.cjs"
```

`external_endpoint` 不是普通 `run` 路径，没有 CLI probe。

## 升级 / 卸载

停掉前台进程，重新 `git pull` 再编译。配置、SQLite 和附件缓存不会跟着二进制走。

不再用时删掉二进制即可。确认不需要恢复后再删：

- `config.toml`
- `credentials.toml`
- SQLite 以及同目录的 `-wal`、`-shm`
- 附件缓存目录
