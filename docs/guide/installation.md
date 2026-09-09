# 安装

目前没有 GitHub Release。从源码编译：

```bash
git clone https://github.com/M1nt-Ch0c0/lark-codex-bridge.git
cd lark-codex-bridge
npm ci --ignore-scripts --prefix sidecar
npm ci --ignore-scripts --prefix codex-sidecar
cargo build --release --locked
```

二进制在 `target/release/lark-codex-bridge`。日常开发直接 `cargo run --locked -- run`。

## 运行依赖

- Node 20+
- `sidecar/` 和 `codex-sidecar/`（上面的 `npm ci`）
- Codex 精确 `0.149.0` / `0.151.0`（省略 binary 覆盖时用 lockfile 里的 0.151.0）
- 能访问对应租户的飞书或 Lark OpenAPI 和 WebSocket

```bash
cargo run --locked -- lark auth check
cargo run --locked -- lark probe
cargo run --locked -- codex probe
```

## 升级 / 卸载

停掉前台进程，重新 `git pull`，再跑一遍 npm ci 和编译。配置、SQLite 和附件缓存不会跟着二进制走。

不再用时删掉二进制即可。确认不需要恢复后再删：

- `config.toml`
- `credentials.toml`
- SQLite 以及同目录的 `-wal`、`-shm`
- 附件缓存目录
