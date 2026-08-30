# Release 安装手册

项目面向最终用户只推荐 GitHub Release 二进制，不要求本机安装 Rust 工具链。

## 1. 选择产物

打开 [Releases](https://github.com/M1nt-Ch0c0/lark-codex-bridge/releases)，选择最新的
稳定版或明确标记的 prerelease，并下载与平台、CPU 架构匹配的压缩包。

只从项目 Releases 页面下载。early alpha 阶段如果页面尚无可用产物，表示还没有可供普通
用户安装的正式包，不要从第三方镜像获取未知二进制。

## 2. 校验

安装前必须完成完整性校验。Release 需要提供 SHA-256 校验和文件或发布签名；两者都没有时
停止安装，不要执行该二进制。

SHA-256 校验：

Linux：

```bash
sha256sum --check SHA256SUMS
```

macOS：

```bash
shasum -a 256 -c SHA256SUMS
```

Windows PowerShell：

```powershell
Get-FileHash .\lark-codex-bridge.exe -Algorithm SHA256
```

将输出与 Release 页面或校验和文件比较。无法匹配时不要执行二进制。

## 3. 安装

### Linux / macOS

```bash
install -d "$HOME/.local/bin"
install -m 0755 ./lark-codex-bridge "$HOME/.local/bin/lark-codex-bridge"
lark-codex-bridge --version
```

如果 `$HOME/.local/bin` 不在 `PATH`，将它加入 shell 配置后重新打开终端。

### Windows

1. 创建例如 `%LOCALAPPDATA%\Programs\lark-codex-bridge` 的目录。
2. 把 `lark-codex-bridge.exe` 移入该目录。
3. 将目录加入当前用户的 `PATH`。
4. 在新 PowerShell 窗口执行：

```powershell
lark-codex-bridge.exe --version
```

## 4. 运行依赖

- 默认 `spawned_stdio`：Codex CLI 已安装并登录，精确版本为 0.146.0 或 0.149.0；
- 可选 `protocol_sidecar`：部署产物还必须包含 Node 20+，以及与当前平台和 CPU 匹配的
  `codex-sidecar/` 自包含目录；也可显式覆盖为已审核的精确 0.149.0/0.151.0 binary。该目录
  必须同时包含源码、lockfile、生产 `node_modules`、Codex 0.151.0 原生包和
  `artifact-manifest.json`，不能用仓库里的源代码目录代替。CI 会在 Linux、macOS、Windows
  上用真实 lockfile 依赖完成完整协议 smoke，再上传、下载独立的平台目录 artifact，由
  checkout 中的可信校验器先核对 workflow 保存的 manifest SHA-256 和逐文件清单，再按
  已认证清单恢复 GitHub artifact 丢失的 Unix 执行位，并在无 npm、无安装和无 package
  下载的路径中重复 smoke；
- 能访问对应租户的飞书或 Lark OpenAPI 与 WebSocket endpoint；
- 本地时钟和系统证书正常。

安装后先执行 Lark 检查，并按 Codex backend 二选一：

```bash
lark-codex-bridge lark auth check
lark-codex-bridge lark probe

# spawned_stdio
lark-codex-bridge codex probe

# protocol_sidecar（使用成功 CI workflow artifact，或未来 Release 中与平台/CPU 匹配的目录）
lark-codex-bridge codex sidecar-probe \
  --entrypoint /absolute/path/to/codex-sidecar/index.cjs
```

## 5. 升级

1. 按 `Ctrl-C` 停止当前前台进程。
2. 备份配置、SQLite 数据库和附件缓存目录。
3. 下载并校验新 Release。
4. 原子替换旧二进制，保留旧文件直到新版本完成 probe。
5. 重新执行 Lark 检查和所选 backend 的 Codex probe，再启动 bridge。

early alpha 期间不保证配置向后兼容。Release notes 如果要求迁移，应先按对应版本说明操作。

## 6. 卸载

停止 bridge 后删除二进制即可。配置和运行状态不会自动删除；确认不再需要恢复后，才手工
删除以下数据：

- 配置文件；
- `credentials.toml`；
- SQLite 数据库及 `-wal`、`-shm`；
- 专用附件缓存目录。

删除凭证和状态不可恢复。不要用宽泛递归命令指向 HOME、工作区根或不确定路径。
