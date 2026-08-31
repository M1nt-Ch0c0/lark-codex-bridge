# CLI 与配置模块功能手册

## 模块职责

CLI 是唯一的主机侧用户入口，负责解析命令、输出脱敏 probe 结果、登记凭证和启动应用装配。
配置模块负责严格解析 TOML、路径归一化和安全策略准备。

关联代码：

- `src/main.rs`
- `src/cli.rs`
- `src/config.rs`
- `src/lark/credentials.rs`

## 当前命令

```text
lark-codex-bridge run [--config <path>]
lark-codex-bridge codex probe [--binary <path>]
lark-codex-bridge codex sidecar-probe [--node-binary <path>] [--entrypoint <path>] [--codex-binary <path>]
lark-codex-bridge codex adoption-status
lark-codex-bridge lark auth register [--app-id <id> --tenant <feishu|lark>]
lark-codex-bridge lark auth check
lark-codex-bridge lark probe
```

`run` 启动前台常驻进程。当前没有 host service、profile、migrate 或日志导出子命令。

`codex adoption-status` 是纯静态、脱敏的 capability matrix：Linux/macOS 构建把
`spawned_stdio` 和 `protocol_sidecar` 标记为
`available_dedicated_process_ownership`；Windows 构建因缺少 Job
`ACTIVE_PROCESS_ZERO` 证明而标记为 `unavailable_platform_process_tree_proof`。
`external_endpoint` 始终标记为 `unavailable_shared_external_endpoint`。输出同时声明
`supportedPlatforms: ["linux", "macos"]`。该命令不加载配置、不读取 `CODEX_HOME`、
不启动进程，也不连接 endpoint。

## probe 输出契约

probe 与 auth check 都只输出单行 JSON：

- Codex：受支持版本、initialize user agent、平台、supervisor epoch、backend、wire
  protocol/version 和 capability；
- auth check：tenant、bot 名称和 bot open_id；
- Lark probe：tenant、bot 身份、endpoint host、ping interval 和耗时。

禁止输出：

- App Secret、tenant token、Authorization；
- `CODEX_HOME`、账户身份和环境变量；
- 完整 WebSocket URL；
- 用户消息或附件内容。

## 配置行为

`BridgeConfig` 使用 `deny_unknown_fields`。加载顺序：

1. 定位显式或默认配置文件；
2. 解析 TOML；
3. 以配置目录为基准解析数据库、附件、ASR、channel sidecar 和 Codex protocol sidecar
   路径（裸命令名仍交给 `PATH`）；
4. 校验 owner、allow roots 和默认工作区；
5. 生成 `AccessPolicy`、`RouterSettings` 和 Codex 进程参数。

任何一步失败都停止启动。

## 凭证行为

`LARK_APP_ID`、`LARK_APP_SECRET`、`LARK_TENANT` 三个环境变量同时存在时完全覆盖文件。
`LARK_CREDENTIALS_FILE` 只改变文件位置。

文件写入在 Unix 使用临时文件、`fsync`、rename 和 `0600`。环境凭证只读，不会被写回。

## 当前限制

- 无显式配置的首次 `run` onboarding 会登记/复用凭证、写 owner 和安全默认 runtime config；
  已有配置或显式 `--config` 不会被覆盖。
- 单 profile、单默认凭证文件。
- 修改配置后必须重启。
- chat 内命令只有 parser/metadata，尚未接入生产 handler。
