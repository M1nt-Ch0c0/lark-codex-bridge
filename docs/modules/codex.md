# Codex 模块功能手册

## 模块职责

Codex 模块长期管理一个 `codex app-server --listen stdio://` 子进程，或管理一个再启动 Codex
的本地协议 sidecar，完成 JSONL RPC、initialize、thread/turn 生命周期、事件订阅、中断和
进程树重启。

关联代码位于 `src/codex/`。

## 版本和启动

默认 `spawned_stdio` 精确支持 `codex-cli 0.146.0` 和 `0.149.0`；显式
`protocol_sidecar` 精确支持 `0.149.0` 和 `0.151.0`。启动前严格执行版本 probe，输出必须符合：

```text
codex-cli X.Y.Z
```

不接受前后空格、prerelease、build metadata 或不同命令名。通过版本门禁后才启动
app-server 并执行 initialize。

对应的检查命令是：

```bash
lark-codex-bridge codex probe
lark-codex-bridge codex sidecar-probe --entrypoint /absolute/codex-sidecar/index.cjs
```

sidecar 使用固定 v1 hello/configure 握手、33,554,432-byte frame、448 pending 和七个精确
capability；完整实现契约见 [Codex protocol sidecar wire v1](../codex-sidecar-wire-v1.md)。

## Thread 与 turn

- 每个 Lark scope 最多映射一个 active Codex thread；
- 没有 thread 时调用 `thread/start`；
- 已有映射时调用 `thread/resume`；
- 每轮先持久化 turn，再调用 `turn/start`；
- 非幂等 start/resume 不在客户端盲目重试；
- `turn/interrupt` 只表示请求已接受，最终状态仍以 `turn/completed` 为准。

## 事件

模块向 runtime 投影以下主要事件：

- agent message delta；
- item started/completed；
- turn started/completed；
- thread token usage；
- server request 和未知未来通知。

未知 enum/notification 尽量 fail-soft；违反关键顺序、容量或身份契约时 fail closed。

## Supervisor

Supervisor 持有单调递增 epoch。sidecar 模式中的 epoch 仍只由 Rust 持有，不写入本地 wire。
子进程或 sidecar/Codex 进程树退出后：

1. 当前 client 和 subscription 失效；
2. 处于不确定窗口的 turn 保留 uncertain；
3. 按退避策略重新启动；
4. 新连接获得新 epoch。

runtime 不能把旧 epoch 的请求或事件误归入新连接。

## 输入

文本作为 Codex user input。图片使用 `localImage`。普通文件以结构化文本描述传入，包含
受控缓存中的 canonical path、hash 和字节数。

## 当前限制

- 普通 `run` 支持 owned `spawned_stdio` 和 `protocol_sidecar`；显式 shared external endpoint
  仍在 mutation-driven 装配路径 fail closed。
- 外部 persisted thread 接管尚未实现（Issue #4）。
- Schema 自动同步和兼容矩阵尚未实现（Issue #7）。
- server request 的飞书 approve/deny UI 尚未实现；建议 approval policy 保持 `never`。
