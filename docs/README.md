# 文档中心

文档按读者和用途分为三层。

## 使用与运维

- [Release 安装](guide/installation.md)：下载、校验、升级和卸载。
- [配置手册](guide/configuration.md)：完整 TOML、路径、权限和环境变量。
- [运行与维护](guide/operations.md)：探针、启动、数据目录、备份和退出。
- [故障排查](guide/troubleshooting.md)：按错误阶段定位常见问题。

## 模块功能手册

[模块功能手册](modules/README.md)面向使用者、测试人员和维护者，说明每个模块提供什么、
输入输出是什么、如何观察以及当前有哪些限制。

| 模块 | 功能手册 |
| --- | --- |
| CLI 与配置 | [modules/cli-and-configuration.md](modules/cli-and-configuration.md) |
| Codex app-server | [modules/codex.md](modules/codex.md) |
| Lark channel | [modules/lark.md](modules/lark.md) |
| Runtime 与路由 | [modules/runtime.md](modules/runtime.md) |
| Durable store 与恢复 | [modules/store-and-recovery.md](modules/store-and-recovery.md) |
| 回复投影与 outbox | [modules/replies-and-outbox.md](modules/replies-and-outbox.md) |
| 附件缓存 | [modules/attachments.md](modules/attachments.md) |

## 开发架构手册

[开发架构手册](architecture/README.md)面向代码贡献者，记录组件边界、状态机、并发模型、
故障语义和推荐验证入口。

| 架构主题 | 手册 |
| --- | --- |
| 总体架构 | [architecture/overview.md](architecture/overview.md) |
| CLI、配置与应用装配 | [architecture/app-cli-config.md](architecture/app-cli-config.md) |
| Codex 子系统 | [architecture/codex.md](architecture/codex.md) |
| Lark 子系统 | [architecture/lark.md](architecture/lark.md) |
| Runtime、路由与策略 | [architecture/runtime.md](architecture/runtime.md) |
| Store 与恢复 | [architecture/store.md](architecture/store.md) |
| Render 与 outbox | [architecture/outbox-render.md](architecture/outbox-render.md) |
| Attachment cache | [architecture/attachments.md](architecture/attachments.md) |
| 测试与变更验收 | [architecture/testing.md](architecture/testing.md) |

协议 ADR：[Codex protocol sidecar wire v1](codex-sidecar-wire-v1.md)。

## 文档状态约定

- **已实现**：生产装配路径已调用并有自动化测试。
- **底层已具备**：存在 API 或 store seam，但用户入口尚未接线。
- **规划中**：不能作为当前产品能力宣传。

代码行为与文档冲突时，以当前默认分支代码和测试为准，并应在同一变更中修正文档。
