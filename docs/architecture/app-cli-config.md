# CLI、配置与应用装配架构

## 分层

`src/main.rs` 只初始化 tracing、调用 CLI，并把顶层错误转换成退出码。

`src/cli.rs`：

- 定义稳定 CLI shape；
- 调用 probe/auth flow；
- 把 `run --config` 转给应用装配；
- 保证 probe 输出字段白名单。

`src/config.rs`：

- 解析严格 TOML；
- 校验 owner、allow roots 和路径；
- 构建 Codex、router 和 storage 参数；
- 手写 `Debug`，隐藏路径和值。

`src/app.rs`：

- 按依赖顺序创建组件；
- 在部分启动失败时逆序清理；
- 驱动 inbound receiver；
- 管理生产退出顺序。

## 启动顺序

1. 加载 `BridgeConfig`；
2. 加载环境或文件 credentials；
3. 创建 `AccessPolicy` 和 `RouterSettings`；
4. 打开 `StoreHandle`；
5. 打开并 reconcile `AttachmentCache`；
6. prepare `DurableIntake`；
7. 启动 `LarkBridge`；
8. 启动 `AppServerSupervisor`；
9. 启动 outbox sink/pump；
10. 启动带附件的 `Router`；
11. 进入 `drive_inbound`。

后创建的组件依赖前面的组件。任何中间失败必须停止已创建组件，不能遗留 app-server 子进程
或 store writer。

## 退出顺序

生产退出顺序不是简单逆序：

1. Lark transport 停止生产新事件；
2. Router/ScopeActor 完成有限 finalization；
3. outbox pump 停止；
4. attachment cache 被 drop，释放实例锁；
5. Store writer flush/stop。

这个顺序保证 actor 在退出前仍能写 outbox，pump 在 actor 完成前仍可投递，store 最后关闭。

## Extension seam

`OutboundFactory` 是应用装配的依赖注入点。生产使用 `ProductionOutboundFactory`，测试可注入
fake sink/pump，而无需复制完整 startup。

新增组件时应回答：

- 它依赖哪些已启动组件？
- 部分启动失败如何清理？
- shutdown 时谁先停止生产，谁最后停止持久化？
- 是否新增有界队列或 secret？

## 配置不变量

- `deny_unknown_fields` 保持；
- 配置错误为静态分类；
- 相对 runtime path 只相对 config 文件；
- 默认 sandbox 不应因新增功能放宽；
- 配置 migration 必须显式、可备份、可回滚，不能在解析失败时猜测。
