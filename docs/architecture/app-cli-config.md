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
6. 启动 `AppServerSupervisor`，首次永久 degraded 时在入站装配前 fail closed；
7. prepare `DurableIntake`；
8. 启动 `LarkBridge`；
9. 启动 outbox sink/pump；
10. 启动带附件的 `Router`；
11. 启动 attachment runtime reconcile actor；
12. 进入 `drive_inbound`。

后创建的组件依赖前面的组件。任何中间失败必须停止已创建组件，不能遗留 app-server 子进程
或 store writer。

入站装配期间持续监听 supervisor；Router task 接管 ownership、完成 terminal 检查并回传
startup ack 后，应用层才可打印 `bridge runtime ready`。ack 前的启动 future 若被取消，先通过
协作式 cancellation 统一停止 tool task、等待 stale sweep、关闭 supervisor 并 reconcile 附件；
有界 watchdog 只在清理超时后强制中止。永久 supervisor reason 只沿应用错误链交给 CLI
stderr；tracing 和 scope actor 只接收静态状态或无内容的 terminal 标志。运行期进入 terminal
状态时，等待 client 的 received 入站必须原子拒绝并写入静态 notice，不能无限等待下一次
supervisor 通知。

## 退出顺序

生产退出顺序不是简单逆序：

1. Lark transport 停止生产新事件；
2. attachment runtime reconcile actor cancel 并有界 join；
3. Router/ScopeActor 完成有限 finalization，并执行 terminal reconcile；
4. outbox pump 停止；
5. attachment cache 被 drop，释放实例锁；
6. Store writer flush/stop。

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
- 只有配置加载器明确列出的 runtime path 才相对 config 文件解析；Codex protocol sidecar
  的 entrypoint/home 与带路径分隔符的命令路径也按配置目录解析；
- 默认 sandbox 不应因新增功能放宽；
- 配置 migration 必须显式、可备份、可回滚，不能在解析失败时猜测。
