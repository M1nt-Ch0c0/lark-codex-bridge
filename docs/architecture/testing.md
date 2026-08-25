# 测试与变更验收

## 测试层级

### 纯函数/状态机

- protocol DTO；
- command parser；
- policy；
- ReplyProjector；
- limit/path validation。

要求确定性、无网络、可覆盖边界和未知未来字段。

### 组件测试

- SQLite store；
- RPC duplex；
- supervisor + fake child；
- Lark HTTP/WebSocket stub；
- attachment filesystem；
- outbox pump。

这些测试验证并发、取消、crash window 和容量。

### 装配测试

验证 `app.rs` 的启动失败清理、正常 driver、producer 关闭和 shutdown 顺序。

### 真实 smoke

- `codex_smoke`：真实认证 Codex；
- `lark_smoke`：真实 PersonalAgent、会话和 WebSocket。

真实 smoke 必须显式门控。测试显示 ignored/skip 只能说明未执行，不能作为通过证据。

## 标准门禁

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-targets --all-features --locked
cargo test --doc --all-features --locked
cargo build --release --locked
git diff --check
```

本页给开发者记录源码门禁，不是最终用户安装方式。根 README 和使用手册只使用 Release
二进制命令。

## 文档门禁

文档变更至少检查：

- 所有相对 Markdown 链接存在；
- 命令来自当前 `--help`；
- 配置字段来自 `BridgeConfig`；
- 当前限制没有被写成已实现；
- 没有 secret、token、真实 open_id/chat_id 或敏感路径；
- 根 README 不出现源码运行命令；
- Release asset 名称没有在 workflow 未定义时被臆造。

## 高风险变更

以下变更需要额外 adversarial 测试：

- 非幂等 RPC 重试；
- outbox 顺序和 uncertain 分类；
- store 多事务拆分；
- scope control priority；
- card callback/approval；
- attachment 删除与锁顺序；
- policy/allow root 放宽；
- 任何新增无界集合、队列或日志 tail。

## 提交证据

PR/提交说明应区分：

- 静态检查；
- 单元/组件测试；
- fake end-to-end；
- 真实 Codex；
- 真实 Lark；
- 未执行的门禁。

不要把“代码可编译”描述成真实消息链路已验证。
