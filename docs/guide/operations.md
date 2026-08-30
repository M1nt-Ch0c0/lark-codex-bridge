# 运行与维护手册

## 启动前检查

```bash
lark-codex-bridge --version
lark-codex-bridge codex probe
lark-codex-bridge lark auth check
lark-codex-bridge lark probe
```

三个检查分别验证：

- bridge 二进制可执行；
- Codex 版本、app-server 启动和 initialize 握手；
- 飞书凭证、bot 身份、WebSocket endpoint 以及 ping/pong。

probe 输出只包含脱敏结构字段，不包含 secret、token、完整 endpoint 或用户正文。

## 启动

```bash
RUST_LOG=info \
  lark-codex-bridge run --config /absolute/path/to/config.toml
```

当前只提供前台运行。后台 service 子命令尚未实现；不要把未来文档里的 service 行为当作
当前能力。

## 消息准入

- 私聊：owner 可直接发送。
- 群聊：owner 必须直接 @机器人。
- 话题：以 `chat_id + thread_id` 形成独立 scope，也必须直接 @机器人。
- `@all` 不算 @机器人。
- bot、系统和未授权 sender 会被拒绝或忽略。

每个 scope 同时只运行一个 turn；运行期间的新消息进入下一轮。不同 scope 可在全局许可数内
并发。

## 回复

- commentary 类型的 Codex 输出达到时间和字符阈值后更新进度卡；
- 独立 final answer 单独发送；
- 没有独立 final 时，把完整 fallback 内容收口到既有进度卡；
- 空输出不发送空消息；
- 出站失败进入持久重试或 uncertain 状态，不盲目重复不可判定写入。

## 数据和备份

需要一起备份：

- `config.toml`；
- `credentials.toml`；
- SQLite 文件及同目录的 `-wal`、`-shm`；
- 附件缓存目录。

一致性备份的推荐方式是先按 `Ctrl-C` 完成有序退出，再复制文件。运行中只复制主数据库可能
遗漏 WAL 内容。

## 有序退出

按 `Ctrl-C` 后，应用按以下顺序收口：

1. 停止 Lark 入站 producer；
2. cancel 并有界等待 attachment runtime reconcile actor；
3. 停止并等待 scope actor 完成有限 finalization，再执行 terminal reconcile；
4. 停止 outbox pump；
5. 释放附件缓存锁；
6. 停止 SQLite writer。

不要把常规退出做成强制 kill。进程崩溃后，下次启动会校验 attachment cache 和持久状态，
运行期也会逐批完成 attachment reconcile；但周期性全量 inbox 重扫尚未实现。

## 观察

默认日志写到 stderr。使用 `RUST_LOG` 控制级别：

```bash
RUST_LOG=lark_codex_bridge=debug lark-codex-bridge run --config /path/config.toml
```

Debug 日志按设计只记录分类、计数和长度。发现 secret、token、用户正文或敏感绝对路径时，
应按安全缺陷处理。
