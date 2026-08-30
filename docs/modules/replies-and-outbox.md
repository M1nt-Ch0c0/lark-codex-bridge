# 回复投影与 outbox 模块功能手册

## 模块职责

Render 将 Codex 事件投影成用户可见的进度和终答；outbox 将这些副作用持久化并按顺序发送到
Lark。两者分离，避免网络失败改变回复语义。

关联代码：

- `src/render/mod.rs`
- `src/outbox/`
- `src/store/outbox.rs`

## 回复契约

1. 明确标记为 final answer 的最后一条 agent message 是独立终答。
2. final-only turn 不创建进度卡。
3. clean-empty turn 不发送空消息。
4. progress 失败不能吞掉 final。
5. 没有独立 final 时，在既有进度卡内收口完整 fallback，不再发送重复文本。
6. Lark 返回非空 `message_id` 后才算 final 已送达。

## 进度

只有 commentary phase 可进入进度。delta 先按 item 缓冲，等 `ItemCompleted` 暴露 phase 后
才决定是否显示，防止 final delta 提前泄漏到进度。

进度同时受最小时间间隔、最小新增字符和总长度限制。持久 enqueue 失败时 projector 可恢复
最近 checkpoint，让 terminal fallback 不丢文本。

## Outbox 操作

典型操作：

- 创建进度卡；
- 更新进度卡；
- finalize 进度卡；
- 发送独立 Markdown `post` 终答；
- 发送拒绝/失败通知。

每行有确定性 idempotency key，后继不能越过失败前驱。

## 重试和 uncertain

- 临时 HTTP/transport 失败：增加 attempt，原子 defer 当前行及后继；
- definitive permanent failure：记录 terminal receipt；
- 请求可能已到达但响应未知：停在 uncertain，不自动重发；
- 断线或 shutdown 后，未发送的 claimed tail 返回 pending，不增加 attempt。

## 内容处理

- 无进度卡的当前独立终答持久化为 payload v2 `reply_markdown_post`，通过 Lark
  `msg_type=post`/`tag=md` 发送；它不是 text-only reply；
- 进度创建、更新和终态使用同一条 Card 2.0 `interactive` 消息，正文为 `tag=markdown`；
- 拒绝、过载、失败和中断等短通知仍使用纯文本；
- 历史 payload v1 `reply_text` 保持原来的纯文本载体，不根据 Markdown 标点升级类型；
- post 与 Card 2.0 分别经过载体专用的 Markdown 净化、wire-aware 上限和确定性分片/截断；
- 邮箱样式文本中的 `@` 会被审计掩码，npm scope、版本和 @mention 不受影响；
- standalone post 同时按 Unicode 标量数和最终序列化 wire bytes 确定性分片；
- 超过总预算时带明确截断标记；
- Debug 只记录 part 数量和字符数。

## 当前限制

- 用户无法通过 `/status` 查看 parked uncertain 行，因为 handler 尚未接线；
- 交互卡按钮尚未路由；
- 尚无外部 dead-letter 管理命令。
