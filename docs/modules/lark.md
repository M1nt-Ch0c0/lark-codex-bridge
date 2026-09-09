# Lark 模块功能手册

## 模块职责

Lark 模块覆盖凭证、tenant token、OpenAPI、WebSocket transport、protobuf frame、事件归一化
和生产入站队列。

关联代码位于 `src/lark/`。

## 租户

支持：

- `feishu`：飞书中国区；
- `lark`：Lark 国际版。

tenant 决定 accounts、OpenAPI 和 WebSocket bootstrap endpoint，凭证不能跨 tenant 混用。

## 认证

支持两种登记方式：

- 设备授权流程创建或绑定 PersonalAgent；
- 使用已有 App ID，secret 通过环境变量或显式参数提供。

tenant token provider 负责缓存和刷新 token；永久认证错误与临时网络错误分开分类。

## Transport

入站事件始终走官方 Node SDK sidecar（`sidecar/`）。查询、媒体下载和出站仍走 Rust OpenAPI。
没有 native WebSocket 开关，启动失败也不会回退。

sidecar 完成协议配置并由 SDK 报告 `connected` 后才算启动成功。Rust 在 POSIX 上拥有整个
sidecar 进程组、在 Windows 上拥有 Job object。协议见 [channel-wire-v1](../channel-wire-v1.md)。

## 事件归一化

当前生产入口处理 `im.message.receive_v1`，构造稳定 `InboundEvent`：

- sender open_id；
- chat_id；
- message_id、create_time；
- p2p/group/topic mode；
- direct mention 结果；
- parent_id/quote 单跳关系；
- 文本；
- 图片/文件资源描述。

Scope 规则：

- 私聊、普通群：`im:<chat_id>`；
- 话题：`im:<chat_id>:thread:<thread_id>`。

话题事件缺少 thread_id 时会做一次受控回填；失败时记录 degradation，不猜测 scope。

## 消息准入

Lark normalizer 只负责协议和结构，不负责最终授权。owner、mention 和 workspace 决策由 runtime
policy 执行。

## 出站

`LarkApi` 提供文本回复、卡片创建/更新、资源下载和消息查询等受控接口。outbox 只有获得
非空 `message_id` 才把最终发送记为成功。

## 当前限制

- 已识别的 `card.action.trigger` 会合成 slash command 进入 runtime；无法识别的回调只 ACK；
- 引用正文、合并转发和话题首次介入的最近上文会注入 Codex；不会递归整段聊天历史；
- 文档评论入口尚未实现；
- 真实 Lark smoke 必须显式启用，普通测试中的 skip 不是通过证据。
