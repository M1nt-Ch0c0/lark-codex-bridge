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

启动流程：

1. 使用 App ID/Secret 获取 WebSocket endpoint；
2. 建立 TLS WebSocket；
3. 解码 frame 和分片；
4. 处理 ping/pong；
5. 把完整事件交给 normalizer；
6. 断线后按 server 配置和本地上限退避重连。

frame、分片集合、队列和 payload 都有计数/字节上限。

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

- card action 目前返回 unsupported，不进入 runtime；
- 引用正文、merge-forward 和完整话题历史尚未注入 Codex；
- 文档评论入口尚未实现；
- 真实 Lark smoke 必须显式启用，普通测试中的 skip 不是通过证据。
