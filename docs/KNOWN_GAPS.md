# 已知遗留项

本文只记录相对批准规格或参考行为的稳定、用户可见缺口，不记录开发过程、临时计划或
Agent 接管状态。

## ONBOARD-001：首次启动未形成一键闭环

- 状态：Open
- 记录日期：2026-08-15
- 优先级：P1（阻塞无手工配置的首次试用，不影响已配置实例的数据面正确性）
- 范围：CLI、onboarding、配置与凭证控制面
- AI 估算：最小闭环 2–4 AEU；完整多 Bot profile parity 另需 5–8 AEU

### 参考行为

参考实现允许操作者直接执行 `lark-channel-bridge run`。默认配置不存在时，它进入扫码
向导，创建或绑定 PersonalAgent，识别应用创建者，创建 profile 托管的安全默认工作区，
持久化完整配置并继续启动。操作者不需要手工查找 `open_id` 或编写配置文件。

### 当前行为

Rust 实现的 `lark auth register` 已能完成扫码注册和凭证校验，但只持久化 App ID、App
Secret 与 tenant。注册响应携带的授权人 `open_id` 没有进入运行配置；`run` 只读取已有
`config.toml`，不会生成默认配置或工作区。`LARK_CREDENTIALS_FILE` 只能作为底层凭证路径
覆盖，不等同于参考实现的 profile onboarding。

### 最小修复边界

在现有应用运行时之前增加 onboarding 协调层：

1. 仅在默认凭证或默认运行配置缺失时进入首次启动流程；显式 `--config` 永不被改写。
2. 从可信注册响应或应用 owner API 获取创建者 `open_id`，不要求用户复制原始 ID。
3. 创建范围受限的 profile 托管工作区，禁止使用文件系统根、HOME 根、系统目录或临时根。
4. 为数据库和附件缓存生成 profile-local 路径。
5. 使用私有权限和原子替换分别持久化凭证与运行配置，然后复用现有 `app` 装配启动。
6. 失败时不留下可被误认为完成的半配置；重复执行必须幂等。

此修复不应修改 Router、ScopeActor、Codex RPC、SQLite 业务 schema、Outbox 或 Attachment
Cache 的核心协议，也不得放宽现有 owner gate、workspace allow-list 或 fail-closed 规则。

### 验收标准

1. 全新状态下执行不带配置路径的 `run`，完成扫码后能直接进入可收发消息的前台运行。
2. 自动生成的 owner 是当前应用视角下的可信创建者身份，不能使用 Bot 自身 `open_id`。
3. 自动工作区和状态路径通过现有安全校验，凭证文件保持 owner-only 权限。
4. 已有配置、显式配置和失败的 onboarding 均不会被静默覆盖或损坏。
5. 单元测试覆盖新注册、已有凭证、取消、重复运行、写入失败和并发首次启动。
6. README 的首次启动命令可以在受支持平台上复现，不再要求手工填写 TOML 或原始 ID。

完整的多 Bot profile 创建、选择、迁移和隔离仍属于后续 parity；它可以复用本项的
onboarding 协调层，但不应扩大最小闭环的运行时改动范围。
