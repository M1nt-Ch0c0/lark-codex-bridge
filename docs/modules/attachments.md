# 附件模块功能手册

## 模块职责

附件模块下载 Lark 图片/文件，写入私有内容寻址缓存，为当前 turn 建立 lease，并在 turn
terminal 后释放。磁盘路径只由 SHA-256 决定，不使用用户文件名。

关联代码：

- `src/runtime/attachments.rs`
- `src/store/attachments.rs`
- `src/lark/api.rs`

## 支持的输入

- 图片：转换为 Codex `localImage`；
- 普通文件：转换为包含 canonical cache path、SHA-256、字节数和受控显示名的结构化文本。

未知或暂不支持的 media type 不会伪装成成功附件。

## 安装流程

1. 校验 resource key、数量、文件名和 MIME；
2. 下载受单文件上限保护的 bytes；
3. 在缓存目录创建同目录临时文件；
4. 流式计算 SHA-256，写入并 `fsync`；
5. atomic rename 到 hash 文件；
6. 提交 attachment row 和 turn lease。

磁盘安装先于 store commit。崩溃最多产生可 reconcile 的 orphan file，不产生指向缺失文件的
有效 row。

## Cache 安全

`AttachmentCache::open` 要求：

- 路径不是 symlink；
- 空目录可初始化，非空目录必须有正确 marker；
- Unix 权限可收紧到 `0700`；
- marker 和实例锁为 `0600`；
- 能获取非阻塞 OS 独占锁。

错误配置到 HOME 或业务目录时拒绝扫描和 chmod。

## Lease

- 获取附件时绑定 turn row；
- Completed/Failed/Interrupted 后释放；
- Uncertain 在旧 supervisor epoch 生命周期结束后释放；
- 有 lease 的对象不参与普通 GC。

## GC 与 reconcile

GC 按年龄、条目数、总字节和批次上限删除无 lease 内容。顺序是先删 store row，再删文件。

reconcile 使用可恢复目录迭代器，每次只处理有界批次：

- 删除 orphan temp；
- 删除无 store row 的文件；
- 删除大小/hash 不一致内容；
- 删除指向缺失内容的 row；
- 清理 stale lease；
- 从上次游标继续，重复调用后收敛。

## 当前限制

- 每消息资源数、单文件大小、单 turn 总字节均为编译时上限；
- 最后一个附件下载后才可能发现 turn 总预算超限；
- 启动装配当前只执行一个 reconcile 批次，极端目录需要后续 pass；
- 故障文件系统上的单次阻塞 I/O 没有严格 wall-clock 上限。
