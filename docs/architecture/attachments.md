# Attachment cache 架构

## 组件

- `ResourceDownloader`：外部资源下载 trait；
- `LarkResourceDownloader`：生产 Lark API adapter；
- `AttachmentCache`：文件、row、lease、GC、reconcile；
- `AttachmentLimits`：统一硬边界；
- `store::attachments`：durable metadata。

## 锁层级

1. OS advisory instance lock：阻止两个进程共用缓存；
2. cache mutation mutex：串行 fetch/install/gc/reconcile 的破坏性对；
3. reconcile iterator mutex：保护可恢复目录游标；
4. SQLite 单 writer：串行 row/lease 事务。

持有顺序必须稳定，不能在 store 回调中反向获取 cache lock。

## 文件协议

缓存根包含：

- 固定 marker；
- instance lock file；
- 以 SHA-256 hex 命名的内容文件；
- 同目录临时文件。

显示文件名永远不是路径组件。删除操作只允许缓存根的直接子文件，防止 path traversal。

## Fetch crash window

```text
download
  → temp write + hash
  → fsync temp
  → atomic rename
  → verify final
  → store attachment row
  → store lease
```

如果在 store commit 前崩溃，留下 orphan file；reconcile 可删除。如果先写 row 再写文件，
会产生 row 指向不存在内容，因此禁止改变顺序。

## GC crash window

```text
verify no lease
  → delete store row
  → delete file
```

在两步之间崩溃只留下 orphan file。反向顺序会留下 dangling row。

## Cancellation

blocking 文件操作在 Tokio blocking pool 中执行，并携带 owned mutex guard。调用 future 被取消
时，后台 mutation 仍保持锁直到完成，避免另一个任务观察半完成状态。

## Reconcile

reconcile 不做一次性全目录 collect。`ReadDir` 游标每次消费有限条目后放回 cache，下一次调用
继续。候选在真正修改前要重新验证，因为目录扫描和 apply 之间状态可能变化。

## 扩展检查表

新增 media 类型或缓存元数据时检查：

- resource key 是否仍不能影响路径；
- MIME/文件名是否只作受限 metadata；
- 单件和 turn 聚合预算是否共用同一 Limits；
- terminal/uncertain 是否释放 lease；
- Windows rename 和 Unix fsync/permission 行为；
- Debug/error 是否只含 hash、kind、size 等允许字段。

## 推荐测试

- `tests/attachments.rs`：cache 核心；
- `tests/runtime_scope.rs`：turn 集成；
- `tests/store.rs`：row/lease；
- `tests/lark_api.rs`：下载边界。
