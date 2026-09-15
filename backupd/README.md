# backupd — 本地增量备份服务（仅 API）

针对「备份显示成功、恢复时缺一段」这类失败而设计：快照**完成前**必须逐一验证清单引用的所有内容块真实存在于块存储中；恢复时逐文件核验 SHA-256 与长度，任何缺块都会在报告里点名，而不是让维护人员只看到一个空的上传队列。

## 架构

```
cmd/backupd            HTTP 服务入口
internal/server        JSON/HTTP API（Go 1.22+ 路由）
internal/backup        快照扫描 / 恢复引擎
internal/chunkstore    内容寻址块存储（独立目录 chunks/<sha[:2]>/<sha>，临时文件+rename 落盘）
internal/store         SQLite 清单（modernc.org/sqlite，纯 Go，无 cgo）
```

- 分块：`github.com/restic/chunker`，固定多项式 `0x3DA3358B4DC173`，min 2 KiB / avg 16 KiB / max 64 KiB。多项式固定 ⇒ 块边界跨快照、跨重启确定，小改动只产生少量新块。
- 清单表：`snapshots` / `files` / `chunks` / `file_chunks` / `missing_chunks` / `repair_attempts` / `purge_batches` / `purge_items` / `purge_audit`。

## 运行

```sh
go build -o bin/backupd ./cmd/backupd
./bin/backupd -addr 127.0.0.1:8471 -data ./backup-data [-enable-fault-injection]
```

`-enable-fault-injection` 仅用于演示/测试，允许快照请求携带 `fault_after_chunks` 模拟提交中断。

## API

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/v1/healthz` | 健康检查 |
| POST | `/v1/snapshots` | `{source_root, fault_after_chunks?}` 创建快照（同步） |
| GET | `/v1/snapshots` | 快照列表 |
| GET | `/v1/snapshots/{id}` | 快照详情（状态、统计、缺块数） |
| GET | `/v1/snapshots/{id}/files` | 清单（路径/类型/权限/长度/摘要/状态） |
| GET | `/v1/snapshots/{id}/missing` | **缺块清单：文件 + 块哈希 + 原因** |
| POST | `/v1/snapshots/{id}/repair` | `{repair_id, fault_after_chunks?}` 修复带缺块的 failed 快照 |
| GET | `/v1/snapshots/{id}/repairs` | 该快照的修复尝试记录 |
| GET | `/v1/snapshots/{id}/repairs/{repair_id}` | 单次修复尝试记录 |
| POST | `/v1/restore` | `{snapshot_id, target_dir, allow_incomplete?}` 恢复 |
| POST | `/v1/retention` | `{source_root, keep_last_complete, dry_run?, purge_id?, window_seconds?, operator?}` 保留预览/创建清理批次 |
| GET | `/v1/purges` | 清理批次列表 |
| GET | `/v1/purges/{purge_id}` | 批次详情（计划、快照项、审计流水） |
| POST | `/v1/purges/{purge_id}/undo` | `{snapshot_id}` 撤销单个 / `{all:true}` 整批撤销 |
| POST | `/v1/purges/{purge_id}/execute` | `{force?}` 到期执行清理 |

## 修复（repair）

对带缺块的 `failed` 快照按原 `source_root` 补齐缺失块（`complete`/`running`/`incomplete` 不可修复，语义不变）：

1. **源树核对**：清单中每个条目逐一比对——普通文件的 size、mtime、整文件 SHA-256，符号链接目标，目录存在性。任何缺失/变化逐项报告（`problems`），快照、清单、缺块记录全部保持不变，绝不把新内容混入旧快照。
2. **补齐缺块**：按同一固定分块参数重切源文件，产出的块序列必须与清单完全一致（第二道防护）；只写入块存储中不存在的块——哈希/长度经 `Put` 校验，已有块复用，绝不覆盖，不写清单。
3. **原子转正**：全部引用重新校验通过后，在**单个 SQLite 事务**内完成 `failed → complete`、清理 `missing_chunks`、记录尝试成功；事务失败整体回滚。
4. **幂等与断点续修**：`repair_id` 为幂等键，尝试记录持久化于 `repair_attempts` 表。已成功的 `(snapshot_id, repair_id)` 重复请求直接回放结果（`replayed: true`），不重写清单；崩溃/中断留下的 `running` 记录在重启后用同一 `repair_id` 重试即可安全续跑（已写入的块按内容寻址天然幂等）。

## 延迟清理与撤销（retention + delayed purge）

按 `source_root` 执行「保留最近 N 个 `complete` 快照」，但应用不再直接删除，而是留出可撤销窗口：

- **预览**：`dry_run: true` 只读计算将待清理的快照、保留快照（含原因）、预计可回收块数与字节数，不修改任何数据。
- **应用（创建批次）**：需提供幂等键 `purge_id` 与撤销窗口 `window_seconds`。超期 `complete` 快照在单个事务内被置为 `pending_purge` 并登记为批次快照项，同时记录策略参数、计划执行时间 `execute_after`、操作者与审计流水。**清单与块此刻零删除**。同一 `purge_id` 重复提交返回已存批次（`replayed`），参数不同则冲突；服务重启后批次可继续查询与执行。
- **撤销（窗口内）**：`undo` 按快照或整批撤销。快照仅状态列从 `pending_purge` 翻回 `complete`——文件/块清单从未被触碰，立即可完整恢复。整批撤销后批次为 `cancelled`，不可再执行。
- **执行（到期后）**：`execute` 默认要求窗口到期（`force` 可人工覆盖）。执行时逐项重新校验：状态已变化（被撤销/被保护）或 **repair 进行中**的快照跳过并报告，批次保持 `open` 可再次执行；`failed`/`incomplete`/`running` 永远不会进入批次。清单删除与零引用块计算（基于所有未真正删除快照的 `file_chunks`）在**单个事务**内提交，之后**才**删除块文件；块文件删除失败只留无害孤儿块（记入 `warnings`），绝不会出现清单仍引用已删块的状态。重复执行为幂等回放。
- **迁移**：旧库打开时自动建齐 `purge_batches`/`purge_items`/`purge_audit` 三表；早期 `repair_attempts` 的 `REFERENCES snapshots` 外键自动重建移除（修复与清理记录是历史数据，必须比快照活得更久）。

## 快照状态机

- `complete`：扫描无误，且清单引用的每个块都通过 `Verify`（读盘重算 SHA-256）。
- `incomplete`：有文件在读取期间被修改（mtime/size 在重读 3 次后仍不稳定），该文件标记 `unstable`，内容保留最后一次读取结果。
- `failed`：扫描出错，或验证发现引用的块不在块存储中；缺块逐条写入 `missing_chunks` 表。
- `pending_purge`：保留策略已将其列入清理批次，等待窗口到期执行；清单完整，可撤销回 `complete`。

## 安全与一致性保证

- **扫描不跟随符号链接**（全程 `Lstat`）；链接目标经词法解析后越出源根目录的，记录为 `escaped`，永不跟随、永不恢复。
- **恢复根目录校验**：`target_dir` 本身若是符号链接（`MkdirAll` 会跟随已存在的链接）或非目录，直接拒绝，恢复写入不会穿透到允许根目录之外。
- **恢复不覆盖**：文件以 `O_EXCL` 创建，已存在即 `skipped_exists`；失败的文件删除半成品，不留残段。
- **恢复防链接逃逸**：清单路径词法检查不得含 `..`；目标路径的祖先目录逐一 `Lstat`，遇符号链接拒绝写入；越界链接目标 `skipped_unsafe_link`。
- **目录权限保留**：`MkdirAll` 后显式 `Chmod` 为清单记录的权限（绕过 umask）；文件恢复后 `Chmod` + `Chtimes`。
- **空文件**：0 个块，摘要为空串 SHA-256（`e3b0…`），正常恢复与核验。
- **失败快照默认拒绝恢复**；`allow_incomplete` 强制恢复时，缺块文件逐个 `failed` 并给出具体块哈希。

## 演示

```sh
./demo/demo.sh
```

覆盖：基线快照 → 小改动复用旧块（22 块只新增 1 块）→ 空文件 → 扫描中写入的文件标 `unstable` →
`fault_after_chunks=3` 模拟提交中断得到 `failed` 快照与具体缺块清单 → 恢复到新目录并
`diff -r` 独立核对 → 重复恢复全部 `skipped_exists` → 符号链接目标根被拒 → 失败快照拒绝/强制恢复 →
保留 dry-run 零副作用 → 创建清理批次（快照先 `pending_purge`）→ 同 `purge_id` 幂等回放 → 重启后批次/审计可查 →
窗口内撤销并完整恢复 → 窗口未到期执行被拒 → 整批撤销 → 到期执行只回收零引用块、共享块与空文件不受影响 →
重复执行幂等 → 修复中途崩溃 + 重启后同一 `repair_id` 续修成功、快照原子转 `complete` →
源文件被改写/删除时修复逐项报告且数据库不变。
