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
- 清单表：`snapshots` / `files` / `chunks` / `file_chunks` / `missing_chunks`。

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
| POST | `/v1/restore` | `{snapshot_id, target_dir, allow_incomplete?}` 恢复 |
| POST | `/v1/retention` | `{source_root, keep_last_complete, dry_run?}` 保留策略预览/应用 |

## 保留策略（retention）

按 `source_root` 执行「保留最近 N 个 `complete` 快照」：

- **预览**：`dry_run: true` 只读计算，返回将删除的快照、保留的快照（含 `failed`/`incomplete` 及其保留原因）、待回收块数与字节数，不修改任何数据。
- **应用**：`dry_run` 缺省/false 时执行。先在**单个 SQLite 事务**内删除目标快照的全部清单行，并在同事务内基于仍保留快照的 `file_chunks` 引用清掉零引用块注册行；**提交之后**才从块目录删除这些零引用块。共享块被任一保留快照引用即安全；空文件无块，天然不受影响。
- **失败语义**：清单事务失败则整体回滚，无任何变化；块文件删除失败只留无害孤儿文件（记入报告 `warnings`），绝不会出现清单已删却仍引用已删数据的半状态。
- **可重复执行**：同一策略再次应用返回空删除集与零回收；`failed`/`incomplete`/`running` 快照永远不在删除范围内。

## 快照状态机

- `complete`：扫描无误，且清单引用的每个块都通过 `Verify`（读盘重算 SHA-256）。
- `incomplete`：有文件在读取期间被修改（mtime/size 在重读 3 次后仍不稳定），该文件标记 `unstable`，内容保留最后一次读取结果。
- `failed`：扫描出错，或验证发现引用的块不在块存储中；缺块逐条写入 `missing_chunks` 表。

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
保留策略 dry-run 预览（零副作用）→ 应用后旧 `complete` 快照删除、零引用块回收、保留快照仍可恢复 → 再次执行结果稳定。
