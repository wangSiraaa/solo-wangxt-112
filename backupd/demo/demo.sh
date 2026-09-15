#!/usr/bin/env bash
# End-to-end demo of the backupd service:
#   1. baseline snapshot of a sample tree
#   2. small edit -> chunk reuse across snapshots (dedup)
#   3. empty file handling
#   4. file changing mid-scan -> snapshot marked incomplete
#   5. interrupted commit -> failed snapshot with concrete missing chunks
#   6. restore to a new directory with digest+length verification
#   7. re-restore -> existing files are never overwritten
#   8. restoring a failed snapshot is refused unless forced
set -euo pipefail

DEMO=/tmp/backupd-demo
SRC=$DEMO/src
RESTORE=$DEMO/restore
DATA=$DEMO/data
ADDR=127.0.0.1:8471
BASE=http://$ADDR
BIN=/workspace/backupd/bin/backupd

say()  { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
post() { curl -sS -X POST "$BASE$1" -H 'Content-Type: application/json' -d "$2"; }

rm -rf "$DEMO"
mkdir -p "$SRC" "$DATA"

# ---------------------------------------------------------------- sample tree
say "准备源目录 $SRC"
mkdir -p "$SRC/docs" "$SRC/private"
chmod 750 "$SRC/private"                       # 目录权限需要被保留
for i in $(seq 1 4000); do echo "alpha report line $i"; done > "$SRC/docs/report.txt"
for i in $(seq 1 3000); do echo "beta  metric line $i"; done > "$SRC/docs/metrics.txt"
head -c 200000 /dev/urandom > "$SRC/blob.bin"  # 随机二进制
echo "top secret" > "$SRC/private/notes.txt"
: > "$SRC/empty.txt"                           # 空文件
ln -s docs/report.txt "$SRC/latest-report"     # 根目录内的合法符号链接
ln -s /etc/passwd "$SRC/escape-hatch"          # 越出根目录的符号链接
find "$SRC" -printf '%y %m %p -> %l\n' | sort -k3

# ---------------------------------------------------------------- start server
say "启动 backupd (data=$DATA)"
"$BIN" -addr "$ADDR" -data "$DATA" -enable-fault-injection > "$DEMO/server.log" 2>&1 &
SRV=$!
trap 'kill $SRV 2>/dev/null || true' EXIT
for i in $(seq 1 50); do curl -sf "$BASE/v1/healthz" > /dev/null && break || sleep 0.1; done

restart_server() {
  kill $SRV 2>/dev/null || true; wait $SRV 2>/dev/null || true
  "$BIN" -addr "$ADDR" -data "$DATA" -enable-fault-injection >> "$DEMO/server.log" 2>&1 &
  SRV=$!
  for i in $(seq 1 50); do curl -sf "$BASE/v1/healthz" > /dev/null && break || sleep 0.1; done
}

# ---------------------------------------------------------------- snapshot 1
say "快照 1：基线"
post /v1/snapshots "{\"source_root\": \"$SRC\"}" | tee "$DEMO/snap1.json" | jq '{id,status,file_count,dir_count,symlink_count,total_bytes,chunks_added,chunks_reused}'

# ---------------------------------------------------------------- snapshot 2
say "快照 2：小改动（report.txt 追加一行）应大量复用旧块"
echo "alpha report line 4001 (revised)" >> "$SRC/docs/report.txt"
post /v1/snapshots "{\"source_root\": \"$SRC\"}" | tee "$DEMO/snap2.json" | jq '{id,status,chunks_added,chunks_reused,total_bytes}'
echo "块目录实际占用：$(du -sh "$DATA/chunks" | cut -f1)，块数：$(find "$DATA/chunks" -type f | wc -l)"

say "空文件 empty.txt 在清单中的样子（0 块、空内容摘要）"
jq '.[] | select(.path=="empty.txt")' < <(curl -sS "$BASE/v1/snapshots/2/files")

say "越界符号链接 escape-hatch 被记录为 escaped（只登记，不跟随）"
jq '.[] | select(.type=="symlink") | {path,link_target,status}' < <(curl -sS "$BASE/v1/snapshots/2/files")

# ---------------------------------------------------------------- snapshot 3
say "快照 3：扫描期间持续写入的文件 -> 重读后标为 unstable，快照 incomplete"
head -c 1000000 /dev/urandom > "$SRC/active.log"
( while :; do printf 'appended while scanning\n' >> "$SRC/active.log"; done ) &
WRITER=$!
sleep 0.2
post /v1/snapshots "{\"source_root\": \"$SRC\"}" | jq '{id,status,error,unstable_files}'
kill $WRITER 2>/dev/null || true
wait $WRITER 2>/dev/null || true
jq '.[] | select(.path=="active.log") | {path,status,size}' < <(curl -sS "$BASE/v1/snapshots/3/files")

# ---------------------------------------------------------------- snapshot 4
say "快照 4：新增 late.bin 后模拟提交中断（fault_after_chunks=3）-> failed，且能查到具体缺块"
head -c 300000 /dev/urandom > "$SRC/late.bin"   # 全新内容，保证本次有大量新块要上传
post /v1/snapshots "{\"source_root\": \"$SRC\", \"fault_after_chunks\": 3}" | jq '{id,status,error,missing_chunks}'
say "维护人员视角：失败快照的缺块清单（文件 + 块哈希 + 原因）"
curl -sS "$BASE/v1/snapshots/4/missing" | jq '.[0:5]'
echo "缺块总数：$(curl -sS "$BASE/v1/snapshots/4/missing" | jq 'length')"

# ---------------------------------------------------------------- restore
say "恢复快照 2 到新目录 $RESTORE（逐文件核验摘要与长度）"
post /v1/restore "{\"snapshot_id\": 2, \"target_dir\": \"$RESTORE\"}" | tee "$DEMO/restore1.json" \
  | jq '{status, restored, skipped, failed}'
jq '.entries[] | select(.status != "restored")' "$DEMO/restore1.json" || true

say "独立核对：源目录与恢复结果逐字节一致（排除快照 2 之后出现的文件与被拒的越界链接）"
diff -r --no-dereference --exclude=active.log --exclude=late.bin --exclude=escape-hatch "$SRC" "$RESTORE" && echo "DIFF_OK: 内容一致"
test "$(stat -c %a "$RESTORE/private")" = "750" && echo "PERM_OK: private 目录权限 750 已保留"
test -L "$RESTORE/latest-report" && echo "LINK_OK: 根内符号链接已保留 -> $(readlink "$RESTORE/latest-report")"
test ! -e "$RESTORE/escape-hatch" && echo "ESCAPE_OK: 越界符号链接未被恢复"
test ! -s "$RESTORE/empty.txt" && test -f "$RESTORE/empty.txt" && echo "EMPTY_OK: 空文件已恢复"

say "再次恢复到同一目录：已有文件一律 skipped_exists，绝不覆盖"
post /v1/restore "{\"snapshot_id\": 2, \"target_dir\": \"$RESTORE\"}" | jq '{status, restored, skipped, failed}'

say "目标根目录本身是指向外部的符号链接：拒绝恢复，外部目录不得出现新文件"
mkdir -p "$DEMO/external"
ln -s "$DEMO/external" "$DEMO/restore-link"
post /v1/restore "{\"snapshot_id\": 2, \"target_dir\": \"$DEMO/restore-link\"}" | jq .
test -z "$(ls -A "$DEMO/external")" && echo "ROOTLINK_OK: 外部目录无新文件"
test -L "$DEMO/restore-link" && echo "ROOTLINK_OK: 符号链接未被替换为真实目录"

say "恢复失败的快照 4：默认拒绝"
post /v1/restore "{\"snapshot_id\": 4, \"target_dir\": \"$DEMO/restore-failed\"}" | jq .
say "强制恢复（allow_incomplete）：缺块的文件逐个报 failed，而不是静默缺一段"
post /v1/restore "{\"snapshot_id\": 4, \"target_dir\": \"$DEMO/restore-failed\", \"allow_incomplete\": true}" \
  | jq '{status, restored, failed, sample_failures: [.entries[] | select(.status=="failed")] | .[0:3]}'

say "保留策略：dry-run 预览（保留最近 1 个 complete 快照），不得修改任何数据"
SNAPS_BEFORE=$(curl -sS "$BASE/v1/snapshots" | jq length)
CHUNKS_BEFORE=$(find "$DATA/chunks" -type f -not -name '.tmp-*' | wc -l)
post /v1/retention "{\"source_root\": \"$SRC\", \"keep_last_complete\": 1, \"dry_run\": true}" \
  | jq '{dry_run, applied, pending_snapshots, kept: [.kept_snapshots[] | {id,status}], reclaim_chunks, reclaim_bytes}'
test "$(curl -sS "$BASE/v1/snapshots" | jq length)" = "$SNAPS_BEFORE" && echo "DRYRUN_OK: 快照清单未变"
test "$(find "$DATA/chunks" -type f -not -name '.tmp-*' | wc -l)" = "$CHUNKS_BEFORE" && echo "DRYRUN_OK: 块目录未变"

say "应用保留：创建清理批次 purge-0001（撤销窗口 3600 秒），超期快照置为待清理而非删除"
post /v1/retention "{\"source_root\": \"$SRC\", \"keep_last_complete\": 1, \"purge_id\": \"purge-0001\", \"window_seconds\": 3600, \"operator\": \"demo\"}" \
  | jq '{applied, purge_id, execute_after, pending_snapshots}'
test "$(curl -sS "$BASE/v1/snapshots/1" | jq -r .status)" = "pending_purge" && echo "PURGE_OK: 快照 1 先待清理，未被删除"
test -n "$(curl -sS "$BASE/v1/snapshots/1/files" | jq -r '.[0].path')" && echo "PURGE_OK: 待清理快照清单完整"

say "同一 purge_id 重复提交：幂等回放，不产生新批次"
post /v1/retention "{\"source_root\": \"$SRC\", \"keep_last_complete\": 1, \"purge_id\": \"purge-0001\", \"window_seconds\": 3600}" \
  | jq '{replayed, pending_snapshots}'
test "$(curl -sS "$BASE/v1/purges" | jq length)" = "1" && echo "PURGE_OK: 批次唯一"

say "重启服务：批次、待清理状态与审计记录持久可查"
restart_server
curl -sS "$BASE/v1/purges/purge-0001" \
  | jq '{batch: .batch | {purge_id, status, execute_after}, items: [.items[] | {snapshot_id, state}], audit: [.audit[] | .action]}'

say "窗口内撤销快照 1：回到 complete，清单未被重写，可完整恢复"
post "/v1/purges/purge-0001/undo" '{"snapshot_id": 1}' | jq '{batch_status, undone_snapshots}'
test "$(curl -sS "$BASE/v1/snapshots/1" | jq -r .status)" = "complete" && echo "UNDO_OK: 快照 1 恢复为 complete"
post /v1/restore "{\"snapshot_id\": 1, \"target_dir\": \"$DEMO/restore-undone\"}" | jq '{status, restored, failed}'
test "$(wc -l < "$DEMO/restore-undone/docs/report.txt")" = "4000" && echo "UNDO_OK: 撤销后快照可完整恢复（内容未变）"

say "窗口未到期执行清理：拒绝"
post "/v1/purges/purge-0001/execute" '{}' | jq .

say "整批撤销 purge-0001：批次取消"
post "/v1/purges/purge-0001/undo" '{"all": true}' | jq '{batch_status, undone_snapshots}'

say "再次应用保留（purge-0002，窗口 0 秒）并到期执行清理：只回收零引用块"
post /v1/retention "{\"source_root\": \"$SRC\", \"keep_last_complete\": 1, \"purge_id\": \"purge-0002\", \"window_seconds\": 0}" \
  | jq '{applied, pending_snapshots}'
post "/v1/purges/purge-0002/execute" '{}' | jq '{status, purged_snapshots, reclaim_chunks, reclaim_bytes, warnings}'
curl -sS "$BASE/v1/snapshots/1" | jq .
echo "块目录：$CHUNKS_BEFORE -> $(find "$DATA/chunks" -type f -not -name '.tmp-*' | wc -l) 块"

say "清理后：保留的快照 2 仍可完整恢复（共享块与空文件不受影响）"
post /v1/restore "{\"snapshot_id\": 2, \"target_dir\": \"$DEMO/restore-after-purge\"}" | jq '{status, restored, failed}'
diff -r --no-dereference --exclude=active.log --exclude=late.bin --exclude=escape-hatch "$SRC" "$DEMO/restore-after-purge" \
  && echo "PURGE_OK: 保留快照恢复内容一致"
test -f "$DEMO/restore-after-purge/empty.txt" && echo "PURGE_OK: 空文件恢复正常"

say "重复执行 purge-0002：幂等稳定，无二次删除"
post "/v1/purges/purge-0002/execute" '{}' | jq '{status, replayed, purged_snapshots, reclaim_chunks}'

say "修复流程：对 failed 快照 4 发起修复，模拟修复中途崩溃（fault_after_chunks=2）"
post "/v1/snapshots/4/repair" '{"repair_id": "repair-0001", "fault_after_chunks": 2}' | jq . || true

say "重启服务（模拟进程中断），持久化的尝试记录仍在"
restart_server
curl -sS "$BASE/v1/snapshots/4/repairs" | jq '.[] | {repair_id, status, attempts}'
curl -sS "$BASE/v1/snapshots/4/repairs/repair-0001" | jq '{repair_id, status, attempts}'

say "同一 repair_id 重试：安全续跑，最终成功，快照原子转为 complete"
post "/v1/snapshots/4/repair" '{"repair_id": "repair-0001"}' \
  | jq '{status, snapshot_status, attempts, chunks_supplied, bytes_supplied, verified_files}'
curl -sS "$BASE/v1/snapshots/4" | jq '{id, status, missing_chunks, error}'
test "$(curl -sS "$BASE/v1/snapshots/4/missing" | jq length)" = "0" && echo "REPAIR_OK: 缺块记录已清理"

say "相同 repair_id 重复请求：幂等回放，不重写清单"
post "/v1/snapshots/4/repair" '{"repair_id": "repair-0001"}' | jq '{status, replayed, attempts}'

say "修复后的快照 4 可完整恢复（late.bin 逐字节一致）"
post /v1/restore "{\"snapshot_id\": 4, \"target_dir\": \"$DEMO/restore-repaired\"}" | jq '{status, restored, failed}'
cmp "$SRC/late.bin" "$DEMO/restore-repaired/late.bin" && echo "REPAIR_OK: late.bin 内容一致"

say "源文件被改写/删除后：修复逐项报告并保持数据库不变"
head -c 300000 /dev/urandom > "$SRC/newfile.bin"
SNAP5=$(post /v1/snapshots "{\"source_root\": \"$SRC\", \"fault_after_chunks\": 2}" | jq -r .id)
echo "  新失败快照 id=$SNAP5（missing=$(curl -sS "$BASE/v1/snapshots/$SNAP5" | jq -r .missing_chunks)）"
echo "tampered after snapshot" >> "$SRC/newfile.bin"
rm "$SRC/docs/metrics.txt"
MISS_BEFORE=$(curl -sS "$BASE/v1/snapshots/$SNAP5/missing" | jq length)
post "/v1/snapshots/$SNAP5/repair" '{"repair_id": "repair-0002"}' | jq '{status, problems}'
test "$(curl -sS "$BASE/v1/snapshots/$SNAP5" | jq -r .status)" = "failed" && echo "REPAIR_OK: 源文件变化时快照保持 failed"
test "$(curl -sS "$BASE/v1/snapshots/$SNAP5/missing" | jq length)" = "$MISS_BEFORE" && echo "REPAIR_OK: 缺块记录未被改动"

say "全部快照一览（保留策略与修复之后）"
curl -sS "$BASE/v1/snapshots" | jq '.[] | {id,status,chunks_added,chunks_reused,missing_chunks,unstable_files}'

say "演示完成"
