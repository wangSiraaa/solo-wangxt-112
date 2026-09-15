package backup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"backupd/internal/chunkstore"
	"backupd/internal/store"
)

// snapshotOnce creates one complete snapshot of src.
func snapshotOnce(t *testing.T, svc *Service, src string) *store.Snapshot {
	t.Helper()
	snap, err := svc.CreateSnapshot(SnapshotRequest{SourceRoot: src})
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snap.Status != store.StatusComplete {
		t.Fatalf("snapshot status = %s, want complete", snap.Status)
	}
	return snap
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// chunkRefsOf returns every chunk hash referenced by a snapshot's files.
func chunkRefsOf(t *testing.T, svc *Service, snapID int64) map[string]bool {
	t.Helper()
	files, err := svc.db.ListFiles(snapID)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]bool{}
	for _, f := range files {
		if f.Type != "file" {
			continue
		}
		refs, err := svc.db.FileChunks(f.ID)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range refs {
			out[r.SHA256] = true
		}
	}
	return out
}

func countChunkFiles(t *testing.T, svc *Service) int {
	t.Helper()
	n := 0
	root := svc.cs.Dir()
	err := filepath.WalkDir(root, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && !strings.HasPrefix(d.Name(), ".tmp-") {
			n++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func applyRetention(t *testing.T, svc *Service, src, purgeID string, keep int, window int64) *RetentionReport {
	t.Helper()
	rep, err := svc.Retention(RetentionRequest{
		SourceRoot: src, KeepLastComplete: keep, PurgeID: purgeID, WindowSeconds: window,
	})
	if err != nil {
		t.Fatalf("retention apply: %v", err)
	}
	return rep
}

// Retention apply no longer deletes: the expired snapshot goes
// pending_purge inside a persistent batch; executing the batch purges it
// and only zero-reference chunks are reclaimed.
func TestRetentionPendsThenExecutePurges(t *testing.T) {
	svc := newTestService(t)
	src := t.TempDir()

	body := strings.Repeat("alpha line\n", 20000)
	writeFile(t, filepath.Join(src, "a.txt"), body)
	writeFile(t, filepath.Join(src, "empty.txt"), "")
	snap1 := snapshotOnce(t, svc, src)
	writeFile(t, filepath.Join(src, "a.txt"), body+"one more line\n")
	snap2 := snapshotOnce(t, svc, src)
	refs2 := chunkRefsOf(t, svc, snap2.ID)

	rep := applyRetention(t, svc, src, "p1", 1, 0)
	if !rep.Applied || len(rep.PendingSnapshots) != 1 || rep.PendingSnapshots[0] != snap1.ID {
		t.Fatalf("report = %+v", rep)
	}
	if rep.ReclaimChunks == 0 {
		t.Fatal("reclaim estimate should be > 0")
	}
	s1, _ := svc.db.GetSnapshot(snap1.ID)
	if s1.Status != store.StatusPendingPurge {
		t.Fatalf("snap1 status = %s, want pending_purge", s1.Status)
	}
	// Manifest intact while pending: files still listed.
	if files, _ := svc.db.ListFiles(snap1.ID); len(files) == 0 {
		t.Fatal("pending snapshot lost its manifest")
	}

	exec, err := svc.ExecutePurge("p1", ExecutePurgeRequest{})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if exec.Status != store.PurgeExecuted || len(exec.PurgedSnapshots) != 1 || exec.PurgedSnapshots[0] != snap1.ID {
		t.Fatalf("execute report = %+v", exec)
	}
	if gone, _ := svc.db.GetSnapshot(snap1.ID); gone != nil {
		t.Fatal("snap1 still visible after purge")
	}
	for sha := range refs2 {
		if !svc.cs.Has(sha) {
			t.Fatalf("shared chunk %s reclaimed", sha)
		}
	}

	// Kept snapshot restores, empty file included.
	dest := filepath.Join(t.TempDir(), "restore")
	rrep, err := svc.Restore(RestoreRequest{SnapshotID: snap2.ID, TargetDir: dest})
	if err != nil || rrep.Failed != 0 {
		t.Fatalf("restore after purge: %v %+v", err, rrep)
	}
	if fi, err := os.Stat(filepath.Join(dest, "empty.txt")); err != nil || fi.Size() != 0 {
		t.Fatalf("empty file broken after purge: %v %v", fi, err)
	}
}

// failed/incomplete snapshots never enter a batch; pending snapshots are
// not re-selected by a later retention run.
func TestRetentionKeepsFailedIncompletePending(t *testing.T) {
	svc := newTestService(t)
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "a.txt"), strings.Repeat("x", 100000))

	snap1 := snapshotOnce(t, svc, src)
	writeFile(t, filepath.Join(src, "a.txt"), strings.Repeat("y", 100000))
	snap2 := snapshotOnce(t, svc, src)

	failedID, _ := svc.db.CreateSnapshot(src)
	_ = svc.db.FinishSnapshot(store.Snapshot{ID: failedID, Status: store.StatusFailed, Error: "boom"})
	incompleteID, _ := svc.db.CreateSnapshot(src)
	_ = svc.db.FinishSnapshot(store.Snapshot{ID: incompleteID, Status: store.StatusIncomplete})

	rep := applyRetention(t, svc, src, "p1", 1, 0)
	if len(rep.PendingSnapshots) != 1 || rep.PendingSnapshots[0] != snap1.ID {
		t.Fatalf("pending = %v", rep.PendingSnapshots)
	}
	kept := map[int64]bool{}
	for _, k := range rep.KeptSnapshots {
		kept[k.ID] = true
	}
	for _, id := range []int64{snap2.ID, failedID, incompleteID} {
		if !kept[id] {
			t.Fatalf("snapshot %d not kept: %+v", id, rep.KeptSnapshots)
		}
	}

	// A second retention run does not re-select the pending snapshot.
	rep2 := applyRetention(t, svc, src, "p2", 1, 0)
	if len(rep2.PendingSnapshots) != 0 {
		t.Fatalf("second run pended %v, want none", rep2.PendingSnapshots)
	}
}

// dry-run computes the plan but changes nothing: no batch, no status flips,
// no registry or chunk-dir changes.
func TestRetentionDryRunNoChanges(t *testing.T) {
	svc := newTestService(t)
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "a.txt"), strings.Repeat("z", 50000))
	snap1 := snapshotOnce(t, svc, src)
	writeFile(t, filepath.Join(src, "a.txt"), strings.Repeat("w", 50000))
	snapshotOnce(t, svc, src)

	regBefore, _ := svc.db.CountChunks()
	filesBefore := countChunkFiles(t, svc)

	rep, err := svc.Retention(RetentionRequest{SourceRoot: src, KeepLastComplete: 1, DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Applied || len(rep.PendingSnapshots) != 1 || rep.PendingSnapshots[0] != snap1.ID {
		t.Fatalf("dry-run report = %+v", rep)
	}
	s1, _ := svc.db.GetSnapshot(snap1.ID)
	if s1.Status != store.StatusComplete {
		t.Fatalf("dry-run flipped snapshot to %s", s1.Status)
	}
	if batches, _ := svc.db.ListPurgeBatches(); len(batches) != 0 {
		t.Fatalf("dry-run created a batch: %+v", batches)
	}
	if regAfter, _ := svc.db.CountChunks(); regAfter != regBefore {
		t.Fatal("dry-run changed chunk registry")
	}
	if got := countChunkFiles(t, svc); got != filesBefore {
		t.Fatal("dry-run changed chunk dir")
	}
}

// Apply requires a purge_id; the same purge_id replays without effects.
func TestRetentionApplyIdempotent(t *testing.T) {
	svc := newTestService(t)
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "a.txt"), strings.Repeat("q", 80000))
	snap1 := snapshotOnce(t, svc, src)
	writeFile(t, filepath.Join(src, "a.txt"), strings.Repeat("Q", 80000))
	snapshotOnce(t, svc, src)

	if _, err := svc.Retention(RetentionRequest{SourceRoot: src, KeepLastComplete: 1}); err == nil {
		t.Fatal("apply without purge_id succeeded")
	}
	applyRetention(t, svc, src, "p1", 1, 0)

	replay, err := svc.Retention(RetentionRequest{SourceRoot: src, KeepLastComplete: 1, PurgeID: "p1"})
	if err != nil {
		t.Fatal(err)
	}
	if !replay.Replayed || len(replay.PendingSnapshots) != 1 || replay.PendingSnapshots[0] != snap1.ID {
		t.Fatalf("replay = %+v", replay)
	}
	if batches, _ := svc.db.ListPurgeBatches(); len(batches) != 1 {
		t.Fatalf("replay created extra batches: %d", len(batches))
	}

	// Same purge_id with a different policy is a conflict.
	if _, err := svc.Retention(RetentionRequest{SourceRoot: src, KeepLastComplete: 0, PurgeID: "p1"}); err == nil {
		t.Fatal("purge_id reuse with different policy accepted")
	}

	// Execute twice: the second run is a stable replay.
	if _, err := svc.ExecutePurge("p1", ExecutePurgeRequest{}); err != nil {
		t.Fatal(err)
	}
	filesAfterFirst := countChunkFiles(t, svc)
	again, err := svc.ExecutePurge("p1", ExecutePurgeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if !again.Replayed || again.ReclaimChunks != 0 {
		t.Fatalf("second execute = %+v", again)
	}
	if got := countChunkFiles(t, svc); got != filesAfterFirst {
		t.Fatalf("second execute changed chunk dir: %d -> %d", filesAfterFirst, got)
	}
}

// Undo returns a pending snapshot to complete without touching its
// manifest; it restores fully afterwards.
func TestPurgeUndoRestoresSnapshot(t *testing.T) {
	svc := newTestService(t)
	src := t.TempDir()
	body := strings.Repeat("undo me\n", 20000)
	writeFile(t, filepath.Join(src, "a.txt"), body)
	snap1 := snapshotOnce(t, svc, src)
	writeFile(t, filepath.Join(src, "a.txt"), body+"v2\n")
	snapshotOnce(t, svc, src)

	filesBefore, _ := svc.db.ListFiles(snap1.ID)
	applyRetention(t, svc, src, "p1", 1, 3600)

	undo, err := svc.UndoPurge("p1", UndoPurgeRequest{SnapshotID: snap1.ID})
	if err != nil {
		t.Fatalf("undo: %v", err)
	}
	if len(undo.UndoneSnapshots) != 1 || undo.UndoneSnapshots[0] != snap1.ID {
		t.Fatalf("undo report = %+v", undo)
	}
	s1, _ := svc.db.GetSnapshot(snap1.ID)
	if s1.Status != store.StatusComplete {
		t.Fatalf("after undo status = %s", s1.Status)
	}
	filesAfter, _ := svc.db.ListFiles(snap1.ID)
	if len(filesAfter) != len(filesBefore) {
		t.Fatalf("undo rewrote the manifest: %d -> %d files", len(filesBefore), len(filesAfter))
	}
	for i := range filesBefore {
		if filesBefore[i] != filesAfter[i] {
			t.Fatalf("manifest row changed by undo: %+v -> %+v", filesBefore[i], filesAfter[i])
		}
	}

	dest := filepath.Join(t.TempDir(), "restore")
	rrep, err := svc.Restore(RestoreRequest{SnapshotID: snap1.ID, TargetDir: dest})
	if err != nil || rrep.Failed != 0 {
		t.Fatalf("restore undone snapshot: %v %+v", err, rrep)
	}
	got, _ := os.ReadFile(filepath.Join(dest, "a.txt"))
	if string(got) != body {
		t.Fatal("content mismatch after undo")
	}

	// Undo-all cancels the batch; a cancelled batch cannot execute.
	if _, err := svc.UndoPurge("p1", UndoPurgeRequest{All: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.ExecutePurge("p1", ExecutePurgeRequest{Force: true}); err == nil {
		t.Fatal("executed a cancelled batch")
	}
}

// Shared chunks survive when one snapshot is purged and the other is kept
// or undone.
func TestPurgeSharedChunksWithUndo(t *testing.T) {
	svc := newTestService(t)
	src := t.TempDir()
	shared := strings.Repeat("shared\n", 20000)
	writeFile(t, filepath.Join(src, "shared.txt"), shared)
	writeFile(t, filepath.Join(src, "v.txt"), strings.Repeat("v1\n", 20000))
	snapA := snapshotOnce(t, svc, src)
	writeFile(t, filepath.Join(src, "v.txt"), strings.Repeat("v2\n", 20000))
	snapB := snapshotOnce(t, svc, src)

	// Pend A only, then undo it: it must be protected at execute time.
	applyRetention(t, svc, src, "pA", 1, 0)
	if _, err := svc.UndoPurge("pA", UndoPurgeRequest{SnapshotID: snapA.ID}); err != nil {
		t.Fatal(err)
	}
	// pA still has no pending items; executing it purges nothing.
	exec, err := svc.ExecutePurge("pA", ExecutePurgeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(exec.PurgedSnapshots) != 0 {
		t.Fatalf("undone snapshot was purged: %+v", exec)
	}

	// Now pend both (keep 0) but undo B: only A may be purged.
	applyRetention(t, svc, src, "pB", 0, 0)
	if _, err := svc.UndoPurge("pB", UndoPurgeRequest{SnapshotID: snapB.ID}); err != nil {
		t.Fatal(err)
	}
	exec, err = svc.ExecutePurge("pB", ExecutePurgeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(exec.PurgedSnapshots) != 1 || exec.PurgedSnapshots[0] != snapA.ID {
		t.Fatalf("execute = %+v", exec)
	}
	if gone, _ := svc.db.GetSnapshot(snapA.ID); gone != nil {
		t.Fatal("snapA not purged")
	}
	b, _ := svc.db.GetSnapshot(snapB.ID)
	if b == nil || b.Status != store.StatusComplete {
		t.Fatalf("undone snapB harmed: %+v", b)
	}

	dest := filepath.Join(t.TempDir(), "restore")
	rrep, err := svc.Restore(RestoreRequest{SnapshotID: snapB.ID, TargetDir: dest})
	if err != nil || rrep.Failed != 0 {
		t.Fatalf("restore B: %v %+v", err, rrep)
	}
	got, _ := os.ReadFile(filepath.Join(dest, "shared.txt"))
	if string(got) != shared {
		t.Fatal("shared content lost")
	}
}

// A snapshot with a repair in progress is skipped at execute time and the
// batch stays open; once the repair settles, the purge proceeds.
func TestPurgeExecuteSkipsRunningRepair(t *testing.T) {
	svc := newTestService(t)
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "a.txt"), strings.Repeat("r", 90000))
	snap1 := snapshotOnce(t, svc, src)
	writeFile(t, filepath.Join(src, "a.txt"), strings.Repeat("R", 90000))
	snapshotOnce(t, svc, src)

	applyRetention(t, svc, src, "p1", 1, 0)

	// Simulate an in-flight repair on the pending snapshot.
	if _, err := svc.db.BeginRepairAttempt(snap1.ID, "r-x"); err != nil {
		t.Fatal(err)
	}
	exec, err := svc.ExecutePurge("p1", ExecutePurgeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(exec.PurgedSnapshots) != 0 || len(exec.SkippedSnapshots) != 1 {
		t.Fatalf("execute = %+v", exec)
	}
	if exec.Status != store.PurgeOpen {
		t.Fatalf("batch closed despite skip: %+v", exec)
	}
	if s1, _ := svc.db.GetSnapshot(snap1.ID); s1.Status != store.StatusPendingPurge {
		t.Fatalf("snapshot with running repair was purged: %s", s1.Status)
	}

	// Repair settles; re-execute purges.
	attempt, _ := svc.db.GetRepairAttempt(snap1.ID, "r-x")
	_ = svc.db.FinishRepairAttempt(attempt.ID, store.RepairFailed, "aborted", 0, 0)
	exec, err = svc.ExecutePurge("p1", ExecutePurgeRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if exec.Status != store.PurgeExecuted || len(exec.PurgedSnapshots) != 1 {
		t.Fatalf("re-execute = %+v", exec)
	}
}

// An unexpired window blocks execution unless forced.
func TestPurgeWindowEnforcement(t *testing.T) {
	svc := newTestService(t)
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "a.txt"), strings.Repeat("w", 50000))
	snapshotOnce(t, svc, src)
	writeFile(t, filepath.Join(src, "a.txt"), strings.Repeat("W", 50000))
	snapshotOnce(t, svc, src)

	applyRetention(t, svc, src, "p1", 1, 3600)
	if _, err := svc.ExecutePurge("p1", ExecutePurgeRequest{}); err == nil ||
		!strings.Contains(err.Error(), "not expired") {
		t.Fatalf("execute inside window: %v", err)
	}
	exec, err := svc.ExecutePurge("p1", ExecutePurgeRequest{Force: true})
	if err != nil || exec.Status != store.PurgeExecuted {
		t.Fatalf("forced execute: %v %+v", err, exec)
	}
}

// Batches survive a restart: created before, queryable and executable after.
func TestPurgeRestartStable(t *testing.T) {
	svc := newTestService(t)
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "a.txt"), strings.Repeat("s", 60000))
	snap1 := snapshotOnce(t, svc, src)
	writeFile(t, filepath.Join(src, "a.txt"), strings.Repeat("S", 60000))
	snapshotOnce(t, svc, src)
	applyRetention(t, svc, src, "p1", 1, 0)

	// Simulate restart on the same data dir.
	dataDir := svc.cs.Dir()
	svc.db.Close()
	db, err := store.Open(filepath.Join(filepath.Dir(dataDir), "backup.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cs, err := chunkstore.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	svc2 := NewService(db, cs, false)

	detail, err := svc2.GetPurgeBatch("p1")
	if err != nil || detail == nil {
		t.Fatalf("batch lost after restart: %v %v", detail, err)
	}
	if len(detail.Items) != 1 || detail.Items[0].SnapshotID != snap1.ID ||
		detail.Items[0].State != store.PurgeItemPending {
		t.Fatalf("items after restart = %+v", detail.Items)
	}
	if len(detail.Audit) == 0 {
		t.Fatal("audit trail lost after restart")
	}

	exec, err := svc2.ExecutePurge("p1", ExecutePurgeRequest{})
	if err != nil || exec.Status != store.PurgeExecuted {
		t.Fatalf("execute after restart: %v %+v", err, exec)
	}
	if gone, _ := svc2.db.GetSnapshot(snap1.ID); gone != nil {
		t.Fatal("snap1 not purged after restart")
	}
}
