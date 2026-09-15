package backup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"backupd/internal/chunkstore"
	"backupd/internal/store"
)

// newFaultyTestService builds a service with fault injection enabled.
func newFaultyTestService(t *testing.T) (*Service, string) {
	t.Helper()
	dir := t.TempDir()
	db, err := store.Open(filepath.Join(dir, "backup.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	cs, err := chunkstore.Open(filepath.Join(dir, "chunks"))
	if err != nil {
		t.Fatalf("open chunk store: %v", err)
	}
	return NewService(db, cs, true), dir
}

// faultySnapshot creates a failed snapshot with missing chunks via fault injection.
func faultySnapshot(t *testing.T, svc *Service, src string, faultAfter int) *store.Snapshot {
	t.Helper()
	snap, err := svc.CreateSnapshot(SnapshotRequest{SourceRoot: src, FaultAfterChunks: faultAfter})
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snap.Status != store.StatusFailed {
		t.Fatalf("snapshot status = %s, want failed", snap.Status)
	}
	if snap.MissingChunks == 0 {
		t.Fatal("fault-injected snapshot has no missing chunks")
	}
	return snap
}

// Source untouched after a failed snapshot: repair succeeds, the snapshot
// turns complete and restores fully; replaying the same repair_id is a no-op.
func TestRepairSuccessAndRestore(t *testing.T) {
	svc, _ := newFaultyTestService(t)
	src := t.TempDir()
	body := strings.Repeat("repair me\n", 30000) // ~300KB, many chunks
	writeFile(t, filepath.Join(src, "data.txt"), body)
	writeFile(t, filepath.Join(src, "empty.txt"), "")
	snap := faultySnapshot(t, svc, src, 2)

	rep, err := svc.Repair(snap.ID, RepairRequest{RepairID: "r-1"})
	if err != nil {
		t.Fatalf("repair: %v", err)
	}
	if rep.Status != store.RepairSucceeded || rep.SnapshotStatus != store.StatusComplete {
		t.Fatalf("repair report = %+v", rep)
	}
	if rep.ChunksSupplied == 0 || rep.VerifiedFiles != 2 {
		t.Fatalf("repair report = %+v", rep)
	}

	fixed, err := svc.db.GetSnapshot(snap.ID)
	if err != nil || fixed.Status != store.StatusComplete || fixed.MissingChunks != 0 {
		t.Fatalf("snapshot after repair = %+v, err %v", fixed, err)
	}
	if missing, _ := svc.db.MissingChunks(snap.ID); len(missing) != 0 {
		t.Fatalf("missing_chunks not cleared: %d rows", len(missing))
	}

	// Full restore of the repaired snapshot, empty file included.
	dest := filepath.Join(t.TempDir(), "restore")
	rrep, err := svc.Restore(RestoreRequest{SnapshotID: snap.ID, TargetDir: dest})
	if err != nil || rrep.Failed != 0 {
		t.Fatalf("restore repaired snapshot: %v %+v", err, rrep)
	}
	got, _ := os.ReadFile(filepath.Join(dest, "data.txt"))
	if string(got) != body {
		t.Fatal("data.txt content mismatch after repair")
	}
	if fi, err := os.Stat(filepath.Join(dest, "empty.txt")); err != nil || fi.Size() != 0 {
		t.Fatalf("empty file broken: %v %v", fi, err)
	}

	// Idempotent replay: same repair_id, no manifest changes.
	finishBefore := fixed.FinishedAt
	regBefore, _ := svc.db.CountChunks()
	replay, err := svc.Repair(snap.ID, RepairRequest{RepairID: "r-1"})
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !replay.Replayed || replay.Status != store.RepairSucceeded {
		t.Fatalf("replay = %+v", replay)
	}
	again, _ := svc.db.GetSnapshot(snap.ID)
	if again.FinishedAt != finishBefore {
		t.Fatal("replay rewrote the snapshot row")
	}
	if regAfter, _ := svc.db.CountChunks(); regAfter != regBefore {
		t.Fatal("replay rewrote the chunk registry")
	}

	// Persisted attempt record.
	attempts, err := svc.db.ListRepairAttempts(snap.ID)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("attempts = %+v, err %v", attempts, err)
	}
	if attempts[0].Status != store.RepairSucceeded || attempts[0].RepairID != "r-1" {
		t.Fatalf("attempt record = %+v", attempts[0])
	}
}

// A changed or deleted source file must refuse the repair and leave the
// snapshot, the manifest and the missing-chunk records untouched.
func TestRepairRefusesChangedOrMissingSource(t *testing.T) {
	svc, _ := newFaultyTestService(t)
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "keep.txt"), strings.Repeat("a", 120000))
	writeFile(t, filepath.Join(src, "gone.txt"), strings.Repeat("b", 60000))
	snap := faultySnapshot(t, svc, src, 1)

	missingBefore, _ := svc.db.MissingChunks(snap.ID)
	regBefore, _ := svc.db.CountChunks()
	filesBefore := countChunkFiles(t, svc)

	writeFile(t, filepath.Join(src, "keep.txt"), strings.Repeat("a", 120000)+"tampered\n")
	if err := os.Remove(filepath.Join(src, "gone.txt")); err != nil {
		t.Fatal(err)
	}

	rep, err := svc.Repair(snap.ID, RepairRequest{RepairID: "r-x"})
	if err != nil {
		t.Fatalf("repair returned transport error: %v", err)
	}
	if rep.Status != store.RepairFailed {
		t.Fatalf("repair report = %+v", rep)
	}
	if len(rep.Problems) != 2 {
		t.Fatalf("want 2 problems (changed + missing), got %+v", rep.Problems)
	}

	still, _ := svc.db.GetSnapshot(snap.ID)
	if still.Status != store.StatusFailed {
		t.Fatalf("snapshot status = %s, want failed", still.Status)
	}
	missingAfter, _ := svc.db.MissingChunks(snap.ID)
	if len(missingAfter) != len(missingBefore) {
		t.Fatalf("missing_chunks changed: %d -> %d", len(missingBefore), len(missingAfter))
	}
	if regAfter, _ := svc.db.CountChunks(); regAfter != regBefore {
		t.Fatal("chunk registry changed by refused repair")
	}
	if got := countChunkFiles(t, svc); got != filesBefore {
		t.Fatalf("chunk dir changed by refused repair: %d -> %d", filesBefore, got)
	}
}

// A repair interrupted mid-flight (simulated crash) leaves a persisted
// "running" attempt; after a restart the same repair_id finishes the job.
func TestRepairRetryAfterInterruption(t *testing.T) {
	svc, dataDir := newFaultyTestService(t)
	src := t.TempDir()
	body := strings.Repeat("crash-safe\n", 30000)
	writeFile(t, filepath.Join(src, "data.txt"), body)
	snap := faultySnapshot(t, svc, src, 1)

	_, err := svc.Repair(snap.ID, RepairRequest{RepairID: "r-crash", FaultAfterChunks: 2})
	if err != ErrSimulatedCrash {
		t.Fatalf("want ErrSimulatedCrash, got %v", err)
	}
	attempt, _ := svc.db.GetRepairAttempt(snap.ID, "r-crash")
	if attempt == nil || attempt.Status != store.RepairRunning || attempt.Attempts != 1 {
		t.Fatalf("attempt after crash = %+v", attempt)
	}
	if still, _ := svc.db.GetSnapshot(snap.ID); still.Status != store.StatusFailed {
		t.Fatal("crashed repair changed snapshot state")
	}

	// Simulate a process restart: close everything, reopen on the same dir.
	svc.db.Close()
	db, err := store.Open(filepath.Join(dataDir, "backup.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cs, err := chunkstore.Open(filepath.Join(dataDir, "chunks"))
	if err != nil {
		t.Fatal(err)
	}
	svc2 := NewService(db, cs, true)

	// The persisted attempt survived the restart.
	attempt, _ = svc2.db.GetRepairAttempt(snap.ID, "r-crash")
	if attempt == nil || attempt.Status != store.RepairRunning {
		t.Fatalf("attempt after restart = %+v", attempt)
	}

	rep, err := svc2.Repair(snap.ID, RepairRequest{RepairID: "r-crash"})
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if rep.Status != store.RepairSucceeded || rep.Attempts != 2 {
		t.Fatalf("retry report = %+v", rep)
	}
	fixed, _ := svc2.db.GetSnapshot(snap.ID)
	if fixed.Status != store.StatusComplete {
		t.Fatalf("snapshot = %s after retry", fixed.Status)
	}

	dest := filepath.Join(t.TempDir(), "restore")
	if rrep, err := svc2.Restore(RestoreRequest{SnapshotID: snap.ID, TargetDir: dest}); err != nil || rrep.Failed != 0 {
		t.Fatalf("restore after retry: %v %+v", err, rrep)
	}
	got, _ := os.ReadFile(filepath.Join(dest, "data.txt"))
	if string(got) != body {
		t.Fatal("content mismatch after interrupted+retried repair")
	}
}

// Repair, retention and restore must not eat each other's shared chunks:
// two snapshots share an unchanged file; the newer one is repaired, the
// older one is then expired by retention, and the repaired snapshot must
// still restore everything.
func TestRepairThenRetentionSharedChunks(t *testing.T) {
	svc, _ := newFaultyTestService(t)
	src := t.TempDir()
	shared := strings.Repeat("shared\n", 20000)
	writeFile(t, filepath.Join(src, "shared.txt"), shared)
	writeFile(t, filepath.Join(src, "v.txt"), strings.Repeat("v1\n", 30000))
	snapA := snapshotOnce(t, svc, src) // complete

	writeFile(t, filepath.Join(src, "v.txt"), strings.Repeat("v2\n", 30000))
	snapB := faultySnapshot(t, svc, src, 1) // failed, missing v2 chunks

	if _, err := svc.Repair(snapB.ID, RepairRequest{RepairID: "r-b"}); err != nil {
		t.Fatalf("repair: %v", err)
	}
	if s, _ := svc.db.GetSnapshot(snapB.ID); s.Status != store.StatusComplete {
		t.Fatalf("snapB = %s after repair", s.Status)
	}

	// Retention expires the older complete snapshot A; B stays. Apply only
	// creates the purge batch; the explicit execute does the deletion.
	rep, err := svc.Retention(RetentionRequest{SourceRoot: src, KeepLastComplete: 1, PurgeID: "p1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.PendingSnapshots) != 1 || rep.PendingSnapshots[0] != snapA.ID {
		t.Fatalf("retention report = %+v", rep)
	}
	if _, err := svc.ExecutePurge("p1", ExecutePurgeRequest{}); err != nil {
		t.Fatalf("execute purge: %v", err)
	}

	// B still restores both the repaired file and the shared file.
	dest := filepath.Join(t.TempDir(), "restore")
	rrep, err := svc.Restore(RestoreRequest{SnapshotID: snapB.ID, TargetDir: dest})
	if err != nil || rrep.Failed != 0 {
		t.Fatalf("restore B after retention: %v %+v", err, rrep)
	}
	got, _ := os.ReadFile(filepath.Join(dest, "v.txt"))
	if string(got) != strings.Repeat("v2\n", 30000) {
		t.Fatal("repaired file content wrong after retention")
	}
	got, _ = os.ReadFile(filepath.Join(dest, "shared.txt"))
	if string(got) != shared {
		t.Fatal("shared file content wrong after retention")
	}
}

// complete and incomplete snapshots are not repairable.
func TestRepairRejectsNonFailedSnapshots(t *testing.T) {
	svc, _ := newFaultyTestService(t)
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "a.txt"), "hello")
	snap := snapshotOnce(t, svc, src)

	if _, err := svc.Repair(snap.ID, RepairRequest{RepairID: "r1"}); err == nil ||
		!strings.Contains(err.Error(), "already complete") {
		t.Fatalf("repair on complete snapshot: %v", err)
	}

	incID, err := svc.db.CreateSnapshot(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.db.FinishSnapshot(store.Snapshot{ID: incID, Status: store.StatusIncomplete}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Repair(incID, RepairRequest{RepairID: "r2"}); err == nil ||
		!strings.Contains(err.Error(), "incomplete") {
		t.Fatalf("repair on incomplete snapshot: %v", err)
	}
}
