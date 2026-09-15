package backup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

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

// Two snapshots share most chunks; deleting the older one must not touch
// the shared chunks, and the kept snapshot (empty file included) must
// still restore.
func TestRetentionSharedChunksAndRestore(t *testing.T) {
	svc := newTestService(t)
	src := t.TempDir()

	body := strings.Repeat("alpha line\n", 20000) // ~220KB, many chunks
	writeFile(t, filepath.Join(src, "a.txt"), body)
	writeFile(t, filepath.Join(src, "empty.txt"), "")
	snap1 := snapshotOnce(t, svc, src)

	writeFile(t, filepath.Join(src, "a.txt"), body+"one more line\n") // small edit
	snap2 := snapshotOnce(t, svc, src)

	refs1 := chunkRefsOf(t, svc, snap1.ID)
	refs2 := chunkRefsOf(t, svc, snap2.ID)
	shared := 0
	for sha := range refs2 {
		if refs1[sha] {
			shared++
		}
	}
	if shared == 0 {
		t.Fatal("snapshots share no chunks; test setup broken")
	}

	rep, err := svc.Retention(RetentionRequest{SourceRoot: src, KeepLastComplete: 1})
	if err != nil {
		t.Fatalf("retention: %v", err)
	}
	if !rep.Applied || len(rep.DeleteSnapshots) != 1 || rep.DeleteSnapshots[0] != snap1.ID {
		t.Fatalf("report = %+v", rep)
	}
	if rep.ReclaimChunks == 0 {
		t.Fatal("nothing reclaimed, expected at least the edited-out chunks")
	}
	if len(rep.Warnings) != 0 {
		t.Fatalf("warnings: %v", rep.Warnings)
	}

	// Old snapshot is gone from the manifest.
	if gone, err := svc.db.GetSnapshot(snap1.ID); err != nil || gone != nil {
		t.Fatalf("deleted snapshot still visible: %v %v", gone, err)
	}

	// Every chunk the kept snapshot references is still in the store.
	for sha := range refs2 {
		if !svc.cs.Has(sha) {
			t.Fatalf("shared chunk %s was reclaimed", sha)
		}
	}

	// The kept snapshot restores, empty file included.
	dest := filepath.Join(t.TempDir(), "restore")
	rrep, err := svc.Restore(RestoreRequest{SnapshotID: snap2.ID, TargetDir: dest})
	if err != nil {
		t.Fatalf("restore after retention: %v", err)
	}
	if rrep.Failed != 0 {
		t.Fatalf("restore report = %+v", rrep)
	}
	got, err := os.ReadFile(filepath.Join(dest, "a.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != body+"one more line\n" {
		t.Fatal("a.txt content mismatch after retention")
	}
	fi, err := os.Stat(filepath.Join(dest, "empty.txt"))
	if err != nil || fi.Size() != 0 {
		t.Fatalf("empty file broken after retention: %v %v", fi, err)
	}
}

// failed/incomplete snapshots are never eligible for deletion.
func TestRetentionKeepsFailedAndIncomplete(t *testing.T) {
	svc := newTestService(t)
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "a.txt"), strings.Repeat("x", 100000))

	snap1 := snapshotOnce(t, svc, src)
	writeFile(t, filepath.Join(src, "a.txt"), strings.Repeat("y", 100000))
	snap2 := snapshotOnce(t, svc, src)

	// Directly register a failed and an incomplete snapshot for this root.
	failedID, err := svc.db.CreateSnapshot(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.db.FinishSnapshot(store.Snapshot{ID: failedID, Status: store.StatusFailed, Error: "boom"}); err != nil {
		t.Fatal(err)
	}
	incompleteID, err := svc.db.CreateSnapshot(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.db.FinishSnapshot(store.Snapshot{ID: incompleteID, Status: store.StatusIncomplete}); err != nil {
		t.Fatal(err)
	}

	rep, err := svc.Retention(RetentionRequest{SourceRoot: src, KeepLastComplete: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.DeleteSnapshots) != 1 || rep.DeleteSnapshots[0] != snap1.ID {
		t.Fatalf("delete set = %v, want only snapshot %d", rep.DeleteSnapshots, snap1.ID)
	}
	kept := map[int64]bool{}
	for _, k := range rep.KeptSnapshots {
		kept[k.ID] = true
	}
	for _, id := range []int64{snap2.ID, failedID, incompleteID} {
		if !kept[id] {
			t.Fatalf("snapshot %d missing from kept set %+v", id, rep.KeptSnapshots)
		}
		if s, err := svc.db.GetSnapshot(id); err != nil || s == nil {
			t.Fatalf("snapshot %d was deleted", id)
		}
	}
}

// dry-run must not change the manifest, the chunk registry, or the chunk dir.
func TestRetentionDryRunNoChanges(t *testing.T) {
	svc := newTestService(t)
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "a.txt"), strings.Repeat("z", 50000))
	snap1 := snapshotOnce(t, svc, src)
	writeFile(t, filepath.Join(src, "a.txt"), strings.Repeat("w", 50000))
	snapshotOnce(t, svc, src)

	snapsBefore, err := svc.db.ListSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	regBefore, err := svc.db.CountChunks()
	if err != nil {
		t.Fatal(err)
	}
	filesBefore := countChunkFiles(t, svc)

	rep, err := svc.Retention(RetentionRequest{SourceRoot: src, KeepLastComplete: 1, DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	// The preview still reports what an apply would do...
	if rep.Applied || !rep.DryRun {
		t.Fatalf("report = %+v", rep)
	}
	if len(rep.DeleteSnapshots) != 1 || rep.DeleteSnapshots[0] != snap1.ID || rep.ReclaimChunks == 0 {
		t.Fatalf("preview report = %+v", rep)
	}

	// ...but nothing changed.
	snapsAfter, _ := svc.db.ListSnapshots()
	regAfter, _ := svc.db.CountChunks()
	filesAfter := countChunkFiles(t, svc)
	if len(snapsAfter) != len(snapsBefore) {
		t.Fatalf("dry-run changed snapshot count %d -> %d", len(snapsBefore), len(snapsAfter))
	}
	if regAfter != regBefore {
		t.Fatalf("dry-run changed chunk registry %d -> %d", regBefore, regAfter)
	}
	if filesAfter != filesBefore {
		t.Fatalf("dry-run changed chunk dir %d -> %d files", filesBefore, filesAfter)
	}
	if s, _ := svc.db.GetSnapshot(snap1.ID); s == nil {
		t.Fatal("dry-run deleted a snapshot")
	}
}

// Applying the same policy twice is stable: the second run deletes and
// reclaims nothing.
func TestRetentionIdempotent(t *testing.T) {
	svc := newTestService(t)
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "a.txt"), strings.Repeat("q", 80000))
	snapshotOnce(t, svc, src)
	writeFile(t, filepath.Join(src, "a.txt"), strings.Repeat("Q", 80000))
	snapshotOnce(t, svc, src)

	first, err := svc.Retention(RetentionRequest{SourceRoot: src, KeepLastComplete: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.DeleteSnapshots) != 1 || first.ReclaimChunks == 0 {
		t.Fatalf("first apply = %+v", first)
	}
	filesAfterFirst := countChunkFiles(t, svc)

	second, err := svc.Retention(RetentionRequest{SourceRoot: src, KeepLastComplete: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.DeleteSnapshots) != 0 || second.ReclaimChunks != 0 || second.ReclaimBytes != 0 {
		t.Fatalf("second apply not stable: %+v", second)
	}
	if got := countChunkFiles(t, svc); got != filesAfterFirst {
		t.Fatalf("chunk dir changed on re-run: %d -> %d", filesAfterFirst, got)
	}

	// A dry-run afterwards agrees with the stable state.
	dry, err := svc.Retention(RetentionRequest{SourceRoot: src, KeepLastComplete: 1, DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(dry.DeleteSnapshots) != 0 || dry.ReclaimChunks != 0 {
		t.Fatalf("dry-run after apply = %+v", dry)
	}
}
