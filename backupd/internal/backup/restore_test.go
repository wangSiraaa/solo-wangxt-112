package backup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"backupd/internal/chunkstore"
	"backupd/internal/store"
)

func newTestService(t *testing.T) *Service {
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
	return NewService(db, cs, false)
}

// buildSourceTree creates a tree exercising internal/escaping symlinks,
// custom dir modes and an empty file, then snapshots it.
func buildSourceTree(t *testing.T, svc *Service) *store.Snapshot {
	t.Helper()
	src := t.TempDir()

	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(os.MkdirAll(filepath.Join(src, "docs"), 0o755))
	must(os.WriteFile(filepath.Join(src, "docs", "report.txt"),
		[]byte(strings.Repeat("report line\n", 5000)), 0o644))
	must(os.MkdirAll(filepath.Join(src, "private"), 0o750))
	must(os.WriteFile(filepath.Join(src, "private", "notes.txt"), []byte("secret\n"), 0o644))
	must(os.WriteFile(filepath.Join(src, "empty.txt"), nil, 0o644))
	must(os.Symlink("docs/report.txt", filepath.Join(src, "latest-report"))) // stays inside root
	must(os.Symlink("/etc/passwd", filepath.Join(src, "escape-hatch")))      // escapes root

	snap, err := svc.CreateSnapshot(SnapshotRequest{SourceRoot: src})
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snap.Status != store.StatusComplete {
		t.Fatalf("snapshot status = %s, want complete (err %s)", snap.Status, snap.Error)
	}
	return snap
}

// The fix: a target_dir that is itself a symlink to an external directory
// must be refused, and the external directory must stay untouched.
func TestRestoreRejectsSymlinkTargetRoot(t *testing.T) {
	svc := newTestService(t)
	snap := buildSourceTree(t, svc)

	external := t.TempDir()
	link := filepath.Join(t.TempDir(), "restore-link")
	if err := os.Symlink(external, link); err != nil {
		t.Fatal(err)
	}

	_, err := svc.Restore(RestoreRequest{SnapshotID: snap.ID, TargetDir: link})
	if err == nil {
		t.Fatal("restore through symlinked target_dir succeeded, want refusal")
	}
	if !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("error %q does not mention symlink", err)
	}

	entries, rerr := os.ReadDir(external)
	if rerr != nil {
		t.Fatal(rerr)
	}
	if len(entries) != 0 {
		t.Fatalf("external directory gained %d file(s) through the symlink", len(entries))
	}
	// The symlink itself must still be a symlink (not replaced by a real dir).
	fi, lerr := os.Lstat(link)
	if lerr != nil {
		t.Fatal(lerr)
	}
	if fi.Mode()&os.ModeSymlink == 0 {
		t.Fatal("restore-link was replaced by a real directory")
	}
}

// A target_dir that exists as a regular file is refused too.
func TestRestoreRejectsNonDirTargetRoot(t *testing.T) {
	svc := newTestService(t)
	snap := buildSourceTree(t, svc)

	f := filepath.Join(t.TempDir(), "a-file")
	if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Restore(RestoreRequest{SnapshotID: snap.ID, TargetDir: f}); err == nil {
		t.Fatal("restore onto a regular-file target_dir succeeded, want refusal")
	}
}

// Regression: restoring to a real directory keeps working — internal
// symlinks, escaping-link refusal, dir modes, empty files, no-overwrite.
func TestRestoreToRealDirRegression(t *testing.T) {
	svc := newTestService(t)
	snap := buildSourceTree(t, svc)
	dest := filepath.Join(t.TempDir(), "restore")

	rep, err := svc.Restore(RestoreRequest{SnapshotID: snap.ID, TargetDir: dest})
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if rep.Status != "ok" || rep.Failed != 0 {
		t.Fatalf("report = %+v", rep)
	}

	// internal symlink preserved
	link := filepath.Join(dest, "latest-report")
	target, err := os.Readlink(link)
	if err != nil {
		t.Fatalf("internal symlink not restored: %v", err)
	}
	if target != "docs/report.txt" {
		t.Fatalf("symlink target = %q", target)
	}

	// escaping symlink refused, nothing created at its path
	var escaped *RestoreEntry
	for i := range rep.Entries {
		if rep.Entries[i].Path == "escape-hatch" {
			escaped = &rep.Entries[i]
		}
	}
	if escaped == nil || escaped.Status != "skipped_unsafe_link" {
		t.Fatalf("escape-hatch entry = %+v", escaped)
	}
	if _, err := os.Lstat(filepath.Join(dest, "escape-hatch")); !os.IsNotExist(err) {
		t.Fatal("escape-hatch exists in restore destination")
	}

	// directory mode preserved
	fi, err := os.Stat(filepath.Join(dest, "private"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o750 {
		t.Fatalf("private mode = %o, want 750", fi.Mode().Perm())
	}

	// empty file restored as empty regular file
	fi, err = os.Stat(filepath.Join(dest, "empty.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != 0 || !fi.Mode().IsRegular() {
		t.Fatalf("empty.txt: size=%d mode=%v", fi.Size(), fi.Mode())
	}

	// content verified byte-for-byte
	got, err := os.ReadFile(filepath.Join(dest, "docs", "report.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != strings.Repeat("report line\n", 5000) {
		t.Fatal("report.txt content mismatch")
	}

	// re-restore: everything already there, nothing overwritten
	rep2, err := svc.Restore(RestoreRequest{SnapshotID: snap.ID, TargetDir: dest})
	if err != nil {
		t.Fatalf("re-restore: %v", err)
	}
	if rep2.Restored != 0 {
		t.Fatalf("re-restore restored %d entries, want 0", rep2.Restored)
	}
	for _, e := range rep2.Entries {
		switch e.Status {
		case "skipped_exists", "skipped_unsafe_link":
		default:
			t.Fatalf("re-restore entry %s has status %s", e.Path, e.Status)
		}
	}
}
