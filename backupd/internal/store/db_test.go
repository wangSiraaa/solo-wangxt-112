package store

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

// A database created by an older backupd (no repair_attempts table) must
// open cleanly: migration adds the table and existing rows survive.
func TestMigrateOldDB(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")

	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = raw.Exec(`
CREATE TABLE snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  source_root TEXT NOT NULL,
  started_at TEXT NOT NULL,
  finished_at TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL,
  error TEXT NOT NULL DEFAULT '',
  file_count INTEGER NOT NULL DEFAULT 0,
  dir_count INTEGER NOT NULL DEFAULT 0,
  symlink_count INTEGER NOT NULL DEFAULT 0,
  total_bytes INTEGER NOT NULL DEFAULT 0,
  chunks_added INTEGER NOT NULL DEFAULT 0,
  chunks_reused INTEGER NOT NULL DEFAULT 0,
  unstable_files INTEGER NOT NULL DEFAULT 0,
  missing_chunks INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  snapshot_id INTEGER NOT NULL,
  path TEXT NOT NULL, type TEXT NOT NULL,
  mode INTEGER NOT NULL DEFAULT 0, size INTEGER NOT NULL DEFAULT 0,
  mtime_ns INTEGER NOT NULL DEFAULT 0,
  link_target TEXT NOT NULL DEFAULT '', sha256 TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'ok'
);
CREATE TABLE chunks (sha256 TEXT PRIMARY KEY, size INTEGER NOT NULL, first_seen_snapshot INTEGER NOT NULL);
CREATE TABLE file_chunks (
  file_id INTEGER NOT NULL, seq INTEGER NOT NULL,
  chunk_sha256 TEXT NOT NULL, size INTEGER NOT NULL,
  PRIMARY KEY (file_id, seq)
);
CREATE TABLE missing_chunks (
  snapshot_id INTEGER NOT NULL, file_path TEXT NOT NULL,
  chunk_sha256 TEXT NOT NULL, reason TEXT NOT NULL
);
INSERT INTO snapshots (source_root, started_at, status) VALUES ('/old/root', '2026-01-01T00:00:00Z', 'failed');
INSERT INTO missing_chunks (snapshot_id, file_path, chunk_sha256, reason) VALUES (1, 'a.bin', 'deadbeef', 'chunk not in store');
CREATE TABLE repair_attempts (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  repair_id       TEXT NOT NULL,
  snapshot_id     INTEGER NOT NULL REFERENCES snapshots(id),
  started_at      TEXT NOT NULL,
  finished_at     TEXT NOT NULL DEFAULT '',
  status          TEXT NOT NULL,
  error           TEXT NOT NULL DEFAULT '',
  attempts        INTEGER NOT NULL DEFAULT 1,
  chunks_supplied INTEGER NOT NULL DEFAULT 0,
  bytes_supplied  INTEGER NOT NULL DEFAULT 0,
  UNIQUE (snapshot_id, repair_id)
);
INSERT INTO repair_attempts (repair_id, snapshot_id, started_at, status)
  VALUES ('r-old', 1, '2026-01-02T00:00:00Z', 'failed');
`)
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	db, err := Open(path)
	if err != nil {
		t.Fatalf("open old db: %v", err)
	}
	defer db.Close()

	// Old data intact.
	snap, err := db.GetSnapshot(1)
	if err != nil || snap == nil {
		t.Fatalf("old snapshot lost: %v %v", snap, err)
	}
	if snap.Status != StatusFailed {
		t.Fatalf("old snapshot status = %s", snap.Status)
	}
	missing, err := db.MissingChunks(1)
	if err != nil || len(missing) != 1 {
		t.Fatalf("old missing_chunks lost: %v %v", missing, err)
	}

	// repair_attempts migrated and usable.
	id, err := db.BeginRepairAttempt(1, "r-migration")
	if err != nil {
		t.Fatalf("repair_attempts not migrated: %v", err)
	}
	if err := db.FinishRepairAttempt(id, RepairFailed, "check", 0, 0); err != nil {
		t.Fatal(err)
	}
	attempt, err := db.GetRepairAttempt(1, "r-migration")
	if err != nil || attempt == nil {
		t.Fatalf("attempt not persisted: %v %v", attempt, err)
	}
	if attempt.Status != RepairFailed || attempt.Error != "check" {
		t.Fatalf("attempt = %+v", attempt)
	}

	// The pre-migration attempt (written while the table still had the
	// snapshots foreign key) survived the rebuild.
	old, err := db.GetRepairAttempt(1, "r-old")
	if err != nil || old == nil {
		t.Fatalf("pre-migration attempt lost: %v %v", old, err)
	}

	// The rebuilt table no longer references snapshots: purging the
	// snapshot must not be blocked and must keep the repair history.
	if _, err := db.sql.Exec(`DELETE FROM missing_chunks WHERE snapshot_id=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.sql.Exec(`DELETE FROM snapshots WHERE id=1`); err != nil {
		t.Fatalf("snapshot delete blocked by FK (migration incomplete): %v", err)
	}
	if a, _ := db.GetRepairAttempt(1, "r-old"); a == nil {
		t.Fatal("repair history deleted with snapshot")
	}

	// Purge tables migrated and usable.
	batchID, err := db.CreatePurgeBatch(PurgeBatch{
		PurgeID: "p-migration", SourceRoot: "/old/root", KeepLastComplete: 1,
		ExecuteAfter: "2026-01-03T00:00:00Z",
	}, nil)
	if err != nil {
		t.Fatalf("purge_batches not migrated: %v", err)
	}
	batch, err := db.GetPurgeBatch("p-migration")
	if err != nil || batch == nil || batch.ID != batchID {
		t.Fatalf("batch not persisted: %v %v", batch, err)
	}
}
