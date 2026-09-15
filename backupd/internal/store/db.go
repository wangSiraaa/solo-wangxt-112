// Package store manages the backup manifest in SQLite: snapshots, file
// entries, chunk references and the missing-chunk report.
package store

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Snapshot lifecycle states.
const (
	StatusRunning      = "running"
	StatusComplete     = "complete"
	StatusIncomplete   = "incomplete"    // finished, but some files were unstable mid-scan
	StatusFailed       = "failed"        // scan/commit error or chunks missing from store
	StatusPendingPurge = "pending_purge" // slated for deletion, still undoable
)

// Purge batch states.
const (
	PurgeOpen      = "open"
	PurgeExecuted  = "executed"
	PurgeCancelled = "cancelled"
)

// Purge item states.
const (
	PurgeItemPending = "pending"
	PurgeItemUndone  = "undone"
	PurgeItemPurged  = "purged"
)

// File entry states.
const (
	FileOK       = "ok"
	FileUnstable = "unstable" // changed while being read; last-read content kept
	FileEscaped  = "escaped"  // symlink whose target leaves the source root
)

// Repair attempt states.
const (
	RepairRunning   = "running"
	RepairSucceeded = "succeeded"
	RepairFailed    = "failed"
)

type Snapshot struct {
	ID            int64  `json:"id"`
	SourceRoot    string `json:"source_root"`
	StartedAt     string `json:"started_at"`
	FinishedAt    string `json:"finished_at,omitempty"`
	Status        string `json:"status"`
	Error         string `json:"error,omitempty"`
	FileCount     int64  `json:"file_count"`
	DirCount      int64  `json:"dir_count"`
	SymlinkCount  int64  `json:"symlink_count"`
	TotalBytes    int64  `json:"total_bytes"`
	ChunksAdded   int64  `json:"chunks_added"`
	ChunksReused  int64  `json:"chunks_reused"`
	UnstableFiles int64  `json:"unstable_files"`
	MissingChunks int64  `json:"missing_chunks"`
}

type FileEntry struct {
	ID         int64  `json:"id"`
	SnapshotID int64  `json:"snapshot_id"`
	Path       string `json:"path"` // slash-separated, relative to source root
	Type       string `json:"type"` // file | dir | symlink
	Mode       int64  `json:"mode"`
	Size       int64  `json:"size"`
	MtimeNs    int64  `json:"mtime_ns"`
	LinkTarget string `json:"link_target,omitempty"`
	SHA256     string `json:"sha256,omitempty"`
	Status     string `json:"status"`
}

type ChunkRef struct {
	FileID   int64  `json:"file_id"`
	FilePath string `json:"file_path"`
	Seq      int64  `json:"seq"`
	SHA256   string `json:"sha256"`
	Size     int64  `json:"size"`
}

type MissingChunk struct {
	SnapshotID int64  `json:"snapshot_id"`
	FilePath   string `json:"file_path"`
	ChunkSHA   string `json:"chunk_sha256"`
	Reason     string `json:"reason"`
}

type DB struct {
	sql *sql.DB
}

func Open(path string) (*DB, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)", path)
	sqlDB, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	sqlDB.SetMaxOpenConns(1)
	db := &DB{sql: sqlDB}
	if err := db.migrate(); err != nil {
		sqlDB.Close()
		return nil, err
	}
	if err := db.migrateRepairAttemptsFK(); err != nil {
		sqlDB.Close()
		return nil, err
	}
	return db, nil
}

// migrateRepairAttemptsFK rebuilds repair_attempts on databases created
// before the snapshots foreign key was dropped: repair records are historical
// and must outlive the snapshots they refer to (purge deletes snapshots).
func (db *DB) migrateRepairAttemptsFK() error {
	var ddl string
	err := db.sql.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name='repair_attempts'`).Scan(&ddl)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	if !strings.Contains(ddl, "REFERENCES snapshots") {
		return nil // already migrated
	}
	stmts := []string{
		`ALTER TABLE repair_attempts RENAME TO repair_attempts_old`,
		`CREATE TABLE repair_attempts (
		  id              INTEGER PRIMARY KEY AUTOINCREMENT,
		  repair_id       TEXT NOT NULL,
		  snapshot_id     INTEGER NOT NULL,
		  started_at      TEXT NOT NULL,
		  finished_at     TEXT NOT NULL DEFAULT '',
		  status          TEXT NOT NULL,
		  error           TEXT NOT NULL DEFAULT '',
		  attempts        INTEGER NOT NULL DEFAULT 1,
		  chunks_supplied INTEGER NOT NULL DEFAULT 0,
		  bytes_supplied  INTEGER NOT NULL DEFAULT 0,
		  UNIQUE (snapshot_id, repair_id)
		)`,
		`INSERT INTO repair_attempts SELECT id, repair_id, snapshot_id, started_at, finished_at,
		  status, error, attempts, chunks_supplied, bytes_supplied FROM repair_attempts_old`,
		`DROP TABLE repair_attempts_old`,
	}
	for _, q := range stmts {
		if _, err := db.sql.Exec(q); err != nil {
			return fmt.Errorf("migrate repair_attempts: %w", err)
		}
	}
	return nil
}

func (db *DB) Close() error { return db.sql.Close() }

func (db *DB) migrate() error {
	_, err := db.sql.Exec(`
CREATE TABLE IF NOT EXISTS snapshots (
  id             INTEGER PRIMARY KEY AUTOINCREMENT,
  source_root    TEXT NOT NULL,
  started_at     TEXT NOT NULL,
  finished_at    TEXT NOT NULL DEFAULT '',
  status         TEXT NOT NULL,
  error          TEXT NOT NULL DEFAULT '',
  file_count     INTEGER NOT NULL DEFAULT 0,
  dir_count      INTEGER NOT NULL DEFAULT 0,
  symlink_count  INTEGER NOT NULL DEFAULT 0,
  total_bytes    INTEGER NOT NULL DEFAULT 0,
  chunks_added   INTEGER NOT NULL DEFAULT 0,
  chunks_reused  INTEGER NOT NULL DEFAULT 0,
  unstable_files INTEGER NOT NULL DEFAULT 0,
  missing_chunks INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS files (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),
  path        TEXT NOT NULL,
  type        TEXT NOT NULL,
  mode        INTEGER NOT NULL DEFAULT 0,
  size        INTEGER NOT NULL DEFAULT 0,
  mtime_ns    INTEGER NOT NULL DEFAULT 0,
  link_target TEXT NOT NULL DEFAULT '',
  sha256      TEXT NOT NULL DEFAULT '',
  status      TEXT NOT NULL DEFAULT 'ok',
  UNIQUE (snapshot_id, path)
);
CREATE TABLE IF NOT EXISTS chunks (
  sha256              TEXT PRIMARY KEY,
  size                INTEGER NOT NULL,
  first_seen_snapshot INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS file_chunks (
  file_id      INTEGER NOT NULL REFERENCES files(id),
  seq          INTEGER NOT NULL,
  chunk_sha256 TEXT NOT NULL,
  size         INTEGER NOT NULL,
  PRIMARY KEY (file_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_file_chunks_sha ON file_chunks(chunk_sha256);
CREATE TABLE IF NOT EXISTS missing_chunks (
  snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),
  file_path   TEXT NOT NULL,
  chunk_sha256 TEXT NOT NULL,
  reason      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_missing_snap ON missing_chunks(snapshot_id);
CREATE TABLE IF NOT EXISTS repair_attempts (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  repair_id       TEXT NOT NULL,
  snapshot_id     INTEGER NOT NULL,
  started_at      TEXT NOT NULL,
  finished_at     TEXT NOT NULL DEFAULT '',
  status          TEXT NOT NULL,
  error           TEXT NOT NULL DEFAULT '',
  attempts        INTEGER NOT NULL DEFAULT 1,
  chunks_supplied INTEGER NOT NULL DEFAULT 0,
  bytes_supplied  INTEGER NOT NULL DEFAULT 0,
  UNIQUE (snapshot_id, repair_id)
);
CREATE INDEX IF NOT EXISTS idx_repair_snap ON repair_attempts(snapshot_id);
CREATE TABLE IF NOT EXISTS purge_batches (
  id                 INTEGER PRIMARY KEY AUTOINCREMENT,
  purge_id           TEXT NOT NULL UNIQUE,
  source_root        TEXT NOT NULL,
  keep_last_complete INTEGER NOT NULL,
  created_at         TEXT NOT NULL,
  execute_after      TEXT NOT NULL,
  status             TEXT NOT NULL,
  executed_at        TEXT NOT NULL DEFAULT '',
  operator           TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS purge_items (
  batch_id    INTEGER NOT NULL REFERENCES purge_batches(id),
  snapshot_id INTEGER NOT NULL,
  state       TEXT NOT NULL DEFAULT 'pending',
  undone_at   TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (batch_id, snapshot_id)
);
CREATE TABLE IF NOT EXISTS purge_audit (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id    INTEGER NOT NULL REFERENCES purge_batches(id),
  snapshot_id INTEGER NOT NULL DEFAULT 0,
  action      TEXT NOT NULL,
  at          TEXT NOT NULL,
  detail      TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_purge_audit_batch ON purge_audit(batch_id);
`)
	return err
}

func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func (db *DB) CreateSnapshot(sourceRoot string) (int64, error) {
	res, err := db.sql.Exec(
		`INSERT INTO snapshots (source_root, started_at, status) VALUES (?,?,?)`,
		sourceRoot, now(), StatusRunning)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

// FinishSnapshot stores the final status and counters.
func (db *DB) FinishSnapshot(s Snapshot) error {
	_, err := db.sql.Exec(`UPDATE snapshots SET finished_at=?, status=?, error=?,
		file_count=?, dir_count=?, symlink_count=?, total_bytes=?,
		chunks_added=?, chunks_reused=?, unstable_files=?, missing_chunks=?
		WHERE id=?`,
		now(), s.Status, s.Error, s.FileCount, s.DirCount, s.SymlinkCount,
		s.TotalBytes, s.ChunksAdded, s.ChunksReused, s.UnstableFiles,
		s.MissingChunks, s.ID)
	return err
}

func (db *DB) InsertFile(snapshotID int64, e FileEntry) (int64, error) {
	res, err := db.sql.Exec(`INSERT INTO files
		(snapshot_id, path, type, mode, size, mtime_ns, link_target, sha256, status)
		VALUES (?,?,?,?,?,?,?,?,?)`,
		snapshotID, e.Path, e.Type, e.Mode, e.Size, e.MtimeNs, e.LinkTarget, e.SHA256, e.Status)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (db *DB) InsertFileChunk(fileID int64, seq int64, sha string, size int) error {
	_, err := db.sql.Exec(`INSERT INTO file_chunks (file_id, seq, chunk_sha256, size) VALUES (?,?,?,?)`,
		fileID, seq, sha, size)
	return err
}

// NoteChunk records a chunk hash in the global table; reports whether it is new.
func (db *DB) NoteChunk(sha string, size int, snapshotID int64) (isNew bool, err error) {
	res, err := db.sql.Exec(`INSERT OR IGNORE INTO chunks (sha256, size, first_seen_snapshot) VALUES (?,?,?)`,
		sha, size, snapshotID)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// SnapshotChunkRefs returns every chunk reference of a snapshot, joined with
// the owning file path, ordered for deterministic verification.
func (db *DB) SnapshotChunkRefs(snapshotID int64) ([]ChunkRef, error) {
	rows, err := db.sql.Query(`SELECT fc.file_id, f.path, fc.seq, fc.chunk_sha256, fc.size
		FROM file_chunks fc JOIN files f ON f.id = fc.file_id
		WHERE f.snapshot_id = ? ORDER BY f.path, fc.seq`, snapshotID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ChunkRef
	for rows.Next() {
		var r ChunkRef
		if err := rows.Scan(&r.FileID, &r.FilePath, &r.Seq, &r.SHA256, &r.Size); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (db *DB) InsertMissingChunk(snapshotID int64, path, sha, reason string) error {
	_, err := db.sql.Exec(`INSERT INTO missing_chunks (snapshot_id, file_path, chunk_sha256, reason) VALUES (?,?,?,?)`,
		snapshotID, path, sha, reason)
	return err
}

func (db *DB) MissingChunks(snapshotID int64) ([]MissingChunk, error) {
	rows, err := db.sql.Query(`SELECT snapshot_id, file_path, chunk_sha256, reason
		FROM missing_chunks WHERE snapshot_id = ? ORDER BY file_path, chunk_sha256`, snapshotID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []MissingChunk{}
	for rows.Next() {
		var m MissingChunk
		if err := rows.Scan(&m.SnapshotID, &m.FilePath, &m.ChunkSHA, &m.Reason); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

func (db *DB) GetSnapshot(id int64) (*Snapshot, error) {
	row := db.sql.QueryRow(`SELECT id, source_root, started_at, finished_at, status, error,
		file_count, dir_count, symlink_count, total_bytes, chunks_added, chunks_reused,
		unstable_files, missing_chunks FROM snapshots WHERE id = ?`, id)
	var s Snapshot
	err := row.Scan(&s.ID, &s.SourceRoot, &s.StartedAt, &s.FinishedAt, &s.Status, &s.Error,
		&s.FileCount, &s.DirCount, &s.SymlinkCount, &s.TotalBytes, &s.ChunksAdded,
		&s.ChunksReused, &s.UnstableFiles, &s.MissingChunks)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &s, nil
}

func (db *DB) ListSnapshots() ([]Snapshot, error) {
	return db.querySnapshots(`SELECT id, source_root, started_at, finished_at, status, error,
		file_count, dir_count, symlink_count, total_bytes, chunks_added, chunks_reused,
		unstable_files, missing_chunks FROM snapshots ORDER BY id`)
}

// SnapshotsForRoot lists snapshots of one source root, oldest first.
func (db *DB) SnapshotsForRoot(root string) ([]Snapshot, error) {
	return db.querySnapshots(`SELECT id, source_root, started_at, finished_at, status, error,
		file_count, dir_count, symlink_count, total_bytes, chunks_added, chunks_reused,
		unstable_files, missing_chunks FROM snapshots WHERE source_root = ? ORDER BY id`, root)
}

func (db *DB) querySnapshots(q string, args ...any) ([]Snapshot, error) {
	rows, err := db.sql.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Snapshot{}
	for rows.Next() {
		var s Snapshot
		if err := rows.Scan(&s.ID, &s.SourceRoot, &s.StartedAt, &s.FinishedAt, &s.Status, &s.Error,
			&s.FileCount, &s.DirCount, &s.SymlinkCount, &s.TotalBytes, &s.ChunksAdded,
			&s.ChunksReused, &s.UnstableFiles, &s.MissingChunks); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// CountChunks returns the number of chunk-registry rows.
func (db *DB) CountChunks() (int64, error) {
	var n int64
	err := db.sql.QueryRow(`SELECT COUNT(*) FROM chunks`).Scan(&n)
	return n, err
}

// ReclaimedChunk is a chunk that became unreferenced after a retention run.
type ReclaimedChunk struct {
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

func idsToArgs(ids []int64) []any {
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	return args
}

// ReclaimableChunks is the read-only preview of DeleteSnapshots: registry
// chunks not referenced by any snapshot outside excludeIDs (this also covers
// registry rows that nothing references, e.g. chunks of torn reads).
func (db *DB) ReclaimableChunks(excludeIDs []int64) ([]ReclaimedChunk, error) {
	if len(excludeIDs) == 0 {
		return nil, nil
	}
	q := `SELECT sha256, size FROM chunks c WHERE NOT EXISTS (
		SELECT 1 FROM file_chunks fc JOIN files f ON f.id = fc.file_id
		WHERE fc.chunk_sha256 = c.sha256
		  AND f.snapshot_id NOT IN (` + placeholders(len(excludeIDs)) + `)) ORDER BY sha256`
	rows, err := db.sql.Query(q, idsToArgs(excludeIDs)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanReclaimed(rows)
}

// deleteSnapshotsAndOrphansTx removes every manifest row of the given
// snapshots, then computes the chunk-registry rows left with zero references
// from the remaining file_chunks and removes them from the registry. It runs
// inside the caller's transaction; the returned chunks may be deleted from
// the chunk directory after commit.
func (db *DB) deleteSnapshotsAndOrphansTx(tx *sql.Tx, ids []int64) ([]ReclaimedChunk, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	ph := placeholders(len(ids))
	args := idsToArgs(ids)
	stmts := []string{
		`DELETE FROM file_chunks WHERE file_id IN (SELECT id FROM files WHERE snapshot_id IN (` + ph + `))`,
		`DELETE FROM files WHERE snapshot_id IN (` + ph + `)`,
		`DELETE FROM missing_chunks WHERE snapshot_id IN (` + ph + `)`,
		`DELETE FROM snapshots WHERE id IN (` + ph + `)`,
	}
	for _, q := range stmts {
		if _, err := tx.Exec(q, args...); err != nil {
			return nil, err
		}
	}
	rows, err := tx.Query(`SELECT sha256, size FROM chunks c
		WHERE NOT EXISTS (SELECT 1 FROM file_chunks fc WHERE fc.chunk_sha256 = c.sha256)
		ORDER BY sha256`)
	if err != nil {
		return nil, err
	}
	orphans, err := scanReclaimed(rows)
	rows.Close()
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(`DELETE FROM chunks WHERE NOT EXISTS
		(SELECT 1 FROM file_chunks fc WHERE fc.chunk_sha256 = chunks.sha256)`); err != nil {
		return nil, err
	}
	return orphans, nil
}

func scanReclaimed(rows interface {
	Next() bool
	Scan(...any) error
	Err() error
}) ([]ReclaimedChunk, error) {
	var out []ReclaimedChunk
	for rows.Next() {
		var c ReclaimedChunk
		if err := rows.Scan(&c.SHA256, &c.Size); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// RepairAttempt is the persisted, idempotent record of one logical repair
// operation (keyed by snapshot_id + repair_id).
type RepairAttempt struct {
	ID             int64  `json:"id"`
	RepairID       string `json:"repair_id"`
	SnapshotID     int64  `json:"snapshot_id"`
	StartedAt      string `json:"started_at"`
	FinishedAt     string `json:"finished_at,omitempty"`
	Status         string `json:"status"` // running | succeeded | failed
	Error          string `json:"error,omitempty"`
	Attempts       int64  `json:"attempts"`
	ChunksSupplied int64  `json:"chunks_supplied"`
	BytesSupplied  int64  `json:"bytes_supplied"`
}

// BeginRepairAttempt registers a new attempt or re-arms an interrupted/failed one
// (same snapshot+repair_id): the row goes back to running and the execution
// counter increases. A succeeded row is never reset here — callers check it
// first and replay instead.
func (db *DB) BeginRepairAttempt(snapshotID int64, repairID string) (int64, error) {
	if _, err := db.sql.Exec(`INSERT INTO repair_attempts (repair_id, snapshot_id, started_at, status, attempts)
		VALUES (?,?,?,?,1)
		ON CONFLICT (snapshot_id, repair_id) DO UPDATE SET
			started_at=excluded.started_at, finished_at='', status=?, error='',
			chunks_supplied=0, bytes_supplied=0,
			attempts=repair_attempts.attempts+1`,
		repairID, snapshotID, now(), RepairRunning, RepairRunning); err != nil {
		return 0, err
	}
	var id int64
	err := db.sql.QueryRow(`SELECT id FROM repair_attempts WHERE snapshot_id=? AND repair_id=?`,
		snapshotID, repairID).Scan(&id)
	return id, err
}

// FinishRepairAttempt records the outcome of a failed attempt. Success goes
// through CompleteRepairAndSnapshot so the snapshot transition and the
// attempt record commit atomically.
func (db *DB) FinishRepairAttempt(id int64, status, errMsg string, chunks, bytes int64) error {
	_, err := db.sql.Exec(`UPDATE repair_attempts SET status=?, finished_at=?, error=?,
		chunks_supplied=?, bytes_supplied=? WHERE id=?`,
		status, now(), errMsg, chunks, bytes, id)
	return err
}

// CompleteRepairAndSnapshot atomically, in one transaction: flips the
// snapshot failed->complete (guarded on its current state), clears its
// missing_chunks and marks the repair attempt succeeded.
func (db *DB) CompleteRepairAndSnapshot(attemptID, snapshotID int64, chunks, bytes int64) error {
	tx, err := db.sql.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	res, err := tx.Exec(`UPDATE snapshots SET status=?, error='', finished_at=?, missing_chunks=0
		WHERE id=? AND status=?`, StatusComplete, now(), snapshotID, StatusFailed)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		return fmt.Errorf("snapshot %d no longer in failed state; repair not committed", snapshotID)
	}
	if _, err := tx.Exec(`DELETE FROM missing_chunks WHERE snapshot_id=?`, snapshotID); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE repair_attempts SET status=?, finished_at=?, error='',
		chunks_supplied=?, bytes_supplied=? WHERE id=?`,
		RepairSucceeded, now(), chunks, bytes, attemptID); err != nil {
		return err
	}
	return tx.Commit()
}

func (db *DB) GetRepairAttempt(snapshotID int64, repairID string) (*RepairAttempt, error) {
	row := db.sql.QueryRow(`SELECT id, repair_id, snapshot_id, started_at, finished_at, status,
		error, attempts, chunks_supplied, bytes_supplied
		FROM repair_attempts WHERE snapshot_id=? AND repair_id=?`, snapshotID, repairID)
	var a RepairAttempt
	err := row.Scan(&a.ID, &a.RepairID, &a.SnapshotID, &a.StartedAt, &a.FinishedAt,
		&a.Status, &a.Error, &a.Attempts, &a.ChunksSupplied, &a.BytesSupplied)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (db *DB) ListRepairAttempts(snapshotID int64) ([]RepairAttempt, error) {
	rows, err := db.sql.Query(`SELECT id, repair_id, snapshot_id, started_at, finished_at, status,
		error, attempts, chunks_supplied, bytes_supplied
		FROM repair_attempts WHERE snapshot_id=? ORDER BY id`, snapshotID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []RepairAttempt{}
	for rows.Next() {
		var a RepairAttempt
		if err := rows.Scan(&a.ID, &a.RepairID, &a.SnapshotID, &a.StartedAt, &a.FinishedAt,
			&a.Status, &a.Error, &a.Attempts, &a.ChunksSupplied, &a.BytesSupplied); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (db *DB) ListFiles(snapshotID int64) ([]FileEntry, error) {
	rows, err := db.sql.Query(`SELECT id, snapshot_id, path, type, mode, size, mtime_ns,
		link_target, sha256, status FROM files WHERE snapshot_id = ? ORDER BY path`, snapshotID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []FileEntry{}
	for rows.Next() {
		var e FileEntry
		if err := rows.Scan(&e.ID, &e.SnapshotID, &e.Path, &e.Type, &e.Mode, &e.Size,
			&e.MtimeNs, &e.LinkTarget, &e.SHA256, &e.Status); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// FileChunks returns the ordered chunk list of one file entry.
func (db *DB) FileChunks(fileID int64) ([]ChunkRef, error) {
	rows, err := db.sql.Query(`SELECT file_id, '', seq, chunk_sha256, size
		FROM file_chunks WHERE file_id = ? ORDER BY seq`, fileID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ChunkRef{}
	for rows.Next() {
		var r ChunkRef
		if err := rows.Scan(&r.FileID, &r.FilePath, &r.Seq, &r.SHA256, &r.Size); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// PurgeBatch is one delayed-deletion plan created by a retention apply.
type PurgeBatch struct {
	ID               int64  `json:"id"`
	PurgeID          string `json:"purge_id"`
	SourceRoot       string `json:"source_root"`
	KeepLastComplete int    `json:"keep_last_complete"`
	CreatedAt        string `json:"created_at"`
	ExecuteAfter     string `json:"execute_after"`
	Status           string `json:"status"` // open | executed | cancelled
	ExecutedAt       string `json:"executed_at,omitempty"`
	Operator         string `json:"operator,omitempty"`
}

// PurgeItem is one snapshot inside a purge batch.
type PurgeItem struct {
	BatchID    int64  `json:"batch_id"`
	SnapshotID int64  `json:"snapshot_id"`
	State      string `json:"state"` // pending | undone | purged
	UndoneAt   string `json:"undone_at,omitempty"`
}

// PurgeAuditEntry is one audited operation on a batch.
type PurgeAuditEntry struct {
	ID         int64  `json:"id"`
	BatchID    int64  `json:"batch_id"`
	SnapshotID int64  `json:"snapshot_id,omitempty"`
	Action     string `json:"action"`
	At         string `json:"at"`
	Detail     string `json:"detail,omitempty"`
}

// CreatePurgeBatch atomically, in one transaction: inserts the batch and its
// items, flips the selected snapshots complete->pending_purge (guarded per
// row) and writes the audit entry.
func (db *DB) CreatePurgeBatch(b PurgeBatch, snapshotIDs []int64) (int64, error) {
	tx, err := db.sql.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	res, err := tx.Exec(`INSERT INTO purge_batches
		(purge_id, source_root, keep_last_complete, created_at, execute_after, status, operator)
		VALUES (?,?,?,?,?,?,?)`,
		b.PurgeID, b.SourceRoot, b.KeepLastComplete, now(), b.ExecuteAfter, PurgeOpen, b.Operator)
	if err != nil {
		return 0, err
	}
	batchID, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	for _, id := range snapshotIDs {
		if _, err := tx.Exec(`INSERT INTO purge_items (batch_id, snapshot_id, state) VALUES (?,?,?)`,
			batchID, id, PurgeItemPending); err != nil {
			return 0, err
		}
		r, err := tx.Exec(`UPDATE snapshots SET status=? WHERE id=? AND status=?`,
			StatusPendingPurge, id, StatusComplete)
		if err != nil {
			return 0, err
		}
		if n, _ := r.RowsAffected(); n != 1 {
			return 0, fmt.Errorf("snapshot %d not in complete state; batch not created", id)
		}
	}
	if _, err := tx.Exec(`INSERT INTO purge_audit (batch_id, snapshot_id, action, at, detail) VALUES (?,?,?,?,?)`,
		batchID, 0, "created", now(),
		fmt.Sprintf("keep_last_complete=%d, %d snapshot(s) pending", b.KeepLastComplete, len(snapshotIDs))); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return batchID, nil
}

func scanBatch(row interface{ Scan(...any) error }) (*PurgeBatch, error) {
	var b PurgeBatch
	err := row.Scan(&b.ID, &b.PurgeID, &b.SourceRoot, &b.KeepLastComplete, &b.CreatedAt,
		&b.ExecuteAfter, &b.Status, &b.ExecutedAt, &b.Operator)
	if err != nil {
		return nil, err
	}
	return &b, nil
}

func (db *DB) GetPurgeBatch(purgeID string) (*PurgeBatch, error) {
	b, err := scanBatch(db.sql.QueryRow(`SELECT id, purge_id, source_root, keep_last_complete,
		created_at, execute_after, status, executed_at, operator
		FROM purge_batches WHERE purge_id=?`, purgeID))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return b, err
}

func (db *DB) ListPurgeBatches() ([]PurgeBatch, error) {
	rows, err := db.sql.Query(`SELECT id, purge_id, source_root, keep_last_complete,
		created_at, execute_after, status, executed_at, operator
		FROM purge_batches ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []PurgeBatch{}
	for rows.Next() {
		b, err := scanBatch(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *b)
	}
	return out, rows.Err()
}

func (db *DB) PurgeItems(batchID int64) ([]PurgeItem, error) {
	rows, err := db.sql.Query(`SELECT batch_id, snapshot_id, state, undone_at
		FROM purge_items WHERE batch_id=? ORDER BY snapshot_id`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []PurgeItem{}
	for rows.Next() {
		var it PurgeItem
		if err := rows.Scan(&it.BatchID, &it.SnapshotID, &it.State, &it.UndoneAt); err != nil {
			return nil, err
		}
		out = append(out, it)
	}
	return out, rows.Err()
}

func (db *DB) PurgeAudit(batchID int64) ([]PurgeAuditEntry, error) {
	rows, err := db.sql.Query(`SELECT id, batch_id, snapshot_id, action, at, detail
		FROM purge_audit WHERE batch_id=? ORDER BY id`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []PurgeAuditEntry{}
	for rows.Next() {
		var a PurgeAuditEntry
		if err := rows.Scan(&a.ID, &a.BatchID, &a.SnapshotID, &a.Action, &a.At, &a.Detail); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// SnapshotHasRunningRepair reports whether a snapshot has a repair attempt
// in flight; such snapshots must never be purged.
func (db *DB) SnapshotHasRunningRepair(snapshotID int64) (bool, error) {
	var n int64
	err := db.sql.QueryRow(`SELECT COUNT(*) FROM repair_attempts WHERE snapshot_id=? AND status=?`,
		snapshotID, RepairRunning).Scan(&n)
	return n > 0, err
}

// UndoPurgeItem flips one pending item back: the snapshot returns to
// complete (its file/chunk manifest was never touched) and the item is
// marked undone, in one transaction.
func (db *DB) UndoPurgeItem(batchID, snapshotID int64) error {
	tx, err := db.sql.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	r, err := tx.Exec(`UPDATE snapshots SET status=? WHERE id=? AND status=?`,
		StatusComplete, snapshotID, StatusPendingPurge)
	if err != nil {
		return err
	}
	if n, _ := r.RowsAffected(); n != 1 {
		return fmt.Errorf("snapshot %d not in pending_purge state", snapshotID)
	}
	r, err = tx.Exec(`UPDATE purge_items SET state=?, undone_at=? WHERE batch_id=? AND snapshot_id=? AND state=?`,
		PurgeItemUndone, now(), batchID, snapshotID, PurgeItemPending)
	if err != nil {
		return err
	}
	if n, _ := r.RowsAffected(); n != 1 {
		return fmt.Errorf("purge item %d not pending in batch %d", snapshotID, batchID)
	}
	if _, err := tx.Exec(`INSERT INTO purge_audit (batch_id, snapshot_id, action, at, detail) VALUES (?,?,?,?,?)`,
		batchID, snapshotID, "undo_snapshot", now(), ""); err != nil {
		return err
	}
	return tx.Commit()
}

// UndoPurgeBatch undoes every still-pending item and cancels the batch.
func (db *DB) UndoPurgeBatch(batchID int64) ([]int64, error) {
	tx, err := db.sql.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.Query(`SELECT snapshot_id FROM purge_items WHERE batch_id=? AND state=?`,
		batchID, PurgeItemPending)
	if err != nil {
		return nil, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if ids == nil {
		ids = []int64{}
	}
	for _, id := range ids {
		if _, err := tx.Exec(`UPDATE snapshots SET status=? WHERE id=? AND status=?`,
			StatusComplete, id, StatusPendingPurge); err != nil {
			return nil, err
		}
	}
	if _, err := tx.Exec(`UPDATE purge_items SET state=?, undone_at=? WHERE batch_id=? AND state=?`,
		PurgeItemUndone, now(), batchID, PurgeItemPending); err != nil {
		return nil, err
	}
	r, err := tx.Exec(`UPDATE purge_batches SET status=? WHERE id=? AND status=?`,
		PurgeCancelled, batchID, PurgeOpen)
	if err != nil {
		return nil, err
	}
	if n, _ := r.RowsAffected(); n != 1 {
		return nil, fmt.Errorf("purge batch %d not open", batchID)
	}
	if _, err := tx.Exec(`INSERT INTO purge_audit (batch_id, snapshot_id, action, at, detail) VALUES (?,?,?,?,?)`,
		batchID, 0, "undo_batch", now(), fmt.Sprintf("%d snapshot(s) restored to complete", len(ids))); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return ids, nil
}

// ExecutePurgeBatch atomically, in one transaction: deletes the manifest of
// the given snapshots (computing zero-reference chunks from all surviving
// file_chunks), marks their items purged, optionally closes the batch as
// executed, and writes the audit entry. Chunk files are removed by the
// caller after commit.
func (db *DB) ExecutePurgeBatch(batchID int64, ids []int64, markExecuted bool) ([]ReclaimedChunk, error) {
	tx, err := db.sql.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	orphans, err := db.deleteSnapshotsAndOrphansTx(tx, ids)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		if _, err := tx.Exec(`UPDATE purge_items SET state=? WHERE batch_id=? AND snapshot_id=? AND state=?`,
			PurgeItemPurged, batchID, id, PurgeItemPending); err != nil {
			return nil, err
		}
	}
	if markExecuted {
		r, err := tx.Exec(`UPDATE purge_batches SET status=?, executed_at=? WHERE id=? AND status=?`,
			PurgeExecuted, now(), batchID, PurgeOpen)
		if err != nil {
			return nil, err
		}
		if n, _ := r.RowsAffected(); n != 1 {
			return nil, fmt.Errorf("purge batch %d not open", batchID)
		}
	}
	if _, err := tx.Exec(`INSERT INTO purge_audit (batch_id, snapshot_id, action, at, detail) VALUES (?,?,?,?,?)`,
		batchID, 0, "executed", now(), fmt.Sprintf("%d snapshot(s) purged", len(ids))); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return orphans, nil
}
