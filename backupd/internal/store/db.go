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
	StatusRunning    = "running"
	StatusComplete   = "complete"
	StatusIncomplete = "incomplete" // finished, but some files were unstable mid-scan
	StatusFailed     = "failed"     // scan/commit error or chunks missing from store
)

// File entry states.
const (
	FileOK       = "ok"
	FileUnstable = "unstable" // changed while being read; last-read content kept
	FileEscaped  = "escaped"  // symlink whose target leaves the source root
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
	return db, nil
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

// DeleteSnapshots removes every manifest row of the given snapshots in a
// single transaction, then — inside the same transaction — computes the
// chunk-registry rows left with zero references from the remaining
// file_chunks and removes them from the registry. The returned list is what
// the caller may delete from the chunk directory after commit; a failure
// there leaves harmless orphan files, never an inconsistent manifest.
func (db *DB) DeleteSnapshots(ids []int64) ([]ReclaimedChunk, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	tx, err := db.sql.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

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
	if err := tx.Commit(); err != nil {
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
