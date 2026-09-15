// Package backup implements snapshot creation and restore.
package backup

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"backupd/internal/chunkstore"
	"backupd/internal/store"

	"github.com/restic/chunker"
)

// Chunking parameters: small enough that a one-line edit to a text file only
// invalidates a couple of chunks (good dedup demos), large enough to stay cheap.
const (
	chunkMin     = 2 * 1024
	chunkMax     = 64 * 1024
	chunkAvgBits = 14 // ~16 KiB average
	// Fixed polynomial: chunk boundaries are deterministic across snapshots
	// and restarts, which is what makes cross-snapshot dedup work.
	chunkPol = chunker.Pol(0x3DA3358B4DC173)

	maxStabilityAttempts = 3
)

type Service struct {
	db           *store.DB
	cs           *chunkstore.Store
	faultEnabled bool
	mu           sync.Mutex // one snapshot/restore at a time
}

func NewService(db *store.DB, cs *chunkstore.Store, faultEnabled bool) *Service {
	return &Service{db: db, cs: cs, faultEnabled: faultEnabled}
}

type SnapshotRequest struct {
	SourceRoot string `json:"source_root"`
	// FaultAfterChunks, when >0 and the server allows fault injection,
	// simulates an interrupted commit: the first N chunk uploads of this
	// snapshot succeed, later ones are silently lost while the manifest
	// still references them. The snapshot then fails verification and the
	// lost chunks show up in the missing-chunk report.
	FaultAfterChunks int `json:"fault_after_chunks,omitempty"`
}

type snapshotStats struct {
	files, dirs, symlinks int64
	totalBytes            int64
	chunksAdded           int64
	chunksReused          int64
	unstable              int64
}

// CreateSnapshot scans the source tree, chunks every regular file into the
// content-addressed store, then verifies that every referenced chunk is
// really present before marking the snapshot complete.
func (s *Service) CreateSnapshot(req SnapshotRequest) (*store.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	root, err := filepath.Abs(req.SourceRoot)
	if err != nil {
		return nil, fmt.Errorf("bad source_root: %w", err)
	}
	if resolved, err := filepath.EvalSymlinks(root); err == nil {
		root = resolved
	}
	fi, err := os.Stat(root)
	if err != nil {
		return nil, fmt.Errorf("source_root: %w", err)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("source_root %s is not a directory", root)
	}
	if req.FaultAfterChunks > 0 && !s.faultEnabled {
		return nil, fmt.Errorf("fault_after_chunks rejected: server not started with -enable-fault-injection")
	}

	snapID, err := s.db.CreateSnapshot(root)
	if err != nil {
		return nil, err
	}

	st := &snapshotStats{}
	fault := &faultState{limit: req.FaultAfterChunks}
	scanErr := s.scan(root, snapID, st, fault)

	// Pre-commit gate: every chunk the manifest references must verify
	// against the store, no matter how the scan ended.
	missing := 0
	refs, err := s.db.SnapshotChunkRefs(snapID)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, r := range refs {
		if seen[r.SHA256] { // same chunk referenced by many files: check once
			continue
		}
		seen[r.SHA256] = true
		if _, err := s.cs.Verify(r.SHA256); err != nil {
			missing++
			_ = s.db.InsertMissingChunk(snapID, r.FilePath, r.SHA256, err.Error())
		}
	}

	snap := store.Snapshot{
		ID:            snapID,
		SourceRoot:    root,
		Status:        store.StatusComplete,
		FileCount:     st.files,
		DirCount:      st.dirs,
		SymlinkCount:  st.symlinks,
		TotalBytes:    st.totalBytes,
		ChunksAdded:   st.chunksAdded,
		ChunksReused:  st.chunksReused,
		UnstableFiles: st.unstable,
		MissingChunks: int64(missing),
	}
	switch {
	case scanErr != nil:
		snap.Status = store.StatusFailed
		snap.Error = scanErr.Error()
	case missing > 0:
		snap.Status = store.StatusFailed
		snap.Error = fmt.Sprintf("%d referenced chunk(s) missing from store", missing)
	case st.unstable > 0:
		snap.Status = store.StatusIncomplete
		snap.Error = fmt.Sprintf("%d file(s) changed while being read", st.unstable)
	}
	if err := s.db.FinishSnapshot(snap); err != nil {
		return nil, err
	}
	return s.db.GetSnapshot(snapID)
}

// faultState simulates chunk uploads lost to an interrupted commit.
type faultState struct {
	limit int // uploads allowed to succeed; 0 = no fault
	count int
}

// drop reports whether this upload should be "lost".
func (f *faultState) drop() bool {
	if f.limit <= 0 {
		return false
	}
	f.count++
	return f.count > f.limit
}

func (s *Service) scan(root string, snapID int64, st *snapshotStats, fault *faultState) error {
	walkErr := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == root {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		info, err := os.Lstat(path) // never follow symlinks
		if err != nil {
			return err
		}
		mode := int64(info.Mode().Perm())

		switch {
		case info.IsDir():
			st.dirs++
			_, err := s.db.InsertFile(snapID, store.FileEntry{
				Path: rel, Type: "dir", Mode: mode, MtimeNs: info.ModTime().UnixNano(),
				Status: store.FileOK,
			})
			return err

		case info.Mode()&os.ModeSymlink != 0:
			st.symlinks++
			target, err := os.Readlink(path)
			if err != nil {
				return err
			}
			status := store.FileOK
			if !linkWithinRoot(root, filepath.Dir(path), target) {
				status = store.FileEscaped // recorded, but never followed/restored
			}
			_, err = s.db.InsertFile(snapID, store.FileEntry{
				Path: rel, Type: "symlink", Mode: mode, LinkTarget: target,
				MtimeNs: info.ModTime().UnixNano(), Status: status,
			})
			return err

		case info.Mode().IsRegular():
			st.files++
			return s.snapshotFile(path, rel, snapID, st, fault)

		default:
			// sockets, devices, fifos: not backable, note and skip
			_, err := s.db.InsertFile(snapID, store.FileEntry{
				Path: rel, Type: "other", Mode: mode, MtimeNs: info.ModTime().UnixNano(),
				Status: "skipped_unsupported",
			})
			return err
		}
	})
	return walkErr
}

// snapshotFile chunks one regular file into the store. A file that changes
// while it is being read is re-read; after maxStabilityAttempts it is kept
// with status "unstable" so the snapshot is marked incomplete instead of
// silently archiving a torn read.
func (s *Service) snapshotFile(path, rel string, snapID int64, st *snapshotStats, fault *faultState) error {
	var (
		refs     []store.ChunkRef
		sha      string
		size     int64
		unstable bool
	)
	for attempt := 1; ; attempt++ {
		before, err := os.Lstat(path)
		if err != nil {
			return err
		}
		refs, sha, size, err = s.chunkFile(path, snapID, st, fault)
		if err != nil {
			return fmt.Errorf("chunk %s: %w", rel, err)
		}
		after, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if before.ModTime().Equal(after.ModTime()) && before.Size() == after.Size() && after.Size() == size {
			break // stable read
		}
		if attempt >= maxStabilityAttempts {
			unstable = true
			st.unstable++
			break
		}
	}

	status := store.FileOK
	if unstable {
		status = store.FileUnstable
	}
	st.totalBytes += size
	fileID, err := s.db.InsertFile(snapID, store.FileEntry{
		Path: rel, Type: "file", Mode: fileMode(path), Size: size,
		MtimeNs: fileMtimeNs(path), SHA256: sha, Status: status,
	})
	if err != nil {
		return err
	}
	for _, r := range refs {
		if err := s.db.InsertFileChunk(fileID, r.Seq, r.SHA256, int(r.Size)); err != nil {
			return err
		}
	}
	return nil
}

func fileMode(path string) int64 {
	if fi, err := os.Lstat(path); err == nil {
		return int64(fi.Mode().Perm())
	}
	return 0
}

func fileMtimeNs(path string) int64 {
	if fi, err := os.Lstat(path); err == nil {
		return fi.ModTime().UnixNano()
	}
	return 0
}

// chunkFile splits one file and stores its chunks, returning the ordered
// references, the whole-file digest and the length read.
func (s *Service) chunkFile(path string, snapID int64, st *snapshotStats, fault *faultState) ([]store.ChunkRef, string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, "", 0, err
	}
	defer f.Close()

	c := chunker.NewWithBoundaries(f, chunkPol, chunkMin, chunkMax)
	c.SetAverageBits(chunkAvgBits)

	var refs []store.ChunkRef
	h := sha256.New()
	var size int64
	buf := make([]byte, chunkMax)
	for {
		chunk, err := c.Next(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, "", 0, err
		}
		sum := sha256.Sum256(chunk.Data)
		sha := hex.EncodeToString(sum[:])
		h.Write(chunk.Data)
		size += int64(len(chunk.Data))
		refs = append(refs, store.ChunkRef{Seq: int64(len(refs)), SHA256: sha, Size: int64(len(chunk.Data))})

		if _, err := s.db.NoteChunk(sha, len(chunk.Data), snapID); err != nil {
			return nil, "", 0, err
		}
		if !s.cs.Has(sha) {
			if fault.drop() {
				continue // simulated: upload lost to interrupted commit
			}
			if err := s.cs.Put(sha, chunk.Data); err != nil {
				return nil, "", 0, err
			}
			st.chunksAdded++
		} else {
			st.chunksReused++
		}
	}
	return refs, hex.EncodeToString(h.Sum(nil)), size, nil
}

// linkWithinRoot reports whether a symlink target, resolved lexically
// relative to the link's own directory, stays inside root. The scan never
// follows symlinks, and restores refuse links that escape.
func linkWithinRoot(root, linkDir, target string) bool {
	var resolved string
	if filepath.IsAbs(target) {
		resolved = filepath.Clean(target)
	} else {
		resolved = filepath.Clean(filepath.Join(linkDir, target))
	}
	root = filepath.Clean(root)
	return resolved == root || strings.HasPrefix(resolved, root+string(os.PathSeparator))
}
