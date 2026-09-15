package backup

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"backupd/internal/store"

	"github.com/restic/chunker"
)

// ErrSimulatedCrash is returned when fault injection aborts a repair
// mid-flight; the attempt row is deliberately left "running" so a retry
// (after a restart) resumes it, exactly like a real process crash.
var ErrSimulatedCrash = errors.New("simulated crash: repair interrupted")

type RepairRequest struct {
	// RepairID is the idempotency key chosen by the caller. Repeating a
	// succeeded (snapshot_id, repair_id) pair replays the stored result
	// without touching the manifest.
	RepairID string `json:"repair_id"`
	// FaultAfterChunks, when >0 and fault injection is enabled, aborts the
	// repair after N chunk uploads, simulating a crashed process.
	FaultAfterChunks int `json:"fault_after_chunks,omitempty"`
}

type RepairProblem struct {
	Path   string `json:"path"`
	Detail string `json:"detail"`
}

type RepairReport struct {
	SnapshotID     int64           `json:"snapshot_id"`
	RepairID       string          `json:"repair_id"`
	Status         string          `json:"status"` // succeeded | failed
	SnapshotStatus string          `json:"snapshot_status"`
	Attempts       int64           `json:"attempts"`
	VerifiedFiles  int             `json:"verified_files"`
	ChunksSupplied int64           `json:"chunks_supplied"`
	BytesSupplied  int64           `json:"bytes_supplied"`
	Problems       []RepairProblem `json:"problems,omitempty"`
	Replayed       bool            `json:"replayed"` // idempotent replay of a stored success
}

// Repair re-supplies the missing chunks of a failed snapshot from its
// original source_root and atomically marks the snapshot complete.
//
// Safety contract:
//   - every manifest entry is checked against the source tree first
//     (size, mtime, whole-file SHA-256; symlink targets; dir existence).
//     Any mismatch is reported per entry and nothing changes — new content
//     is never mixed into an old snapshot;
//   - re-chunking uses the same fixed chunker parameters, and the produced
//     chunk list must equal the manifest's, as a second guard;
//   - chunk writes go through the content-addressed store: hash/length
//     verified, existing chunks reused, nothing overwritten;
//   - the snapshot transition, missing_chunks cleanup and the attempt
//     record commit in one SQLite transaction, so an interrupted repair
//     (crash, killed process) is safely retried with the same repair_id.
func (s *Service) Repair(snapshotID int64, req RepairRequest) (*RepairReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.RepairID == "" {
		return nil, errors.New("repair_id is required")
	}
	if req.FaultAfterChunks > 0 && !s.faultEnabled {
		return nil, errors.New("fault_after_chunks rejected: server not started with -enable-fault-injection")
	}
	snap, err := s.db.GetSnapshot(snapshotID)
	if err != nil {
		return nil, err
	}
	if snap == nil {
		return nil, fmt.Errorf("snapshot %d not found", snapshotID)
	}

	// Idempotent replay: a stored success is returned as-is, no manifest writes.
	if prev, err := s.db.GetRepairAttempt(snapshotID, req.RepairID); err != nil {
		return nil, err
	} else if prev != nil && prev.Status == store.RepairSucceeded {
		return &RepairReport{
			SnapshotID: snapshotID, RepairID: req.RepairID,
			Status: store.RepairSucceeded, SnapshotStatus: snap.Status,
			Attempts: prev.Attempts, ChunksSupplied: prev.ChunksSupplied,
			BytesSupplied: prev.BytesSupplied, Replayed: true,
		}, nil
	}

	switch snap.Status {
	case store.StatusComplete:
		return nil, fmt.Errorf("snapshot %d is already complete", snapshotID)
	case store.StatusRunning:
		return nil, fmt.Errorf("snapshot %d is still running", snapshotID)
	case store.StatusIncomplete:
		return nil, fmt.Errorf("snapshot %d is incomplete; repair only applies to failed snapshots with missing chunks", snapshotID)
	}
	missing, err := s.db.MissingChunks(snapshotID)
	if err != nil {
		return nil, err
	}
	if len(missing) == 0 {
		return nil, fmt.Errorf("snapshot %d has no missing chunks recorded", snapshotID)
	}

	attemptID, err := s.db.BeginRepairAttempt(snapshotID, req.RepairID)
	if err != nil {
		return nil, err
	}
	fail := func(msg string, problems []RepairProblem, chunks, bytes int64) (*RepairReport, error) {
		_ = s.db.FinishRepairAttempt(attemptID, store.RepairFailed, msg, chunks, bytes)
		return &RepairReport{
			SnapshotID: snapshotID, RepairID: req.RepairID,
			Status: store.RepairFailed, SnapshotStatus: store.StatusFailed,
			Problems: problems, ChunksSupplied: chunks, BytesSupplied: bytes,
		}, nil
	}

	// Gate 1: every manifest entry must still match the source tree.
	files, err := s.db.ListFiles(snapshotID)
	if err != nil {
		return nil, err
	}
	problems := verifySources(snap.SourceRoot, files)
	if len(problems) > 0 {
		return fail(fmt.Sprintf("%d source entrie(s) missing or changed", len(problems)), problems, 0, 0)
	}

	// Gate 2: re-chunk the files that reference missing chunks and supply
	// exactly those chunks to the store.
	missingSet := map[string]bool{}
	for _, m := range missing {
		missingSet[m.ChunkSHA] = true
	}
	fault := &faultState{limit: req.FaultAfterChunks}
	var supplied, suppliedBytes int64
	for _, f := range files {
		if f.Type != "file" {
			continue
		}
		refs, err := s.db.FileChunks(f.ID)
		if err != nil {
			return nil, err
		}
		needs := false
		for _, r := range refs {
			if missingSet[r.SHA256] {
				needs = true
				break
			}
		}
		if !needs {
			continue
		}
		n, b, err := s.resupplyFile(snap.SourceRoot, f, refs, fault)
		if err != nil {
			if errors.Is(err, ErrSimulatedCrash) {
				return nil, err // attempt row stays "running": safe to retry
			}
			return fail(err.Error(), nil, supplied, suppliedBytes)
		}
		supplied += n
		suppliedBytes += b
	}

	// Gate 3: the whole snapshot must verify now, same rule as at commit.
	stillMissing, err := s.missingChunkRefs(snapshotID)
	if err != nil {
		return nil, err
	}
	if len(stillMissing) > 0 {
		return fail(fmt.Sprintf("%d chunk(s) still missing after resupply", len(stillMissing)),
			nil, supplied, suppliedBytes)
	}

	// Atomic commit: snapshot -> complete, missing_chunks cleared, attempt recorded.
	if err := s.db.CompleteRepairAndSnapshot(attemptID, snapshotID, supplied, suppliedBytes); err != nil {
		return nil, err
	}
	attempt, _ := s.db.GetRepairAttempt(snapshotID, req.RepairID)
	rep := &RepairReport{
		SnapshotID: snapshotID, RepairID: req.RepairID,
		Status: store.RepairSucceeded, SnapshotStatus: store.StatusComplete,
		VerifiedFiles: countRegularFiles(files), ChunksSupplied: supplied,
		BytesSupplied: suppliedBytes,
	}
	if attempt != nil {
		rep.Attempts = attempt.Attempts
	}
	return rep, nil
}

func countRegularFiles(files []store.FileEntry) int {
	n := 0
	for _, f := range files {
		if f.Type == "file" {
			n++
		}
	}
	return n
}

// verifySources checks every manifest entry against the live source tree.
// Any difference is reported; the caller must treat a non-empty result as
// "do not touch anything".
func verifySources(root string, files []store.FileEntry) []RepairProblem {
	var probs []RepairProblem
	for _, f := range files {
		p := filepath.Join(root, filepath.FromSlash(f.Path))
		switch f.Type {
		case "file":
			fi, err := os.Lstat(p)
			if err != nil {
				probs = append(probs, RepairProblem{f.Path, "source file missing: " + err.Error()})
				continue
			}
			if !fi.Mode().IsRegular() {
				probs = append(probs, RepairProblem{f.Path, "no longer a regular file"})
				continue
			}
			if fi.Size() != f.Size {
				probs = append(probs, RepairProblem{f.Path,
					fmt.Sprintf("size changed: manifest %d, now %d", f.Size, fi.Size())})
				continue
			}
			if fi.ModTime().UnixNano() != f.MtimeNs {
				probs = append(probs, RepairProblem{f.Path, "mtime changed"})
				continue
			}
			sha, err := hashFileContent(p)
			if err != nil {
				probs = append(probs, RepairProblem{f.Path, "unreadable: " + err.Error()})
				continue
			}
			if sha != f.SHA256 {
				probs = append(probs, RepairProblem{f.Path, "content changed (sha256 mismatch)"})
			}
		case "dir":
			fi, err := os.Lstat(p)
			if err != nil {
				probs = append(probs, RepairProblem{f.Path, "source directory missing: " + err.Error()})
			} else if !fi.IsDir() {
				probs = append(probs, RepairProblem{f.Path, "no longer a directory"})
			}
		case "symlink":
			target, err := os.Readlink(p)
			if err != nil {
				probs = append(probs, RepairProblem{f.Path, "source symlink missing: " + err.Error()})
			} else if target != f.LinkTarget {
				probs = append(probs, RepairProblem{f.Path,
					fmt.Sprintf("symlink target changed: manifest %q, now %q", f.LinkTarget, target)})
			}
		}
	}
	return probs
}

func hashFileContent(path string) (string, error) {
	fh, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer fh.Close()
	h := sha256.New()
	if _, err := io.Copy(h, fh); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// resupplyFile re-chunks one verified source file with the fixed chunker
// parameters and stores exactly the chunks not already present. The produced
// chunk list must equal the manifest's — a mismatch means the content does
// not reproduce the snapshot and nothing may be written.
func (s *Service) resupplyFile(root string, f store.FileEntry, manifestRefs []store.ChunkRef, fault *faultState) (int64, int64, error) {
	fh, err := os.Open(filepath.Join(root, filepath.FromSlash(f.Path)))
	if err != nil {
		return 0, 0, err
	}
	defer fh.Close()

	c := chunker.NewWithBoundaries(fh, chunkPol, chunkMin, chunkMax)
	c.SetAverageBits(chunkAvgBits)
	buf := make([]byte, chunkMax)

	var supplied, suppliedBytes int64
	seq := 0
	for {
		chunk, err := c.Next(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, 0, err
		}
		if seq >= len(manifestRefs) {
			return 0, 0, fmt.Errorf("rechunked %s produced more chunks than the manifest", f.Path)
		}
		sum := sha256.Sum256(chunk.Data)
		sha := hex.EncodeToString(sum[:])
		mr := manifestRefs[seq]
		if mr.SHA256 != sha || mr.Size != int64(len(chunk.Data)) {
			return 0, 0, fmt.Errorf("chunk layout mismatch for %s at chunk %d", f.Path, seq)
		}
		seq++
		if s.cs.Has(sha) {
			continue // shared or already supplied: reuse, never overwrite
		}
		if fault.drop() {
			return 0, 0, ErrSimulatedCrash
		}
		if err := s.cs.Put(sha, chunk.Data); err != nil { // Put re-verifies hash/length
			return 0, 0, err
		}
		supplied++
		suppliedBytes += int64(len(chunk.Data))
	}
	if seq != len(manifestRefs) {
		return 0, 0, fmt.Errorf("rechunked %s produced fewer chunks than the manifest", f.Path)
	}
	return supplied, suppliedBytes, nil
}
