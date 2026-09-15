package backup

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"

	"backupd/internal/store"
)

type RetentionRequest struct {
	SourceRoot string `json:"source_root"`
	// KeepLastComplete keeps the N most recent complete snapshots of
	// source_root; older complete snapshots are deleted. Snapshots in any
	// other state (failed, incomplete, running) are always kept.
	KeepLastComplete int `json:"keep_last_complete"`
	// DryRun computes the report without changing the manifest or the
	// chunk directory.
	DryRun bool `json:"dry_run,omitempty"`
}

type KeptSnapshot struct {
	ID     int64  `json:"id"`
	Status string `json:"status"`
	Reason string `json:"reason"`
}

type RetentionReport struct {
	SourceRoot       string         `json:"source_root"`
	KeepLastComplete int            `json:"keep_last_complete"`
	DryRun           bool           `json:"dry_run"`
	Applied          bool           `json:"applied"`
	DeleteSnapshots  []int64        `json:"delete_snapshots"`
	KeptSnapshots    []KeptSnapshot `json:"kept_snapshots"`
	ReclaimChunks    int            `json:"reclaim_chunks"`
	ReclaimBytes     int64          `json:"reclaim_bytes"`
	Warnings         []string       `json:"warnings,omitempty"`
}

// normalizeRoot resolves a path the same way snapshot creation does, so a
// retention call addresses the same source_root strings stored in the
// manifest. The path itself does not need to exist anymore.
func normalizeRoot(p string) (string, error) {
	root, err := filepath.Abs(p)
	if err != nil {
		return "", err
	}
	if resolved, err := filepath.EvalSymlinks(root); err == nil {
		root = resolved
	}
	return root, nil
}

// Retention applies (or previews, with DryRun) the keep-last-N-complete
// policy for one source root.
//
// Ordering guarantees on apply: the manifest is updated in a single SQLite
// transaction (snapshot/file/file_chunks/missing rows go, chunk-registry
// rows left with zero references are dropped), and only after that commit
// are the now-unreferenced chunks removed from the chunk directory. A
// failure during file removal is reported in Warnings and leaves harmless
// orphan files — never a manifest that references deleted data.
func (s *Service) Retention(req RetentionRequest) (*RetentionReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.SourceRoot == "" {
		return nil, errors.New("source_root is required")
	}
	if req.KeepLastComplete < 0 {
		return nil, errors.New("keep_last_complete must be >= 0")
	}
	root, err := normalizeRoot(req.SourceRoot)
	if err != nil {
		return nil, fmt.Errorf("bad source_root: %w", err)
	}

	snaps, err := s.db.SnapshotsForRoot(root)
	if err != nil {
		return nil, err
	}

	rep := &RetentionReport{
		SourceRoot:       root,
		KeepLastComplete: req.KeepLastComplete,
		DryRun:           req.DryRun,
		DeleteSnapshots:  []int64{},
		KeptSnapshots:    []KeptSnapshot{},
	}
	var completeIDs []int64
	for _, sn := range snaps {
		if sn.Status == store.StatusComplete {
			completeIDs = append(completeIDs, sn.ID)
			continue
		}
		rep.KeptSnapshots = append(rep.KeptSnapshots, KeptSnapshot{
			ID: sn.ID, Status: sn.Status,
			Reason: "only complete snapshots are eligible for deletion",
		})
	}
	keepFrom := len(completeIDs) - req.KeepLastComplete
	if keepFrom < 0 {
		keepFrom = 0
	}
	for _, id := range completeIDs[keepFrom:] {
		rep.KeptSnapshots = append(rep.KeptSnapshots, KeptSnapshot{
			ID: id, Status: store.StatusComplete, Reason: "within keep_last_complete window",
		})
	}
	rep.DeleteSnapshots = append(rep.DeleteSnapshots, completeIDs[:keepFrom]...)
	sort.Slice(rep.KeptSnapshots, func(i, j int) bool {
		return rep.KeptSnapshots[i].ID < rep.KeptSnapshots[j].ID
	})

	var reclaim []store.ReclaimedChunk
	if req.DryRun {
		reclaim, err = s.db.ReclaimableChunks(rep.DeleteSnapshots)
		if err != nil {
			return nil, err
		}
		return rep.withReclaim(reclaim), nil
	}

	reclaim, err = s.db.DeleteSnapshots(rep.DeleteSnapshots)
	if err != nil {
		return nil, err
	}
	rep.Applied = true
	for _, c := range reclaim {
		if err := s.cs.Remove(c.SHA256); err != nil {
			rep.Warnings = append(rep.Warnings,
				fmt.Sprintf("chunk %s left on disk: %v", c.SHA256, err))
		}
	}
	return rep.withReclaim(reclaim), nil
}

func (rep *RetentionReport) withReclaim(reclaim []store.ReclaimedChunk) *RetentionReport {
	for _, c := range reclaim {
		rep.ReclaimChunks++
		rep.ReclaimBytes += c.Size
	}
	return rep
}
