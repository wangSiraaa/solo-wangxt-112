package backup

import (
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"time"

	"backupd/internal/store"
)

type RetentionRequest struct {
	SourceRoot string `json:"source_root"`
	// KeepLastComplete keeps the N most recent complete snapshots of
	// source_root; older complete snapshots become purge candidates.
	// Snapshots in any other state (failed, incomplete, running,
	// pending_purge) are never candidates.
	KeepLastComplete int `json:"keep_last_complete"`
	// DryRun computes the plan without changing anything.
	DryRun bool `json:"dry_run,omitempty"`
	// PurgeID is the idempotency key of the purge batch created by an
	// apply. Required unless DryRun. Repeating the same purge_id replays
	// the stored batch without side effects.
	PurgeID string `json:"purge_id,omitempty"`
	// WindowSeconds is the undo window: the batch becomes executable only
	// after this many seconds. 0 means immediately executable.
	WindowSeconds int64 `json:"window_seconds,omitempty"`
	// Operator is recorded in the batch audit trail.
	Operator string `json:"operator,omitempty"`
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
	Replayed         bool           `json:"replayed,omitempty"`
	PurgeID          string         `json:"purge_id,omitempty"`
	ExecuteAfter     string         `json:"execute_after,omitempty"`
	PendingSnapshots []int64        `json:"pending_snapshots"` // dry-run: would pend
	KeptSnapshots    []KeptSnapshot `json:"kept_snapshots"`
	ReclaimChunks    int            `json:"reclaim_chunks"` // plan-time estimate
	ReclaimBytes     int64          `json:"reclaim_bytes"`
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

// Retention plans (DryRun) or schedules (apply) the keep-last-N-complete
// policy for one source root. Apply never deletes directly: it creates a
// persistent purge batch, flips the expired complete snapshots to
// pending_purge and leaves actual deletion to the explicit execute call
// after the undo window expires.
func (s *Service) Retention(req RetentionRequest) (*RetentionReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if req.SourceRoot == "" {
		return nil, errors.New("source_root is required")
	}
	if req.KeepLastComplete < 0 {
		return nil, errors.New("keep_last_complete must be >= 0")
	}
	if req.WindowSeconds < 0 {
		return nil, errors.New("window_seconds must be >= 0")
	}
	if !req.DryRun && req.PurgeID == "" {
		return nil, errors.New("purge_id is required when applying (dry_run is false)")
	}
	root, err := normalizeRoot(req.SourceRoot)
	if err != nil {
		return nil, fmt.Errorf("bad source_root: %w", err)
	}

	rep := &RetentionReport{
		SourceRoot:       root,
		KeepLastComplete: req.KeepLastComplete,
		DryRun:           req.DryRun,
		PurgeID:          req.PurgeID,
		PendingSnapshots: []int64{},
		KeptSnapshots:    []KeptSnapshot{},
	}

	// Idempotent replay of an already-created batch.
	if !req.DryRun && req.PurgeID != "" {
		existing, err := s.db.GetPurgeBatch(req.PurgeID)
		if err != nil {
			return nil, err
		}
		if existing != nil {
			if existing.SourceRoot != root || existing.KeepLastComplete != req.KeepLastComplete {
				return nil, fmt.Errorf("purge_id %q already used with different policy parameters", req.PurgeID)
			}
			items, err := s.db.PurgeItems(existing.ID)
			if err != nil {
				return nil, err
			}
			rep.Replayed = true
			rep.ExecuteAfter = existing.ExecuteAfter
			for _, it := range items {
				rep.PendingSnapshots = append(rep.PendingSnapshots, it.SnapshotID)
			}
			return rep, nil
		}
	}

	snaps, err := s.db.SnapshotsForRoot(root)
	if err != nil {
		return nil, err
	}
	var completeIDs []int64
	for _, sn := range snaps {
		switch sn.Status {
		case store.StatusComplete:
			completeIDs = append(completeIDs, sn.ID)
		case store.StatusPendingPurge:
			rep.KeptSnapshots = append(rep.KeptSnapshots, KeptSnapshot{
				ID: sn.ID, Status: sn.Status, Reason: "already pending purge in an open batch",
			})
		default:
			rep.KeptSnapshots = append(rep.KeptSnapshots, KeptSnapshot{
				ID: sn.ID, Status: sn.Status,
				Reason: "only complete snapshots are eligible for deletion",
			})
		}
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
	pending := completeIDs[:keepFrom]
	rep.PendingSnapshots = append(rep.PendingSnapshots, pending...)
	sort.Slice(rep.KeptSnapshots, func(i, j int) bool {
		return rep.KeptSnapshots[i].ID < rep.KeptSnapshots[j].ID
	})

	// Plan-time reclaim estimate: chunks referenced by nobody outside the
	// pending set. The authoritative computation happens at execute time.
	reclaim, err := s.db.ReclaimableChunks(pending)
	if err != nil {
		return nil, err
	}
	for _, c := range reclaim {
		rep.ReclaimChunks++
		rep.ReclaimBytes += c.Size
	}

	if req.DryRun || len(pending) == 0 {
		return rep, nil // dry-run changes nothing; empty plan needs no batch
	}

	executeAfter := time.Now().UTC().Add(time.Duration(req.WindowSeconds) * time.Second)
	if _, err := s.db.CreatePurgeBatch(store.PurgeBatch{
		PurgeID:          req.PurgeID,
		SourceRoot:       root,
		KeepLastComplete: req.KeepLastComplete,
		ExecuteAfter:     executeAfter.Format(time.RFC3339Nano),
		Operator:         req.Operator,
	}, pending); err != nil {
		return nil, err
	}
	rep.Applied = true
	rep.ExecuteAfter = executeAfter.Format(time.RFC3339Nano)
	return rep, nil
}
