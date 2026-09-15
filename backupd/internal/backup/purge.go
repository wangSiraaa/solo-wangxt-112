package backup

import (
	"errors"
	"fmt"
	"time"

	"backupd/internal/store"
)

// PurgeBatchDetail is the query view of one batch: plan, items, audit trail.
type PurgeBatchDetail struct {
	Batch store.PurgeBatch        `json:"batch"`
	Items []store.PurgeItem       `json:"items"`
	Audit []store.PurgeAuditEntry `json:"audit"`
}

// GetPurgeBatch returns the batch with its items and audit trail.
func (s *Service) GetPurgeBatch(purgeID string) (*PurgeBatchDetail, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.purgeBatchDetail(purgeID)
}

func (s *Service) purgeBatchDetail(purgeID string) (*PurgeBatchDetail, error) {
	batch, err := s.db.GetPurgeBatch(purgeID)
	if err != nil {
		return nil, err
	}
	if batch == nil {
		return nil, nil
	}
	items, err := s.db.PurgeItems(batch.ID)
	if err != nil {
		return nil, err
	}
	audit, err := s.db.PurgeAudit(batch.ID)
	if err != nil {
		return nil, err
	}
	return &PurgeBatchDetail{Batch: *batch, Items: items, Audit: audit}, nil
}

// ListPurgeBatches returns all batches, oldest first.
func (s *Service) ListPurgeBatches() ([]store.PurgeBatch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.ListPurgeBatches()
}

type UndoPurgeRequest struct {
	// SnapshotID undoes a single snapshot; All undoes the whole batch.
	SnapshotID int64 `json:"snapshot_id,omitempty"`
	All        bool  `json:"all,omitempty"`
}

type UndoPurgeReport struct {
	PurgeID         string  `json:"purge_id"`
	BatchStatus     string  `json:"batch_status"`
	UndoneSnapshots []int64 `json:"undone_snapshots"`
}

// UndoPurge returns pending snapshots of a batch to complete. Their
// file/chunk manifests were never touched while pending, so an undone
// snapshot is immediately restorable.
func (s *Service) UndoPurge(purgeID string, req UndoPurgeRequest) (*UndoPurgeReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	batch, err := s.db.GetPurgeBatch(purgeID)
	if err != nil {
		return nil, err
	}
	if batch == nil {
		return nil, fmt.Errorf("purge batch %q not found", purgeID)
	}
	if batch.Status != store.PurgeOpen {
		return nil, fmt.Errorf("purge batch %q is %s; cannot undo", purgeID, batch.Status)
	}
	rep := &UndoPurgeReport{PurgeID: purgeID, UndoneSnapshots: []int64{}}

	if req.All {
		ids, err := s.db.UndoPurgeBatch(batch.ID)
		if err != nil {
			return nil, err
		}
		rep.UndoneSnapshots = ids
		rep.BatchStatus = store.PurgeCancelled
		return rep, nil
	}

	if req.SnapshotID == 0 {
		return nil, errors.New("snapshot_id or all is required")
	}
	items, err := s.db.PurgeItems(batch.ID)
	if err != nil {
		return nil, err
	}
	found := false
	for _, it := range items {
		if it.SnapshotID == req.SnapshotID {
			found = true
			if it.State != store.PurgeItemPending {
				return nil, fmt.Errorf("snapshot %d is %s in batch %q", req.SnapshotID, it.State, purgeID)
			}
		}
	}
	if !found {
		return nil, fmt.Errorf("snapshot %d not in purge batch %q", req.SnapshotID, purgeID)
	}
	if err := s.db.UndoPurgeItem(batch.ID, req.SnapshotID); err != nil {
		return nil, err
	}
	rep.UndoneSnapshots = []int64{req.SnapshotID}
	rep.BatchStatus = store.PurgeOpen
	return rep, nil
}

type ExecutePurgeRequest struct {
	// Force executes before the undo window expires (manual override).
	Force bool `json:"force,omitempty"`
}

type SkippedSnapshot struct {
	SnapshotID int64  `json:"snapshot_id"`
	Reason     string `json:"reason"`
}

type ExecutePurgeReport struct {
	PurgeID          string            `json:"purge_id"`
	Status           string            `json:"status"` // executed | open (open when items were skipped)
	PurgedSnapshots  []int64           `json:"purged_snapshots"`
	SkippedSnapshots []SkippedSnapshot `json:"skipped_snapshots,omitempty"`
	ReclaimChunks    int               `json:"reclaim_chunks"`
	ReclaimBytes     int64             `json:"reclaim_bytes"`
	Warnings         []string          `json:"warnings,omitempty"`
	Replayed         bool              `json:"replayed"`
}

// ExecutePurge performs the actual deletion of an open batch once its undo
// window has expired. Safety rules, re-checked at execution time:
//   - only items still pending whose snapshot is still pending_purge are
//     purged; undone, protected (status changed) or repair-in-progress
//     snapshots are skipped and reported;
//   - the manifest deletion and the zero-reference chunk computation (over
//     every surviving snapshot's file_chunks) commit in one transaction;
//   - chunk files are removed only after that commit; a removal failure is
//     reported in Warnings and leaves a harmless orphan, never a manifest
//     referencing deleted data.
//
// If any item is skipped the batch stays open and can be executed again.
func (s *Service) ExecutePurge(purgeID string, req ExecutePurgeRequest) (*ExecutePurgeReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	batch, err := s.db.GetPurgeBatch(purgeID)
	if err != nil {
		return nil, err
	}
	if batch == nil {
		return nil, fmt.Errorf("purge batch %q not found", purgeID)
	}
	items, err := s.db.PurgeItems(batch.ID)
	if err != nil {
		return nil, err
	}
	rep := &ExecutePurgeReport{
		PurgeID:          purgeID,
		Status:           batch.Status,
		PurgedSnapshots:  []int64{},
		SkippedSnapshots: []SkippedSnapshot{},
	}

	if batch.Status == store.PurgeExecuted {
		rep.Replayed = true // stable replay: nothing happens twice
		for _, it := range items {
			if it.State == store.PurgeItemPurged {
				rep.PurgedSnapshots = append(rep.PurgedSnapshots, it.SnapshotID)
			}
		}
		return rep, nil
	}
	if batch.Status == store.PurgeCancelled {
		return nil, fmt.Errorf("purge batch %q is cancelled", purgeID)
	}

	due, err := time.Parse(time.RFC3339Nano, batch.ExecuteAfter)
	if err != nil {
		return nil, fmt.Errorf("batch %q has unparsable execute_after %q", purgeID, batch.ExecuteAfter)
	}
	if !req.Force && time.Now().UTC().Before(due) {
		return nil, fmt.Errorf("undo window of batch %q not expired; executable after %s", purgeID, batch.ExecuteAfter)
	}

	var ids []int64
	for _, it := range items {
		if it.State != store.PurgeItemPending {
			continue // undone or already purged
		}
		snap, err := s.db.GetSnapshot(it.SnapshotID)
		if err != nil {
			return nil, err
		}
		if snap == nil {
			ids = append(ids, it.SnapshotID) // gone already; clean residual rows
			continue
		}
		if snap.Status != store.StatusPendingPurge {
			rep.SkippedSnapshots = append(rep.SkippedSnapshots, SkippedSnapshot{
				SnapshotID: it.SnapshotID,
				Reason:     fmt.Sprintf("snapshot status is %s, not pending_purge (protected)", snap.Status),
			})
			continue
		}
		if busy, err := s.db.SnapshotHasRunningRepair(it.SnapshotID); err != nil {
			return nil, err
		} else if busy {
			rep.SkippedSnapshots = append(rep.SkippedSnapshots, SkippedSnapshot{
				SnapshotID: it.SnapshotID, Reason: "repair in progress",
			})
			continue
		}
		ids = append(ids, it.SnapshotID)
	}

	closeBatch := len(rep.SkippedSnapshots) == 0
	orphans, err := s.db.ExecutePurgeBatch(batch.ID, ids, closeBatch)
	if err != nil {
		return nil, err
	}
	rep.PurgedSnapshots = append(rep.PurgedSnapshots, ids...)
	if closeBatch {
		rep.Status = store.PurgeExecuted
	}
	for _, c := range orphans {
		rep.ReclaimChunks++
		rep.ReclaimBytes += c.Size
		if err := s.cs.Remove(c.SHA256); err != nil {
			rep.Warnings = append(rep.Warnings,
				fmt.Sprintf("chunk %s left on disk: %v", c.SHA256, err))
		}
	}
	return rep, nil
}
