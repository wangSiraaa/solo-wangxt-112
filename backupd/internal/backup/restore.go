package backup

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"backupd/internal/store"
)

type RestoreRequest struct {
	SnapshotID int64  `json:"snapshot_id"`
	TargetDir  string `json:"target_dir"`
	// AllowIncomplete permits restoring snapshots that are not "complete"
	// (unstable files, failed snapshots with missing chunks). Affected
	// files are flagged per-entry in the report.
	AllowIncomplete bool `json:"allow_incomplete,omitempty"`
}

type RestoreEntry struct {
	Path   string `json:"path"`
	Type   string `json:"type"`
	Status string `json:"status"` // restored | skipped_exists | skipped_unsafe_link | failed
	Detail string `json:"detail,omitempty"`
}

type RestoreReport struct {
	SnapshotID int64          `json:"snapshot_id"`
	TargetDir  string         `json:"target_dir"`
	Status     string         `json:"status"` // ok | completed_with_errors | refused
	Entries    []RestoreEntry `json:"entries"`
	Restored   int            `json:"restored"`
	Skipped    int            `json:"skipped"`
	Failed     int            `json:"failed"`
}

// Restore materializes a snapshot into targetDir. It never overwrites
// existing files, never follows symlinks (its own included), and verifies
// the sha256 and length of every restored regular file against the manifest.
func (s *Service) Restore(req RestoreRequest) (*RestoreReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap, err := s.db.GetSnapshot(req.SnapshotID)
	if err != nil {
		return nil, err
	}
	if snap == nil {
		return nil, fmt.Errorf("snapshot %d not found", req.SnapshotID)
	}
	if snap.Status == store.StatusRunning {
		return nil, fmt.Errorf("snapshot %d is still running", snap.ID)
	}
	if snap.Status != store.StatusComplete && !req.AllowIncomplete {
		return nil, fmt.Errorf("snapshot %d is %s; pass allow_incomplete to restore anyway", snap.ID, snap.Status)
	}

	target, err := filepath.Abs(req.TargetDir)
	if err != nil {
		return nil, fmt.Errorf("bad target_dir: %w", err)
	}
	if err := ensureRealDir(target); err != nil {
		return nil, err
	}

	files, err := s.db.ListFiles(snap.ID)
	if err != nil {
		return nil, err
	}

	rep := &RestoreReport{SnapshotID: snap.ID, TargetDir: target, Status: "ok"}
	for _, f := range files {
		entry := s.restoreOne(target, f)
		rep.Entries = append(rep.Entries, entry)
		switch entry.Status {
		case "restored":
			rep.Restored++
		case "failed":
			rep.Failed++
		default:
			rep.Skipped++
		}
	}
	if rep.Failed > 0 {
		rep.Status = "completed_with_errors"
	}
	return rep, nil
}

func (s *Service) restoreOne(targetRoot string, f store.FileEntry) RestoreEntry {
	entry := RestoreEntry{Path: f.Path, Type: f.Type}

	// Lexical containment: the manifest path itself must stay under target.
	cleanRel := filepath.Clean(filepath.FromSlash(f.Path))
	if cleanRel == ".." || strings.HasPrefix(cleanRel, ".."+string(os.PathSeparator)) || filepath.IsAbs(cleanRel) {
		entry.Status = "failed"
		entry.Detail = "path escapes target directory"
		return entry
	}
	dest := filepath.Join(targetRoot, cleanRel)

	// Ancestors we created (or that pre-exist) must be real directories,
	// never symlinks — otherwise a restored symlink could redirect later
	// writes outside the target root.
	if err := checkAncestors(targetRoot, dest); err != nil {
		entry.Status = "failed"
		entry.Detail = err.Error()
		return entry
	}

	switch f.Type {
	case "dir":
		if _, err := os.Lstat(dest); err == nil {
			entry.Status = "skipped_exists"
			return entry
		}
		if err := os.MkdirAll(dest, os.FileMode(f.Mode)); err != nil {
			entry.Status = "failed"
			entry.Detail = err.Error()
			return entry
		}
		_ = os.Chmod(dest, os.FileMode(f.Mode)) // umask may have narrowed it
		entry.Status = "restored"

	case "symlink":
		if !linkWithinRoot(targetRoot, filepath.Dir(dest), f.LinkTarget) {
			entry.Status = "skipped_unsafe_link"
			entry.Detail = fmt.Sprintf("target %q escapes restore root", f.LinkTarget)
			return entry
		}
		if _, err := os.Lstat(dest); err == nil {
			entry.Status = "skipped_exists"
			return entry
		}
		if err := os.Symlink(f.LinkTarget, dest); err != nil {
			entry.Status = "failed"
			entry.Detail = err.Error()
			return entry
		}
		entry.Status = "restored"

	case "file":
		if f.Status == store.FileUnstable {
			entry.Detail = "source file changed during scan; content is best-effort"
		}
		if _, err := os.Lstat(dest); err == nil {
			entry.Status = "skipped_exists" // never overwrite
			return entry
		}
		if err := s.restoreFile(dest, f); err != nil {
			os.Remove(dest) // do not leave a partial file behind
			entry.Status = "failed"
			entry.Detail = err.Error()
			return entry
		}
		entry.Status = "restored"

	default:
		entry.Status = "skipped_exists"
		entry.Detail = "unsupported entry type " + f.Type
	}
	return entry
}

// restoreFile writes one regular file from its chunks with O_EXCL, then
// verifies the assembled content against the manifest digest and length.
func (s *Service) restoreFile(dest string, f store.FileEntry) error {
	refs, err := s.db.FileChunks(f.ID)
	if err != nil {
		return err
	}
	out, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_EXCL, os.FileMode(f.Mode))
	if err != nil {
		return err
	}
	h := sha256.New()
	var written int64
	for _, r := range refs {
		data, err := s.cs.Get(r.SHA256)
		if err != nil {
			out.Close()
			return fmt.Errorf("chunk %s: %w", r.SHA256, err)
		}
		if int64(len(data)) != r.Size {
			out.Close()
			return fmt.Errorf("chunk %s: length %d, manifest says %d", r.SHA256, len(data), r.Size)
		}
		if _, err := out.Write(data); err != nil {
			out.Close()
			return err
		}
		h.Write(data)
		written += int64(len(data))
	}
	if err := out.Close(); err != nil {
		return err
	}
	if written != f.Size {
		return fmt.Errorf("length mismatch: wrote %d bytes, manifest says %d", written, f.Size)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != f.SHA256 {
		return fmt.Errorf("digest mismatch: got %s, manifest says %s", got, f.SHA256)
	}
	if err := os.Chmod(dest, os.FileMode(f.Mode)); err != nil {
		return err
	}
	mt := time.Unix(0, f.MtimeNs)
	_ = os.Chtimes(dest, mt, mt)
	return nil
}

// ensureRealDir validates the restore root itself before anything is
// created or written. MkdirAll follows existing symlinks, so without this
// check a target_dir that is a symlink to somewhere outside the allowed
// root would silently redirect every restored file there.
func ensureRealDir(dir string) error {
	fi, err := os.Lstat(dir)
	switch {
	case err == nil:
		if fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("target_dir %s is a symlink; refusing to restore through it", dir)
		}
		if !fi.IsDir() {
			return fmt.Errorf("target_dir %s exists and is not a directory", dir)
		}
		return nil
	case os.IsNotExist(err):
		return os.MkdirAll(dir, 0o755)
	default:
		return err
	}
}

// checkAncestors makes sure every directory between root and dest is a real
// directory, not a symlink.
func checkAncestors(root, dest string) error {
	rel, err := filepath.Rel(root, filepath.Dir(dest))
	if err != nil {
		return err
	}
	cur := root
	for _, part := range strings.Split(rel, string(os.PathSeparator)) {
		if part == "." || part == "" {
			continue
		}
		cur = filepath.Join(cur, part)
		fi, err := os.Lstat(cur)
		if err != nil {
			if os.IsNotExist(err) {
				return nil // remaining components will be created as real dirs
			}
			return err
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("refusing to follow symlinked ancestor %s", cur)
		}
		if !fi.IsDir() {
			return fmt.Errorf("ancestor %s is not a directory", cur)
		}
	}
	return nil
}
