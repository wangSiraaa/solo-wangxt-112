// Package chunkstore keeps content-addressed chunks in a separate directory:
// chunks/<sha256[:2]>/<sha256>. Writes go through a temp file + rename so a
// reader never observes a partial chunk.
package chunkstore

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
)

type Store struct {
	dir string
}

func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Store{dir: dir}, nil
}

func (s *Store) Dir() string { return s.dir }

func (s *Store) pathFor(sha string) (string, error) {
	if len(sha) != 64 {
		return "", fmt.Errorf("invalid chunk hash %q", sha)
	}
	return filepath.Join(s.dir, sha[:2], sha), nil
}

func (s *Store) Has(sha string) bool {
	p, err := s.pathFor(sha)
	if err != nil {
		return false
	}
	_, err = os.Stat(p)
	return err == nil
}

// Put writes data under its sha256. The data is hashed and compared against
// the expected name first; a mismatch means the caller computed a bad digest.
func (s *Store) Put(sha string, data []byte) error {
	sum := sha256.Sum256(data)
	if got := hex.EncodeToString(sum[:]); got != sha {
		return fmt.Errorf("chunk digest mismatch: computed %s, expected %s", got, sha)
	}
	p, err := s.pathFor(sha)
	if err != nil {
		return err
	}
	if _, err := os.Stat(p); err == nil {
		return nil // already stored
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(p), ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, p); err != nil {
		os.Remove(tmpName)
		return err
	}
	return nil
}

func (s *Store) Get(sha string) ([]byte, error) {
	p, err := s.pathFor(sha)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("chunk not in store")
		}
		return nil, err
	}
	sum := sha256.Sum256(data)
	if got := hex.EncodeToString(sum[:]); got != sha {
		return nil, fmt.Errorf("chunk %s corrupted on disk (digest %s)", sha, got)
	}
	return data, nil
}

// Verify re-reads the chunk from disk and checks its digest. It returns the
// on-disk size. This is the pre-commit gate for snapshots.
func (s *Store) Verify(sha string) (int64, error) {
	p, err := s.pathFor(sha)
	if err != nil {
		return 0, err
	}
	data, err := os.ReadFile(p)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, fmt.Errorf("chunk not in store")
		}
		return 0, err
	}
	sum := sha256.Sum256(data)
	if got := hex.EncodeToString(sum[:]); got != sha {
		return 0, fmt.Errorf("digest mismatch on disk (got %s)", got)
	}
	return int64(len(data)), nil
}
