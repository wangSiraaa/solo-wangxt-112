// Package server exposes the backup service over a small JSON/HTTP API.
package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"backupd/internal/backup"
	"backupd/internal/store"
)

type Server struct {
	svc *backup.Service
	db  *store.DB
	mux *http.ServeMux
}

func New(svc *backup.Service, db *store.DB) *Server {
	s := &Server{svc: svc, db: db}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/healthz", s.healthz)
	mux.HandleFunc("POST /v1/snapshots", s.createSnapshot)
	mux.HandleFunc("GET /v1/snapshots", s.listSnapshots)
	mux.HandleFunc("GET /v1/snapshots/{id}", s.getSnapshot)
	mux.HandleFunc("GET /v1/snapshots/{id}/files", s.listFiles)
	mux.HandleFunc("GET /v1/snapshots/{id}/missing", s.listMissing)
	mux.HandleFunc("POST /v1/snapshots/{id}/repair", s.repairSnapshot)
	mux.HandleFunc("GET /v1/snapshots/{id}/repairs", s.listRepairs)
	mux.HandleFunc("GET /v1/snapshots/{id}/repairs/{repair_id}", s.getRepair)
	mux.HandleFunc("POST /v1/restore", s.restore)
	mux.HandleFunc("POST /v1/retention", s.retention)
	mux.HandleFunc("GET /v1/purges", s.listPurges)
	mux.HandleFunc("GET /v1/purges/{purge_id}", s.getPurge)
	mux.HandleFunc("POST /v1/purges/{purge_id}/undo", s.undoPurge)
	mux.HandleFunc("POST /v1/purges/{purge_id}/execute", s.executePurge)
	s.mux = mux
	return s
}

func (s *Server) Handler() http.Handler { return s.mux }

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeErr(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) createSnapshot(w http.ResponseWriter, r *http.Request) {
	var req backup.SnapshotRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if req.SourceRoot == "" {
		writeErr(w, http.StatusBadRequest, errors.New("source_root is required"))
		return
	}
	snap, err := s.svc.CreateSnapshot(req)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	code := http.StatusCreated
	if snap.Status != store.StatusComplete {
		code = http.StatusOK // snapshot recorded, but flagged incomplete/failed
	}
	writeJSON(w, code, snap)
}

func (s *Server) listSnapshots(w http.ResponseWriter, _ *http.Request) {
	snaps, err := s.db.ListSnapshots()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, snaps)
}

func pathID(r *http.Request) (int64, error) {
	return strconv.ParseInt(r.PathValue("id"), 10, 64)
}

func (s *Server) getSnapshot(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	snap, err := s.db.GetSnapshot(id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if snap == nil {
		writeErr(w, http.StatusNotFound, errors.New("snapshot not found"))
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *Server) listFiles(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	files, err := s.db.ListFiles(id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, files)
}

func (s *Server) listMissing(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	missing, err := s.db.MissingChunks(id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, missing)
}

func (s *Server) restore(w http.ResponseWriter, r *http.Request) {
	var req backup.RestoreRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if req.TargetDir == "" {
		writeErr(w, http.StatusBadRequest, errors.New("target_dir is required"))
		return
	}
	rep, err := s.svc.Restore(req)
	if err != nil {
		writeErr(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

func (s *Server) repairSnapshot(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	var req backup.RepairRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	rep, err := s.svc.Repair(id, req)
	if err != nil {
		if errors.Is(err, backup.ErrSimulatedCrash) {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
		writeErr(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

func (s *Server) listRepairs(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	attempts, err := s.db.ListRepairAttempts(id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, attempts)
}

func (s *Server) getRepair(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	attempt, err := s.db.GetRepairAttempt(id, r.PathValue("repair_id"))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if attempt == nil {
		writeErr(w, http.StatusNotFound, errors.New("repair attempt not found"))
		return
	}
	writeJSON(w, http.StatusOK, attempt)
}

func (s *Server) listPurges(w http.ResponseWriter, _ *http.Request) {
	batches, err := s.svc.ListPurgeBatches()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, batches)
}

func (s *Server) getPurge(w http.ResponseWriter, r *http.Request) {
	detail, err := s.svc.GetPurgeBatch(r.PathValue("purge_id"))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	if detail == nil {
		writeErr(w, http.StatusNotFound, errors.New("purge batch not found"))
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

func (s *Server) undoPurge(w http.ResponseWriter, r *http.Request) {
	var req backup.UndoPurgeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	rep, err := s.svc.UndoPurge(r.PathValue("purge_id"), req)
	if err != nil {
		writeErr(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

func (s *Server) executePurge(w http.ResponseWriter, r *http.Request) {
	var req backup.ExecutePurgeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	rep, err := s.svc.ExecutePurge(r.PathValue("purge_id"), req)
	if err != nil {
		writeErr(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

func (s *Server) retention(w http.ResponseWriter, r *http.Request) {
	var req backup.RetentionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	rep, err := s.svc.Retention(req)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, rep)
}
