package main

import (
	"flag"
	"log"
	"net/http"
	"path/filepath"

	"backupd/internal/backup"
	"backupd/internal/chunkstore"
	"backupd/internal/server"
	"backupd/internal/store"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8471", "listen address")
	dataDir := flag.String("data", "./backup-data", "data directory (backup.db + chunks/)")
	fault := flag.Bool("enable-fault-injection", false,
		"accept fault_after_chunks in snapshot requests (demo/testing only)")
	flag.Parse()

	db, err := store.Open(filepath.Join(*dataDir, "backup.db"))
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer db.Close()

	cs, err := chunkstore.Open(filepath.Join(*dataDir, "chunks"))
	if err != nil {
		log.Fatalf("open chunk store: %v", err)
	}

	svc := backup.NewService(db, cs, *fault)
	srv := server.New(svc, db)

	log.Printf("backupd listening on %s (data=%s fault-injection=%v)", *addr, *dataDir, *fault)
	log.Fatal(http.ListenAndServe(*addr, srv.Handler()))
}
