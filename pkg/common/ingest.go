package common

import (
	"os"
	"path/filepath"

	"github.com/linxGnu/grocksdb"
)

func Ingest(sstPath, dbPath string) error {
	var files []string
	entries, err := os.ReadDir(sstPath)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		files = append(files, filepath.Join(sstPath, entry.Name()))
	}
	e, err := NewRocksDbEngine(dbPath)
	if err != nil {
		return err
	}
	e.readonly = false
	e.dbOps.SetCreateIfMissing(true)
	if err := e.Open(); err != nil {
		return err
	}
	return e.db.IngestExternalFile(files, grocksdb.NewDefaultIngestExternalFileOptions())

}
