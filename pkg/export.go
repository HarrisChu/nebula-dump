package pkg

import (
	"bytes"
	"fmt"

	gorocksdb "github.com/linxGnu/grocksdb"
)

type (
	Exporter interface {
		Export(out string, parts []int) error
	}
)

var _ Exporter = &RocksdbDump{}

func NewExporter() Exporter{
	return &RocksdbDump{}
}

func (r *RocksdbDump) Export(out string, parts []int) error {
	for part := range parts {
		go func(part int) {
			err := r.exportPart(out, part)
			if err != nil {
				panic(err)
			}
		}(part)
	}
	return nil
}

func (r *RocksdbDump) exportPart(out string, part int) error {
	batchSize, index := 100, 0
	for kType := range []int{kTag, kEdge, kVertex, kIndex} {

		writeDB, err := gorocksdb.OpenDb(gorocksdb.NewDefaultOptions(), fmt.Sprintf("%s/%d/%d.sst", out, part, kType))
		defer writeDB.Close()
		if err != nil {
			return err
		}
		start, err := GetPart(part, kType)
		if err != nil {
			return err
		}
		end, err := GetPart(part+1, kType)
		if err != nil {
			return err
		}
		iter := r.db.NewIterator(gorocksdb.NewDefaultReadOptions())
		defer iter.Close()
		iter.Seek(start)
		writeBatch := gorocksdb.NewWriteBatch()
		for ; iter.Valid(); iter.Next() {
			if bytes.Compare(iter.Key().Data(), end) >= 0 {
				if writeBatch.Count() != 0 {
					err := r.write(writeDB, writeBatch)
					if err != nil {
						return err
					}
				}
				break
			}
			index++
			writeBatch.Put(iter.Key().Data(), iter.Value().Data())
			if writeBatch.Count() == batchSize {
				err := r.write(writeDB, writeBatch)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (r *RocksdbDump) write(db *gorocksdb.DB, batch *gorocksdb.WriteBatch) error {
	ops := gorocksdb.NewDefaultWriteOptions()
	return db.Write(ops, batch)
}
