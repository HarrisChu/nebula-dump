package pkg

import (
	"bytes"

	gorocksdb "github.com/linxGnu/grocksdb"
)

type (
	KV struct {
		Key   []byte
		Value []byte
	}

	KVString struct {
		Key   string
		Value string
	}

	engine struct {
		db       *gorocksdb.DB
		readonly bool
		dbOps    *gorocksdb.Options
		readOps  *gorocksdb.ReadOptions
		writeOps *gorocksdb.WriteOptions
		path     string
	}
)

func NewKV(key []byte, value []byte) *KV {
	kv := &KV{}
	kv.Key = make([]byte, len(key))
	kv.Value = make([]byte, len(value))
	copy(kv.Key, key)
	copy(kv.Value, value)
	return kv
}

func newRocksDb(path string) (*engine, error) {
	e := &engine{}
	e.path = path
	e.readonly = true
	return e, nil
}

func (e *engine) open() error {
	var (
		db  *gorocksdb.DB
		err error
	)
	if e.db != nil {
		return nil
	}
	if e.readonly {
		db, err = gorocksdb.OpenDbForReadOnly(e.dbOps, e.path, false)
	} else {
		db, err = gorocksdb.OpenDb(e.dbOps, e.path)
	}
	if err != nil {
		return err
	}
	e.db = db
	return nil
}

func (e *engine) prefix(p []byte) ([]*KV, error) {
	if e.db == nil {
		err := e.open()
		if err != nil {
			return nil, err
		}
	}
	kvs := make([]*KV, 0)
	iter := e.db.NewIterator(e.readOps)
	defer iter.Close()
	iter.Seek(p)
	l := len(p)
	for ; iter.Valid(); iter.Next() {
		if bytes.Compare(p, iter.Key().Data()[:l]) != 0 {
			break
		}
		kv := NewKV(iter.Key().Data(), iter.Value().Data())
		kvs = append(kvs, kv)
	}
	return kvs, nil
}
