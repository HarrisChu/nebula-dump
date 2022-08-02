package common

import (
	"bytes"
	"fmt"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"

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

	Engine struct {
		db       *gorocksdb.DB
		readonly bool
		dbOps    *gorocksdb.Options
		readOps  *gorocksdb.ReadOptions
		writeOps *gorocksdb.WriteOptions
		path     string
	}

	conditionFunc func([]byte) bool
)

func NewKV(key []byte, value []byte) *KV {
	kv := &KV{}
	kv.Key = make([]byte, len(key))
	kv.Value = make([]byte, len(value))
	copy(kv.Key, key)
	copy(kv.Value, value)
	return kv
}

func NewRocksDbEngine(path string) (*Engine, error) {
	e := &Engine{}
	e.dbOps = gorocksdb.NewDefaultOptions()
	e.readOps = gorocksdb.NewDefaultReadOptions()
	e.writeOps = gorocksdb.NewDefaultWriteOptions()
	e.path = path
	e.readonly = true
	return e, nil
}

func (e *Engine) Open() error {
	var (
		db  *gorocksdb.DB
		err error
	)
	if e.db != nil {
		return nil
	}
	if e.path == "" {
		return fmt.Errorf("muse provide a valid rocksdb path.")
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

func (e *Engine) Prefix(p []byte, limit int) ([]*KV, error) {
	return e.PrefixWithCondition(p, limit, nil, nil)
}

func (e *Engine) PrefixWithCondition(p []byte, limit int, keyCondition, valueCondition conditionFunc) ([]*KV, error) {
	Logger.Debugf("prefix with condition, prefix is %v", p)
	if limit < 1 {
		return []*KV{}, nil
	}
	if e.db == nil {
		err := e.Open()
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
		if len(iter.Key().Data()) < len(p) {
			break
		}
		if bytes.Compare(p, iter.Key().Data()[:l]) != 0 {
			break
		}
		if len(kvs) == limit {
			break
		}
		if keyCondition != nil && !keyCondition(iter.Key().Data()) {
			continue
		}
		if valueCondition != nil && !valueCondition(iter.Value().Data()) {
			continue
		}
		kv := NewKV(iter.Key().Data(), iter.Value().Data())
		kvs = append(kvs, kv)
	}
	return kvs, nil

}

func deserialize(pf thrift.ProtocolFactory, data *[]byte, s thrift.Struct) error {
	transport := thrift.NewMemoryBufferWithData(*data)
	protocol := pf.GetProtocol(transport)
	err := s.Read(protocol)
	if err != nil {
		return err
	}
	return nil
}

func serialize(pf thrift.ProtocolFactory, data *[]byte, s thrift.Struct) error {
	transport := thrift.NewMemoryBuffer()
	protocol := pf.GetProtocol(transport)
	err := s.Write(protocol)
	if err != nil {
		return err
	}
	*data = make([]byte, len(transport.Bytes()))
	copy(*data, transport.Bytes())
	return nil
}

func CompactSerializer(s thrift.Struct, data *[]byte) error {
	pf := thrift.NewCompactProtocolFactory()
	return serialize(pf, data, s)
}

func CompactDeserializer(s thrift.Struct, data *[]byte) error {
	pf := thrift.NewCompactProtocolFactory()
	return deserialize(pf, data, s)
}
