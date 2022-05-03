package pkg

import (
	"fmt"

	"github.com/harrischu/nebula-dump/pkg/common"
)

type (
	Parser interface {
		Parse(*common.KV) (*common.KVString, error)
		New(engine *common.Engine, opt *Option) Parser
		Prefix() ([]*common.KV, error)
	}

	Dumper interface {
		ParseAll() ([]*common.KVString, error)
		Prefix() ([]*common.KV, error)
	}

	Option struct {
		SpaceID    int32
		PartID     int32
		TagID      int32
		EdgeID     int32
		IndexID    int32
		Limit      int
		MetaAddres string
		VID        string
		Src        string
		Dst        string
	}

	MetaDumper struct {
		dataPath string
		keyType  string
		parser   Parser
		option   *Option
		engine   *common.Engine
	}

	StorageDumper struct {
		dataPath    string
		keyType     string
		parser      Parser
		option      *Option
		engine      *common.Engine
		metaAddress string
	}
)

func NewMetaParser(path string, keyType MetaKeyType, option *Option) (Dumper, error) {
	m := &MetaDumper{}
	p, ok := MetaKeyTypeMap[keyType]
	if !ok {
		return nil, fmt.Errorf("cannot find the key type, keyType is %s", keyType)
	}
	e, err := common.NewRocksDbEngine(path)
	if err != nil {
		return nil, err
	}
	p = p.New(e, option)
	m.engine = e
	m.keyType = string(keyType)
	m.parser = p
	m.option = option

	return m, nil
}

func (m *MetaDumper) ParseAll() ([]*common.KVString, error) {
	r := make([]*common.KVString, 0)
	kvs, err := m.Prefix()
	if err != nil {
		return nil, err
	}
	for _, kv := range kvs {
		kvstring, err := m.parser.Parse(kv)
		if err != nil {
			return nil, fmt.Errorf("key is %v, value is %v, err: %v", kv.Key, kv.Value, err)
		}
		r = append(r, kvstring)

	}
	return r, nil
}

func (m *MetaDumper) Prefix() ([]*common.KV, error) {
	return m.parser.Prefix()
}

func NewStorageParser(path string, keyType StorageKeyType, option *Option) (Dumper, error) {
	s := &StorageDumper{}
	p, ok := StorageKeyTypeMap[keyType]
	if !ok {
		return nil, fmt.Errorf("cannot find the key type, keyType is %s", keyType)
	}
	e, err := common.NewRocksDbEngine(path)
	if err != nil {
		return nil, err
	}
	p = p.New(e, option)
	s.engine = e
	s.keyType = string(keyType)
	s.parser = p
	s.option = option

	return s, nil
}

func (m *StorageDumper) ParseAll() ([]*common.KVString, error) {
	r := make([]*common.KVString, 0)
	kvs, err := m.Prefix()
	if err != nil {
		return nil, err
	}
	for _, kv := range kvs {
		kvstring, err := m.parser.Parse(kv)
		if err != nil {
			return nil, fmt.Errorf("key is %v, value is %v, err: %v", kv.Key, kv.Value, err)
		}
		r = append(r, kvstring)

	}
	return r, nil
}

func (m *StorageDumper) Prefix() ([]*common.KV, error) {
	return m.parser.Prefix()

}
