package pkg

import (
	"fmt"

	"github.com/harrischu/nebula-dump/pkg/common"
	"github.com/harrischu/nebula-dump/pkg/meta"
)

type (
	Parser interface {
		ParseAll() (*[]common.KVString, error)
		Prefix() (*[]common.KV, error)
	}

	Counter interface {
		Count() (int, error)
	}

	MyDumper interface {
		Parser
		Counter
	}

	Option struct {
		SpaceID int
		PartID  int
		TagID   int
		EdgeID  int
	}

	MetaDumper struct {
		dataPath string
		keyType  string
		option   *Option
		engine   *common.Engine
	}

	StorageParser struct {
		dataPath string
		keyType  string
		option   *Option
		engine   *common.Engine
	}
)

func NewMetaParser(path, keyType string, option *Option) (MyDumper, error) {
	m := &MetaDumper{}
	if _, ok := meta.MetaKeyTypeMap[meta.MetaKeyType(keyType)]; !ok {
		return nil, fmt.Errorf("cannot find the key type, keyType is %s", keyType)
	}
	e, err := common.NewRocksDbEngine(path)
	if err != nil {
		return nil, err
	}
	m.engine = e
	m.keyType = keyType

	return m, nil
}

func (m *MetaDumper) ParseAll() (*[]common.KVString, error) {
	if err := m.engine.Open(); err != nil {
		return nil, err
	}
	return nil, nil
}

func (m *MetaDumper) Prefix() (*[]common.KV, error) {
	return nil, nil
}
func (m *MetaDumper) Count() (int, error) {
	return 0, nil
}

func (m *MetaDumper) getPrefix() ([]byte, error) {
	var key []byte
	switch meta.MetaKeyTypeMap[meta.MetaKeyType(m.keyType)] {

	}

	return key, nil

}
