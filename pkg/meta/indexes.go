package meta

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/harrischu/nebula-dump/pkg"
	"github.com/harrischu/nebula-dump/pkg/common"
	"github.com/vesoft-inc/nebula-go/v3/nebula/meta"
)

// indexParser
// key: __indexes__ + space id + index id
// value:  CompactSerializer of index
type indexParser struct {
	opts   *pkg.Option
	key    string
	engine *common.Engine
}

func (p *indexParser) New(engine *common.Engine, opts *pkg.Option) pkg.Parser {
	return &indexParser{opts, "__indexes__", engine}
}

func (p *indexParser) Parse(kv *common.KV) (*common.KVString, error) {
	var (
		kvstring = &common.KVString{}
		spaceID  int32
		indexID  int32
	)
	s := []byte(p.key)
	l := len(s)
	if !bytes.Equal(kv.Key[:len(s)], s) {
		return nil, fmt.Errorf("cannot parse key")
	}
	m := l + common.Sizeof(spaceID)
	n := m + common.Sizeof(indexID)
	space, index := kv.Key[l:m], kv.Key[m:n]
	if err := common.ConvertBytesToInt(&spaceID, &space, common.ByteOrder); err != nil {
		return nil, err
	}
	if err := common.ConvertBytesToInt(&indexID, &index, common.ByteOrder); err != nil {
		return nil, err
	}

	kvstring.Key = fmt.Sprintf("space:%d, index:%d", spaceID, indexID)

	indexItem, err := parseIndex(kv.Value)
	if err != nil {
		return nil, err
	}

	names := make([]string, 0)
	for _, f := range indexItem.Fields {
		names = append(names, fmt.Sprintf("%s", string(f.Name)))
	}

	kvstring.Value = fmt.Sprintf(
		"name:%s, fields:%v",
		indexItem.IndexName,
		strings.Join(names, ","),
	)
	return kvstring, nil
}

func (p *indexParser) Prefix() ([]*common.KV, error) {
	s := []byte(p.key)
	var (
		spaceID []byte
		IndexID []byte
	)
	if p.opts.SpaceID != -1 {
		if err := common.ConvertIntToBytes(&p.opts.SpaceID, &spaceID, common.ByteOrder); err != nil {
			return nil, err
		}
		s = append(s, spaceID...)

		if p.opts.IndexID != -1 {
			if err := common.ConvertIntToBytes(&p.opts.IndexID, &IndexID, common.ByteOrder); err != nil {
				return nil, err
			}
			s = append(s, IndexID...)
		}
	}
	return p.engine.Prefix(s, p.opts.Limit)
}

func parseIndex(v []byte) (*meta.IndexItem, error) {
	m := meta.NewIndexItem()
	err := common.CompactDeserializer(m, &v)
	if err != nil {
		return nil, err
	}
	return m, nil
}
