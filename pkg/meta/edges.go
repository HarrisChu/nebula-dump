package meta

import (
	"bytes"
	"fmt"
	"math"
	"strings"

	"github.com/harrischu/nebula-dump/pkg"
	"github.com/harrischu/nebula-dump/pkg/common"
)

// edgeParser
// key: __edge__ + space id + tag id + schema version
// value: length of name (4 bit) + name + CompactSerializer of schema
type edgeParser struct {
	opts   *pkg.Option
	key    string
	engine *common.Engine
}

func (p *edgeParser) New(engine *common.Engine, opts *pkg.Option) pkg.Parser {
	return &edgeParser{opts, "__edges__", engine}
}

func (p *edgeParser) Parse(kv *common.KV) (*common.KVString, error) {
	var (
		kvstring   = &common.KVString{}
		spaceID    int32
		EdgeID     int32
		versionNum int64
		lengthNum  int32
	)
	s := []byte(p.key)
	l := len(s)
	if !bytes.Equal(kv.Key[:len(s)], s) {
		return nil, fmt.Errorf("cannot parse key")
	}
	m := l + common.Sizeof(spaceID)
	n := m + common.Sizeof(EdgeID)
	o := n + common.Sizeof(versionNum)
	space, edge, version := kv.Key[l:m], kv.Key[m:n], kv.Key[n:o]
	if err := common.ConvertBytesToInt(&spaceID, &space, common.ByteOrder); err != nil {
		return nil, err
	}
	if err := common.ConvertBytesToInt(&EdgeID, &edge, common.ByteOrder); err != nil {
		return nil, err
	}
	if err := common.ConvertBytesToInt(&versionNum, &version, common.ByteOrder); err != nil {
		return nil, err
	}
	// follow nebula logic
	versionNum = math.MaxInt64 - versionNum
	kvstring.Key = fmt.Sprintf("space:%d, edge:%d, version:%d", spaceID, EdgeID, versionNum)
	length := kv.Value[:4]
	if err := common.ConvertBytesToInt(&lengthNum, &length, common.ByteOrder); err != nil {
		return nil, err
	}
	name := kv.Value[4 : 4+int(lengthNum)]
	schema, err := parseSchema(kv.Value[4+int(lengthNum):])
	if err != nil {
		return nil, err
	}
	columns := make([]string, 0)
	for _, c := range schema.Columns {
		columns = append(columns, string(c.Name))
	}
	kvstring.Value = fmt.Sprintf(
		"name:%s, columns:%v",
		name,
		strings.Join(columns, ","),
	)
	return kvstring, nil
}

func (p *edgeParser) Prefix() ([]*common.KV, error) {
	s := []byte(p.key)
	var (
		spaceID []byte
		EdgeID  []byte
	)
	if p.opts.SpaceID != -1 {
		if err := common.ConvertIntToBytes(&p.opts.SpaceID, &spaceID, common.ByteOrder); err != nil {
			return nil, err
		}
		s = append(s, spaceID...)

		if p.opts.EdgeID != 0 {
			if err := common.ConvertIntToBytes(&p.opts.EdgeID, &EdgeID, common.ByteOrder); err != nil {
				return nil, err
			}
			s = append(s, EdgeID...)
		}
	}
	return p.engine.Prefix(s, p.opts.Limit)
}
