package meta

import (
	"bytes"
	"fmt"

	"github.com/harrischu/nebula-dump/pkg"
	"github.com/harrischu/nebula-dump/pkg/common"
	"github.com/vesoft-inc/nebula-go/v3/nebula/meta"
)

// spaceParser
// key: __spaces__ + space id
// value: CompactSerializer of SpaceDesc
type sparceParser struct {
	opts   *pkg.Option
	key    string
	engine *common.Engine
}

func (p *sparceParser) New(engine *common.Engine, opts *pkg.Option) pkg.Parser {
	return &sparceParser{opts, "__spaces__", engine}
}

func (p *sparceParser) Parse(kv *common.KV) (*common.KVString, error) {
	var (
		kvstring = &common.KVString{}
		spaceID  int32
	)

	s := []byte(p.key)
	if !bytes.Equal(kv.Key[:len(s)], s) {
		return nil, fmt.Errorf("cannot parse key")
	}
	b := kv.Key[len(s):]
	if err := common.ConvertBytesToInt(&spaceID, &b, common.ByteOrder); err != nil {
		return nil, err
	}
	spaceDesc, err := parseSpaceDesc(kv.Value)
	if err != nil {
		return nil, err
	}
	kvstring.Key = fmt.Sprintf("space: %d", spaceID)
	kvstring.Value = fmt.Sprintf(
		"name:%s, partition_num:%d, replica_fator:%d, vid_type:%s(%d)",
		spaceDesc.SpaceName,
		spaceDesc.PartitionNum,
		spaceDesc.ReplicaFactor,
		spaceDesc.VidType.Type,
		spaceDesc.VidType.TypeLength,
	)
	return kvstring, nil
}

func (p *sparceParser) Prefix() ([]*common.KV, error) {
	s := []byte(p.key)
	if p.opts.SpaceID != -1 {
		var spaceID []byte
		if err := common.ConvertIntToBytes(&p.opts.SpaceID, &spaceID, common.ByteOrder); err != nil {
			return nil, err
		}
		s = append(s, spaceID...)
	}
	return p.engine.Prefix(s, p.opts.Limit)
}

func parseSpaceDesc(v []byte) (*meta.SpaceDesc, error) {
	m := meta.NewSpaceDesc()
	err := common.CompactDeserializer(m, &v)
	if err != nil {
		return nil, err
	}
	return m, nil
}
