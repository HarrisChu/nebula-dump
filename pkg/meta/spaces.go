package meta

import (
	"bytes"
	"fmt"

	"github.com/harrischu/nebula-dump/pkg/common"
	"github.com/vesoft-inc/nebula-go/v3/nebula/meta"
)

// spaceParser
// key: __spaces__ + space id
// value: CompactSerializer of SpaceDesc
func spaceParser(kv *common.KV) (*common.KVString, error) {
	var (
		kvstring = &common.KVString{}
		spaceID  int32
	)

	s := []byte("__spaces__")
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
		"name:%s, partition_num:%d, replica_fator:%d, vid_type:%s",
		spaceDesc.SpaceName,
		spaceDesc.PartitionNum,
		spaceDesc.ReplicaFactor,
		spaceDesc.VidType.Type,
	)
	return kvstring, nil
}

func parseSpaceDesc(v []byte) (*meta.SpaceDesc, error) {
	m := meta.NewSpaceDesc()
	err := common.CompactDeserializer(m, &v)
	if err != nil {
		return nil, err
	}
	return m, nil
}
