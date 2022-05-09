package meta

import (
	"bytes"
	"fmt"

	"github.com/harrischu/nebula-dump/pkg/common"
)

// partParser
// key: __parts__ + space id + part id
// value: dataversion (4 bit) + host string
func partParser(kv *common.KV) (*common.KVString, error) {
	var (
		kvstring = &common.KVString{}
		spaceId  int32
		partId   int32
		version  int32
	)
	s := []byte("__parts__")
	if !bytes.Equal(kv.Key[:len(s)], s) {
		return nil, fmt.Errorf("cannot parse key")
	}
	space := kv.Key[len(s) : len(s)+4]
	part := kv.Key[len(s)+4:]
	v := kv.Value[:4]
	if err := common.ConvertBytesToInt(&spaceId, &space, common.ByteOrder); err != nil {
		return nil, err
	}
	if err := common.ConvertBytesToInt(&partId, &part, common.ByteOrder); err != nil {
		return nil, err
	}
	if err := common.ConvertBytesToInt(&version, &v, common.ByteOrder); err != nil {
		return nil, err
	}
	kvstring.Key = fmt.Sprintf("space:%d, part:%d", spaceId, partId)
	kvstring.Value = fmt.Sprintf("version:%d, hosts are %s", version, string(kv.Value[4:]))
	return kvstring, nil
}
