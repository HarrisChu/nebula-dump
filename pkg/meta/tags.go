package meta

import (
	"bytes"
	"fmt"
	"math"
	"strings"

	"github.com/harrischu/nebula-dump/pkg/common"
	"github.com/vesoft-inc/nebula-go/v3/nebula/meta"
)

// tagParser
// key: __tags__ + space id + tag id + schema version
// value: length of name (4 bit) + name + CompactSerializer of schema
func tagParser(kv *common.KV) (*common.KVString, error) {
	var (
		kvstring   = &common.KVString{}
		spaceID    int32
		tagID      int32
		versionNum int64
		lengthNum  int32
	)
	s := []byte("__tags__")
	l := len(s)
	if !bytes.Equal(kv.Key[:len(s)], s) {
		return nil, fmt.Errorf("cannot parse key")
	}
	space, tag, version := kv.Key[l:l+4], kv.Key[l+4:l+8], kv.Key[l+8:]
	if err := common.ConvertBytesToInt(&spaceID, &space, common.ByteOrder); err != nil {
		return nil, err
	}
	if err := common.ConvertBytesToInt(&tagID, &tag, common.ByteOrder); err != nil {
		return nil, err
	}
	if err := common.ConvertBytesToInt(&versionNum, &version, common.ByteOrder); err != nil {
		return nil, err
	}
	// follow nebula logic
	versionNum = math.MaxInt64 - versionNum
	kvstring.Key = fmt.Sprintf("space:%d, tag:%d, version:%d", spaceID, tagID, versionNum)
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


func parseSchema(v []byte) (*meta.Schema, error) {
	m := meta.NewSchema()
	err := common.CompactDeserializer(m, &v)
	if err != nil {
		return nil, err
	}
	return m, nil
}