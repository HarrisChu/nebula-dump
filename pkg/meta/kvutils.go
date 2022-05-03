package meta

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/harrischu/nebula-dump/pkg"
	"github.com/vesoft-inc/nebula-go/v3/nebula/meta"
)

type parsefunc func(kv *pkg.KV) (*pkg.KVString, error)

var parserMap = map[string]Parser{
	"space": parsefunc(spaceParser),
}

type Parser interface {
	Parse(*pkg.KV) (*pkg.KVString, error)
}

func (f parsefunc) Parse(kv *pkg.KV) (*pkg.KVString, error) {
	return f(kv)
}

func parseSpaceDesc(v []byte) (*meta.SpaceDesc, error) {
	m := meta.NewSpaceDesc()
	err := pkg.CompactDeserializer(m, &v)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func parseSchema(v []byte) (*meta.Schema, error) {
	m := meta.NewSchema()
	err := pkg.CompactDeserializer(m, &v)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// spaceParser
// key: __spaces__ + space id
// value: CompactSerializer of SpaceDesc
func spaceParser(kv *pkg.KV) (*pkg.KVString, error) {
	var (
		kvstring = &pkg.KVString{}
		spaceID  int32
	)

	s := []byte("__spaces__")
	if !bytes.Equal(kv.Key[:len(s)], s) {
		return nil, fmt.Errorf("cannot parse key")
	}
	b := kv.Key[len(s):]
	if err := pkg.ConvertBytesToInt(&spaceID, &b, pkg.ByteOrder); err != nil {
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

// partParser
// key: __parts__ + space id + part id
// value: dataversion (4 bit) + host string
func partParser(kv *pkg.KV) (*pkg.KVString, error) {
	var (
		kvstring = &pkg.KVString{}
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
	if err := pkg.ConvertBytesToInt(&spaceId, &space, pkg.ByteOrder); err != nil {
		return nil, err
	}
	if err := pkg.ConvertBytesToInt(&partId, &part, pkg.ByteOrder); err != nil {
		return nil, err
	}
	if err := pkg.ConvertBytesToInt(&version, &v, pkg.ByteOrder); err != nil {
		return nil, err
	}
	kvstring.Key = fmt.Sprintf("space:%d, part:%d", spaceId, partId)
	kvstring.Value = fmt.Sprintf("version:%d, hosts are %s", version, string(kv.Value[4:]))
	return kvstring, nil
}

// tagParser
// key: __tags__ + space id + tag id + schema version
// value: length of name (4 bit) + name + CompactSerializer of schema
func tagParser(kv *pkg.KV) (*pkg.KVString, error) {
	var (
		kvstring   = &pkg.KVString{}
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
	if err := pkg.ConvertBytesToInt(&spaceID, &space, pkg.ByteOrder); err != nil {
		return nil, err
	}
	if err := pkg.ConvertBytesToInt(&tagID, &tag, pkg.ByteOrder); err != nil {
		return nil, err
	}
	if err := pkg.ConvertBytesToInt(&versionNum, &version, pkg.ByteOrder); err != nil {
		return nil, err
	}
	// follow nebula logic
	versionNum = int64(^uint64(0)>>1) - versionNum
	kvstring.Key = fmt.Sprintf("space:%d, tag:%d, version:%d", spaceID, tagID, versionNum)
	length := kv.Value[:4]
	if err := pkg.ConvertBytesToInt(&lengthNum, &length, pkg.ByteOrder); err != nil {
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
func edgeParser(kv *pkg.KV) (*pkg.KVString, error) {
	return nil, nil
}
func leaderParser(kv *pkg.KV) (*pkg.KVString, error) {
	return nil, nil
}
