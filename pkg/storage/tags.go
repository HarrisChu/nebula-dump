package storage

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/harrischu/nebula-dump/pkg"
	"github.com/harrischu/nebula-dump/pkg/common"
	"github.com/harrischu/nebula-dump/pkg/schemacache"
)

// tagParser
// key: (type + part) + vid + tag id(4bit)
// value: versionLength + version + value
type tagParser struct {
	opts   *pkg.Option
	engine *common.Engine
	client *common.MetaClient
	schema schemacache.Schemacache
}

func (p *tagParser) New(engine *common.Engine, opts *pkg.Option) pkg.Parser {
	return &tagParser{
		opts:   opts,
		engine: engine,
	}
}

func (p *tagParser) Parse(kv *common.KV) (*common.KVString, error) {
	var (
		kvstring = &common.KVString{}
		partID   int32
		tagID    int32
		vid      string
		err      error
	)
	vidLength := p.schema.GetSpace(p.opts.SpaceID).GetProperties().GetVidType().TypeLength

	pt, v, t := kv.Key[:common.Sizeof(partID)], kv.Key[common.Sizeof(partID):common.Sizeof(partID)+int(vidLength)], kv.Key[common.Sizeof(partID)+int(vidLength):]
	if err = common.ConvertBytesToInt(&partID, &pt, common.ByteOrder); err != nil {
		return nil, err
	}
	partID = partID | tagID
	partID >>= 8
	if vid, err = getVidString(v, p.opts.SpaceID, p.schema); err != nil {
		return nil, err
	}
	if err = common.ConvertBytesToInt(&tagID, &t, common.ByteOrder); err != nil {
		return nil, err
	}
	kvstring.Key = fmt.Sprintf("part:%d, vid:%s, tag:%d", partID, vid, tagID)

	rowData, err := decodeValue("tag", kv.Value, p.opts.SpaceID, tagID, p.schema)
	if err != nil {
		return nil, err
	}
	valuse := make([]string, 0)
	valuse = append(valuse, fmt.Sprintf("version:%d", rowData.version))
	row := rowData.dataset.Rows[0]

	for i := 0; i < len(rowData.dataset.ColumnNames); i++ {
		valuse = append(valuse, fmt.Sprintf("%s:%s", rowData.dataset.ColumnNames[i], formatValue(row.GetValues()[i])))
	}
	valuse = append(valuse, fmt.Sprintf("timestamp:%d", rowData.timestamp))
	kvstring.Value = strings.Join(valuse, ", ")

	return kvstring, nil
}

func (p *tagParser) Prefix() ([]*common.KV, error) {
	s := make([]byte, 0)
	var part int32
	if err := verifyOption(p.opts); err != nil {
		return nil, err
	}

	if p.opts.PartID == -1 && p.opts.VID == "" {
		return nil, fmt.Errorf("must provide a valid part or a valid VID")
	}
	schema, err := schemacache.NewFileCache(p.opts.MetaAddres)
	if err != nil {
		return nil, err
	}
	if err := schema.Update(); err != nil {
		return nil, err
	}

	p.schema = schema
	space := schema.GetSpace(p.opts.SpaceID)
	if space == nil {
		return nil, fmt.Errorf("cannot find the space")
	}

	if p.opts.VID == "" {
		part = p.opts.PartID
	} else {
		bs, err := getVidByte(p.opts.VID, p.opts.SpaceID, p.schema)
		if err != nil {
			return nil, err
		}
		id, err := common.GetPartID(bs, space.GetProperties().GetPartitionNum())
		if err != nil {
			return nil, err
		}
		part = id
	}

	item := part<<8 | kTag
	var partData []byte
	if err := common.ConvertIntToBytes(&item, &partData, common.ByteOrder); err != nil {
		return nil, err
	}
	s = append(s, partData...)
	// append vid
	if p.opts.VID != "" {
		vidBy, err := getVidByte(p.opts.VID, p.opts.SpaceID, schema)
		if err != nil {
			return nil, err
		}
		s = append(s, vidBy...)
	}

	if p.opts.TagID != -1 {
		var tag []byte
		if err := common.ConvertIntToBytes(&p.opts.TagID, &tag, common.ByteOrder); err != nil {
			return nil, err
		}
		fn := func(key []byte) bool {
			l := len(tag)
			n := len(key)
			if bytes.Compare(key[n-l:], tag) == 0 {
				return true
			}
			return false
		}
		return p.engine.PrefixWithCondition(s, p.opts.Limit, fn, nil)
	}
	return p.engine.Prefix(s, p.opts.Limit)
}
