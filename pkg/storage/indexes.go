package storage

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/harrischu/nebula-dump/pkg"
	"github.com/harrischu/nebula-dump/pkg/common"
	"github.com/harrischu/nebula-dump/pkg/schemacache"
	"github.com/vesoft-inc/nebula-go/v3/nebula"
	"github.com/vesoft-inc/nebula-go/v3/nebula/meta"
)

// indexParser
// key: (type + part) + src + edge type(4bit) + dst
// value: []
type indexParser struct {
	opts    *pkg.Option
	engine  *common.Engine
	client  *common.MetaClient
	schema  schemacache.Schemacache
	index   *meta.IndexItem
	hasNull bool
}

type indexValues struct {
	buf    []byte
	values []*nebula.Value
	index  *meta.IndexItem
	pos    int16
}

func (p *indexParser) New(engine *common.Engine, opts *pkg.Option) pkg.Parser {
	return &indexParser{
		opts:    opts,
		engine:  engine,
		hasNull: false,
	}
}

func (p *indexParser) Parse(kv *common.KV) (*common.KVString, error) {
	var (
		kvstring = &common.KVString{}
		partID   int32
		indexID  int32
		vid      string
		err      error
	)
	vidLength := p.schema.GetSpace(p.opts.SpaceID).GetProperties().GetVidType().TypeLength
	n := len(kv.Key)
	pt, i, v, id := kv.Key[:common.Sizeof(partID)],
		kv.Key[common.Sizeof(partID):common.Sizeof(partID)+common.Sizeof(indexID)],
		kv.Key[common.Sizeof(partID)+common.Sizeof(indexID):n-int(vidLength)],
		kv.Key[n-int(vidLength):]
	if err = common.ConvertBytesToInt(&partID, &pt, common.ByteOrder); err != nil {
		return nil, err
	}
	partID = partID | kIndex
	partID >>= 8
	if vid, err = getVidString(id, p.opts.SpaceID, p.schema); err != nil {
		return nil, err
	}
	if err = common.ConvertBytesToInt(&indexID, &i, common.ByteOrder); err != nil {
		return nil, err
	}
	// index values
	iv, err := newIndexValues(v, p.index)
	if err != nil {
		return nil, err
	}
	var values []string
	var nullableBit = kv.Key[n-int(vidLength)-2 : n-int(vidLength)]
	var nbit uint16
	if err := common.ConvertBytesToInt(&nbit, &nullableBit, common.ByteOrder); err != nil {
		return nil, err
	}
	for i, f := range p.index.GetFields() {
		if nbit&(0x8000>>i) == 0x8000>>i {
			values = append(values, fmt.Sprintf("%s:%s", f.GetName(), "__null__"))
		} else {
			values = append(values, fmt.Sprintf("%s:%s", f.GetName(), formatValue(iv.values[i])))
		}
	}

	kvstring.Key = fmt.Sprintf("part:%d, index:%d, %s, vid:%s",
		partID, indexID, strings.Join(values, ","), vid)

	return kvstring, nil
}

func (p *indexParser) Prefix() ([]*common.KV, error) {
	s := make([]byte, 0)
	var (
		part    int32
		vidBs   []byte
		indexBs []byte
	)

	if err := verifyOption(p.opts); err != nil {
		return nil, err
	}
	if p.opts.IndexID == -1 {
		return nil, fmt.Errorf("must provide a valid index id")
	}
	if p.opts.PartID == -1 && p.opts.VID == "" {
		return nil, fmt.Errorf("must provide a valid part or vid")
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
	indexes := schema.GetIndexes(p.opts.SpaceID)
	for _, i := range indexes {
		if i.GetIndexID() == p.opts.IndexID {
			p.index = i
			break
		}
	}
	if p.index == nil {
		return nil, fmt.Errorf("not a valid index id")
	}

	for _, f := range p.index.GetFields() {
		if f.GetNullable() && p.hasNull {
			p.hasNull = true
		}
	}

	if p.opts.VID == "" {
		part = p.opts.PartID
	} else {
		vidBs, err = getVidByte(p.opts.VID, p.opts.SpaceID, schema)
		if err != nil {
			return nil, err
		}
		vidLength := space.GetProperties().GetVidType().GetTypeLength()
		id, err := common.GetPartID(vidBs, space.GetProperties().GetPartitionNum(), vidLength)
		if err != nil {
			return nil, err
		}
		part = id
	}

	item := part<<8 | kIndex
	var partData []byte
	if err := common.ConvertIntToBytes(&item, &partData, common.ByteOrder); err != nil {
		return nil, err
	}
	s = append(s, partData...)
	//index id
	if err := common.ConvertIntToBytes(&p.opts.IndexID, &indexBs, common.ByteOrder); err != nil {
		return nil, err
	}
	s = append(s, indexBs...)

	if p.opts.VID != "" {
		var length int
		var hasNull bool = false
		for _, f := range p.index.GetFields() {
			if f.GetNullable() && !hasNull {
				hasNull = true
			}
			switch f.GetType().GetType() {
			case nebula.PropertyType_GEOGRAPHY:
				return nil, fmt.Errorf("not implement the type %v", f.GetType())

			case nebula.PropertyType_FIXED_STRING:
				length += int(f.GetType().GetTypeLength())

			default:
				l, err := getIndexTypeLength(f.GetType().GetType())
				if err != nil {
					return nil, err
				}
				length += l
			}
		}
		//int16 for nullable bitmap
		if hasNull {
			length += 2
		}
		fn := func(key []byte) bool {
			vid := key[common.Sizeof(p.opts.PartID)+common.Sizeof(p.opts.IndexID)+length:]
			expectVid := make([]byte, len(vid))
			copy(expectVid, vidBs)
			if bytes.Compare(vid, expectVid) != 0 {
				common.Logger.Debugf("ignore the key, expect is %v, actual is %v, the key is %v", vidBs, vid, key)
				return false
			} else {
				return true
			}
		}

		return p.engine.PrefixWithCondition(s, p.opts.Limit, fn, nil)

	}
	return p.engine.Prefix(s, p.opts.Limit)
}

func newIndexValues(buf []byte, index *meta.IndexItem) (*indexValues, error) {
	newBuf := make([]byte, len(buf))
	copy(newBuf, buf)
	vs := &indexValues{buf: newBuf, index: index}
	for _, f := range vs.index.GetFields() {
		var l int16
		if f.GetType().GetType() == nebula.PropertyType_FIXED_STRING {
			l = f.GetType().GetTypeLength()
		} else {
			t, err := getIndexTypeLength(f.GetType().GetType())
			if err != nil {
				return nil, err
			}
			l = int16(t)
		}
		b := vs.buf[vs.pos : vs.pos+l]
		vs.pos += l
		v, err := GetIndexValue(b, f.GetType().GetType())
		if err != nil {
			return nil, err
		}

		vs.values = append(vs.values, v)
	}
	return vs, nil
}
