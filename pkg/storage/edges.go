package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	"github.com/harrischu/nebula-dump/pkg"
	"github.com/harrischu/nebula-dump/pkg/common"
	"github.com/harrischu/nebula-dump/pkg/schemacache"
)

// edgeParser
// key: (type + part) + src + edge type(4bit) + dst
// value: versionLength + version + value
type edgeParser struct {
	opts   *pkg.Option
	engine *common.Engine
	client *common.MetaClient
	schema schemacache.Schemacache
}

func (p *edgeParser) New(engine *common.Engine, opts *pkg.Option) pkg.Parser {
	return &edgeParser{
		opts:   opts,
		engine: engine,
	}
}

func (p *edgeParser) Parse(kv *common.KV) (*common.KVString, error) {
	var (
		kvstring  = &common.KVString{}
		partID    int32
		edgeType  int32
		left      string
		right     string
		err       error
		vidLength int16
		rank      uint64
	)
	vidLength = p.schema.GetSpace(p.opts.SpaceID).GetProperties().GetVidType().TypeLength
	n := len(kv.Key) - 1

	pt, l, e, k, r := kv.Key[:common.Sizeof(partID)],
		kv.Key[common.Sizeof(partID):common.Sizeof(partID)+int(vidLength)],
		kv.Key[common.Sizeof(partID)+int(vidLength):common.Sizeof(partID)+int(vidLength)+common.Sizeof(edgeType)],
		kv.Key[common.Sizeof(partID)+int(vidLength)+common.Sizeof(edgeType):common.Sizeof(partID)+int(vidLength)+common.Sizeof(edgeType)+common.Sizeof(rank)],
		kv.Key[common.Sizeof(partID)+int(vidLength)+common.Sizeof(edgeType)+common.Sizeof(rank):n]

	if err = common.ConvertBytesToInt(&partID, &pt, common.ByteOrder); err != nil {
		return nil, err
	}
	partID = partID | kEdge
	partID >>= 8
	if left, err = getVidString(l, p.opts.SpaceID, p.schema); err != nil {
		return nil, err
	}
	if right, err = getVidString(r, p.opts.SpaceID, p.schema); err != nil {
		return nil, err
	}
	if err = common.ConvertBytesToInt(&edgeType, &e, common.ByteOrder); err != nil {
		return nil, err
	}
	//rank
	if err = common.ConvertBytesToInt(&rank, &k, binary.BigEndian); err != nil {
		return nil, err
	}
	rank ^= 1 << 63
	if edgeType > 0 {
		kvstring.Key = fmt.Sprintf("part:%d, src:%s, edge:%d, dst:%s, rank:%d", partID, left, edgeType, right, rank)
	} else {
		kvstring.Key = fmt.Sprintf("part:%d, src:%s, edge:%d, dst:%s, rank:%d", partID, right, edgeType, left, rank)
	}

	rowData, err := decodeValue("edge", kv.Value, p.opts.SpaceID, int32(math.Abs(float64(edgeType))), p.schema)
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

func (p *edgeParser) Prefix() ([]*common.KV, error) {
	common.Logger.Debugf("Prefix edge key")
	var (
		// true is for positive edge
		direct    bool = true
		part      int32
		left      string
		right     string
		rank      int64
		vidLength int16
	)
	s := make([]byte, 0)

	if err := verifyOption(p.opts); err != nil {
		return nil, err
	}
	if p.opts.PartID == -1 && p.opts.Src == "" && p.opts.Dst == "" {
		return nil, fmt.Errorf("must provide a valid part or a valid src/dst")
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
	vidLength = space.GetProperties().GetVidType().GetTypeLength()

	if p.opts.EdgeID != 0 {
		if p.opts.EdgeID < 0 {
			direct = false
		}
	} else {
		if p.opts.Src == "" && p.opts.Dst != "" {
			direct = false
		}
	}

	if direct {
		left = p.opts.Src
		right = p.opts.Dst
	} else {
		left = p.opts.Dst
		right = p.opts.Src
	}

	if left == "" {
		part = p.opts.PartID
	} else {
		//get part by id
		var bs []byte

		bs, err := getVidByte(left, p.opts.SpaceID, p.schema)
		if err != nil {
			return nil, err
		}
		id, err := common.GetPartID(bs, space.GetProperties().GetPartitionNum())
		if err != nil {
			return nil, err
		}
		part = id
	}

	item := part<<8 | kEdge
	var partData []byte
	if err := common.ConvertIntToBytes(&item, &partData, common.ByteOrder); err != nil {
		return nil, err
	}
	s = append(s, partData...)
	// append id
	if left != "" {
		vidBy, err := getVidByte(left, p.opts.SpaceID, schema)
		if err != nil {
			return nil, err
		}
		s = append(s, vidBy...)
	}
	// filter
	if p.opts.EdgeID != 0 || right != "" {
		fn := func(key []byte) bool {
			// a extra byte
			n := len(key) - 1
			_, edgeKey, _, rightKey := key[common.Sizeof(part):common.Sizeof(part)+int(vidLength)],
				key[common.Sizeof(part)+int(vidLength):common.Sizeof(part)+int(vidLength)+common.Sizeof(p.opts.EdgeID)],
				key[n-int(vidLength)-common.Sizeof(rank):n-int(vidLength)],
				key[n-int(vidLength):n]
			if p.opts.EdgeID != 0 {
				var edge []byte
				if err := common.ConvertIntToBytes(&p.opts.EdgeID, &edge, common.ByteOrder); err != nil {
					panic(err)
				}
				if bytes.Compare(edgeKey, edge) != 0 {
					common.Logger.Debugf("ignore key, direct is %v, expect is %v, actual is %v, key is %v", direct, edge, edgeKey, key)
					common.Logger.Debugf("space length is %d", vidLength)
					return false
				}
			}
			if right != "" {
				vidBs, err := getVidByte(right, p.opts.SpaceID, p.schema)
				if err != nil {
					panic(err)
				}
				if bytes.Compare(rightKey, vidBs) != 0 {
					common.Logger.Debugf("ignore key, direct is %v, right is %v, key is %v", direct, rightKey, key)
					return false
				}
			}
			return true
		}
		return p.engine.PrefixWithCondition(s, p.opts.Limit, fn, nil)
	}

	fn := func(key []byte) bool {
		var edge int32
		edgeKey := key[common.Sizeof(p.opts.PartID)+int(vidLength) : common.Sizeof(p.opts.PartID)+int(vidLength)+common.Sizeof(p.opts.PartID)]
		if err := common.ConvertBytesToInt(&edge, &edgeKey, common.ByteOrder); err != nil {
			panic(err)
		}
		if direct && edge < 0 {
			common.Logger.Debugf("ignore key, direct is %v, edge is %d, key is %v", direct, edge, key)
			return false
		}
		if !direct && edge > 0 {
			common.Logger.Debugf("ignore key, direct is %v, edge is %d, key is %v", direct, edge, key)
			return false
		}
		return true
	}

	return p.engine.PrefixWithCondition(s, p.opts.Limit, fn, nil)
}
