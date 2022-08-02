package storage

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/harrischu/nebula-dump/pkg"
	"github.com/harrischu/nebula-dump/pkg/common"
	"github.com/harrischu/nebula-dump/pkg/schemacache"
	"github.com/vesoft-inc/nebula-go/v3/nebula"
	"github.com/vesoft-inc/nebula-go/v3/nebula/meta"
)

var (
	kTag   int32 = 0x00000001
	kEdge  int32 = 0x00000002
	kIndex int32 = 0x00000003
)

type rowReader struct {
	headerLength    int32
	nullBytesLength int32
	schema          *meta.Schema
	buf             []byte
	pos             int32
}

type rowData struct {
	version   int64
	dataset   *nebula.DataSet
	timestamp int64
}

func NewRowReader(s *meta.Schema, buf []byte, headerLength int32) *rowReader {
	c := &rowReader{
		schema:       s,
		buf:          buf,
		headerLength: headerLength,
		pos:          0,
	}
	nullCount := 0
	for _, c := range s.Columns {
		if c.Nullable {
			nullCount++
		}
	}
	if nullCount != 0 {
		c.nullBytesLength = int32(((nullCount - 1) >> 3) + 1)
	}

	return c
}

func init() {
	if pkg.StorageKeyTypeMap == nil {
		pkg.StorageKeyTypeMap = make(map[pkg.StorageKeyType]pkg.Parser)
		pkg.StorageKeyTypeMap[pkg.StorageKeyTags] = &tagParser{}
		pkg.StorageKeyTypeMap[pkg.StorageKeyEdges] = &edgeParser{}
		pkg.StorageKeyTypeMap[pkg.StorageKeyIndexes] = &indexParser{}
	}
}

// must provide space id, meta address for storage parser.
func verifyOption(opt *pkg.Option) error {
	if opt.SpaceID == -1 {
		return fmt.Errorf("must provide a valid space id")
	}
	if opt.MetaAddres == "" {
		return fmt.Errorf("must provide a valid meta address")
	}

	return nil
}

func getVidByte(vid string, spaceID int32, schema schemacache.Schemacache) ([]byte, error) {
	if schema == nil {
		panic("must provide a valid schema")
	}

	space := schema.GetSpace(spaceID)
	vidType := space.GetProperties().GetVidType().GetType()
	vidLength := space.GetProperties().GetVidType().TypeLength
	b := make([]byte, vidLength)

	if vidType == nebula.PropertyType_INT64 {
		temp, err := strconv.Atoi(vid)
		if err != nil {
			return nil, err
		}
		v := int64(temp)
		if err := common.ConvertIntToBytes(&v, &b, common.ByteOrder); err != nil {
			return nil, err
		}
	} else {
		temp := []byte(vid)
		if len(temp) > int(vidLength) {
			return nil, fmt.Errorf("invalid vid length, vid is %s, length is %d", vid, vidLength)
		}
		for i := 0; i < len(temp); i++ {
			b[i] = temp[i]
		}
	}
	return b, nil
}

func getVidString(vid []byte, spaceID int32, schema schemacache.Schemacache) (string, error) {
	if schema == nil {
		panic("must provide a valid schema")
	}
	space := schema.GetSpace(spaceID)
	vidType := space.GetProperties().GetVidType().GetType()
	var s string

	if vidType == nebula.PropertyType_INT64 {
		var temp int64
		if err := common.ConvertBytesToInt(&temp, &vid, common.ByteOrder); err != nil {
			return "", err
		}
		s = strconv.Itoa(int(temp))
	} else {
		s = string(vid)

	}
	return s, nil
}

func decodeValue(t string, value []byte, spaceID, id int32, schema schemacache.Schemacache) (*rowData, error) {
	var (
		version      int64
		headerLength int32
	)
	var vb []byte = make([]byte, 8)
	b := value[0]
	l := b & 0x07
	if l == 0 {
		version = 0
		headerLength = 1
	} else {
		for i := 0; i < int(l); i++ {
			vb[i] = value[1+i]
		}
		if err := common.ConvertBytesToInt(&version, &vb, common.ByteOrder); err != nil {
			return nil, err
		}
		headerLength = int32(l) + 1
	}
	s := getSchema(t, spaceID, id, version, schema)
	if s == nil {
		return nil, fmt.Errorf("cannot get the schema")
	}

	r := NewRowReader(s, value, headerLength)
	row, err := r.read()
	if err != nil {
		return nil, err
	}
	ds := nebula.NewDataSet()
	for _, c := range s.GetColumns() {
		ds.ColumnNames = append(ds.ColumnNames, c.GetName())
	}
	ts := value[len(value)-8:]
	var timestamp int64
	if err := common.ConvertBytesToInt(&timestamp, &ts, common.ByteOrder); err != nil {
		return nil, err
	}
	ds.Rows = append(ds.Rows, row)
	data := &rowData{
		version:   version,
		dataset:   ds,
		timestamp: timestamp,
	}
	return data, nil

}

func getSchema(t string, spaceID, id int32, version int64, schema schemacache.Schemacache) *meta.Schema {
	var s *meta.Schema
	switch t {
	case "tag":
		tags := schema.GetTags(spaceID)
		for _, t := range tags {
			if t.TagID == id && t.Version == version {
				s = t.Schema
				break
			}
		}

	case "edge":
		edges := schema.GetEdges(spaceID)
		for _, t := range edges {
			if t.EdgeType == id && t.Version == version {
				s = t.Schema
				break
			}
		}
	}
	return s
}

func (r *rowReader) read() (*nebula.Row, error) {
	values := make([]*nebula.Value, len(r.schema.Columns))
	for i := 0; i < len(r.schema.Columns); i++ {
		f := r.schema.Columns[i]
		t := f.GetType()
		v, err := r.getValue(t)
		if err != nil {
			return nil, err
		}
		values[i] = v
	}
	return &nebula.Row{Values: values}, nil

}

func (r *rowReader) isNull(pos int32) bool {
	bits := []byte{0x80, 0x40, 0x20, 0x10, 0x08, 0x04, 0x02, 0x01}

	offset := r.headerLength + (pos >> 3)
	flag := r.buf[offset] & bits[pos&0x0000000000000007]
	return flag != 0
}

func (r *rowReader) getValue(t *meta.ColumnTypeDef) (*nebula.Value, error) {
	var b []byte
	offset := r.headerLength + r.nullBytesLength + r.pos
	switch t.GetType() {
	case nebula.PropertyType_BOOL:
		b = r.buf[offset : offset+1]
		r.pos += 1

	case nebula.PropertyType_INT64:
		b = r.buf[offset : offset+8]
		r.pos += 8

	case nebula.PropertyType_FIXED_STRING:
		b = r.buf[offset : offset+int32(t.GetTypeLength())]
		r.pos += int32(t.GetTypeLength())

	case nebula.PropertyType_STRING:
		var (
			strOffset int32
			strLen    int32
		)
		d := r.buf[offset : offset+int32(common.Sizeof(strOffset))]
		if err := common.ConvertBytesToInt(&strOffset, &d, common.ByteOrder); err != nil {
			return nil, err
		}
		r.pos += int32(common.Sizeof(strOffset))
		l := r.buf[offset+int32(common.Sizeof(strOffset)) : offset+int32(common.Sizeof(strOffset))+int32(common.Sizeof(strLen))]

		if err := common.ConvertBytesToInt(&strLen, &l, common.ByteOrder); err != nil {
			return nil, err
		}
		r.pos += int32(common.Sizeof(strLen))
		b = r.buf[strOffset : strOffset+strLen]

	case nebula.PropertyType_DATETIME:
		b = r.buf[offset : offset+2+1+1+1+1+1+4]
		r.pos += 2 + 1 + 1 + 1 + 1 + 1 + 4
	default:
		return nil, fmt.Errorf("not support this tpye: %d", t.GetType())
	}
	return GetValue(b, t.GetType())
}

// copy from nebula-go
func formatValue(value *nebula.Value) string {
	if value.IsSetNVal() {
		return value.GetNVal().String()
	} else if value.IsSetBVal() {
		return fmt.Sprintf("%t", value.GetBVal())
	} else if value.IsSetIVal() {
		return fmt.Sprintf("%d", value.GetIVal())
	} else if value.IsSetFVal() {
		fStr := strconv.FormatFloat(value.GetFVal(), 'g', -1, 64)
		if !strings.Contains(fStr, ".") {
			fStr = fStr + ".0"
		}
		return fStr
	} else if value.IsSetSVal() {
		return `"` + string(value.GetSVal()) + `"`
	} else if value.IsSetDVal() { // Date yyyy-mm-dd
		date := value.GetDVal()
		return fmt.Sprintf("%04d-%02d-%02d",
			date.GetYear(),
			date.GetMonth(),
			date.GetDay())
	} else if value.IsSetTVal() { // Time HH:MM:SS.MSMSMS
		rawTime := value.GetTVal()
		return fmt.Sprintf("%02d:%02d:%02d.%06d",
			rawTime.GetHour(),
			rawTime.GetMinute(),
			rawTime.GetSec(),
			rawTime.GetMicrosec())
	} else if value.IsSetDtVal() { // DateTime yyyy-mm-ddTHH:MM:SS.MSMSMS
		rawDateTime := value.GetDtVal()

		return fmt.Sprintf("%d-%02d-%02dT%02d:%02d:%02d.%06d",
			rawDateTime.GetYear(),
			rawDateTime.GetMonth(),
			rawDateTime.GetDay(),
			rawDateTime.GetHour(),
			rawDateTime.GetMinute(),
			rawDateTime.GetSec(),
			rawDateTime.GetMicrosec())
	} else if value.IsSetVVal() { // Vertex format: ("VertexID" :tag1{k0: v0,k1: v1}:tag2{k2: v2})
		return "not support yet"
	} else if value.IsSetEVal() { // Edge format: [:edge src->dst @ranking {propKey1: propVal1}]
		return "not support yet"
	} else if value.IsSetPVal() {
		return "not support yet"
	} else if value.IsSetLVal() { // List
		lval := value.GetLVal()
		var strs []string
		for _, val := range lval.Values {
			strs = append(strs, formatValue(val))
		}
		return fmt.Sprintf("[%s]", strings.Join(strs, ", "))
	} else if value.IsSetMVal() { // Map
		// {k0: v0, k1: v1}
		mval := value.GetMVal()
		var keyList []string
		var output []string
		kvs := mval.Kvs
		for k := range kvs {
			keyList = append(keyList, k)
		}
		sort.Strings(keyList)
		for _, k := range keyList {
			output = append(output, fmt.Sprintf("%s: %s", k, formatValue(kvs[k])))
		}
		return fmt.Sprintf("{%s}", strings.Join(output, ", "))
	} else if value.IsSetUVal() {
		return "not support yet"
	} else if value.IsSetGgVal() {
		return "not support yet"
	} else if value.IsSetDuVal() {
		duval := value.GetDuVal()
		totalSeconds := duval.GetSeconds() + int64(duval.GetMicroseconds())/1000000
		remainMicroSeconds := duval.GetMicroseconds() % 1000000
		s := fmt.Sprintf("P%vMT%v.%06d000S", duval.GetMonths(), totalSeconds, remainMicroSeconds)
		return s
	} else { // is empty
		return ""
	}
}

func GetValue(b []byte, t nebula.PropertyType) (*nebula.Value, error) {
	v := nebula.NewValue()
	switch t {
	case nebula.PropertyType_BOOL:
		var value bool
		if b[0] == 1 {
			value = true
		} else {
			value = false
		}
		v.SetBVal(&value)

	case nebula.PropertyType_INT64:
		var value int64
		if err := common.ConvertBytesToInt(&value, &b, common.ByteOrder); err != nil {
			return nil, err
		}
		v.IVal = &value
		v.SetIVal(&value)

	case nebula.PropertyType_FIXED_STRING:
		v.SetSVal(b)
	case nebula.PropertyType_STRING:
		v.SetSVal(b)

	case nebula.PropertyType_DATETIME:
		var (
			year     int16
			month    int8
			day      int8
			hour     int8
			minute   int8
			second   int8
			microsec int32
		)
		y, m, d, h, mi, s, ms := b[:2],
			b[2:2+1],
			b[2+1:2+1+1],
			b[2+1+1:2+1+1+1],
			b[2+1+1+1:2+1+1+1+1],
			b[2+1+1+1+1:2+1+1+1+1+1],
			b[2+1+1+1+1+1:2+1+1+1+1+1+4]
		if err := common.ConvertBytesToInt(&year, &y, common.ByteOrder); err != nil {
			return nil, err
		}
		if err := common.ConvertBytesToInt(&month, &m, common.ByteOrder); err != nil {
			return nil, err
		}
		if err := common.ConvertBytesToInt(&day, &d, common.ByteOrder); err != nil {
			return nil, err
		}
		if err := common.ConvertBytesToInt(&hour, &h, common.ByteOrder); err != nil {
			return nil, err
		}
		if err := common.ConvertBytesToInt(&minute, &mi, common.ByteOrder); err != nil {
			return nil, err
		}
		if err := common.ConvertBytesToInt(&second, &s, common.ByteOrder); err != nil {
			return nil, err
		}
		if err := common.ConvertBytesToInt(&microsec, &ms, common.ByteOrder); err != nil {
			return nil, err
		}

		dt := nebula.NewDateTime()
		dt.SetYear(year)
		dt.SetMonth(month)
		dt.SetDay(day)
		dt.SetHour(hour)
		dt.SetMinute(minute)
		dt.SetSec(second)
		dt.SetMicrosec(microsec)
		v.SetDtVal(dt)

	default:
		return nil, fmt.Errorf("not support this tpye: %d", t)
	}
	return v, nil
}
