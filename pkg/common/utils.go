package common

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"unsafe"
)

const (
	kTag       int = 0x00000001
	kEdge          = 0x00000002
	kIndex         = 0x00000003
	kSystem        = 0x00000004
	kOperation     = 0x00000005
	kKeyValue      = 0x00000006
	kVertex        = 0x00000007
)

var ByteOrder binary.ByteOrder

func isLittleEndian() bool {
	var i int32 = 0x01020304
	u := unsafe.Pointer(&i)
	pb := (*byte)(u)
	b := *pb
	return (b == 0x04)
}

func init() {
	if isLittleEndian() {
		ByteOrder = binary.LittleEndian
	} else {
		ByteOrder = binary.BigEndian
	}
}

func ConvertBytesToInt(i interface{}, b *[]byte, order binary.ByteOrder) error {
	var l int
	switch i.(type) {
	case *int8, *uint8:
		l = 1
	case *int16, *uint16:
		l = 2
	case *int32, *uint32:
		l = 4
	case *int64, *uint64:
		l = 8
	default:
		return fmt.Errorf("must provide an integer point")
	}
	n := len(*b)
	if n < l {
		t := make([]byte, l-n)
		*b = append(*b, t...)
	}
	bytebuff := bytes.NewBuffer(*b)
	if err := binary.Read(bytebuff, order, i); err != nil {
		return err
	}
	return nil

}

func ConvertBytesToString(s *string, b *[]byte) {
	r := make([]string, 0)
	for _, i := range *b {
		r = append(r, strconv.Itoa(int(i)))
	}
	*s = strings.Join(r, ",")
}

func ConvertIntToBytes(i interface{}, b *[]byte, order binary.ByteOrder) error {
	switch i.(type) {
	case *int8:
	case *uint8:
	case *int16:
	case *uint16:
	case *int32:
	case *uint32:
	case *int64, *int:
	case *uint64, *uint:
	default:
		return fmt.Errorf("must provide an integer point")
	}

	bytebuf := bytes.NewBuffer([]byte{})
	if err := binary.Write(bytebuf, order, i); err != nil {
		return err
	}
	*b = bytebuf.Bytes()
	return nil
}

func GetPartPrefix(partID int, keyType int) ([]byte, error) {
	var data []byte
	prefixPart := int32((partID << 8) | keyType)
	if err := ConvertIntToBytes(&prefixPart, &data, ByteOrder); err != nil {
		return nil, err
	}
	return data, nil
}

func GetPartID(vid []byte, count int32, length int16) (int32, error) {
	var partID int32
	if int(length) < len(vid) {
		return 0, fmt.Errorf("vid length is too long")
	}
	bs := make([]byte, length, length)
	copy(bs, vid)

	if len(bs) == 8 {
		var v int64
		if err := ConvertBytesToInt(&v, &vid, ByteOrder); err != nil {
			return 0, err
		}
		partID = int32(v%int64(count)) + 1

	} else {
		uv := NebulaMmhash(vid)
		partID = int32(uv%uint64(count) + 1)
	}
	return partID, nil
}

func CovertToBytes(keyType string, s string) ([]byte, error) {
	switch keyType {
	case "bytes":
		return byteStringToBytes(s)
	case "string":
		return stringToBytes(s)
	case "int":
		return intToBytes(s)
	default:
		return nil, fmt.Errorf("cannot find the type")
	}
}

func byteStringToBytes(s string) ([]byte, error) {
	result := make([]byte, 0)
	ss := strings.Split(s, ",")
	for _, v := range ss {
		i, err := strconv.ParseUint(v, 10, 8)
		if err != nil {
			return nil, err
		}
		result = append(result, byte(i))
	}
	return result, nil
}

func stringToBytes(s string) ([]byte, error) {
	return []byte(s), nil
}

func intToBytes(s string) ([]byte, error) {
	var data []byte
	d, err := strconv.ParseInt(s, 10, 0)
	if err != nil {
		return nil, err
	}
	if err := ConvertIntToBytes(&d, &data, ByteOrder); err != nil {
		return nil, err
	}
	return data, nil
}

func Sizeof(v interface{}) int {
	switch v.(type) {
	case int8, uint8:
		return 1
	case int16, uint16:
		return 2
	case int32, uint32:
		return 4
	case int64, uint64:
		return 8
	default:
		panic("invalid type")
	}
}
