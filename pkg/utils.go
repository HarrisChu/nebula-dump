package pkg

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
	case *int64, *uint64, *int, *uint:
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

func GetPart(partID int, keyType int) ([]byte, error) {
	var data []byte
	prefixPart := int32((partID << 8) | keyType)
	if err := ConvertIntToBytes(&prefixPart, &data, ByteOrder); err != nil {
		return nil, err
	}
	return data, nil
}
