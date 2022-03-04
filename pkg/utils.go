package pkg

import (
	"bytes"
	"encoding/binary"
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

var byteOrder binary.ByteOrder

func int64ToBytes(n int, order binary.ByteOrder) ([]byte, error) {
	data := int64(n)
	bytebuf := bytes.NewBuffer([]byte{})
	if err := binary.Write(bytebuf, order, data); err != nil {
		return nil, err
	}
	return bytebuf.Bytes(), nil
}

func int32ToBytes(n int, order binary.ByteOrder) ([]byte, error) {
	data := int32(n)
	bytebuf := bytes.NewBuffer([]byte{})
	if err := binary.Write(bytebuf, order, data); err != nil {
		return nil, err
	}
	return bytebuf.Bytes(), nil
}

func bytesToInt(bys []byte, order binary.ByteOrder) (int, error) {
	bytebuff := bytes.NewBuffer(bys)
	var data int64
	if err := binary.Read(bytebuff, order, &data); err != nil {
		return 0, err
	}
	return int(data), nil
}

func isLittleEndian() bool {
	var i int32 = 0x01020304
	u := unsafe.Pointer(&i)
	pb := (*byte)(u)
	b := *pb
	return (b == 0x04)
}

func init() {
	if isLittleEndian() {
		byteOrder = binary.LittleEndian
	} else {
		byteOrder = binary.BigEndian
	}
}

func ConvertBytesToInt(bys []byte) (int, error) {
	n := len(bys)
	if n < 8 {
		t := make([]byte, 8-n)
		bys = append(bys, t...)
	}
	return bytesToInt(bys, byteOrder)
}

func ConvertIntToBytes(i int) ([]byte, error) {
	return int64ToBytes(i, byteOrder)
}

func GetPart(partID int, keyType int) ([]byte, error) {
	prefixPart := (partID << 8) | keyType
	data, err := int32ToBytes(prefixPart, byteOrder)
	if err != nil {
		return nil, err
	}
	return data, nil
}
