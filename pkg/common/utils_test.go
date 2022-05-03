package common

import (
	"encoding/binary"
	"testing"
)

func TestIntToBytes(t *testing.T) {
	a := int32((255 << 8) | 0x00000001)
	t.Log(a)
	var b []byte
	err := ConvertIntToBytes(&a, &b, binary.LittleEndian)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(b)
	t.Log([]byte{0xff})
	t.Fatal(1)
}

func TestB(t *testing.T) {
	a := []byte{148, 255, 255, 255}
	var b int32
	err := ConvertBytesToInt(&b, &a, binary.LittleEndian)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(b)
	t.Fatal(1)

}
