package pkg

import (
	"encoding/binary"
	"testing"
)

func TestIntToBytes(t *testing.T) {
	a := (255 << 8) | 0x00000001
	t.Log(a)
	t.Log(int32ToBytes(a, binary.LittleEndian))
	t.Log([]byte{0xff})
	t.Fatal(1)
}
