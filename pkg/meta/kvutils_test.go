package meta

import (
	"testing"

	"github.com/harrischu/nebula-dump/pkg/common"
	"github.com/stretchr/testify/assert"
	"github.com/vesoft-inc/nebula-go/v3/nebula/meta"
)

func TestSpace(t *testing.T) {
	spaceDesc := meta.NewSpaceDesc()
	spaceDesc.SpaceName = []byte("harris")

	kv := &common.KV{
		Key: []byte{95, 95, 115, 112, 97, 99, 101, 115, 95, 95, 1, 0, 0, 0},
		Value: []byte{24, 5, 115, 102, 49, 48, 48, 21, 192, 1, 21, 6, 24, 4, 117,
			116, 102, 56, 24, 8, 117, 116, 102, 56, 95, 98, 105, 110, 28, 21, 4, 20,
			16, 0, 25, 72, 31, 100, 101, 102, 97, 117, 108, 116, 95, 122, 111, 110,
			101, 95, 49, 57, 50, 46, 49, 54, 56, 46, 49, 53, 46, 51, 51, 95, 57, 55,
			55, 57, 35, 100, 101, 102, 97, 117, 108, 116, 95, 122, 111, 110, 101, 95, 115, 116, 111, 114, 97, 103, 101, 45, 48, 46, 115, 116, 111, 114, 97, 103, 101, 95, 57, 55, 55, 57, 35, 100, 101, 102, 97, 117, 108, 116, 95, 122, 111, 110, 101, 95, 115, 116, 111, 114, 97, 103, 101, 45, 49, 46, 115, 116, 111, 114, 97, 103, 101, 95, 57, 55, 55, 57, 35, 100, 101, 102, 97, 117, 108, 116, 95, 122, 111, 110, 101, 95, 115, 116, 111, 114, 97, 103, 101, 45, 50, 46, 115, 116, 111, 114, 97, 103, 101, 95, 57, 55, 55, 57, 0},
	}
	desc, err := parseSpaceDesc(kv.Value)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, []byte("sf100"), desc.SpaceName)
	assert.Equal(t, int32(96), desc.PartitionNum)

}

func TestPart(t *testing.T) {
	kv := &common.KV{
		Key: []byte{95, 95, 112, 97, 114, 116, 115, 95, 95, 1, 0, 0, 0, 1, 0, 0, 0},
		Value: []byte{2, 0, 0, 0, 115, 116, 111, 114, 97, 103, 101, 45, 50, 46, 115,
			116, 111, 114, 97, 103, 101, 58, 57, 55, 55, 57, 44, 32, 115, 116, 111, 114, 97,
			103, 101, 45, 48, 46, 115, 116, 111, 114, 97, 103, 101, 58, 57, 55, 55, 57, 44, 32,
			115, 116, 111, 114, 97, 103, 101, 45, 49, 46, 115, 116, 111, 114, 97, 103, 101, 58, 57, 55, 55, 57},
	}
	kvstring, err := partParser(kv)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "space:1, part:1", kvstring.Key)
	assert.Equal(t, "version:2, hosts are storage-2.storage:9779, storage-0.storage:9779, storage-1.storage:9779", kvstring.Value)
}

func TestTag(t *testing.T) {
	kv := &common.KV{
		Key:   []byte{95, 95, 116, 97, 103, 115, 95, 95, 1, 0, 0, 0, 2, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 127},
		Value: []byte{4, 0, 0, 0, 80, 111, 115, 116, 25, 124, 24, 9, 105, 109, 97, 103, 101, 70, 105, 108, 101, 28, 21, 12, 0, 33, 0, 24, 12, 99, 114, 101, 97, 116, 105, 111, 110, 68, 97, 116, 101, 28, 21, 50, 0, 33, 0, 24, 10, 108, 111, 99, 97, 116, 105, 111, 110, 73, 80, 28, 21, 12, 0, 33, 0, 24, 11, 98, 114, 111, 119, 115, 101, 114, 85, 115, 101, 100, 28, 21, 12, 0, 33, 0, 24, 8, 108, 97, 110, 103, 117, 97, 103, 101, 28, 21, 12, 0, 33, 0, 24, 7, 99, 111, 110, 116, 101, 110, 116, 28, 21, 12, 0, 33, 0, 24, 6, 108, 101, 110, 103, 116, 104, 28, 21, 4, 0, 33, 0, 28, 0, 0},
	}
	kvstring, err := tagParser(kv)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "space:1, tag:2, version:0", kvstring.Key)
	assert.Equal(t, "name:Post, columns:imageFile,creationDate,locationIP,browserUsed,language,content,length", kvstring.Value)
}
