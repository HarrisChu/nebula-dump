package meta

import "github.com/harrischu/nebula-dump/pkg/common"

type (
	parsefunc   func(kv *common.KV) (*common.KVString, error)
	MetaKeyType string
)

const (
	metaKeySpaces MetaKeyType = "spaces"
	metaKeyParts              = "parts"
	metaKeyTags               = "tags"
)

var MetaKeyTypeMap map[MetaKeyType]Parser

func init() {
	MetaKeyTypeMap = make(map[MetaKeyType]Parser)
	MetaKeyTypeMap[metaKeySpaces] = parsefunc(spaceParser)
	MetaKeyTypeMap[metaKeyParts] = parsefunc(partParser)
	MetaKeyTypeMap[metaKeyTags] = parsefunc(tagParser)

}

type Parser interface {
	Parse(*common.KV) (*common.KVString, error)
}

func (f parsefunc) Parse(kv *common.KV) (*common.KVString, error) {
	return f(kv)
}
