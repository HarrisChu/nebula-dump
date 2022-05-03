package pkg

type (
	MetaKeyType    string
	StorageKeyType string
)

const (
	MetaKeySpaces   MetaKeyType = "spaces"
	MetaKeyParts                = "parts"
	MetaKeyTags                 = "tags"
	MetaKeyEdges                = "edges"
	MetaKeyMachines             = "machines"
	MetaKeyHosts                = "hosts"
	MetaKeyIndexes              = "indexes"
)

const (
	StorageKeyTags    StorageKeyType = "tags"
	StorageKeyEdges                  = "edges"
	StorageKeyIndexes                = "indexes"
)

var MetaKeyTypeMap map[MetaKeyType]Parser
var StorageKeyTypeMap map[StorageKeyType]Parser
