package meta

import (
	"github.com/harrischu/nebula-dump/pkg"
)

func init() {
	if pkg.MetaKeyTypeMap == nil {
		pkg.MetaKeyTypeMap = make(map[pkg.MetaKeyType]pkg.Parser)
		pkg.MetaKeyTypeMap[pkg.MetaKeySpaces] = &sparceParser{}
		pkg.MetaKeyTypeMap[pkg.MetaKeyParts] = &partParser{}
		pkg.MetaKeyTypeMap[pkg.MetaKeyTags] = &tagParser{}
		pkg.MetaKeyTypeMap[pkg.MetaKeyEdges] = &edgeParser{}
		pkg.MetaKeyTypeMap[pkg.MetaKeyMachines] = &machineParser{}
		pkg.MetaKeyTypeMap[pkg.MetaKeyHosts] = &hostParser{}
		pkg.MetaKeyTypeMap[pkg.MetaKeyIndexes] = &indexParser{}
	}
}
