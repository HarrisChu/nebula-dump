package utils

import (
	"github.com/harrischu/nebula-dump/cmd/root"
	"github.com/spf13/cobra"
)

type utilsOptsType struct {
	keyType     string
	key         string
	toType      string
	partsCount  int
	vid         string
	vidType     string
	sstPath     string
	sstToPath   string
	prefix      string
	limit       int
	rocksdbPath string
}

var (
	utilsOpts utilsOptsType
)

var utilCmd = &cobra.Command{
	Use:               "utils",
	Short:             "some utils command",
	Long:              ``,
	CompletionOptions: cobra.CompletionOptions{HiddenDefaultCmd: true},
}

func init() {
	root.RootCmd.AddCommand(utilCmd)
}
