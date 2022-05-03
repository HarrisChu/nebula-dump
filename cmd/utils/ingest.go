package utils

import (
	"github.com/harrischu/nebula-dump/pkg/common"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var ingestCmd = &cobra.Command{
	Use:   "ingest",
	Short: "ingest sst file",
	Long:  ``,
	Example: `

ingest --sstPath . --toPath sst
	`,
	RunE: func(c *cobra.Command, args []string) error {
		err := common.Ingest(utilsOpts.sstPath, utilsOpts.sstToPath)
		return err
	},
}

func init() {
	flags := pflag.NewFlagSet("", pflag.ContinueOnError)
	flags.StringVar(&utilsOpts.sstPath, "sstPath", "", "sst path")
	flags.StringVar(&utilsOpts.sstToPath, "toPath", "", "ingest into a rocksdb engine")
	err := cobra.MarkFlagDirname(flags, "sstPath")
	if err != nil {
		panic(err)
	}
	err = cobra.MarkFlagDirname(flags, "toPath")
	if err != nil {
		panic(err)
	}

	ingestCmd.PersistentFlags().AddFlagSet(flags)

	utilCmd.AddCommand(ingestCmd)
}
