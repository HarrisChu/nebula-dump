package utils

import (
	"github.com/harrischu/nebula-dump/pkg/common"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var scanCmd = &cobra.Command{
	Use:   "scan",
	Short: "scan some key",
	Long:  ``,
	Example: `
	`,
	RunE: func(cmd *cobra.Command, args []string) error {
		prefixBytes, err := common.CovertToBytes("bytes", utilsOpts.prefix)
		if err != nil {
			return err
		}
		engine, err := common.NewRocksDbEngine(utilsOpts.rocksdbPath)
		if err != nil {
			return err
		}
		err = engine.Open()
		if err != nil {
			return err
		}
		kvs, err := engine.Prefix(prefixBytes, utilsOpts.limit)
		if err != nil {
			return err
		}
		for _, kv := range kvs {
			var k, v string
			common.ConvertBytesToString(&k, &kv.Key)
			common.ConvertBytesToString(&v, &kv.Value)
			common.Logger.Infof("key: %s, value: %s", k, v)
		}
		return nil
	},
}

func init() {
	flags := pflag.NewFlagSet("", pflag.ContinueOnError)
	flags.StringVar(&utilsOpts.prefix, "prefix", "", "the prefix key. e.g. 7,0,0,0")
	flags.StringVar(&utilsOpts.rocksdbPath, "path", "", "the rocksdb path")
	flags.IntVar(&utilsOpts.limit, "limit", 20, "limit")
	cobra.MarkFlagRequired(flags, "path")
	cobra.MarkFlagRequired(flags, "prefix")
	err := cobra.MarkFlagDirname(flags, "path")
	if err != nil {
		panic(err)
	}
	scanCmd.PersistentFlags().AddFlagSet(flags)

	utilCmd.AddCommand(scanCmd)
}
