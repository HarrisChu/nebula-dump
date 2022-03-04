package cmd

import (
	"context"

	"github.com/harrischu/nebula-dump/pkg"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
	prefixKey  string
	prefixType int
	partNum    int
)

var countCmd = &cobra.Command{
	Use:   "count",
	Short: "count keys in rocksdb",
	Long:  ``,
	Example: `
nebula-dump count --prefixType 7  --partNum 100 --dir nebula/data/storage/nebula/1/data/
nebula-dump count --keyType bytes --prefixType 7  --partNum 100 --prefix 1,0,0,0 --dir nebula/data/storage/nebula/1/data/
	`,
	Run: func(cmd *cobra.Command, args []string) {
		dumper := pkg.NewDumper()
		err := dumper.Open(rocksdbDir)
		if err != nil {
			logger.Error(err)
		}
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()
		var sum int64
		if prefixKey == "" {
			sum, err = dumper.Count(ctx, partNum, prefixType, nil)
		} else {
			prefix, err := covertToBytes(keyType, prefixKey)
			if err != nil {
				logger.Error(err)
				return
			}
			sum, err = dumper.Count(ctx, partNum, prefixType, prefix)
		}

		if err != nil {
			logger.Error(err)
			return
		}
		logger.Infof("key count is %d", sum)

	},
}

func init() {
	rootCmd.AddCommand(countCmd)
	flags := pflag.NewFlagSet("", pflag.ContinueOnError)
	flags.StringVarP(&prefixKey, "prefix", "", "", "prefix key")
	flags.IntVarP(&prefixType, "prefixType", "", 1, "nebula key type")
	flags.IntVarP(&partNum, "partNum", "", 100, "partition number for nebula")
	countCmd.PersistentFlags().AddFlagSet(flags)
}
