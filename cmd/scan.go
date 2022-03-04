package cmd

import (
	"context"

	"github.com/harrischu/nebula-dump/pkg"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
	startKey string
	endKey   string
	limit    int
)

var scanCmd = &cobra.Command{
	Use:   "scan",
	Short: "scan keys in rocksdb",
	Long:  ``,
	Example: `
nebula-dump scan --keyType bytes --start 255,255,255,255 --limit 2 --dir nebula/data/storage/nebula/1/data/
nebula-dump scan --keyType string --start key1 --end key2 --dir nebula/data/storage/nebula/1/data/
	`,
	Run: func(cmd *cobra.Command, args []string) {
		dumper := pkg.NewDumper()
		err := dumper.Open(rocksdbDir)
		if err != nil {
			panic(err)
		}
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()
		start, err := covertToBytes(keyType, startKey)
		if err != nil {
			logger.Error(err)
			return
		}
		var kvs []*pkg.KV
		if endKey != "" {
			end, err := covertToBytes(keyType, endKey)
			if err != nil {
				logger.Error(err)
				return
			}
			kvs, err = dumper.ScanByRange(ctx, start, end)
			if err != nil {
				logger.Error(err)
				return
			}
		} else {
			kvs, err = dumper.ScanByCount(ctx, start, limit)
			if err != nil {
				logger.Error(err)
				return
			}
		}

		if len(kvs) == 0 {
			logger.Info("cannot scan any key")
		}
		for _, kv := range kvs {
			logger.Infof("key is %v, value is %v. \n", kv.Key, kv.Value)
		}
	},
}

func init() {
	rootCmd.AddCommand(scanCmd)
	flags := pflag.NewFlagSet("", pflag.ContinueOnError)
	flags.StringVarP(&startKey, "start", "", "", "start key")
	flags.StringVarP(&endKey, "end", "", "", "end key")
	flags.IntVarP(&limit, "limit", "", 1, "key count")
	scanCmd.PersistentFlags().AddFlagSet(flags)

}
