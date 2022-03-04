package cmd

import (
	"context"

	"github.com/harrischu/nebula-dump/pkg"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
	key string
)

// getCmd represents the get command
var getCmd = &cobra.Command{
	Use:   "get",
	Short: "get a key in rocksdb",
	Long:  ``,
	Example: `
nebula-dump get --keyType string --key __meta_version__ --dir nebula/data/meta/nebula/0/data/
nebula-dump get --keyType bytes --key 255,255,255,255 --dir nebula/data/storage/nebula/1/data/
	`,
	Run: func(cmd *cobra.Command, args []string) {
		dumper := pkg.NewDumper()
		err := dumper.Open(rocksdbDir)
		if err != nil {
			logger.Error(err)
			return
		}
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()
		key, err := covertToBytes(keyType, key)
		if err != nil {
			logger.Error(err)
			return
		}
		logger.Debugf("key is %v", key)
		kv, err := dumper.Get(ctx, key)
		if err != nil {
			logger.Error(err)
			return
		}
		if kv != nil {
			logger.Infof("key is %v, value is %v. \n", kv.Key, kv.Value)
		} else {
			logger.Error("cannot find the key.")
		}
	},
}

func init() {
	rootCmd.AddCommand(getCmd)
	flags := pflag.NewFlagSet("", pflag.ContinueOnError)
	flags.StringVarP(&key, "key", "", "", "key")
	getCmd.PersistentFlags().AddFlagSet(flags)
}
