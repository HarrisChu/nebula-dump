package utils

import (
	"strconv"
	"strings"

	"github.com/harrischu/nebula-dump/pkg/common"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var convertCmd = &cobra.Command{
	Use:   "convert",
	Short: "convert key type",
	Long:  ``,
	Example: `

convert --keyType string --key __meta_version__ --toType bytes
convert --keyType bytes --key 255,0,0 --toType int
	`,

	Run: func(c *cobra.Command, args []string) {
		key, err := common.CovertToBytes(utilsOpts.keyType, utilsOpts.key)
		if err != nil {
			common.Logger.Error(err)
			return
		}

		switch utilsOpts.toType {
		case "bytes":
			s := make([]string, 0)
			for _, b := range key {
				s = append(s, strconv.Itoa(int(b)))
			}
			common.Logger.Info(strings.Join(s, ","))

		case "int":
			var i int64
			if err := common.ConvertBytesToInt(&i, &key, common.ByteOrder); err != nil {
				common.Logger.Error(err)
			} else {
				common.Logger.Info(i)
			}
		case "string":
			common.Logger.Info(string(key))
		}
	},
}

func init() {
	flags := pflag.NewFlagSet("", pflag.ContinueOnError)
	flags.StringVar(&utilsOpts.key, "key", "", "key")
	flags.StringVar(&utilsOpts.keyType, "keyType", "", "key")
	flags.StringVar(&utilsOpts.toType, "toType", "", "convert type")
	convertCmd.PersistentFlags().AddFlagSet(flags)
	utilCmd.AddCommand(convertCmd)
}
