package cmd

import (
	"strconv"
	"strings"

	"github.com/harrischu/nebula-dump/pkg"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
	// key string
	toType string
)

var utilCmd = &cobra.Command{
	Use:               "utils",
	Short:             "some utils command",
	Long:              ``,
	CompletionOptions: cobra.CompletionOptions{HiddenDefaultCmd: true},
}

var convertCmd = &cobra.Command{
	Use:   "convert",
	Short: "convert key type",
	Long:  ``,
	Example: `

convert --keyType string --key __meta_version__ --toType bytes
convert --keyType bytes --key 255,0,0 --toType int
	`,

	Run: func(cmd *cobra.Command, args []string) {
		key, err := covertToBytes(keyType, key)
		if err != nil {
			logger.Error(err)
			return
		}

		switch toType {
		case "bytes":
			s := make([]string, 0)
			for _, b := range key {
				s = append(s, strconv.Itoa(int(b)))
			}
			logger.Info(strings.Join(s, ","))
		case "int":
			data, err := pkg.ConvertBytesToInt(key)
			if err != nil {
				logger.Error(err)
			} else {
				logger.Info(data)
			}
		case "string":
			logger.Info(string(key))
		}
	},
}

func init() {
	rootCmd.AddCommand(utilCmd)
	flags := pflag.NewFlagSet("", pflag.ContinueOnError)
	flags.StringVarP(&key, "key", "", "", "key")
	flags.StringVarP(&toType, "toType", "", "", "convert type")
	utilCmd.PersistentFlags().AddFlagSet(flags)
	utilCmd.AddCommand(convertCmd)
}
