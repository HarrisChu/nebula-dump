package cmd

import (
	"strconv"
	"strings"

	"github.com/harrischu/nebula-dump/pkg"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
	out        string
	partString string
)

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "export part in rocksdb",
	Long:  ``,
	Example: `
nebula-dump export --dir nebula/data/storage/nebula/1/data/ -out . -parts 1,2,3
	`,
	Run: func(cmd *cobra.Command, args []string) {
		parts := make([]int, 0)
		for _, i := range strings.Split(partString, ",") {
			n, err := strconv.Atoi(i)
			if err != nil {
				logger.Error(err)
				return
			}
			parts = append(parts, n)
		}

		exporter := &pkg.RocksdbDump{}
		err := exporter.Open(rocksdbDir)
		if err != nil {
			logger.Error(err)
			return
		}
		exporter.Export(out, parts)
	},
}

func init() {
	rootCmd.AddCommand(exportCmd)
	flags := pflag.NewFlagSet("", pflag.ContinueOnError)
	flags.StringVarP(&out, "out", "", ".", "output folder")
	flags.StringVarP(&partString, "partNum", "", "", "export part")
	exportCmd.PersistentFlags().AddFlagSet(flags)

	must(cobra.MarkFlagRequired(flags, "out"))
	must(cobra.MarkFlagRequired(flags, "partNum"))
}
