package root

import (
	"os"

	"github.com/harrischu/nebula-dump/pkg"
	"github.com/harrischu/nebula-dump/pkg/common"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
	keyType    string
	rocksdbDir string
	v          bool
)

// RootCmd represents the base command when called without any subcommands
var RootCmd = &cobra.Command{
	Use:   "nebula-dump",
	Short: "A tool to decode nebula-graph data",

	CompletionOptions: cobra.CompletionOptions{HiddenDefaultCmd: false},
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func init() {

	RootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		common.SetUpLogs(os.Stdout, v)
		return nil
	}
}

var Opts = pkg.Option{}

func CommonFlagSetOption() *pflag.FlagSet {
	flags := pflag.NewFlagSet("", pflag.ContinueOnError)
	flags.Int32Var(&Opts.SpaceID, "space", -1, "nebula space id")
	flags.Int32Var(&Opts.PartID, "part", -1, "nebula part id")
	flags.Int32Var(&Opts.TagID, "tag", -1, "nebula tag id")
	flags.Int32Var(&Opts.EdgeID, "edge", 0, "nebula edge id")
	flags.Int32Var(&Opts.IndexID, "index", -1, "nebula index id")
	flags.IntVar(&Opts.Limit, "limit", 20, "limit result")
	flags.BoolVarP(&v, "verbose", "v", false, "verbose")
	return flags
}
