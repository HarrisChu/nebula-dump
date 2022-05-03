package cmd

import (
	"io"
	"os"

	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var (
	keyType    string
	rocksdbDir string
	v          bool
	logger     *logrus.Logger
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "nebula-dump",
	Short: "A tool to decode nebula-graph data",

	CompletionOptions: cobra.CompletionOptions{HiddenDefaultCmd: false},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}

}

func init() {
	flags := pflag.NewFlagSet("", pflag.ContinueOnError)
	flags.StringVarP(&keyType, "keyType", "", "", "key type (string, bytes)")
	flags.StringVarP(&rocksdbDir, "dir", "", "", "rocksdb data directory")
	flags.BoolVarP(&v, "verbose", "v", false, "enable verbose logging")
	cobra.MarkFlagRequired(flags, "dir")
	must(cobra.MarkFlagDirname(flags, "dir"))
	// rootCmd.PersistentFlags().AddFlagSet(flags)

	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		setUpLogs(os.Stdout, v)
		return nil
	}
}

func setUpLogs(out io.Writer, verbose bool) {
	logger = logrus.New()
	logger.SetOutput(out)
	if verbose {
		logrus.SetLevel(logrus.DebugLevel)
	} else {
		logrus.SetLevel(logrus.InfoLevel)
	}
}
