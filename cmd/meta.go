package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var metaCmd = &cobra.Command{
	Use:   "meta",
	Short: "decode meta data in rocksdb",
}

func init() {
	rootCmd.AddCommand(metaCmd)
	metaCmd.AddCommand(prefixCmd)
	o := new(options)
	o.addFlag(nil)
	metaCmd.Flags().AddFlagSet(o.getFlag())
}

var validArgs = []string{"pod", "node", "service", "replicationcontroller"}

var prefixCmd = &cobra.Command{
	Use:   "prefix",
	Short: "",
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("must provide a valid prefix")
		}
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		// p := args[0]
		return nil
	},
	ValidArgs: validArgs,
}
