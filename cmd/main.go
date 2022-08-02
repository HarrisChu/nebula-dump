package cmd

import (
	"os"

	_ "github.com/harrischu/nebula-dump/cmd/meta"
	"github.com/harrischu/nebula-dump/cmd/root"
	_ "github.com/harrischu/nebula-dump/cmd/storage"
	_ "github.com/harrischu/nebula-dump/cmd/utils"
)

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := root.RootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}

}
