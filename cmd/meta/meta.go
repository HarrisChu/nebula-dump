package meta

import (
	"github.com/harrischu/nebula-dump/cmd/root"
	"github.com/harrischu/nebula-dump/pkg"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/harrischu/nebula-dump/pkg/common"
	_ "github.com/harrischu/nebula-dump/pkg/meta"
)

type metaOptsType struct {
	raw     bool
	path    string
	keyType string
}

var metaOpts metaOptsType

var metaCmd = &cobra.Command{
	Use:               "meta",
	Short:             "meta commnads",
	Long:              ``,
	CompletionOptions: cobra.CompletionOptions{HiddenDefaultCmd: true},
}

func init() {
	root.RootCmd.AddCommand(metaCmd)
	flags := pflag.NewFlagSet("", pflag.ContinueOnError)
	flags.StringVar(&metaOpts.path, "path", "", "meta rocksdb data path")
	flags.BoolVar(&metaOpts.raw, "raw", false, "raw data")
	metaCmd.PersistentFlags().AddFlagSet(flags)
	metaCmd.PersistentFlags().AddFlagSet(root.CommonFlagSetOption())

	for t := range pkg.MetaKeyTypeMap {
		r := t
		c := &cobra.Command{
			Use:               string(r),
			CompletionOptions: cobra.CompletionOptions{HiddenDefaultCmd: true},
			RunE: func(cmd *cobra.Command, args []string) error {
				return runMeta(r)
			},
		}
		metaCmd.AddCommand(c)
	}
}

func runMeta(t pkg.MetaKeyType) error {
	metaDump, err := pkg.NewMetaParser(metaOpts.path, t, &root.Opts)
	if err != nil {
		return err
	}
	if metaOpts.raw {
		rs, err := metaDump.Prefix()
		if err != nil {
			return err
		}
		for _, r := range rs {
			var k, v string
			common.ConvertBytesToString(&k, &r.Key)
			common.ConvertBytesToString(&v, &r.Value)
			common.Logger.Infof("key: %s, value: %s", k, v)

		}
	} else {
		rs, err := metaDump.ParseAll()
		if err != nil {
			return err
		}
		for _, r := range rs {
			common.Logger.Infof("key: %s, value: %s", r.Key, r.Value)
		}
	}
	return nil
}
