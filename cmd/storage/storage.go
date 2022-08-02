package storage

import (
	"github.com/harrischu/nebula-dump/cmd/root"
	"github.com/harrischu/nebula-dump/pkg"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/harrischu/nebula-dump/pkg/common"
	_ "github.com/harrischu/nebula-dump/pkg/storage"
)

type storageOptsType struct {
	raw     bool
	path    string
	keyType string
}

var storageOpts storageOptsType

var storageCmd = &cobra.Command{
	Use:               "storage",
	Short:             "stroage commnads",
	Long:              ``,
	CompletionOptions: cobra.CompletionOptions{HiddenDefaultCmd: true},
}

func init() {
	root.RootCmd.AddCommand(storageCmd)
	flags := pflag.NewFlagSet("", pflag.ContinueOnError)
	flags.StringVar(&storageOpts.path, "path", "", "storage rocksdb data path")
	flags.BoolVar(&storageOpts.raw, "raw", false, "raw data")
	flags.StringVar(&root.Opts.VID, "vid", "", "vid")
	flags.StringVar(&root.Opts.Src, "src", "", "vid")
	flags.StringVar(&root.Opts.Dst, "dst", "", "vid")
	flags.StringVar(&root.Opts.MetaAddres, "meta", "", "meta address. e.g. 192.168.8.6:9559")

	cobra.MarkFlagRequired(flags, "meta")
	storageCmd.PersistentFlags().AddFlagSet(flags)

	storageCmd.PersistentFlags().AddFlagSet(root.CommonFlagSetOption())

	for t := range pkg.StorageKeyTypeMap {
		r := t
		c := &cobra.Command{
			Use:               string(r),
			CompletionOptions: cobra.CompletionOptions{HiddenDefaultCmd: true},
			RunE: func(cmd *cobra.Command, args []string) error {
				return runStorage(r)
			},
		}
		storageCmd.AddCommand(c)
	}
}

func runStorage(t pkg.StorageKeyType) error {
	dumper, err := pkg.NewStorageParser(storageOpts.path, t, &root.Opts)
	if err != nil {
		return err
	}
	if storageOpts.raw {
		rs, err := dumper.Prefix()
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
		rs, err := dumper.ParseAll()
		if err != nil {
			return err
		}
		for _, r := range rs {
			common.Logger.Infof("key: %s, value: %s", r.Key, r.Value)
		}
	}
	return nil
}
