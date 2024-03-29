package utils

import (
	"fmt"
	"strconv"

	"github.com/harrischu/nebula-dump/pkg/common"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var getPartIDCmd = &cobra.Command{
	Use:   "partId",
	Short: "get part id",
	Long:  ``,
	Example: `
	`,
	RunE: func(c *cobra.Command, args []string) error {
		var (
			bs  []byte
			err error
		)
		switch utilsOpts.vidType {
		case "string":
			vid := utilsOpts.vid
			bs, err = common.CovertToBytes("string", vid)
			if err != nil {
				return err
			}
		case "int":
			v, err := strconv.ParseInt(utilsOpts.vid, 10, 0)
			utilsOpts.vidLength = 8
			if err != nil {
				return err
			}
			if err := common.ConvertIntToBytes(&v, &bs, common.ByteOrder); err != nil {
				return err
			}
		default:
			return fmt.Errorf("vidType should be int or string")
		}
		id, err := common.GetPartID(bs, int32(utilsOpts.partsCount), int16(utilsOpts.vidLength))
		if err != nil {
			return err
		}
		common.Logger.Infof("part id is: %d", id)
		return nil
	},
}

func init() {
	flags := pflag.NewFlagSet("", pflag.ContinueOnError)
	flags.StringVar(&utilsOpts.vid, "vid", "", "vid")
	flags.StringVar(&utilsOpts.vidType, "vidType", "", "string or int")
	flags.IntVar(&utilsOpts.partsCount, "partsCount", 0, "parts count")
	flags.IntVar(&utilsOpts.vidLength, "vidLength", 0, "vid length, used for fixed string vid type")
	cobra.MarkFlagRequired(flags, "partsCount")
	cobra.MarkFlagRequired(flags, "vid")
	getPartIDCmd.PersistentFlags().AddFlagSet(flags)
	utilCmd.AddCommand(getPartIDCmd)
}
