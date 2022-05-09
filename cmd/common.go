package cmd

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/harrischu/nebula-dump/pkg/common"
	"github.com/spf13/pflag"
)

type options struct {
	flagset *pflag.FlagSet
}

func (o *options) addFlag(fs *pflag.FlagSet) {
	o.flagset.AddFlagSet(fs)
}

func (o *options) getFlag() *pflag.FlagSet {
	if o.flagset != nil {
		return o.flagset
	}
	return nil
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func covertToBytes(keyType string, s string) ([]byte, error) {
	switch keyType {
	case "bytes":
		return byteStringToBytes(s)
	case "string":
		return stringToBytes(s)
	case "int":
		return intToBytes(s)
	default:
		return nil, fmt.Errorf("cannot find the type")
	}

}

func byteStringToBytes(s string) ([]byte, error) {
	result := make([]byte, 0)
	ss := strings.Split(s, ",")
	for _, v := range ss {
		i, err := strconv.ParseUint(v, 10, 8)
		if err != nil {
			return nil, err
		}
		result = append(result, byte(i))
	}
	return result, nil
}

func stringToBytes(s string) ([]byte, error) {
	return []byte(s), nil
}

func intToBytes(s string) ([]byte, error) {
	var data []byte
	d, err := strconv.Atoi(s)
	if err != nil {
		return nil, err
	}
	if err := common.ConvertIntToBytes(&d, &data, common.ByteOrder); err != nil {
		return nil, err
	}
	return data, nil
}
