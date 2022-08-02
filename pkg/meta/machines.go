package meta

import (
	"bytes"
	"fmt"

	"github.com/harrischu/nebula-dump/pkg"
	"github.com/harrischu/nebula-dump/pkg/common"
)

// machineParser
// key: __machines__ + length of host(8bit) + host+ port
// value:
type machineParser struct {
	opts   *pkg.Option
	key    string
	engine *common.Engine
}

func (p *machineParser) New(engine *common.Engine, opts *pkg.Option) pkg.Parser {
	return &machineParser{opts, "__machines__", engine}
}

func (p *machineParser) Parse(kv *common.KV) (*common.KVString, error) {
	var (
		kvstring = &common.KVString{}

		lengthNum int64
		hostStr   string
		portNum   int32
	)
	s := []byte(p.key)
	l := len(s)
	if !bytes.Equal(kv.Key[:len(s)], s) {
		return nil, fmt.Errorf("cannot parse key")
	}
	hostAddr := kv.Key[l:]
	length := hostAddr[:8]

	if err := common.ConvertBytesToInt(&lengthNum, &length, common.ByteOrder); err != nil {
		return nil, err
	}
	host := hostAddr[8 : 8+lengthNum]
	port := hostAddr[8+lengthNum:]

	hostStr = string(host)
	if err := common.ConvertBytesToInt(&portNum, &port, common.ByteOrder); err != nil {
		return nil, err
	}

	kvstring.Key = fmt.Sprintf(
		"host:%s, port:%v",
		hostStr,
		portNum,
	)
	return kvstring, nil
}

func (p *machineParser) Prefix() ([]*common.KV, error) {
	s := []byte(p.key)

	return p.engine.Prefix(s, p.opts.Limit)
}
