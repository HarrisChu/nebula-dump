package meta

import (
	"bytes"
	"fmt"
	"time"

	"github.com/harrischu/nebula-dump/pkg"
	"github.com/harrischu/nebula-dump/pkg/common"
)

// hostParser
// key: __hosts__ + length of hosts(8bit) + hosts + port (4bit)
// value:  dataversion(1bit) + timestamp(8bit) + role(4bit) + sha length(8bit) + sha
type hostParser struct {
	opts   *pkg.Option
	key    string
	engine *common.Engine
}

func (p *hostParser) New(engine *common.Engine, opts *pkg.Option) pkg.Parser {
	return &hostParser{opts, "__hosts__", engine}
}

func (p *hostParser) Parse(kv *common.KV) (*common.KVString, error) {
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
	v, err := p.getValue(kv.Value)
	if err != nil {
		return nil, err
	}
	kvstring.Value = v

	return kvstring, nil
}

func (p *hostParser) Prefix() ([]*common.KV, error) {
	s := []byte(p.key)

	return p.engine.Prefix(s, p.opts.Limit)
}

func (p *hostParser) getValue(v []byte) (string, error) {
	var (
		dataVersion int8
		lastHBInMs  int64
		roleNum     int32
		shaLength   int64
		sha         string
	)
	dataV := v[:1]

	if err := common.ConvertBytesToInt(&dataVersion, &dataV, common.ByteOrder); err != nil {
		return "", err
	}
	if dataVersion != 2 {
		return "", fmt.Errorf("data format is invalid")
	}
	t, r, l := v[1:1+8], v[1+8:1+8+4], v[1+8+4:1+8+4+8]

	if err := common.ConvertBytesToInt(&lastHBInMs, &t, common.ByteOrder); err != nil {
		return "", err
	}
	if err := common.ConvertBytesToInt(&roleNum, &r, common.ByteOrder); err != nil {
		return "", err
	}
	if err := common.ConvertBytesToInt(&shaLength, &l, common.ByteOrder); err != nil {
		return "", err
	}

	sha = string(v[1+8+4+8 : 1+8+4+8+shaLength])
	tm := time.Unix(lastHBInMs/1e3, (lastHBInMs%1e3)*1e6)
	return fmt.Sprintf("time:%s, role:%d, sha: %s", tm.Format("2006-01-02T15:04:05.000Z"), roleNum, sha), nil
}
