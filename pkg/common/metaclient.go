package common

import (
	"fmt"
	"time"

	"github.com/facebook/fbthrift/thrift/lib/go/thrift"
	"github.com/vesoft-inc/nebula-go/v3/nebula"
	"github.com/vesoft-inc/nebula-go/v3/nebula/meta"
)

type MetaClient struct {
	Client *meta.MetaServiceClient
}

const (
	defaultTimeout = 120 * time.Second
)

func NewMetaClient(address string) (*MetaClient, error) {
	timeoutOption := thrift.SocketTimeout(defaultTimeout)
	addressOption := thrift.SocketAddr(address)
	sock, err := thrift.NewSocket(timeoutOption, addressOption)
	if err != nil {
		return nil, fmt.Errorf("open socket failed: %w", err)
	}

	bufferedTranFactory := thrift.NewBufferedTransportFactory(128 << 10)
	transport := thrift.NewFramedTransport(bufferedTranFactory.GetTransport(sock))
	pf := thrift.NewBinaryProtocolFactoryDefault()
	client := meta.NewMetaServiceClientFactory(transport, pf)
	c := &MetaClient{Client: client}
	if err := c.open(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *MetaClient) open() error {
	if !c.Client.IsOpen() {
		if err := c.Client.Open(); err != nil {
			return err
		}
	}
	// if connect to follow
	req := meta.NewListClusterInfoReq()

	resp, err := c.Client.ListCluster(req)
	if err != nil {
		return err
	}
	if resp.Code == nebula.ErrorCode_E_LEADER_CHANGED {
		address := fmt.Sprintf("%s:%d", resp.Leader.Host, resp.Leader.Port)
		newClient, err := NewMetaClient(address)
		if err != nil {
			return err
		}
		if err := newClient.open(); err != nil {
			return err
		}
		c.Client.Close()
		c.Client = newClient.Client
	}

	return nil
}

func (c *MetaClient) Close() error {
	if c.Client != nil {
		return c.Client.Close()
	}
	return nil
}

func (c *MetaClient) listTagSchema(space int32) (*meta.ListTagsResp, error) {
	if err := c.open(); err != nil {
		return nil, err
	}
	req := meta.NewListTagsReq()
	req.SpaceID = space
	resp, err := c.Client.ListTags(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
