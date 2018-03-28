package main

import (
	"strings"

	"github.com/jsvisa/logging"
	hbase "github.com/pingcap/go-hbase"
)

type client struct {
	cli hbase.HBaseClient
}

func newClient(zkHosts string, zkRoot string) (*client, error) {
	addrs := make([]string, 0)

	for _, addr := range strings.Split(zkHosts, ",") {
		addrs = append(addrs, addr)
	}

	cli, err := hbase.NewClient(addrs, zkRoot)
	if err != nil {
		logging.Warningf("NewClient failed: %v", err)
		return nil, err
	}

	return &client{
		cli: cli,
	}, nil
}

func (c *client) close() {
	c.cli.Close()
}
