package main

import (
	"io"
	"log"
	"strings"

	"github.com/jsvisa/logging"
	hbase "github.com/pingcap/go-hbase"
)

type client struct {
	cli    hbase.HBaseClient
	logger *log.Logger
}

func newClient(zkHosts string, zkRoot string, logFile io.Writer) (*client, error) {
	addrs := make([]string, 0)

	for _, addr := range strings.Split(zkHosts, ",") {
		addrs = append(addrs, addr)
	}

	cli, err := hbase.NewClient(addrs, zkRoot)
	if err != nil {
		logging.Warningf("NewClient failed: %v", err)
		return nil, err
	}
	flags := log.LstdFlags

	return &client{
		cli:    cli,
		logger: log.New(logFile, "", flags),
	}, nil
}

func (c *client) close() {
	c.cli.Close()
}
