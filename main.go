package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
)

var (
	version   = "0.1.0"
	zkAddrs   = flag.String("zk-addr", "127.0.0.1:2181", "Zookeeper Addresses, splitted by ','")
	zkPath    = flag.String("zk-path", "/hbase", "Zookeeper HBase path")
	listen    = flag.String("listen", "0.0.0.0:3130", "Listen Addresses, splitted by ','")
	goProcs   = flag.Int("cpus", 2, "The number of CPUS")
	accessLog = flag.String("access-log", "", "Path to store access log")
)

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(*goProcs)

	accessFile := os.Stdout

	if *accessLog != "" {
		f, err := os.OpenFile(*accessLog, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			fmt.Printf("open log file(%s) failed: %v\n", *accessLog, err)
			os.Exit(2)
		}
		defer f.Close()
		accessFile = f
	}

	cli, err := newClient(*zkAddrs, *zkPath, accessFile)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}

	listenAddrs := make([]string, 0)

	for _, addr := range strings.Split(*listen, ",") {
		listenAddrs = append(listenAddrs, addr)
	}

	wg := &sync.WaitGroup{}
	server, err := newRestServer(listenAddrs, wg, cli)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(-1)
	}

	server.Start()
	defer func() {
		server.Stop()
		cli.close()
	}()
	wg.Wait()
}
