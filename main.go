package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync"
)

const VERSION = "0.1.0"

var (
	zkAddrs   = flag.String("zk-addr", "127.0.0.1:2181", "Zookeeper Addresses, splitted by ','")
	zkPath    = flag.String("zk-path", "/hbase", "Zookeeper HBase path")
	listen    = flag.String("listen", "0.0.0.0:3130", "Listen Addresses, splitted by ','")
	accessLog = flag.String("log", "", "Path to store access log")
	version   = flag.Bool("v", false, "Print version")
	goProcs   = flag.Int("cpus", 2, "The number of CPUS")
)

func main() {
	flag.Parse()

	if *version {
		fmt.Printf("hbase-rest version: %s\n", VERSION)
		os.Exit(0)
	}
	if *goProcs > 0 {
		runtime.GOMAXPROCS(*goProcs)
	}

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
