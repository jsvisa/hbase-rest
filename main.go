package main

import (
	"flag"
	"fmt"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/ngaut/log"
	hbase "github.com/pingcap/go-hbase"
)

var (
	rowPrefix = flag.String("p", "", "row prefix")
	method    = flag.String("m", "put", "put, delete, scan, get")
	onlyInit  = flag.Bool("init", false, "only init the hbase schema")
)

var benchTbl = "go-hbase-test"
var cli hbase.HBaseClient

func createTable(tblName string) error {
	desc := hbase.NewTableDesciptor(tblName)
	desc.AddColumnDesc(hbase.NewColumnFamilyDescriptor("cf"))
	err := cli.CreateTable(desc, nil)
	if err != nil {
		dropTable(tblName)
		err = createTable(tblName)
		if err != nil {
			panic(err)
		}
	}
	return err
}

func dropTable(tblName string) {
	cli.DisableTable(tblName)
	cli.DropTable(tblName)
}

func main() {
	flag.Parse()
	runtime.GOMAXPROCS(1)

	prefix := *rowPrefix
	if prefix == "" {
		ts := time.Now()
		prefix = fmt.Sprintf("%d", ts.UnixNano())
	}

	var err error
	cli, err = hbase.NewClient([]string{"10.10.1.0:2181"}, "/hbase")
	if err != nil {
		panic(err)
	}

	if *onlyInit {
		dropTable(benchTbl)
		createTable(benchTbl)
		return
	}

	go func() {
		log.Error(http.ListenAndServe("0.0.0.0:8889", nil))
	}()

	ct := time.Now()
	wg := &sync.WaitGroup{}
	for j := 0; j < 100; j++ {
		wg.Add(1)
		go func(j int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				row := []byte(fmt.Sprintf("row_%s_%d_%d", prefix, i, j))

				log.Infof(string(row))

				switch *method {
				case "get":
					g := hbase.NewGet(row)

					resp, err := cli.Get(benchTbl, g)

					if err != nil {
						log.Error(err)
						return
					}
					if resp != nil {
						log.Infof("get result: %v", resp.Columns)
					}
				case "put":
					p := hbase.NewPut(row)
					p.AddStringValue("cf", "q", "val_"+strconv.Itoa(i*j))
					_, err := cli.Put(benchTbl, p)
					if err != nil {
						log.Error(err)
						return
					}
				case "delete":
					d := hbase.NewDelete(row)
					_, err := cli.Delete(benchTbl, d)

					if err != nil {
						log.Error(err)
						return
					}
				case "scan":
					scan := hbase.NewScan([]byte(benchTbl), 100, cli)
					defer scan.Close()

					scan.StartRow = []byte("row_")
					// scan.StopRow = []byte("row")

					cnt := 0
					for {
						r := scan.Next()
						if r == nil || scan.Closed() {
							break
						}
						log.Infof("scan get %s", string(r.Row))
						cnt++
					}

				}
			}
		}(j)
	}
	wg.Wait()
	elapsed := time.Since(ct)
	log.Errorf("took %s", elapsed)
}
