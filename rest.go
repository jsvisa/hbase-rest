package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/jsvisa/logging"
	hbase "github.com/pingcap/go-hbase"
)

type restServer struct {
	mux       *http.ServeMux
	wg        *sync.WaitGroup
	listeners []net.Listener
}

type restHandler struct {
	client  *client
	handler func(*client, http.ResponseWriter, *http.Request)
}

func (rh restHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "HBaseRest-Go/"+VERSION)
	w.Header().Set("Content-Type", "application/json")

	rh.handler(rh.client, w, r)
}

type request struct {
	Table     string `json:"table"`
	Family    string `json:"family"`
	Qualifier string `json:"qualifier"`
	Row       string `json:"row"`
	Value     string `json:"value"`
}

type getResponse struct {
	Row *row `json:"row"`
}

type cell struct {
	Column    string `json:"column"`
	Timestamp uint64 `json:"timestamp"`
	Value     string `json:"value"`
}

type row struct {
	Key  string  `json:"key"`
	Cell []*cell `json:"cell"`
}

type scanResponse []*row

func (server *restServer) Start() error {
	httpServer := &http.Server{
		Handler: server.mux,
	}

	for _, listener := range server.listeners {
		server.wg.Add(1)
		go func(l net.Listener) {
			defer server.wg.Done()
			httpServer.SetKeepAlivesEnabled(true)
			httpServer.Serve(l)
		}(listener)
	}

	return nil
}

func (server *restServer) Stop() {
	for _, listener := range server.listeners {
		listener.Close()
	}
}

// tcpKeepAliveListener sets TCP keep-alive timeouts on accepted
// connections. It's used by ListenAndServe and ListenAndServeTLS so
// dead TCP connections (e.g. closing laptop mid-download) eventually
// go away.
type tcpKeepAliveListener struct {
	*net.TCPListener
}

func (ln tcpKeepAliveListener) Accept() (c net.Conn, err error) {
	tc, err := ln.AcceptTCP()
	if err != nil {
		return
	}
	tc.SetKeepAlive(true)
	tc.SetKeepAlivePeriod(15 * time.Second)
	return tc, nil
}

func newRestServer(listenAddresses []string, wg *sync.WaitGroup, cli *client) (*restServer, error) {
	server := &restServer{
		wg:        wg,
		mux:       http.NewServeMux(),
		listeners: make([]net.Listener, 0, len(listenAddresses)),
	}

	server.mux.Handle("/", restHandler{cli, handle})

	for _, listenAddress := range listenAddresses {
		ln, err := net.Listen("tcp", listenAddress)
		if err != nil {
			for _, listenerToClose := range server.listeners {
				closeErr := listenerToClose.Close()
				if closeErr != nil {
					fmt.Printf("Could not close listener: %v", closeErr)
				}
			}
			return nil, err
		}
		server.listeners = append(server.listeners, tcpKeepAliveListener{ln.(*net.TCPListener)})
	}

	return server, nil
}

type customResponseWriter struct {
	http.ResponseWriter
	status int
	size   int
}

func (c *customResponseWriter) WriteHeader(status int) {
	c.status = status
	c.ResponseWriter.WriteHeader(status)
}

func (c *customResponseWriter) Write(b []byte) (int, error) {
	size, err := c.ResponseWriter.Write(b)
	c.size += size
	return size, err
}

// request path is /table/column/row
func handle(cli *client, w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 4 {
		w.WriteHeader(400)
		w.Write([]byte("Wrong URL"))
		return
	}

	column := parts[2]

	columnParts := strings.Split(column, ":")
	if len(columnParts) != 2 {
		w.WriteHeader(400)
		w.Write([]byte("Wrong Column"))
		return
	}
	req := &request{
		Table:     parts[1],
		Family:    columnParts[0],
		Qualifier: columnParts[1],
		Row:       "/" + strings.Join(parts[3:], "/"),
	}

	cw := &customResponseWriter{
		ResponseWriter: w,
		size:           0,
		status:         200,
	}

	switch r.Method {
	case "GET":
		handleGet(cli, cw, r, req)
	case "PUT":
		handlePut(cli, cw, r, req)
	case "DELETE":
		handleDelete(cli, cw, r, req)
	}

	cli.logger.Printf("(%s) \"%s %s %s\" %d %d %d",
		r.RemoteAddr, r.Method, r.RequestURI, r.Proto,
		cw.status, cw.size,
		int(time.Since(start).Seconds()*1000))

	return
}

func handleScan(cli *client, w http.ResponseWriter, r *http.Request, req *request) error {
	qs := r.URL.Query()

	batch := 100
	if b := qs.Get("batch"); b != "" {
		if bb, err := strconv.ParseInt(b, 10, 64); err == nil {
			batch = int(bb)
		}
	}

	end := qs.Get("end")

	scan := hbase.NewScan([]byte(req.Table), batch, cli.cli)
	defer scan.Close()

	scan.StartRow = []byte(req.Row)
	scan.StopRow = []byte(end)

	body := make(scanResponse, 0)
	cnt := 0
	for {
		r := scan.Next()
		if r == nil || scan.Closed() || cnt >= batch {
			break
		}

		cells := make([]*cell, 0)

		for k, v := range r.Columns {
			cells = append(cells, &cell{
				Column:    k,
				Value:     string(v.Value),
				Timestamp: v.Ts,
			})
		}

		body = append(body, &row{
			Key:  string(r.Row),
			Cell: cells,
		})
		cnt++
	}

	bodyStr, err := json.Marshal(body)
	if err != nil {
		w.WriteHeader(503)
		return nil
	}
	w.WriteHeader(200)
	w.Write(bodyStr)

	return nil
}

func handleGet(cli *client, w http.ResponseWriter, r *http.Request, req *request) error {
	qs := r.URL.Query()
	isList := false

	if qs.Get("list") == "true" {
		isList = true
	}

	if isList {
		return handleScan(cli, w, r, req)
	}

	g := hbase.NewGet([]byte(req.Row))
	g.AddStringColumn(req.Family, req.Qualifier)
	resp, err := cli.cli.Get(req.Table, g)

	if err != nil {
		log.Error(err)
		http.Error(w, err.Error(), 503)
		return err
	}

	if resp == nil {
		w.WriteHeader(404)
		return err
	}
	cells := make([]*cell, 0)

	for k, v := range resp.Columns {
		cells = append(cells, &cell{
			Column:    k,
			Value:     string(v.Value),
			Timestamp: v.Ts,
		})
	}

	row := &row{
		Key:  string(resp.Row),
		Cell: cells,
	}

	bodyStr, err := json.Marshal(row)

	if err != nil {
		http.Error(w, err.Error(), 503)
		return err
	}
	w.WriteHeader(200)
	w.Write(bodyStr)

	return nil
}

func handlePut(cli *client, w http.ResponseWriter, r *http.Request, req *request) error {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Errorf("read body failed: %v", err)
		w.WriteHeader(414)
		return err
	}

	previousExisted := false
	resp, err := getRow(cli.cli, req.Table, req.Family, req.Qualifier, req.Row)
	if err == nil && resp != nil {
		previousExisted = true
	}

	p := hbase.NewPut([]byte(req.Row))
	p.AddTimestamp(uint64(time.Now().Unix()))
	p.AddStringValue(req.Family, req.Qualifier, string(body))
	succeed, err := cli.cli.Put(req.Table, p)
	if err != nil {
		log.Error(err)
		w.WriteHeader(503)
		return err
	}

	if !succeed {
		log.Errorf("PUT %s false", req.Row)
	}

	if previousExisted {
		w.WriteHeader(204)
	} else {
		w.WriteHeader(201)
	}

	return nil
}

func handleDelete(cli *client, w http.ResponseWriter, r *http.Request, req *request) error {
	previousExisted := false
	resp, err := getRow(cli.cli, req.Table, req.Family, req.Qualifier, req.Row)
	if err == nil && resp != nil {
		previousExisted = true
	}

	d := hbase.NewDelete([]byte(req.Row))
	d.AddStringColumn(req.Family, req.Qualifier)
	_, err = cli.cli.Delete(req.Table, d)

	if err != nil {
		log.Error(err)
		return err
	}

	if previousExisted {
		w.WriteHeader(204)
	} else {
		w.WriteHeader(404)
	}

	return nil
}

func getRow(cli hbase.HBaseClient, table, family, qualifier, row string) (*hbase.ResultRow, error) {
	g := hbase.NewGet([]byte(row))
	g.AddStringColumn(family, qualifier)
	return cli.Get(table, g)
}
