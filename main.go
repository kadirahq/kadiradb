package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io/ioutil"
	"net/http"
	"path"
	"time"

	_ "net/http/pprof"

	"github.com/gogo/protobuf/proto"
	"github.com/kadirahq/go-tools/logger"
	"github.com/kadirahq/go-tools/monitor"
)

const (
	// DefaultAddr is the default address the server will listen to
	DefaultAddr = ":19000"

	// DefaultData is the default data path to store db files
	DefaultData = "/tmp/kadiradb"

	// InitFile contains a json array of databases which will be created
	// or updated when starting kadiradb. Init process will be skipped
	// if the file is missing or invalid.
	InitFile = "init.json"

	// PPROFAddr is the address the pprof server will listen to
	// Make sure this port cannot be accessed from outside
	PPROFAddr = ":6060"
)

var (
	// Logger logs stuff
	Logger = logger.New("kadiradb")
)

func main() {
	addr := flag.String("addr", DefaultAddr, "server address")
	data := flag.String("data", DefaultData, "data to store data files")
	recv := flag.Bool("recv", false, "enable recovery")
	flag.Parse()

	if *addr == "" {
		panic("invalid address: '" + *addr + "'")
	}

	if *data == "" {
		panic("invalid datadir: '" + *data + "'")
	}

	s, err := NewServer(&Options{
		Path:     *data,
		Address:  *addr,
		Recovery: *recv,
	})

	if err != nil {
		panic(err)
	}

	ipath := path.Join(*data, InitFile)
	idata, err := ioutil.ReadFile(ipath)
	if err == nil {
		load(s, idata)
	}

	go printAppMetrics()
	go startPPROFServer()
	Logger.Info(s.Listen())
}

func load(s Server, data []byte) {
	defer Logger.Time(time.Now(), time.Second, "load")
	var reqs []*OpenReq

	err := json.Unmarshal(data, &reqs)
	if err != nil {
		Logger.Error(err)
		return
	}

	for _, req := range reqs {
		reqData, err := proto.Marshal(req)
		if err != nil {
			Logger.Error(err)
			return
		}

		_, err = s.Open(reqData)
		if err != nil {
			Logger.Error(err)
			return
		}
	}
}

func printAppMetrics() {
	buff := bytes.NewBuffer(nil)
	encd := json.NewEncoder(buff)

	for _ = range time.Tick(time.Minute) {
		buff.Reset()
		vals := monitor.Values()
		if err := encd.Encode(vals); err == nil {
			data := buff.Bytes()
			Logger.Print("metrics", string(data))
		}
	}
}

func startPPROFServer() {
	Logger.Info("PPROF: listening on: " + PPROFAddr)
	Logger.Info(http.ListenAndServe(PPROFAddr, nil))
}
