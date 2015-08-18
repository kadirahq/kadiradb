package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"net/http"
	"path"

	_ "net/http/pprof"

	"github.com/gogo/protobuf/proto"
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

	go startPPROFServer()
	Logger.Log(s.Listen())
}

func load(s Server, data []byte) {
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

func startPPROFServer() {
	Logger.Log("PPROF: listening on: " + PPROFAddr)
	Logger.Log(http.ListenAndServe(PPROFAddr, nil))
}
