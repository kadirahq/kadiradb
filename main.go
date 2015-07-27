package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"

	_ "net/http/pprof"

	"github.com/gogo/protobuf/proto"
	"github.com/kadirahq/kadiradb-metrics/kmdb"
)

const (
	// DefaultAddr is the default address the server will listen to
	DefaultAddr = ":19000"

	// PPROFAddr is the address the pprof server will listen to
	// Make sure this port cannot be accessed from outside
	PPROFAddr = ":6060"
)

func main() {
	addr := flag.String("addr", DefaultAddr, "server address")
	path := flag.String("path", "", "path to store data files")
	init := flag.String("init", "", "path of initial dbs file")
	pprof := flag.Bool("pprof", false, "enable pprof")
	write := flag.Bool("write", false, "enable recovery")

	flag.Parse()

	if *addr == "" {
		panic("invalid address: '" + *addr + "'")
	}

	if *path == "" {
		panic("invalid data path: '" + *path + "'")
	}

	s, err := kmdb.NewServer(&kmdb.Options{
		Path:     *path,
		Address:  *addr,
		Recovery: *write,
	})

	if *init != "" {
		createDatabases(s, *init)
	}

	if err != nil {
		panic(err)
	}

	if *pprof {
		go startPPROF()
	}

	log.Println(s.Listen())
}

func createDatabases(s kmdb.Server, name string) {
	file, err := os.Open(name)
	if err != nil {
		panic(err)
	}

	info, err := file.Stat()
	if err != nil {
		panic(err)
	}

	sz := info.Size()
	data := make([]byte, sz)

	n, err := file.Read(data)
	if err != nil {
		panic(err)
	} else if int64(n) != sz {
		panic("error reading init file")
	}

	var reqs []*kmdb.OpenReq
	err = json.Unmarshal(data, &reqs)
	if err != nil {
		panic(err)
	}

	for _, req := range reqs {
		reqData, err := proto.Marshal(req)
		if err != nil {
			panic(err)
		}

		_, err = s.Open(reqData)
		if err != nil {
			panic(err)
		}
	}
}

func startPPROF() {
	log.Println("PPROF:  listening on ", PPROFAddr)
	log.Println(http.ListenAndServe(PPROFAddr, nil))
}
