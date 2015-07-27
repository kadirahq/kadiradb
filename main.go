package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"

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
	init := flag.String("init", "", "json string to create initial dbs")
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

func createDatabases(s kmdb.Server, init string) {
	var reqs []*kmdb.OpenReq
	data := []byte(init)

	err := json.Unmarshal(data, &reqs)
	if err != nil {
		log.Println(err)
		return
	}

	for _, req := range reqs {
		reqData, err := proto.Marshal(req)
		if err != nil {
			log.Println(err)
			return
		}

		_, err = s.Open(reqData)
		if err != nil {
			log.Println(err)
			return
		}
	}
}

func startPPROF() {
	log.Println("PPROF:  listening on ", PPROFAddr)
	log.Println(http.ListenAndServe(PPROFAddr, nil))
}
