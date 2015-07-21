package main

import (
	"flag"
	"log"
	"net/http"

	_ "net/http/pprof"

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
	pprof := flag.Bool("pprof", false, "enable pprof")
	flag.Parse()

	if *addr == "" {
		panic("invalid address: '" + *addr + "'")
	}

	if *path == "" {
		panic("invalid data path: '" + *path + "'")
	}

	s, err := kmdb.NewServer(*addr, *path)
	if err != nil {
		panic(err)
	}

	if *pprof {
		go startPPROF()
	}

	log.Println(s.Listen())
}

func startPPROF() {
	log.Println("PPROF:  listening on ", PPROFAddr)
	log.Println(http.ListenAndServe(PPROFAddr, nil))
}
