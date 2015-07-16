package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/kadirahq/kadiradb-metrics/kmdb"
)

const (
	// DefaultAddr is the default address the server will listen to
	DefaultAddr = ":19000"

	// DefaultPath is the default address the server will listen to
	DefaultPath = "/tmp/kmdb"

	// PPROFAddr is the address the pprof server will listen to
	// Make sure this port cannot be accessed from outside
	PPROFAddr = ":6060"
)

func main() {
	// start pprof
	go startPPROF()

	address := flag.String("addr", DefaultAddr, "server address")
	basePath := flag.String("path", DefaultPath, "data path")
	flag.Parse()

	if *address == "" {
		panic("invalid address")
	}

	// finally, start the simple-rpc server on main
	// app will exit if/when simple-rpc server crashes
	s, err := kmdb.NewServer(*address, *basePath)
	if err != nil {
		panic(err)
	}

	log.Println(s.Listen())
}

func startPPROF() {
	log.Println("PPROF:  listening on ", PPROFAddr)
	log.Println(http.ListenAndServe(PPROFAddr, nil))
}
