package main

import (
	"fmt"

	"github.com/kadirahq/kadiradb-metrics/kmdb"
)

func main() {
	// create a new bddp client
	c := kmdb.NewClient("localhost:19000")
	// connect to given address
	if err := c.Connect(); err != nil {
		panic("ERROR: could not connect")
	}

	sendOpenReq(c)
	sendInfoReq(c)
}

func sendOpenReq(c kmdb.Client) {
	req := &kmdb.OpenReq{
		Name:          "test",
		Resolution:    60000000000,
		EpochDuration: 3600000000000,
		PayloadSize:   16,
		SegmentLength: 100000,
		MaxROEpochs:   10,
		MaxRWEpochs:   2,
	}

	_, err := c.Open(req)
	if err != nil {
		panic(err)
	}
}

func sendInfoReq(c kmdb.Client) {
	req := &kmdb.InfoReq{}
	res, err := c.Info(req)
	if err != nil {
		panic(err)
	}

	fmt.Println("Info:", res)
}
