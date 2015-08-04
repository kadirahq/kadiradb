package main

import (
	"os/exec"
	"testing"

	"github.com/golang/protobuf/proto"
)

const (
	DatabasePath = "/tmp/d1"
)

var (
	s Server
)

func init() {
	err := exec.Command("rm", "-rf", DatabasePath).Run()
	if err != nil {
		panic(err)
	}

	s, err = NewServer(&Options{Path: DatabasePath})
	if err != nil {
		panic(err)
	}

	openReq := &OpenReq{
		Database:    "test-info",
		Resolution:  60,
		Retention:   36000,
		EpochTime:   3600,
		MaxROEpochs: 2,
		MaxRWEpochs: 2,
	}

	openReqData, err := proto.Marshal(openReq)
	if err != nil {
		panic(err)
	}

	_, err = s.Open(openReqData)
	if err != nil {
		panic(err)
	}
}

func TestInfo(t *testing.T) {
	req := []byte{}
	out, err := s.Info(req)
	if err != nil {
		t.Fatal(err)
	}

	res := &InfoRes{}
	err = proto.Unmarshal(out, res)
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Databases) != 1 {
		t.Fatal("should have db info")
	}

	dbInfo := res.Databases[0]

	if dbInfo.Database != "test-info" ||
		dbInfo.Resolution != uint32(60) {
		t.Fatal("wrong values")
	}
}

func TestOpen(t *testing.T) {
	// tested by init fn
}

func TestEdit(t *testing.T) {
	// TODO
}

func TestPut(t *testing.T) {
	// TODO
}

func TestInc(t *testing.T) {
	// TODO
}

func TestGet(t *testing.T) {
	// TODO
}

func TestBatch(t *testing.T) {
	req := &ReqBatch{}
	req.Batch = make([]*Request, 2)
	req.Batch[0] = &Request{InfoReq: &InfoReq{}}
	req.Batch[1] = &Request{InfoReq: &InfoReq{}}

	reqData, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}

	resData, err := s.Batch(reqData)
	if err != nil {
		t.Fatal(err)
	}

	res := &ResBatch{}
	err = proto.Unmarshal(resData, res)
	if err != nil {
		t.Fatal(err)
	}

	if len(res.Batch) != 2 {
		t.Fatal("should have 2 results")
	}

	if res.Batch[0].InfoRes == nil ||
		res.Batch[1].InfoRes == nil {
		t.Fatal("should have 2 info results")
	}
}
