package kmdbclient

import (
	"github.com/golang/protobuf/proto"
	"github.com/kadirahq/kadiradb-metrics/kmdb"
	"github.com/meteorhacks/simple-rpc-go"
)

// Client TODO
type Client interface {
	Connect() (err error)
	Info(req *kmdb.InfoReq) (r *kmdb.InfoRes, err error)
	Open(req *kmdb.OpenReq) (r *kmdb.OpenRes, err error)
	Put(req *kmdb.PutReqBatch) (r *kmdb.PutResBatch, err error)
	Inc(req *kmdb.IncReqBatch) (r *kmdb.IncResBatch, err error)
	Get(req *kmdb.GetReqBatch) (r *kmdb.GetResBatch, err error)
}

type client struct {
	addr string
	cli  srpc.Client
}

// New TODO
func New(addr string) (c Client) {
	cli := srpc.NewClient(addr)
	return &client{addr, cli}
}

func (c *client) Connect() (err error) {
	return c.cli.Connect()
}

func (c *client) Info(req *kmdb.InfoReq) (r *kmdb.InfoRes, err error) {
	pld, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	out, err := c.cli.Call("info", pld)
	if err != nil {
		return nil, err
	}

	r = &kmdb.InfoRes{}
	err = proto.Unmarshal(out, r)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (c *client) Open(req *kmdb.OpenReq) (r *kmdb.OpenRes, err error) {
	pld, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	out, err := c.cli.Call("open", pld)
	if err != nil {
		return nil, err
	}

	r = &kmdb.OpenRes{}
	err = proto.Unmarshal(out, r)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (c *client) Put(req *kmdb.PutReqBatch) (r *kmdb.PutResBatch, err error) {
	pld, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	out, err := c.cli.Call("put", pld)
	if err != nil {
		return nil, err
	}

	r = &kmdb.PutResBatch{}
	err = proto.Unmarshal(out, r)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (c *client) Inc(req *kmdb.IncReqBatch) (r *kmdb.IncResBatch, err error) {
	pld, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	out, err := c.cli.Call("inc", pld)
	if err != nil {
		return nil, err
	}

	r = &kmdb.IncResBatch{}
	err = proto.Unmarshal(out, r)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (c *client) Get(req *kmdb.GetReqBatch) (r *kmdb.GetResBatch, err error) {
	pld, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	out, err := c.cli.Call("get", pld)
	if err != nil {
		return nil, err
	}

	r = &kmdb.GetResBatch{}
	err = proto.Unmarshal(out, r)
	if err != nil {
		return nil, err
	}

	return r, nil
}
