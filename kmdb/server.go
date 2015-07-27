package kmdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"path"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/kadirahq/kadiradb-core/kdb"
	"github.com/meteorhacks/simple-rpc-go/srpc"
)

const (
	// DataPerm will be set as permissions if data directory is created.
	// if data path already exists, it will maintain the old value.
	DataPerm = 0755

	// SegSize is the maximum size of a segment file (default: 120MB)
	SegSize = 120 * 1024 * 1024

	// PointSize is the size of a single metric point in the filesystem
	// Consists of a 64 bit `double` value and a 32 bit `uint32` count
	PointSize = 12
)

var (
	// ErrDatabase is returned when the requested database is not found
	ErrDatabase = errors.New("database not found")
)

// Server handles requests
type Server interface {
	Listen() (err error)
	Info(reqData []byte) (resData []byte, err error)
	Open(reqData []byte) (resData []byte, err error)
	Edit(reqData []byte) (resData []byte, err error)
	Put(reqData []byte) (resData []byte, err error)
	Inc(reqData []byte) (resData []byte, err error)
	Get(reqData []byte) (resData []byte, err error)
	Batch(reqData []byte) (resData []byte, err error)
}

type server struct {
	options   *Options
	databases map[string]kdb.Database
}

// Options has server options
type Options struct {
	Path     string
	Address  string
	Recovery bool
	Verbose  bool
}

// NewServer creates a server to handle requests
func NewServer(options *Options) (s Server, err error) {
	dbs := make(map[string]kdb.Database)
	srv := &server{
		options:   options,
		databases: dbs,
	}

	err = os.MkdirAll(options.Path, DataPerm)
	if err != nil {
		return nil, err
	}

	files, err := ioutil.ReadDir(options.Path)
	if err != nil {
		return nil, err
	}

	now := time.Now().UnixNano()
	fields := []string{`¯\_(ツ)_/¯`}

	for _, finfo := range files {
		fname := finfo.Name()
		dbPath := path.Join(options.Path, fname)

		db, err := kdb.Open(dbPath, options.Recovery)
		if err != nil {
			log.Printf("KDB Open error: %s\n", err)
			continue
		}

		dbmd := db.Info()

		if options.Verbose {
			log.Printf("KDB Info: %s %+v", fname, dbmd)
		}

		var i uint32
		for i = 0; i < dbmd.MaxRWEpochs; i++ {
			ii64 := int64(i)
			start := now - ii64*dbmd.Duration
			end := start + dbmd.Resolution

			// this will trigger a epoch load
			// also acts as a db health check
			_, err = db.One(start, end, fields)
			if err != nil {
				db.Close()
				continue
			}
		}

		dbs[fname] = db
	}

	return srv, nil
}

func (s *server) Listen() (err error) {
	srv := srpc.NewServer(s.options.Address)
	srv.SetHandler("info", s.Info)
	srv.SetHandler("open", s.Open)
	srv.SetHandler("edit", s.Edit)
	srv.SetHandler("put", s.Put)
	srv.SetHandler("inc", s.Inc)
	srv.SetHandler("get", s.Get)
	srv.SetHandler("batch", s.Batch)

	log.Println("SRPCS:  listening on", s.options.Address)
	return srv.Listen()
}

func (s *server) Info(reqData []byte) (resData []byte, err error) {
	if s.options.Verbose {
		log.Println("> info:")
	}

	res, err := s.info(nil)
	if err != nil {
		log.Printf("ERROR: %s\n", err)
		return nil, err
	}

	resData, err = proto.Marshal(res)
	if err != nil {
		log.Printf("ERROR: %s\n", err)
		return nil, err
	}

	return resData, nil
}

func (s *server) Open(reqData []byte) (resData []byte, err error) {
	req := &OpenReq{}
	err = proto.Unmarshal(reqData, req)
	if err != nil {
		log.Printf("ERROR: %s\n", err)
		return nil, err
	}

	if s.options.Verbose {
		log.Println("> open:", req)
	}

	res, err := s.open(req)
	if err != nil {
		log.Printf("ERROR: %s\n", err)
		return nil, err
	}

	resData, err = proto.Marshal(res)
	if err != nil {
		log.Printf("ERROR: %s\n", err)
		return nil, err
	}

	return resData, nil
}

func (s *server) Edit(reqData []byte) (resData []byte, err error) {
	req := &EditReq{}
	err = proto.Unmarshal(reqData, req)
	if err != nil {
		log.Printf("ERROR: %s\n", err)
		return nil, err
	}

	if s.options.Verbose {
		log.Println("> edit:", req)
	}

	res, err := s.edit(req)
	if err != nil {
		log.Printf("ERROR: %s\n", err)
		return nil, err
	}

	resData, err = proto.Marshal(res)
	if err != nil {
		log.Printf("ERROR: %s\n", err)
		return nil, err
	}

	return resData, nil
}

func (s *server) Put(reqData []byte) (resData []byte, err error) {
	req := &PutReq{}
	err = proto.Unmarshal(reqData, req)
	if err != nil {
		return nil, err
	}

	if s.options.Verbose {
		log.Println("> put:", req)
	}

	res, err := s.put(req)
	if err != nil {
		log.Printf("ERROR: %s\n", err)
		return nil, err
	}

	resData, err = proto.Marshal(res)
	if err != nil {
		log.Printf("ERROR: %s\n", err)
		return nil, err
	}

	return resData, nil
}

func (s *server) Inc(reqData []byte) (resData []byte, err error) {
	req := &IncReq{}
	err = proto.Unmarshal(reqData, req)
	if err != nil {
		return nil, err
	}

	if s.options.Verbose {
		log.Println("> inc:", req)
	}

	res, err := s.inc(req)
	if err != nil {
		log.Printf("ERROR: %s\n", err)
		return nil, err
	}

	resData, err = proto.Marshal(res)
	if err != nil {
		log.Printf("ERROR: %s\n", err)
		return nil, err
	}

	return resData, nil
}

func (s *server) Get(reqData []byte) (resData []byte, err error) {
	req := &GetReq{}
	err = proto.Unmarshal(reqData, req)
	if err != nil {
		return nil, err
	}

	if s.options.Verbose {
		log.Println("> get:", req)
	}

	res, err := s.get(req)
	if err != nil {
		log.Printf("ERROR: %s\n", err)
		return nil, err
	}

	resData, err = proto.Marshal(res)
	if err != nil {
		log.Printf("ERROR: %s\n", err)
		return nil, err
	}

	return resData, nil
}

func (s *server) Batch(reqData []byte) (resData []byte, err error) {
	req := &ReqBatch{}
	err = proto.Unmarshal(reqData, req)
	if err != nil {
		return nil, err
	}

	num := len(req.Batch)
	res := &ResBatch{}
	res.Batch = make([]*Response, num)

	if s.options.Verbose {
		log.Println("> get:", req)
	}

	for i, req := range req.Batch {
		response := &Response{}
		var err error

		switch {
		case req.InfoReq != nil:
			response.InfoRes, err = s.info(req.InfoReq)
		case req.OpenReq != nil:
			response.OpenRes, err = s.open(req.OpenReq)
		case req.EditReq != nil:
			response.EditRes, err = s.edit(req.EditReq)
		case req.PutReq != nil:
			response.PutRes, err = s.put(req.PutReq)
		case req.IncReq != nil:
			response.IncRes, err = s.inc(req.IncReq)
		case req.GetReq != nil:
			response.GetRes, err = s.get(req.GetReq)
		}

		if err != nil {
			log.Printf("ERROR: %s\n", err)
			return nil, err
		}

		res.Batch[i] = response
	}

	resData, err = proto.Marshal(res)
	if err != nil {
		log.Printf("ERROR: %s\n", err)
		return nil, err
	}

	return resData, nil
}

func (s *server) info(req *InfoReq) (res *InfoRes, err error) {
	res = &InfoRes{}
	res.Databases = make([]*DBInfo, len(s.databases))

	var i int
	for name, db := range s.databases {
		metadata := db.Info()
		res.Databases[i] = &DBInfo{
			Database:   name,
			Resolution: uint32(metadata.Resolution / 1e9),
		}

		i++ // increment
	}

	return res, nil
}

func (s *server) open(req *OpenReq) (res *OpenRes, err error) {
	res = &OpenRes{}

	_, ok := s.databases[req.Database]
	if !ok {
		poinsCount := uint32(req.EpochTime / req.Resolution)
		ssize := SegSize / (PointSize * poinsCount)

		// FIXME: security issue: req.Name can use ../../
		//        only allow alpha numeric characters and -
		// TODO: store retention period
		db, err := kdb.New(&kdb.Options{
			Path:        path.Join(s.options.Path, req.Database),
			Resolution:  int64(req.Resolution) * 1e9,
			Duration:    int64(req.EpochTime) * 1e9,
			PayloadSize: PointSize,
			SegmentSize: ssize,
			MaxROEpochs: req.MaxROEpochs,
			MaxRWEpochs: req.MaxRWEpochs,
		})

		if err != nil {
			log.Printf("ERROR: %s\n", err)
			return nil, err
		}

		// TODO: update info response cache
		// before that, cache info response
		s.databases[req.Database] = db
	}

	return res, nil
}

func (s *server) edit(req *EditReq) (res *EditRes, err error) {
	res = &EditRes{}
	db, ok := s.databases[req.Database]
	if !ok {
		return nil, ErrDatabase
	}

	// TODO: update retention period
	md := kdb.Metadata{
		MaxROEpochs: req.MaxROEpochs,
		MaxRWEpochs: req.MaxRWEpochs,
	}

	err = db.Edit(&md)
	if err != nil {
		log.Printf("ERROR: %s\n", err)
		return nil, err
	}

	return res, nil
}

func (s *server) put(req *PutReq) (res *PutRes, err error) {
	res = &PutRes{}

	db, ok := s.databases[req.Database]
	if !ok {
		return nil, ErrDatabase
	}

	payload := valToPld(req.Value, req.Count)
	timestamp := int64(req.Timestamp) * 1e9
	err = db.Put(timestamp, req.Fields, payload)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (s *server) inc(req *IncReq) (res *IncRes, err error) {
	res = &IncRes{}

	db, ok := s.databases[req.Database]
	if !ok {
		return nil, ErrDatabase
	}

	metadata := db.Info()
	timestamp := int64(req.Timestamp) * 1e9
	endTime := timestamp + metadata.Resolution
	data, err := db.One(timestamp, endTime, req.Fields)
	if err != nil {
		return nil, err
	}

	val, num := pldToVal(data[0])
	num += req.Count
	val += req.Value
	pld := valToPld(val, num)

	err = db.Put(timestamp, req.Fields, pld)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (s *server) get(req *GetReq) (res *GetRes, err error) {
	res = &GetRes{}

	db, ok := s.databases[req.Database]
	if !ok {
		return nil, ErrDatabase
	}

	startTime := int64(req.StartTime) * 1e9
	endTime := int64(req.EndTime) * 1e9
	dataMap, err := db.Get(startTime, endTime, req.Fields)
	if err != nil {
		return nil, err
	}

	ss := s.newSeriesSet(req.GroupBy)
	for item, value := range dataMap {
		sr := s.newSeries(value, item.Fields)
		ss.add(sr)
	}

	res.Groups = ss.toResult()

	return res, nil
}

func (s *server) newSeries(data [][]byte, fields []string) (sr *series) {
	count := len(data)
	points := make([]*point, count, count)

	for i := 0; i < count; i++ {
		val, num := pldToVal(data[i])
		points[i] = &point{val, num}
	}

	return &series{fields, points, data}
}

func (s *server) newSeriesSet(groupBy []bool) (ss *seriesSet) {
	set := []*series{}
	return &seriesSet{set, groupBy}
}

func valToPld(val float64, num uint32) (pld []byte) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, val)
	binary.Write(buf, binary.LittleEndian, num)
	return buf.Bytes()
}

func pldToVal(pld []byte) (val float64, num uint32) {
	buf := bytes.NewBuffer(pld)
	binary.Read(buf, binary.LittleEndian, &val)
	binary.Read(buf, binary.LittleEndian, &num)
	return val, num
}
