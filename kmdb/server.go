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

	"github.com/gogo/protobuf/proto"
	"github.com/kadirahq/kadiradb-core/kdb"
	"github.com/meteorhacks/simple-rpc-go/srpc"
)

const (
	// DataDirPerm will be set as permissions if data directory is created.
	// if data path already exists, it will maintain the old value.
	DataDirPerm = 0755
)

var (
	// DebugMode TODO
	DebugMode = os.Getenv("debug") != ""
	// ErrBatch TODO
	ErrBatch = errors.New("batch failed")
	// ErrDatabase TODO
	ErrDatabase = errors.New("database not found")
)

// Server TODO
type Server interface {
	Listen() (err error)
	Info(req []byte) (res []byte, err error)
	Open(req []byte) (res []byte, err error)
	Put(req []byte) (res []byte, err error)
	Inc(req []byte) (res []byte, err error)
	Get(req []byte) (res []byte, err error)
}

type server struct {
	address   string
	basePath  string
	databases map[string]kdb.Database
}

// NewServer TODO
func NewServer(address, basePath string) (_s Server, err error) {
	s := &server{
		address:   address,
		basePath:  basePath,
		databases: make(map[string]kdb.Database),
	}

	err = os.MkdirAll(basePath, DataDirPerm)
	if err != nil {
		return nil, err
	}

	files, err := ioutil.ReadDir(basePath)
	if err != nil {
		return nil, err
	}

	now := time.Now().UnixNano()
	fields := []string{":P"}

	for _, finfo := range files {
		fname := finfo.Name()
		dbPath := path.Join(basePath, fname)

		db, err := kdb.Open(dbPath)
		if err != nil {
			log.Printf("KDB Open error: %s\n", err.Error())
			continue
		}

		dbmd := db.Metadata()

		var i int64
		for i = 0; i < dbmd.MaxRWEpochs; i++ {
			start := now - i*dbmd.EpochDuration
			end := start + dbmd.Resolution

			// this will trigger a epoch load
			// also acts as a db health check
			_, err = db.One(start, end, fields)
			if err != nil {
				db.Close()
				continue
			}
		}

		s.databases[fname] = db
	}

	return s, nil
}

func (s *server) Listen() (err error) {
	srv := srpc.NewServer(s.address)
	srv.SetHandler("info", s.Info)
	srv.SetHandler("open", s.Open)
	srv.SetHandler("put", s.Put)
	srv.SetHandler("inc", s.Inc)
	srv.SetHandler("get", s.Get)

	log.Println("SRPCS:  listening on", s.address)
	return srv.Listen()
}

func (s *server) Info(reqData []byte) (resData []byte, err error) {
	if DebugMode {
		log.Println("> info:")
	}

	res := &InfoRes{}
	res.Databases = make([]*DBInfo, len(s.databases))

	var i int
	for name, db := range s.databases {
		metadata := db.Metadata()
		res.Databases[i] = &DBInfo{
			Name:       name,
			Resolution: metadata.Resolution,
		}

		i++ // increment
	}

	resData, err = proto.Marshal(res)
	if err != nil {
		log.Printf("ERROR: %s\n", err.Error())
		return nil, err
	}

	return resData, nil
}

func (s *server) Open(reqData []byte) (resData []byte, err error) {
	req := &OpenReq{}
	res := &OpenRes{}
	err = proto.Unmarshal(reqData, req)
	if err != nil {
		log.Printf("ERROR: %s\n", err.Error())
		return nil, err
	}

	if DebugMode {
		log.Println("> open:", req)
	}

	_, ok := s.databases[req.Name]
	if !ok {
		// FIXME: security issue: req.Name can use ../../
		//        only allow alpha numeric characters and -
		db, err := kdb.New(&kdb.Options{
			BasePath:      path.Join(s.basePath, req.Name),
			Resolution:    req.Resolution,
			EpochDuration: req.EpochDuration,
			PayloadSize:   req.PayloadSize,
			SegmentLength: req.SegmentLength,
			MaxROEpochs:   req.MaxROEpochs,
			MaxRWEpochs:   req.MaxRWEpochs,
		})

		if err != nil {
			log.Printf("ERROR: %s\n", err.Error())
			return nil, err
		}

		s.databases[req.Name] = db
	}

	resData, err = proto.Marshal(res)
	if err != nil {
		log.Printf("ERROR: %s\n", err.Error())
		return nil, err
	}

	return resData, nil
}

func (s *server) Put(reqData []byte) (resData []byte, err error) {
	batch := &PutReqBatch{}
	err = proto.Unmarshal(reqData, batch)
	if err != nil {
		return nil, err
	}

	if DebugMode {
		log.Println("> put:", batch)
	}

	n := len(batch.Batch)
	r := &PutResBatch{}
	r.Batch = make([]*PutRes, n, n)
	var batchError error

	for i := 0; i < n; i++ {
		r.Batch[i], err = s.put(batch.Batch[i])
		if err != nil && batchError == nil {
			log.Printf("ERROR: %s\n", err.Error())
			batchError = ErrBatch
		}
	}

	if batchError != nil {
		return nil, batchError
	}

	resData, err = proto.Marshal(r)
	if err != nil {
		log.Printf("ERROR: %s\n", err.Error())
		return nil, err
	}

	return resData, nil
}

func (s *server) Inc(reqData []byte) (resData []byte, err error) {
	batch := &IncReqBatch{}
	err = proto.Unmarshal(reqData, batch)
	if err != nil {
		return nil, err
	}

	if DebugMode {
		log.Println("> put:", batch)
	}

	n := len(batch.Batch)
	r := &IncResBatch{}
	r.Batch = make([]*IncRes, n, n)
	var batchError error

	for i := 0; i < n; i++ {
		r.Batch[i], err = s.inc(batch.Batch[i])
		if err != nil && batchError == nil {
			log.Printf("ERROR: %s\n", err.Error())
			batchError = ErrBatch
		}
	}

	if batchError != nil {
		return nil, batchError
	}

	resData, err = proto.Marshal(r)
	if err != nil {
		log.Printf("ERROR: %s\n", err.Error())
		return nil, err
	}

	return resData, nil
}

func (s *server) Get(reqData []byte) (resData []byte, err error) {
	batch := &GetReqBatch{}
	err = proto.Unmarshal(reqData, batch)
	if err != nil {
		return nil, err
	}

	if DebugMode {
		log.Println("> put:", batch)
	}

	n := len(batch.Batch)
	r := &GetResBatch{}
	r.Batch = make([]*GetRes, n, n)
	var batchError error

	for i := 0; i < n; i++ {
		r.Batch[i], err = s.get(batch.Batch[i])
		if err != nil && batchError == nil {
			log.Printf("ERROR: %s\n", err.Error())
			batchError = ErrBatch
		}
	}

	if batchError != nil {
		return nil, batchError
	}

	resData, err = proto.Marshal(r)
	if err != nil {
		log.Printf("ERROR: %s\n", err.Error())
		return nil, err
	}

	return resData, nil
}

func (s *server) put(req *PutReq) (res *PutRes, err error) {
	res = &PutRes{}

	db, ok := s.databases[req.Database]
	if !ok {
		return nil, ErrDatabase
	}

	payload := valToPld(req.Value, req.Count)
	err = db.Put(req.Timestamp, req.Fields, payload)
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

	metadata := db.Metadata()
	endTime := req.Timestamp + metadata.Resolution
	data, err := db.One(req.Timestamp, endTime, req.Fields)
	if err != nil {
		return nil, err
	}

	val, num := pldToVal(data[0])
	num += req.Count
	val += req.Value
	pld := valToPld(val, num)

	err = db.Put(req.Timestamp, req.Fields, pld)
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

	dataMap, err := db.Get(req.StartTime, req.EndTime, req.Fields)
	if err != nil {
		return nil, err
	}

	ss := s.newSeriesSet(req.GroupBy)
	for item, value := range dataMap {
		sr := s.newSeries(value, item.Fields)
		ss.add(sr)
	}

	res.Data = ss.toResult()

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

func valToPld(val float64, num int64) (pld []byte) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, val)
	binary.Write(buf, binary.LittleEndian, num)
	return buf.Bytes()
}

func pldToVal(pld []byte) (val float64, num int64) {
	buf := bytes.NewBuffer(pld)
	binary.Read(buf, binary.LittleEndian, &val)
	binary.Read(buf, binary.LittleEndian, &num)
	return val, num
}

// Helper structs for building get results

type point struct {
	value float64
	count int64
}

func (p *point) add(q *point) {
	p.value += q.value
	p.count += q.count
}

func (p *point) toResult() (item *ResPoint) {
	item = &ResPoint{}
	item.Value = p.value
	item.Count = p.count
	return item
}

type series struct {
	fields []string
	points []*point
	data   [][]byte
}

func (sr *series) add(sn *series) {
	count := len(sr.points)
	for i := 0; i < count; i++ {
		sr.points[i].add(sn.points[i])
	}
}

func (sr *series) canMerge(sn *series) (can bool) {
	count := len(sr.fields)
	for i := 0; i < count; i++ {
		if sr.fields[i] != sn.fields[i] {
			return false
		}
	}

	return true
}

func (sr *series) toResult() (item *ResSeries) {
	item = &ResSeries{}
	item.Fields = sr.fields

	count := len(sr.points)
	item.Points = make([]*ResPoint, count, count)
	for i, p := range sr.points {
		point := p.toResult()
		item.Points[i] = point
	}

	return item
}

type seriesSet struct {
	items   []*series
	groupBy []bool
}

func (ss *seriesSet) add(sn *series) {
	ss.grpFields(sn)

	count := len(ss.items)
	for i := 0; i < count; i++ {
		sr := ss.items[i]
		if sr.canMerge(sn) {
			sr.add(sn)
			return
		}
	}

	ss.items = append(ss.items, sn)
}

func (ss *seriesSet) grpFields(sn *series) {
	count := len(sn.fields)
	grouped := make([]string, count, count)

	for i := 0; i < count; i++ {
		if ss.groupBy[i] {
			grouped[i] = sn.fields[i]
		}
	}

	sn.fields = grouped
}

func (ss *seriesSet) toResult() (res []*ResSeries) {
	count := len(ss.items)
	res = make([]*ResSeries, count, count)

	for i := 0; i < count; i++ {
		sr := ss.items[i]
		item := sr.toResult()
		res[i] = item
	}

	return res
}
