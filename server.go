package main

import (
	"errors"
	"io/ioutil"
	"log"
	"os"
	"path"
	"reflect"
	"time"
	"unsafe"

	goerr "github.com/go-errors/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/kadirahq/kadiyadb"
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

	// ErrResolution requested resolution is not valid
	ErrResolution = errors.New("resolution is not valid")
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
	Metrics(reqData []byte) (resData []byte, err error)
}

type server struct {
	options   *Options
	databases map[string]kadiyadb.Database
}

// Options has server options
type Options struct {
	Path     string
	Address  string
	Recovery bool
}

// NewServer creates a server to handle requests
func NewServer(options *Options) (s Server, err error) {
	defer Logger.Time(time.Now(), time.Second, "NewServer")
	dbs := make(map[string]kadiyadb.Database)
	srv := &server{
		options:   options,
		databases: dbs,
	}

	err = os.MkdirAll(options.Path, DataPerm)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	files, err := ioutil.ReadDir(options.Path)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	now := time.Now().UnixNano()
	fields := []string{`¯\_(ツ)_/¯`}

	for _, finfo := range files {
		fname := finfo.Name()
		dbPath := path.Join(options.Path, fname)

		if fname == InitFile {
			continue
		}

		db, err := kadiyadb.Open(dbPath, options.Recovery)
		if err != nil {
			Logger.Error(err)
			continue
		}

		info, err := db.Info()
		if err != nil {
			Logger.Error(err)
			continue
		}

		var i uint32
		for i = 0; i < info.MaxRWEpochs; i++ {
			ii64 := int64(i)
			start := now - ii64*info.Duration
			end := start + info.Resolution

			// this will trigger a epoch load
			// also acts as a db health check
			_, err = db.One(start, end, fields)
			if err != nil {

				if err := db.Close(); err != nil {
					Logger.Error(err)
				}

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
	srv.SetHandler("metrics", s.Metrics)

	log.Println("SRPCS:  listening on", s.options.Address)
	return srv.Listen()
}

func (s *server) Info(reqData []byte) (resData []byte, err error) {

	res, err := s.info(nil)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	resData, err = proto.Marshal(res)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	return resData, nil
}

func (s *server) Open(reqData []byte) (resData []byte, err error) {
	req := &OpenReq{}
	err = proto.Unmarshal(reqData, req)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	res, err := s.open(req)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	resData, err = proto.Marshal(res)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	return resData, nil
}

func (s *server) Edit(reqData []byte) (resData []byte, err error) {
	req := &EditReq{}
	err = proto.Unmarshal(reqData, req)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	res, err := s.edit(req)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	resData, err = proto.Marshal(res)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	return resData, nil
}

func (s *server) Put(reqData []byte) (resData []byte, err error) {
	req := &PutReq{}
	err = proto.Unmarshal(reqData, req)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	res, err := s.put(req)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	resData, err = proto.Marshal(res)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	return resData, nil
}

func (s *server) Inc(reqData []byte) (resData []byte, err error) {
	req := &IncReq{}
	err = proto.Unmarshal(reqData, req)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	res, err := s.inc(req)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	resData, err = proto.Marshal(res)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	return resData, nil
}

func (s *server) Get(reqData []byte) (resData []byte, err error) {
	req := &GetReq{}
	err = proto.Unmarshal(reqData, req)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	res, err := s.get(req)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	resData, err = proto.Marshal(res)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	return resData, nil
}

func (s *server) Batch(reqData []byte) (resData []byte, err error) {
	req := &ReqBatch{}
	err = proto.Unmarshal(reqData, req)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	num := len(req.Batch)
	res := &ResBatch{}
	res.Batch = make([]*Response, num)

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
			return nil, goerr.Wrap(err, 0)
		}

		res.Batch[i] = response
	}

	resData, err = proto.Marshal(res)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	return resData, nil
}

func (s *server) Metrics(reqData []byte) (resData []byte, err error) {
	res := &MetricsRes{}
	resData, err = proto.Marshal(res)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	return resData, nil
}

func (s *server) info(req *InfoReq) (res *InfoRes, err error) {
	defer Logger.Time(time.Now(), time.Second, "server.info")
	res = &InfoRes{}
	res.Databases = make([]*DBInfo, len(s.databases))

	var i int
	for name, db := range s.databases {
		metadata, err := db.Info()
		if err != nil {
			Logger.Error(err)
			continue
		}

		res.Databases[i] = &DBInfo{
			Database:   name,
			Resolution: uint32(metadata.Resolution / 1e9),
		}

		i++ // increment
	}

	return res, nil
}

func (s *server) open(req *OpenReq) (res *OpenRes, err error) {
	defer Logger.Time(time.Now(), 10*time.Second, "server.open")
	res = &OpenRes{}

	db, ok := s.databases[req.Database]
	if !ok {
		poinsCount := uint32(req.EpochTime / req.Resolution)
		ssize := SegSize / (PointSize * poinsCount)

		// FIXME: security issue: req.Name can use ../../
		//        only allow alpha numeric characters and -
		db, err := kadiyadb.New(&kadiyadb.Options{
			Path:        path.Join(s.options.Path, req.Database),
			Resolution:  int64(req.Resolution) * 1e9,
			Retention:   int64(req.Retention) * 1e9,
			Duration:    int64(req.EpochTime) * 1e9,
			PayloadSize: PointSize,
			SegmentSize: ssize,
			MaxROEpochs: req.MaxROEpochs,
			MaxRWEpochs: req.MaxRWEpochs,
		})

		if err != nil {
			return nil, goerr.Wrap(err, 0)
		}

		s.databases[req.Database] = db
	} else {
		// TODO: update retention period
		err = db.Edit(req.MaxROEpochs, req.MaxRWEpochs)
		if err != nil {
			return nil, goerr.Wrap(err, 0)
		}
	}

	return res, nil
}

func (s *server) edit(req *EditReq) (res *EditRes, err error) {
	defer Logger.Time(time.Now(), time.Second, "server.edit")
	res = &EditRes{}
	db, ok := s.databases[req.Database]
	if !ok {
		return nil, goerr.Wrap(ErrDatabase, 0)
	}

	// TODO: update retention period
	err = db.Edit(req.MaxROEpochs, req.MaxRWEpochs)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	return res, nil
}

func (s *server) put(req *PutReq) (res *PutRes, err error) {
	defer Logger.Time(time.Now(), time.Second, "server.put")
	res = &PutRes{}

	db, ok := s.databases[req.Database]
	if !ok {
		return nil, goerr.Wrap(ErrDatabase, 0)
	}

	payload := valToPld(req.Value, req.Count)
	timestamp := int64(req.Timestamp) * 1e9
	err = db.Put(timestamp, req.Fields, payload)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	return res, nil
}

func (s *server) inc(req *IncReq) (res *IncRes, err error) {
	defer Logger.Time(time.Now(), time.Second, "server.inc")
	res = &IncRes{}

	db, ok := s.databases[req.Database]
	if !ok {
		return nil, goerr.Wrap(ErrDatabase, 0)
	}

	metadata, err := db.Info()
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	timestamp := int64(req.Timestamp) * 1e9
	endTime := timestamp + metadata.Resolution
	data, err := db.One(timestamp, endTime, req.Fields)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	val, num := pldToVal(data[0])
	num += req.Count
	val += req.Value
	pld := valToPld(val, num)

	err = db.Put(timestamp, req.Fields, pld)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	return res, nil
}

func (s *server) get(req *GetReq) (res *GetRes, err error) {
	defer Logger.Time(time.Now(), time.Second, "server.get")
	res = &GetRes{}

	db, ok := s.databases[req.Database]
	if !ok {
		return nil, goerr.Wrap(ErrDatabase, 0)
	}

	metadata, err := db.Info()
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	var resolution int64
	if req.Resolution == 0 {
		resolution = metadata.Resolution
	} else {
		resolution = int64(req.Resolution) * 1e9
		if resolution%metadata.Resolution != 0 {
			return nil, goerr.Wrap(ErrResolution, 0)
		}
	}

	startTime := int64(req.StartTime) * 1e9
	startTime -= startTime % resolution

	endTime := int64(req.EndTime) * 1e9
	endTime -= endTime % resolution

	dataMap, err := db.Get(startTime, endTime, req.Fields)
	if err != nil {
		return nil, goerr.Wrap(err, 0)
	}

	ss := s.newSeriesSet(req.GroupBy)
	for item, value := range dataMap {
		sr := s.newSeries(value, item.Fields, startTime, metadata.Resolution, resolution)
		ss.add(sr)
	}

	res.Groups = ss.toResult()

	return res, nil
}

func (s *server) newSeries(data [][]byte, fields []string, start, dres, rres int64) (sr *series) {
	count := len(data)
	points := []*point{}

	var prevTs int64
	var prevPt *point

	// add first point
	if count > 0 {
		val, num := pldToVal(data[0])
		p := &point{val, num}

		points = append(points, p)
		prevTs = start - (start % rres)
		prevPt = p
	}

	for i := 1; i < count; i++ {
		dts := start + dres*int64(i)
		rts := dts - (dts % rres)
		val, num := pldToVal(data[i])
		p := &point{val, num}

		if rts == prevTs {
			prevPt.add(p)
		} else {
			points = append(points, p)
			prevTs = rts
			prevPt = p
		}
	}

	return &series{fields, points, data}
}

func (s *server) newSeriesSet(groupBy []bool) (ss *seriesSet) {
	set := []*series{}
	return &seriesSet{set, groupBy}
}

// func valToPld(val float64, num uint32) (pld []byte) {
// 	buf := new(bytes.Buffer)
// 	binary.Write(buf, binary.LittleEndian, val)
// 	binary.Write(buf, binary.LittleEndian, num)
// 	return buf.Bytes()
// }

func valToPld(val float64, num uint32) (pld []byte) {
	pld = make([]byte, 12)
	valpld := []byte{}
	valhdr := (*reflect.SliceHeader)(unsafe.Pointer(&valpld))
	valhdr.Len = 8
	valhdr.Cap = 8
	valhdr.Data = uintptr(unsafe.Pointer(&val))
	copy(pld, valpld)

	numpld := []byte{}
	numhdr := (*reflect.SliceHeader)(unsafe.Pointer(&numpld))
	numhdr.Len = 8
	numhdr.Cap = 8
	numhdr.Data = uintptr(unsafe.Pointer(&num))
	copy(pld[8:], numpld)

	return pld
}

// func pldToVal(pld []byte) (val float64, num uint32) {
// 	buf := bytes.NewBuffer(pld)
// 	binary.Read(buf, binary.LittleEndian, &val)
// 	binary.Read(buf, binary.LittleEndian, &num)
// 	return val, num
// }

func pldToVal(pld []byte) (val float64, num uint32) {
	valpld := pld[:8]
	valhdr := (*reflect.SliceHeader)(unsafe.Pointer(&valpld))
	v := (*float64)(unsafe.Pointer(valhdr.Data))

	numpld := pld[8:]
	numhdr := (*reflect.SliceHeader)(unsafe.Pointer(&numpld))
	n := (*uint32)(unsafe.Pointer(numhdr.Data))

	return *v, *n
}
