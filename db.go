package kstreamdb

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/vmihailenco/msgpack"
)

// DepthItem represents a single market depth entry.
type DepthItem struct {
	Price    float32
	Quantity uint32
	Orders   uint32
}

// TickData into .tck. symstr +16 len bytes
type TickData struct {
	TradingSymbol string
	IsTradable    bool

	Timestamp time.Time

	LastTradeTime      time.Time
	LastPrice          float32
	LastTradedQuantity uint32

	AverageTradePrice float32

	VolumeTraded      uint32
	TotalBuyQuantity  uint32
	TotalSellQuantity uint32

	DayOpen      float32
	DayHighPrice float32
	DayLowPrice  float32
	LastDayClose float32

	OI        uint32
	OIDayHigh uint32
	OIDayLow  uint32

	Bid [5]DepthItem
	Ask [5]DepthItem
}

//DB Database
type DB struct {
	DataPath string
}

// fileExists checks if a file exists and is not a directory before we
// try using it to prevent further errors.
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func createDirForFile(filepath string) {
	dir := path.Dir(filepath)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			panic(err)
		}
	}
}

func writeMsgpackFile(filePath string, object interface{}) error {
	file, err := os.Create(filePath)
	if err == nil {
		b, err := encodeTicks(object, true)
		if err == nil {
			file.Write(b)
		}
		file.Close()
	}
	return err
}

func encodeTicks(object interface{}, bCompress bool) ([]byte, error) {
	b, err := msgpack.Marshal(object)
	if err != nil {
		return nil, err
	}
	if bCompress {
		var zb bytes.Buffer
		zw := zlib.NewWriter(&zb)
		zw.Write(b)
		zw.Close()
		return zb.Bytes(), nil
	}
	return b, nil
}

func decodeTicks(b io.Reader, object interface{}, bCompress bool) error {
	var out bytes.Buffer
	if bCompress {
		reader, err := zlib.NewReader(b)
		if err != nil {
			return err
		}
		io.Copy(&out, reader)
		reader.Close()
	} else {
		io.Copy(&out, b)
	}
	return msgpack.Unmarshal(out.Bytes(), object)
}

func decodeTicksFromBytes(b []byte, object interface{}, bCompress bool) error {
	reader := bytes.NewReader(b)
	return decodeTicks(reader, object, bCompress)

}

func readMsgpackFile(filePath string, object interface{}) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	err = decodeTicks(file, object, true)
	file.Close()
	return err
}

// SetupDatabase func
func SetupDatabase(DataPath string) DB {
	if DataPath == "" {
		DataPath, _ = ioutil.TempDir("", "kstreamdb")
		os.MkdirAll(DataPath, 0755)
	}
	db := DB{DataPath: DataPath}
	return db
}
func (r *DB) generateTickFilePath(dt time.Time) string {
	filePath := path.Join(r.DataPath, dt.Format("20060102/150405"))
	createDirForFile(filePath)
	suffixID := int64(0)
	for {
		fpath := filePath + fmt.Sprintf("_%03d", suffixID) + ".mpz"
		if fileExists(fpath) {
			suffixID++
			continue
		}
		return fpath
	}
}

// Insert method
func (r *DB) Insert(ticks []TickData) error {
	if len(ticks) == 0 {
		return nil
	}

	fpath := r.generateTickFilePath(ticks[0].Timestamp)
	writeMsgpackFile(fpath, ticks)
	return nil
}

//GetDates returns dates
func (r *DB) GetDates() ([]time.Time, error) {
	dates := make([]time.Time, 0)
	files, err := ioutil.ReadDir(r.DataPath)
	if err == nil {
		for _, f := range files {
			if f.IsDir() {
				dt, err := time.Parse("20060102", f.Name())
				if err == nil {
					dates = append(dates, dt)
				}
			}
		}
	}
	return dates, nil
}

//compressFolder compresses all files in a folder into a single file
func (r *DB) compressFolder(dpath string) {
	filesByMinute := make(map[string][]string)
	keys := make([]string, 0)
	var w filepath.WalkFunc
	w = func(path string, info os.FileInfo, err error) error {
		if (err == nil) && (!info.IsDir()) {
			key := info.Name()[0:4]
			if _, ok := filesByMinute[key]; !ok {
				filesByMinute[key] = make([]string, 0)
				keys = append(keys, key)
			}
			filesByMinute[key] = append(filesByMinute[key], path)
		}
		return err
	}
	filepath.Walk(dpath, w)
	for _, key := range keys {
		if len(filesByMinute[key]) > 1 {
			data := loadDataFromFilesList(filesByMinute[key])
			for _, fName := range filesByMinute[key] {
				os.Remove(fName)
			}
			r.Insert(data)
		}

	}
}

//Compress function compresses data from each date into a single file
func (r *DB) Compress() {
	files, err := ioutil.ReadDir(r.DataPath)
	if err == nil {
		for _, f := range files {
			if f.IsDir() {
				dayPath := path.Join(r.DataPath, f.Name())
				r.compressFolder(dayPath)
			}
		}
	}
}

//PlaybackFunc callback
type PlaybackFunc func(TickData)

func playbackFolder(dpath string, fn PlaybackFunc) error {
	var w filepath.WalkFunc
	w = func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		ticks := new([]TickData)
		readMsgpackFile(path, ticks)
		for _, t := range *ticks {
			fn(t)
		}
		return nil
	}

	return filepath.Walk(dpath, w)
}

func loadDataFromFilesList(files []string) []TickData {
	data := make([]TickData, 0)
	maxworkers := runtime.NumCPU()
	jobs := make(chan string, maxworkers)
	results := make(chan int, 1)
	mapData := make(map[string]*[]TickData)
	mutex := &sync.Mutex{}

	for w := 0; w < maxworkers; w++ {
		go func(fpath <-chan string, result chan<- int) {
			for fp := range fpath {
				ticks := new([]TickData)
				readMsgpackFile(fp, ticks)
				if len(*ticks) > 0 {
					mutex.Lock()
					mapData[fp] = ticks
					mutex.Unlock()
				}
				result <- len(*ticks)
			}
		}(jobs, results)
	}

	i := 0
	processed := 0
	for processed < len(files) {
		if (i < len(files)) && (len(jobs) < cap(jobs)) {
			jobs <- files[i]
			i++
			continue
		}
		<-results
		processed++
	}

	close(jobs)

	for _, fp := range files {
		if val, ok := mapData[fp]; ok {
			data = append(data, *val...)
		}
	}
	return data
}

func loadDataFromFolder(dpath string) ([]TickData, error) {
	files := make([]string, 0)

	err := filepath.Walk(dpath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})

	data := loadDataFromFilesList(files)
	return data, err
}

// LoadDataForDate loads ticks from the date
func (r *DB) LoadDataForDate(dt time.Time) ([]TickData, error) {
	dayPath := path.Join(r.DataPath, dt.Format("20060102"))
	return loadDataFromFolder(dayPath)
}

// LoadAllData loads all ticks from db
func (r *DB) LoadAllData() ([]TickData, error) {
	return loadDataFromFolder(r.DataPath)
}

// PlaybackDate ticks from the date
func (r *DB) PlaybackDate(dt time.Time, fn PlaybackFunc) error {
	dayPath := path.Join(r.DataPath, dt.Format("20060102"))
	return playbackFolder(dayPath, fn)
}

// PlaybackAll all ticks from db
func (r *DB) PlaybackAll(fn PlaybackFunc) error {
	return playbackFolder(r.DataPath, fn)
}

// PlaybackToday all ticks from db
func (r *DB) PlaybackToday(fn PlaybackFunc) error {
	return r.PlaybackDate(time.Now(), fn)
}
