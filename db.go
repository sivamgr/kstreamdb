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
	"time"

	msgpack "github.com/vmihailenco/msgpack/v4"
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
	filePath := path.Join(r.DataPath, dt.Format("2006/01/02/15/04/05"))
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

// PlaybackDate ticks from the date
func (r *DB) PlaybackDate(dt time.Time, fn PlaybackFunc) error {
	dayPath := path.Join(r.DataPath, dt.Format("2006/01/02"))
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
