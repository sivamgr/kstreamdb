package kstreamdb

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

var inTicks []TickData
var outTicks []TickData

// TestDB method to test the db
func TestDB(t *testing.T) {
	dir, _ := ioutil.TempDir("", "kstreamdbtest")
	os.MkdirAll(dir, 0755)
	db := ConfigureDB(dir)
	t1 := TickData{
		TradingSymbol: "TestSym",
		IsTradable:    true,

		Timestamp: time.Now(),

		LastTradeTime:      time.Now(),
		LastPrice:          100.0,
		LastTradedQuantity: 5,

		AverageTradePrice: 99.0,

		VolumeTraded:      555,
		TotalBuyQuantity:  500,
		TotalSellQuantity: 300,

		DayOpen:      98.0,
		DayHighPrice: 103.0,
		DayLowPrice:  94.0,
		LastDayClose: 98.0,

		OI:        0,
		OIDayHigh: 10,
		OIDayLow:  0,

		Bid: [...]DepthItem{{Price: 99.9, Quantity: 100, Orders: 3}, {Price: 99.8, Quantity: 133, Orders: 2}, {Price: 99.7, Quantity: 100, Orders: 1}, {Price: 99.6, Quantity: 100, Orders: 3}, {Price: 99.9, Quantity: 1000, Orders: 3}},
		Ask: [...]DepthItem{{Price: 100, Quantity: 100, Orders: 2}, {Price: 100.1, Quantity: 33, Orders: 2}, {Price: 100.2, Quantity: 10, Orders: 10}, {Price: 100.3, Quantity: 100, Orders: 3}, {Price: 100.4, Quantity: 1000, Orders: 1}},
	}
	t2 := TickData{
		TradingSymbol: "TSYM",
		IsTradable:    true,

		Timestamp: time.Now(),

		LastTradeTime:      time.Now(),
		LastPrice:          1000.0,
		LastTradedQuantity: 66,

		AverageTradePrice: 999.0,

		VolumeTraded:      555,
		TotalBuyQuantity:  500,
		TotalSellQuantity: 300,

		DayOpen:      98.0,
		DayHighPrice: 103.0,
		DayLowPrice:  94.0,
		LastDayClose: 98.0,

		OI:        0,
		OIDayHigh: 10,
		OIDayLow:  0,

		Bid: [...]DepthItem{{Price: 999.9, Quantity: 500, Orders: 3}, {Price: 999.8, Quantity: 133, Orders: 2}, {Price: 999.7, Quantity: 600, Orders: 1}, {Price: 999.6, Quantity: 44, Orders: 3}, {Price: 999.9, Quantity: 80, Orders: 3}},
		Ask: [...]DepthItem{{Price: 1000, Quantity: 1000, Orders: 2}, {Price: 1000.1, Quantity: 33, Orders: 2}, {Price: 1000.2, Quantity: 10, Orders: 10}, {Price: 1000.3, Quantity: 1000, Orders: 3}, {Price: 1000.4, Quantity: 10, Orders: 1}},
	}
	inTicks = make([]TickData, 0)
	outTicks = make([]TickData, 0)
	inTicks = append(inTicks, t1)
	inTicks = append(inTicks, t2)
	//t.Logf("Inserting, %+v", inTicks)

	db.Insert(inTicks)
	db.PlaybackAll(func(t []TickData) {
		outTicks = append(outTicks, t...)
	})
	//t.Logf("Readback, %+v", outTicks)
	if !cmp.Equal(inTicks, outTicks) {
		t.Error("DB Insert and Playback Failed")
	} else {
		t.Log("DB success")
	}

	//os.RemoveAll(dir)
}
