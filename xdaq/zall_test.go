// Copyright 2020 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xdaq_test // import "github.com/go-daq/tdaq/xdaq"

import (
	"encoding/binary"
	"sync"
	"time"

	"github.com/go-daq/tdaq"
	"gonum.org/v1/gonum/stat"
)

type testI64Gen struct {
	N int64 // count
	V int64 // last value

	mu sync.RWMutex
	ch chan int64
}

func (dev *testI64Gen) OnConfig(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /config command...")
	return nil
}

func (dev *testI64Gen) OnInit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /init command...")
	dev.mu.Lock()
	dev.N = 0
	dev.V = 0
	dev.ch = make(chan int64)
	dev.mu.Unlock()
	return nil
}

func (dev *testI64Gen) OnReset(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /init command...")
	dev.mu.Lock()
	dev.N = 0
	dev.V = 0
	dev.ch = make(chan int64)
	dev.mu.Unlock()
	return nil
}

func (dev *testI64Gen) OnStart(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /start command...")
	return nil
}

func (dev *testI64Gen) OnStop(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	dev.mu.RLock()
	n := dev.N
	v := dev.V
	dev.mu.RUnlock()
	ctx.Msg.Infof("received /stop command... -> n=%d, v=%v", n, v)
	return nil
}

func (dev *testI64Gen) OnQuit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /quit command...")
	return nil
}

func (dev *testI64Gen) Output(ctx tdaq.Context, dst *tdaq.Frame) error {
	select {
	case <-ctx.Ctx.Done():
		dst.Body = nil
		return nil
	case data := <-dev.ch:
		dst.Body = make([]byte, 8)
		binary.LittleEndian.PutUint64(dst.Body, uint64(data))
	}
	return nil
}

func (dev *testI64Gen) loop(n int) {
	for i := 0; i < n; i++ {
		dev.mu.Lock()
		dev.ch <- int64(i)
		dev.N++
		dev.V = int64(i)
		dev.mu.Unlock()
	}
}

type testI64Dumper struct {
	N int64 // counter of values seen since /init
	V int64 // last value seen

	want  int64
	drain chan int
	timer *time.Timer
}

func (dev *testI64Dumper) OnConfig(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /config command...")
	return nil
}

func (dev *testI64Dumper) OnInit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /init command...")
	dev.N = +0
	dev.V = -1
	return nil
}

func (dev *testI64Dumper) OnReset(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /reset command...")
	dev.N = +0
	return nil
}

func (dev *testI64Dumper) OnStart(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /start command...")
	return nil
}

func (dev *testI64Dumper) OnStop(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	n := dev.N
	v := dev.V
	ctx.Msg.Infof("received /stop command... v=%d -> n=%d", v, n)
	return nil
}

func (dev *testI64Dumper) OnQuit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /quit command...")
	dev.timer.Stop()
	return nil
}

func (dev *testI64Dumper) Input(ctx tdaq.Context, src tdaq.Frame) error {
	dev.V = int64(binary.LittleEndian.Uint64(src.Body))
	dev.N++
	select {
	case <-dev.timer.C:
		dev.drain <- 0
		return nil
	default:
	}
	if dev.N == dev.want {
		ctx.Msg.Debugf("want=%v, n=%v", dev.N, dev.want)
		select {
		case <-dev.timer.C:
			ctx.Msg.Debugf("want=%v, n=%v (ctx=canceled)", dev.N, dev.want)
			dev.drain <- 0
			return nil
		case dev.drain <- 1:
			ctx.Msg.Debugf("want=%v, n=%v (drained)", dev.N, dev.want)
		}
	}
	ctx.Msg.Debugf("received: v=%d -> n=%d (want=%d)", dev.V, dev.N, dev.want)
	return nil
}

// testI64GenUTC publishes int64 data (time.Now in UTC) on an output end-point.
type testI64GenUTC struct {
	N    int64         // number of values generated since /init.
	Freq time.Duration // frequency of the int64 data sequence generation.

	ch chan int64
}

func (dev *testI64GenUTC) OnConfig(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /config command...")
	return nil
}

func (dev *testI64GenUTC) OnInit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /init command...")
	dev.N = 0
	dev.ch = make(chan int64)
	if dev.Freq <= 0 {
		dev.Freq = 10 * time.Millisecond
	}
	return nil
}

func (dev *testI64GenUTC) OnReset(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /init command...")
	dev.N = 0
	dev.ch = make(chan int64)
	return nil
}

func (dev *testI64GenUTC) OnStart(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /start command...")
	return nil
}

func (dev *testI64GenUTC) OnStop(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	n := dev.N - 1
	ctx.Msg.Infof("received /stop command... -> n=%d", n)
	return nil
}

func (dev *testI64GenUTC) OnQuit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /quit command...")
	return nil
}

func (dev *testI64GenUTC) Output(ctx tdaq.Context, dst *tdaq.Frame) error {
	select {
	case <-ctx.Ctx.Done():
		dst.Body = nil
		return nil
	case data := <-dev.ch:
		dst.Body = make([]byte, 8)
		binary.LittleEndian.PutUint64(dst.Body, uint64(data))
	}
	return nil
}

func (dev *testI64GenUTC) Loop(ctx tdaq.Context) error {
	for {
		select {
		case <-ctx.Ctx.Done():
			return nil
		default:
			select {
			case dev.ch <- dev.now():
				dev.N++
			default:
			}
		}
		time.Sleep(dev.Freq)
	}
}

func (dev *testI64GenUTC) now() int64 {
	return time.Now().UTC().UnixNano()
}

// testI64TOF computes the time-of-flight of an int64 token from an input end-point.
type testI64TOF struct {
	vals   []float64
	N      int64   // counter of values seen since /init
	Mean   float64 // mean of time-of-flight
	StdDev float64 // stddev of time-of-flight
}

func (dev *testI64TOF) OnConfig(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /config command...")
	return nil
}

func (dev *testI64TOF) OnInit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /init command...")
	dev.N = +0
	dev.vals = nil
	dev.Mean = 0
	dev.StdDev = 0
	return nil
}

func (dev *testI64TOF) OnReset(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /reset command...")
	dev.vals = nil
	dev.N = +0
	dev.Mean = 0
	dev.StdDev = 0
	return nil
}

func (dev *testI64TOF) OnStart(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /start command...")
	return nil
}

func (dev *testI64TOF) OnStop(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	n := dev.N
	dev.Mean, dev.StdDev = stat.MeanStdDev(dev.vals, nil)
	ctx.Msg.Infof("received /stop command... -> n=%d (mean=%v, std=%v)", n, dev.Mean, dev.StdDev)
	return nil
}

func (dev *testI64TOF) OnQuit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /quit command...")
	return nil
}

func (dev *testI64TOF) Input(ctx tdaq.Context, src tdaq.Frame) error {
	var (
		now = time.Now().UTC().UnixNano()
		v   = int64(binary.LittleEndian.Uint64(src.Body))
	)

	dev.N++
	dev.vals = append(dev.vals, float64(now-v))
	ctx.Msg.Debugf("received: %d -> %d", v, dev.N)
	return nil
}
