// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xdaq // import "github.com/go-daq/tdaq/xdaq"

import (
	"encoding/binary"
	"time"

	"github.com/go-daq/tdaq"
	"gonum.org/v1/gonum/stat"
)

// I64TOF computes the time-of-flight of an int64 token from an input end-point.
type I64TOF struct {
	vals   []float64
	N      int64   // counter of values seen since /init
	Mean   float64 // mean of time-of-flight
	StdDev float64 // stddev of time-of-flight
}

func (dev *I64TOF) OnConfig(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /config command...")
	return nil
}

func (dev *I64TOF) OnInit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /init command...")
	dev.N = +0
	dev.vals = nil
	dev.Mean = 0
	dev.StdDev = 0
	return nil
}

func (dev *I64TOF) OnReset(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /reset command...")
	dev.vals = nil
	dev.N = +0
	dev.Mean = 0
	dev.StdDev = 0
	return nil
}

func (dev *I64TOF) OnStart(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /start command...")
	return nil
}

func (dev *I64TOF) OnStop(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	n := dev.N
	dev.Mean, dev.StdDev = stat.MeanStdDev(dev.vals, nil)
	ctx.Msg.Infof("received /stop command... -> n=%d (mean=%v, std=%v)", n, dev.Mean, dev.StdDev)
	return nil
}

func (dev *I64TOF) OnQuit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /quit command...")
	return nil
}

func (dev *I64TOF) Input(ctx tdaq.Context, src tdaq.Frame) error {
	var (
		now = time.Now().UTC().UnixNano()
		v   = int64(binary.LittleEndian.Uint64(src.Body))
	)

	dev.N++
	dev.vals = append(dev.vals, float64(now-v))
	ctx.Msg.Debugf("received: %d -> %d", v, dev.N)
	return nil
}
