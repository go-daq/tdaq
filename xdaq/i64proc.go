// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xdaq // import "github.com/go-daq/tdaq/xdaq"

import (
	"encoding/binary"

	"github.com/go-daq/tdaq"
)

// I64Processor consumes int64 data from an input end-point and publishes the
// massaged int64 data on an output end-point.
type I64Processor struct {
	N int64 // counter of values seen since /init
	V int64 // last value seen

	ch chan int64
}

func (dev *I64Processor) OnConfig(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /config command...")
	return nil
}

func (dev *I64Processor) OnInit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /init command...")
	dev.N = +0
	dev.V = -1
	dev.ch = make(chan int64)
	return nil
}

func (dev *I64Processor) OnReset(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /reset command...")
	dev.N = 0
	dev.ch = make(chan int64)
	return nil
}

func (dev *I64Processor) OnStart(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /start command...")
	return nil
}

func (dev *I64Processor) OnStop(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	n := dev.N
	v := dev.V
	ctx.Msg.Infof("received /stop command... v=%d -> n=%d", v, n)
	return nil
}

func (dev *I64Processor) OnQuit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /quit command...")
	return nil
}

func (dev *I64Processor) Input(ctx tdaq.Context, src tdaq.Frame) error {
	dev.V = int64(binary.LittleEndian.Uint64(src.Body))
	dev.N++
	ctx.Msg.Debugf("received: %d -> %d", dev.V, dev.N)
	select {
	case <-ctx.Ctx.Done():
		return nil
	case dev.ch <- dev.V:
		return nil
	}
}

func (dev *I64Processor) Output(ctx tdaq.Context, dst *tdaq.Frame) error {
	select {
	case <-ctx.Ctx.Done():
		dst.Body = nil
		return nil
	case data := <-dev.ch:
		dst.Body = make([]byte, 8)
		binary.LittleEndian.PutUint64(dst.Body, uint64(2*data))
	}
	return nil
}
