// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xdaq // import "github.com/go-daq/tdaq/xdaq"

import (
	"encoding/binary"

	"github.com/go-daq/tdaq"
)

// I64Adder consumes int64 data from 2 input end-points (left and right) and publishes the
// sum int64 data on an output end-point.
type I64Adder struct {
	N int64 // counter of values seen since /init
	V int64 // last value seen

	left  chan int64
	right chan int64
}

func (dev *I64Adder) OnConfig(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /config command...")
	return nil
}

func (dev *I64Adder) OnInit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /init command...")
	dev.N = 0
	dev.V = 0
	dev.left = make(chan int64)
	dev.right = make(chan int64)
	return nil
}

func (dev *I64Adder) OnReset(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /reset command...")
	dev.N = 0
	dev.V = 0
	dev.left = make(chan int64)
	dev.right = make(chan int64)
	return nil
}

func (dev *I64Adder) OnStart(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /start command...")
	return nil
}

func (dev *I64Adder) OnStop(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	n := dev.N
	v := dev.V
	ctx.Msg.Infof("received /stop command... v=%d -> n=%d", v, n)
	return nil
}

func (dev *I64Adder) OnQuit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /quit command...")
	return nil
}

func (dev *I64Adder) Left(ctx tdaq.Context, src tdaq.Frame) error {
	v := int64(binary.LittleEndian.Uint64(src.Body))
	ctx.Msg.Debugf("received: %d (left)", v)
	select {
	case <-ctx.Ctx.Done():
		return nil
	case dev.left <- v:
		return nil
	}
}

func (dev *I64Adder) Right(ctx tdaq.Context, src tdaq.Frame) error {
	v := int64(binary.LittleEndian.Uint64(src.Body))
	ctx.Msg.Debugf("received: %d (right)", v)
	select {
	case <-ctx.Ctx.Done():
		return nil
	case dev.right <- v:
		return nil
	}
}

func (dev *I64Adder) Output(ctx tdaq.Context, dst *tdaq.Frame) error {
	var (
		a int64 = 0
		b int64 = 0
	)

	select {
	case <-ctx.Ctx.Done():
		dst.Body = nil
		return nil
	case a = <-dev.left:
	}

	select {
	case <-ctx.Ctx.Done():
		dst.Body = nil
		return nil
	case b = <-dev.right:
	}

	sum := a + b
	dev.N++
	dev.V = sum

	dst.Body = make([]byte, 8)
	binary.LittleEndian.PutUint64(dst.Body, uint64(sum))

	return nil
}
