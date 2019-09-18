// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xdaq // import "github.com/go-daq/tdaq/xdaq"

import (
	"encoding/binary"

	"github.com/go-daq/tdaq"
)

// I64Dumper dumps int64 values from an input end-point.
type I64Dumper struct {
	N int64 // counter of values seen since /init
	V int64 // last value seen
}

func (dev *I64Dumper) OnConfig(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /config command...")
	return nil
}

func (dev *I64Dumper) OnInit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /init command...")
	dev.N = +0
	dev.V = -1
	return nil
}

func (dev *I64Dumper) OnReset(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /reset command...")
	dev.N = +0
	return nil
}

func (dev *I64Dumper) OnStart(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /start command...")
	return nil
}

func (dev *I64Dumper) OnStop(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	n := dev.N
	v := dev.V
	ctx.Msg.Infof("received /stop command... v=%d -> n=%d", v, n)
	return nil
}

func (dev *I64Dumper) OnQuit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /quit command...")
	return nil
}

func (dev *I64Dumper) Input(ctx tdaq.Context, src tdaq.Frame) error {
	dev.V = int64(binary.LittleEndian.Uint64(src.Body))
	dev.N++
	ctx.Msg.Debugf("received: %d -> %d", dev.V, dev.N)
	return nil
}
