// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaqio // import "github.com/go-daq/tdaq/tdaqio"

import (
	"encoding/binary"

	"github.com/go-daq/tdaq"
)

type I64Processor struct {
	n  int64
	ch chan int64
}

func (dev *I64Processor) OnInit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /init command...")
	dev.n = 0
	dev.ch = make(chan int64)
	return nil
}

func (dev *I64Processor) OnReset(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /reset command...")
	dev.n = 0
	dev.ch = make(chan int64)
	return nil
}

func (dev *I64Processor) OnStart(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /start command...")
	return nil
}

func (dev *I64Processor) OnStop(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	n := dev.n
	ctx.Msg.Debugf("received /stop command... -> n=%d", n)
	return nil
}

func (dev *I64Processor) OnQuit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received %q command...", req.Path)
	return nil
}

func (dev *I64Processor) Input(ctx tdaq.Context, src tdaq.Frame) error {
	v := int64(binary.LittleEndian.Uint64(src.Body))
	dev.n++
	ctx.Msg.Debugf("received: %d -> %d", v, dev.n)
	dev.ch <- v
	return nil
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
