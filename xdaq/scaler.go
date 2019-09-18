// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xdaq // import "github.com/go-daq/tdaq/xdaq"

import (
	"github.com/go-daq/tdaq"
)

// Scaler consumes data from an input end-point and publishes it
// on an output end-point if it is accepted by the Accept function.
type Scaler struct {
	Accept func() bool // decides whether to accept or reject the input data.

	ch chan tdaq.Frame

	acc int64 // number of accepted input data
	tot int64 // total number of input data
}

func (dev *Scaler) OnConfig(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /config command...")
	return nil
}

func (dev *Scaler) OnInit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /init command...")
	if dev.Accept == nil {
		dev.Accept = func() bool { return false }
	}
	dev.ch = make(chan tdaq.Frame)
	dev.acc = 0
	dev.tot = 0
	return nil
}

func (dev *Scaler) OnReset(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /reset command...")
	dev.ch = make(chan tdaq.Frame)
	dev.acc = 0
	dev.tot = 0
	return nil
}

func (dev *Scaler) OnStart(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /start command...")
	return nil
}

func (dev *Scaler) OnStop(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	n := dev.tot
	v := dev.acc
	ctx.Msg.Infof("received /stop command... v=%d/%d -> %g%%", n, v, float64(v)/float64(n)*100)
	return nil
}

func (dev *Scaler) OnQuit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /quit command...")
	return nil
}

func (dev *Scaler) Input(ctx tdaq.Context, src tdaq.Frame) error {
	dev.tot++
	switch {
	case dev.Accept():
		dev.acc++
		ctx.Msg.Debugf("received: %d/%d (accepted)", dev.acc, dev.tot)
		dev.ch <- src
	default:
		ctx.Msg.Debugf("received: %d/%d (rejeted)", dev.acc, dev.tot)
	}
	return nil
}

func (dev *Scaler) Output(ctx tdaq.Context, dst *tdaq.Frame) error {
	select {
	case <-ctx.Ctx.Done():
		dst.Body = nil
		return nil
	case data := <-dev.ch:
		dst.Body = make([]byte, len(data.Body))
		copy(dst.Body, data.Body)
	}
	return nil
}
