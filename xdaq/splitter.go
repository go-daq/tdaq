// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xdaq // import "github.com/go-daq/tdaq/xdaq"

import (
	"github.com/go-daq/tdaq"
)

// Splitter consumes data from an input end-point and publishes it
// on either the "left" or "right" output end-point, whether the Fct function
// returns (resp.) -1 or +1.
type Splitter struct {
	Fct func() int // decides whether to forward the input data on the "left" or the "right".

	left  chan tdaq.Frame
	right chan tdaq.Frame

	acc int64 // number of left-accepted input data
	tot int64 // total number of input data
}

func (dev *Splitter) OnConfig(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /config command...")
	return nil
}

func (dev *Splitter) OnInit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /init command...")
	if dev.Fct == nil {
		dev.Fct = func() int { return -1 }
	}
	dev.left = make(chan tdaq.Frame)
	dev.right = make(chan tdaq.Frame)
	dev.acc = 0
	dev.tot = 0
	return nil
}

func (dev *Splitter) OnReset(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /reset command...")
	dev.left = make(chan tdaq.Frame)
	dev.right = make(chan tdaq.Frame)
	dev.acc = 0
	dev.tot = 0
	return nil
}

func (dev *Splitter) OnStart(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /start command...")
	return nil
}

func (dev *Splitter) OnStop(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	n := dev.tot
	v := dev.acc
	ctx.Msg.Infof("received /stop command... v=%d/%d -> %g%%", n, v, float64(v)/float64(n)*100)
	return nil
}

func (dev *Splitter) OnQuit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /quit command...")
	return nil
}

func (dev *Splitter) Input(ctx tdaq.Context, src tdaq.Frame) error {
	dev.tot++
	switch dev.Fct() {
	case -1:
		select {
		case <-ctx.Ctx.Done():
			ctx.Msg.Debugf("received: %d/%d (left-canceled)", dev.acc, dev.tot)
			return nil

		case dev.left <- src:
			dev.acc++
			ctx.Msg.Debugf("received: %d/%d (left)", dev.acc, dev.tot)
		}
	default:
		select {
		case <-ctx.Ctx.Done():
			ctx.Msg.Debugf("received: %d/%d (right-canceled)", dev.tot-dev.acc, dev.tot)
			return nil
		case dev.right <- src:
			ctx.Msg.Debugf("received: %d/%d (right)", dev.tot-dev.acc, dev.tot)
		}
	}
	return nil
}

func (dev *Splitter) Left(ctx tdaq.Context, dst *tdaq.Frame) error {
	select {
	case <-ctx.Ctx.Done():
		dst.Body = nil
		return nil
	case data := <-dev.left:
		dst.Body = make([]byte, len(data.Body))
		copy(dst.Body, data.Body)
	}
	return nil
}

func (dev *Splitter) Right(ctx tdaq.Context, dst *tdaq.Frame) error {
	select {
	case <-ctx.Ctx.Done():
		dst.Body = nil
		return nil
	case data := <-dev.right:
		dst.Body = make([]byte, len(data.Body))
		copy(dst.Body, data.Body)
	}
	return nil
}
