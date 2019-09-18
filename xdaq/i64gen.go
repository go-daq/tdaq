// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xdaq // import "github.com/go-daq/tdaq/xdaq"

import (
	"encoding/binary"
	"time"

	"github.com/go-daq/tdaq"
)

// I64Gen publishes int64 data on an output end-point.
type I64Gen struct {
	Start int64         // starting value of the sequence of int64 data.
	N     int64         // last value generated in the sequence.
	Freq  time.Duration // frequency of the int64 data sequence generation.

	ch chan int64
}

func (dev *I64Gen) OnConfig(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /config command...")
	return nil
}

func (dev *I64Gen) OnInit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /init command...")
	dev.N = dev.Start
	dev.ch = make(chan int64)
	if dev.Freq <= 0 {
		dev.Freq = 10 * time.Millisecond
	}
	return nil
}

func (dev *I64Gen) OnReset(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /init command...")
	dev.N = 0
	dev.ch = make(chan int64)
	return nil
}

func (dev *I64Gen) OnStart(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /start command...")
	return nil
}

func (dev *I64Gen) OnStop(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	n := dev.N - 1
	ctx.Msg.Infof("received /stop command... -> n=%d", n)
	return nil
}

func (dev *I64Gen) OnQuit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /quit command...")
	return nil
}

func (dev *I64Gen) Output(ctx tdaq.Context, dst *tdaq.Frame) error {
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

func (dev *I64Gen) Loop(ctx tdaq.Context) error {
	for {
		select {
		case <-ctx.Ctx.Done():
			return nil
		default:
			select {
			case dev.ch <- dev.N:
				dev.N++
			default:
			}
		}
		time.Sleep(dev.Freq)
	}
}
