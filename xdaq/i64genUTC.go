// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xdaq // import "github.com/go-daq/tdaq/xdaq"

import (
	"encoding/binary"
	"time"

	"github.com/go-daq/tdaq"
)

// I64GenUTC publishes int64 data (time.Now in UTC) on an output end-point.
type I64GenUTC struct {
	N    int64         // number of values generated since /init.
	Freq time.Duration // frequency of the int64 data sequence generation.

	ch chan int64
}

func (dev *I64GenUTC) OnConfig(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /config command...")
	return nil
}

func (dev *I64GenUTC) OnInit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /init command...")
	dev.N = 0
	dev.ch = make(chan int64)
	if dev.Freq <= 0 {
		dev.Freq = 10 * time.Millisecond
	}
	return nil
}

func (dev *I64GenUTC) OnReset(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /init command...")
	dev.N = 0
	dev.ch = make(chan int64)
	return nil
}

func (dev *I64GenUTC) OnStart(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /start command...")
	return nil
}

func (dev *I64GenUTC) OnStop(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	n := dev.N - 1
	ctx.Msg.Infof("received /stop command... -> n=%d", n)
	return nil
}

func (dev *I64GenUTC) OnQuit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /quit command...")
	return nil
}

func (dev *I64GenUTC) Output(ctx tdaq.Context, dst *tdaq.Frame) error {
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

func (dev *I64GenUTC) Loop(ctx tdaq.Context) error {
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

func (dev *I64GenUTC) now() int64 {
	return time.Now().UTC().UnixNano()
}
