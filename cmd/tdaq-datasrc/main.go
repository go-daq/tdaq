// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Command tdaq-datasrc is a simple program that generates data.
package main

import (
	"context"
	"math/rand"
	"os"
	"time"

	"github.com/go-daq/tdaq"
	"github.com/go-daq/tdaq/flags"
	"github.com/go-daq/tdaq/log"
)

func main() {
	cmd := flags.New()

	dev := datasrc{
		seed: 1234,
	}

	srv := tdaq.New(cmd, os.Stdout)
	srv.CmdHandle("/config", dev.OnConfig)
	srv.CmdHandle("/init", dev.OnInit)
	srv.CmdHandle("/reset", dev.OnReset)
	srv.CmdHandle("/start", dev.OnStart)
	srv.CmdHandle("/stop", dev.OnStop)
	srv.CmdHandle("/quit", dev.OnQuit)

	srv.OutputHandle("/adc", dev.adc)

	srv.RunHandle(dev.run)

	err := srv.Run(context.Background())
	if err != nil {
		log.Panicf("error: %+v", err)
	}
}

type datasrc struct {
	seed int64
	rnd  *rand.Rand

	n    int
	data chan []byte
}

func (dev *datasrc) OnConfig(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /config command...")
	return nil
}

func (dev *datasrc) OnInit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /init command...")
	dev.rnd = rand.New(rand.NewSource(dev.seed))
	dev.data = make(chan []byte, 1024)
	dev.n = 0
	return nil
}

func (dev *datasrc) OnReset(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /reset command...")
	dev.rnd = rand.New(rand.NewSource(dev.seed))
	dev.data = make(chan []byte, 1024)
	dev.n = 0
	return nil
}

func (dev *datasrc) OnStart(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /start command...")
	return nil
}

func (dev *datasrc) OnStop(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	n := dev.n
	ctx.Msg.Debugf("received /stop command... -> n=%d", n)
	return nil
}

func (dev *datasrc) OnQuit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received %q command...", req.Path)
	return nil
}

func (dev *datasrc) adc(ctx tdaq.Context, dst *tdaq.Frame) error {
	select {
	case <-ctx.Ctx.Done():
		dst.Body = nil
		return nil
	case data := <-dev.data:
		dst.Body = data
	}
	return nil
}

func (dev *datasrc) run(ctx tdaq.Context) error {
	for {
		select {
		case <-ctx.Ctx.Done():
			return nil
		default:
			raw := make([]byte, 1024)
			dev.rnd.Read(raw)
			select {
			case dev.data <- raw:
				dev.n++
			default:
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
