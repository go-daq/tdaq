// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Command tdaq-datasrc is a simple program that generates data.
package main

import (
	"context"
	"math/rand"
	"time"

	"github.com/go-daq/tdaq"
	"github.com/go-daq/tdaq/log"
)

func main() {
	dev := datasrc{
		name: "data-src",
		seed: 1234,
	}

	srv := tdaq.New(":44000", dev.name)
	srv.CmdHandle("/config", dev.OnConfig)
	srv.CmdHandle("/init", dev.OnInit)
	srv.CmdHandle("/reset", dev.OnReset)
	srv.CmdHandle("/start", dev.OnStart)
	srv.CmdHandle("/stop", dev.OnStop)
	srv.CmdHandle("/term", dev.OnTerminate)

	srv.OutputHandle("/adc", dev.adc)

	err := srv.Run(context.Background())
	if err != nil {
		log.Panicf("error: %v", err)
	}
}

type datasrc struct {
	name string

	seed int64
	rnd  *rand.Rand

	n    int
	data chan []byte
}

func (dev *datasrc) OnConfig(ctx context.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	log.Debugf("received /config command... (%v)", dev.name)
	return nil
}

func (dev *datasrc) OnInit(ctx context.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	log.Debugf("received /init command... (%v)", dev.name)
	dev.rnd = rand.New(rand.NewSource(dev.seed))
	dev.data = make(chan []byte, 1024)
	dev.n = 0
	return nil
}

func (dev *datasrc) OnReset(ctx context.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	log.Debugf("received /reset command... (%v)", dev.name)
	dev.rnd = rand.New(rand.NewSource(dev.seed))
	dev.data = make(chan []byte, 1024)
	dev.n = 0
	return nil
}

func (dev *datasrc) OnStart(ctx context.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	log.Debugf("received /start command... (%v)", dev.name)
	go dev.run(ctx)
	return nil
}

func (dev *datasrc) OnStop(ctx context.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	n := dev.n
	log.Debugf("received /stop command... (%v) -> n=%d", dev.name, n)
	return nil
}

func (dev *datasrc) OnTerminate(ctx context.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	log.Debugf("received %q command... (%v)", req.Path, dev.name)
	return nil
}

func (dev *datasrc) adc(ctx context.Context, dst *tdaq.Frame) error {
	data := <-dev.data
	dst.Body = data
	return nil
}

func (dev *datasrc) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			raw := make([]byte, 1024)
			rand.Read(raw)
			select {
			case dev.data <- raw:
				dev.n++
			default:
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
