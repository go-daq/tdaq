// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Command tdaq-datasink is a simple program that consumes data.
package main

import (
	"context"

	"github.com/go-daq/tdaq"
	"github.com/go-daq/tdaq/log"
)

func main() {
	dev := device{
		name: "data-sink",
	}

	srv := tdaq.New(":44000", dev.name)
	srv.CmdHandle("/config", dev.OnConfig)
	srv.CmdHandle("/init", dev.OnInit)
	srv.CmdHandle("/reset", dev.OnReset)
	srv.CmdHandle("/start", dev.OnStart)
	srv.CmdHandle("/stop", dev.OnStop)
	srv.CmdHandle("/term", dev.OnTerminate)

	srv.InputHandle("/adc", dev.adc)

	err := srv.Run(context.Background())
	if err != nil {
		log.Panicf("error: %v", err)
	}
}

type device struct {
	name string

	n int
}

func (dev *device) OnConfig(ctx context.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	log.Debugf("received /config command... (%v)", dev.name)
	return nil
}

func (dev *device) OnInit(ctx context.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	log.Debugf("received /init command... (%v)", dev.name)
	dev.n = 0
	return nil
}

func (dev *device) OnReset(ctx context.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	log.Debugf("received /reset command... (%v)", dev.name)
	dev.n = 0
	return nil
}

func (dev *device) OnStart(ctx context.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	log.Debugf("received /start command... (%v)", dev.name)
	return nil
}

func (dev *device) OnStop(ctx context.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	n := dev.n
	log.Debugf("received /stop command... (%v) -> n=%d", dev.name, n)
	return nil
}

func (dev *device) OnTerminate(ctx context.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	log.Debugf("received %q command... (%v)", req.Path, dev.name)
	return nil
}

func (dev *device) adc(ctx context.Context, src tdaq.Frame) error {
	//log.Infof("received: %q (%d) -> %v", src.Body[:16], len(src.Body), dev.n)
	dev.n++
	return nil
}
