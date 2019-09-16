// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Command tdaq-i64-process is a simple program that consumes int64 data, doubles it and passes it on downstream.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"os"

	"github.com/go-daq/tdaq"
	"github.com/go-daq/tdaq/flags"
	"github.com/go-daq/tdaq/log"
)

func main() {

	var (
		iname = flag.String("i", "/adc", "name of the input int64 data stream end-point")
		oname = flag.String("o", "/adc2", "name of the output int64 data stream end-point")
	)

	cmd := flags.New()

	dev := device{}
	srv := tdaq.New(cmd, os.Stdout)
	srv.CmdHandle("/init", dev.OnInit)
	srv.CmdHandle("/start", dev.OnStart)
	srv.CmdHandle("/stop", dev.OnStop)
	srv.CmdHandle("/quit", dev.OnQuit)

	srv.InputHandle(*iname, dev.inadc)
	srv.OutputHandle(*oname, dev.outadc)

	err := srv.Run(context.Background())
	if err != nil {
		log.Panicf("error: %v", err)
	}
}

type device struct {
	n  int64
	ch chan int64
}

func (dev *device) OnInit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /init command...")
	dev.n = 0
	dev.ch = make(chan int64)
	return nil
}

func (dev *device) OnReset(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /reset command...")
	dev.n = 0
	dev.ch = make(chan int64)
	return nil
}

func (dev *device) OnStart(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /start command...")
	return nil
}

func (dev *device) OnStop(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	n := dev.n
	ctx.Msg.Debugf("received /stop command... -> n=%d", n)
	return nil
}

func (dev *device) OnQuit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received %q command...", req.Path)
	return nil
}

func (dev *device) inadc(ctx tdaq.Context, src tdaq.Frame) error {
	v := int64(binary.LittleEndian.Uint64(src.Body))
	dev.n++
	ctx.Msg.Debugf("received: %d -> %d", v, dev.n)
	dev.ch <- v
	return nil
}

func (dev *device) outadc(ctx tdaq.Context, dst *tdaq.Frame) error {
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
