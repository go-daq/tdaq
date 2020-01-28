// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Command tdaq-i64-process is a simple program that consumes int64 data, doubles it and passes it on downstream.
package main

import (
	"context"
	"flag"
	"os"

	"github.com/go-daq/tdaq"
	"github.com/go-daq/tdaq/flags"
	"github.com/go-daq/tdaq/log"
	"github.com/go-daq/tdaq/xdaq"
)

func main() {

	var (
		iname = flag.String("i", "/adc", "name of the input int64 data stream end-point")
		oname = flag.String("o", "/adc2", "name of the output int64 data stream end-point")
	)

	cmd := flags.New()

	dev := xdaq.I64Processor{}
	srv := tdaq.New(cmd, os.Stdout)
	srv.CmdHandle("/config", dev.OnConfig)
	srv.CmdHandle("/init", dev.OnInit)
	srv.CmdHandle("/start", dev.OnStart)
	srv.CmdHandle("/stop", dev.OnStop)
	srv.CmdHandle("/reset", dev.OnReset)
	srv.CmdHandle("/quit", dev.OnQuit)

	srv.InputHandle(*iname, dev.Input)
	srv.OutputHandle(*oname, dev.Output)

	err := srv.Run(context.Background())
	if err != nil {
		log.Panicf("error: %+v", err)
	}
}
