// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Command tdaq-i64-adder is a simple program that consumes 2 streams of int64 data, add them together
// and passes the result on downstream.
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
		lname = flag.String("il", "/left", "name of the left input int64 data stream end-point")
		rname = flag.String("ir", "/right", "name of the right input int64 data stream end-point")
		oname = flag.String("o", "/output", "name of the output int64 data stream end-point")
	)

	cmd := flags.New()

	dev := xdaq.I64Adder{}
	srv := tdaq.New(cmd, os.Stdout)
	srv.CmdHandle("/config", dev.OnConfig)
	srv.CmdHandle("/init", dev.OnInit)
	srv.CmdHandle("/start", dev.OnStart)
	srv.CmdHandle("/stop", dev.OnStop)
	srv.CmdHandle("/reset", dev.OnReset)
	srv.CmdHandle("/quit", dev.OnQuit)

	srv.InputHandle(*lname, dev.Left)
	srv.InputHandle(*rname, dev.Right)
	srv.OutputHandle(*oname, dev.Output)

	err := srv.Run(context.Background())
	if err != nil {
		log.Panicf("error: %v", err)
	}
}
