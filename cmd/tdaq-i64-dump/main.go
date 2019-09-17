// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Command tdaq-i64-dump is a simple program that consumes int64 data and dumps it on screen.
package main

import (
	"context"
	"flag"
	"os"

	"github.com/go-daq/tdaq"
	"github.com/go-daq/tdaq/flags"
	"github.com/go-daq/tdaq/log"
	"github.com/go-daq/tdaq/tdaqio"
)

func main() {

	var (
		iname = flag.String("i", "/adc", "name of the input int64 data stream end-point")
	)

	cmd := flags.New()

	dev := tdaqio.I64Dumper{}
	srv := tdaq.New(cmd, os.Stdout)
	srv.CmdHandle("/init", dev.OnInit)
	srv.CmdHandle("/start", dev.OnStart)
	srv.CmdHandle("/stop", dev.OnStop)
	srv.CmdHandle("/quit", dev.OnQuit)

	srv.InputHandle(*iname, dev.Input)

	err := srv.Run(context.Background())
	if err != nil {
		log.Panicf("error: %v", err)
	}
}
