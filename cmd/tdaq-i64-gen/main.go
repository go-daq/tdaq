// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Command tdaq-i64-gen is a simple program that generates int64 data.
package main

import (
	"context"
	"flag"
	"os"
	"time"

	"github.com/go-daq/tdaq"
	"github.com/go-daq/tdaq/flags"
	"github.com/go-daq/tdaq/log"
	"github.com/go-daq/tdaq/tdaqio"
)

func main() {

	var (
		oname = flag.String("o", "/adc", "name of the output int64 data stream end-point")
		start = flag.Int64("start", 10, "starting value of the sequence of int64 values")
		freq  = flag.Duration("freq", 10*time.Millisecond, "frequency of int64 data stream generation")
	)

	cmd := flags.New()

	dev := tdaqio.I64Gen{
		Start: *start,
		Freq:  *freq,
	}

	srv := tdaq.New(cmd, os.Stdout)
	srv.CmdHandle("/config", dev.OnConfig)
	srv.CmdHandle("/init", dev.OnInit)
	srv.CmdHandle("/start", dev.OnStart)
	srv.CmdHandle("/stop", dev.OnStop)
	srv.CmdHandle("/reset", dev.OnReset)
	srv.CmdHandle("/quit", dev.OnQuit)

	srv.OutputHandle(*oname, dev.Output)

	srv.RunHandle(dev.Loop)

	err := srv.Run(context.Background())
	if err != nil {
		log.Panicf("error: %v", err)
	}
}
