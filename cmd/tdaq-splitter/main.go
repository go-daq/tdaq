// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Command tdaq-splitter is a simple program that consumes data, applies some random
// filtering and passes it on either the left- or right-downstream.
package main

import (
	"context"
	"flag"
	"os"

	"github.com/go-daq/tdaq"
	"github.com/go-daq/tdaq/flags"
	"github.com/go-daq/tdaq/log"
	"github.com/go-daq/tdaq/xdaq"
	"golang.org/x/exp/rand"
)

func main() {

	var (
		iname = flag.String("i", "/input", "name of the input data stream end-point")
		lname = flag.String("ol", "/left", "name of the left output data stream end-point")
		rname = flag.String("or", "/right", "name of the right output data stream end-point")
		frac  = flag.Float64("frac", 0.5, "fraction of input data to accept on the left stream")
		seed  = flag.Uint64("seed", 1234, "seed for the random number generator")
	)

	cmd := flags.New()

	fct := func(seed uint64, frac float64) func() int {
		rnd := rand.New(rand.NewSource(seed))
		return func() int {
			switch v := rnd.Float64(); {
			case v < frac:
				return -1
			default:
				return +1
			}
		}
	}

	dev := xdaq.Splitter{Fct: fct(*seed, *frac)}
	srv := tdaq.New(cmd, os.Stdout)
	srv.CmdHandle("/config", dev.OnConfig)
	srv.CmdHandle("/init", dev.OnInit)
	srv.CmdHandle("/start", dev.OnStart)
	srv.CmdHandle("/stop", dev.OnStop)
	srv.CmdHandle("/reset", func(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
		// reset state of random generator as well
		dev.Fct = fct(*seed, *frac)
		return dev.OnReset(ctx, resp, req)
	})
	srv.CmdHandle("/quit", dev.OnQuit)

	srv.InputHandle(*iname, dev.Input)
	srv.OutputHandle(*lname, dev.Left)
	srv.OutputHandle(*rname, dev.Right)

	err := srv.Run(context.Background())
	if err != nil {
		log.Panicf("error: %v", err)
	}
}
