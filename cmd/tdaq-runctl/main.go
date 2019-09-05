// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main // import "github.com/go-daq/tdaq/cmd/tdaq-runctl"

import (
	"context"
	"flag"
	"os"

	"github.com/go-daq/tdaq"
	"github.com/go-daq/tdaq/log"
)

func main() {
	var (
		addr = flag.String("addr", ":44000", "[addr]:port to listen on")
	)

	flag.Parse()

	rc, err := tdaq.NewRunControl(*addr)
	if err != nil {
		log.Errorf("could not create run control: %v", err)
		os.Exit(1)
	}

	err = rc.Run(context.Background())
	if err != nil {
		log.Errorf("could not run run-ctl: %v", err)
		os.Exit(1)
	}
}
