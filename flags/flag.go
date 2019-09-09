// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package flags provides an easy creation of standard tdaq flag parameters for tdaq processes
package flags // import "github.com/go-daq/tdaq/flags"

import (
	"flag"
	"fmt"
	"os"
)

type Cmd struct {
	Name   string
	RunCtl string

	Args []string
}

func New() Cmd {
	var cmd Cmd

	flag.StringVar(&cmd.Name, "id", "", "name of the tdaq process")
	flag.StringVar(&cmd.RunCtl, "rc-addr", ":44000", "[addr]:port of run-control process")

	flag.Parse()

	cmd.Args = flag.Args()

	if cmd.Name == "" {
		fmt.Fprintf(os.Stderr, "missing tdaq process name\n")
		flag.Usage()
		os.Exit(1)
	}

	return cmd
}
