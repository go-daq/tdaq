// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package flags provides an easy creation of standard tdaq flag parameters for tdaq processes
package flags // import "github.com/go-daq/tdaq/flags"

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/go-daq/tdaq/config"
	"github.com/go-daq/tdaq/log"
)

func New() config.Process {
	var (
		cmd config.Process
		lvl string
	)

	flag.StringVar(&cmd.Name, "id", "", "name of the tdaq process")
	flag.StringVar(&lvl, "lvl", "INFO", "msgstream level")
	flag.StringVar(&cmd.RunCtl, "rc-addr", ":44000", "[addr]:port of run-control process")

	flag.Parse()

	cmd.Args = flag.Args()

	if cmd.Name == "" {
		fmt.Fprintf(os.Stderr, "missing tdaq process name\n")
		flag.Usage()
		os.Exit(1)
	}

	lvl = strings.ToLower(lvl)
	switch {
	case strings.HasPrefix(lvl, "dbg"), strings.HasPrefix(lvl, "debug"):
		cmd.Level = log.LvlDebug
	case strings.HasPrefix(lvl, "info"):
		cmd.Level = log.LvlInfo
	case strings.HasPrefix(lvl, "warn"):
		cmd.Level = log.LvlWarning
	case strings.HasPrefix(lvl, "err"):
		cmd.Level = log.LvlError
	default:
		v, err := strconv.Atoi(lvl)
		if err != nil {
			log.Fatalf("unknown level value %q: %+v", lvl, err)
		}
		cmd.Level = log.Level(v)
	}

	return cmd
}
