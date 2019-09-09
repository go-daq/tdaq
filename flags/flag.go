// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package flags provides an easy creation of standard tdaq flag parameters for tdaq processes
package flags // import "github.com/go-daq/tdaq/flags"

import (
	"flag"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/go-daq/tdaq/config"
	"github.com/go-daq/tdaq/log"
	"golang.org/x/xerrors"
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
		cmd.Name = path.Base(os.Args[0])
	}

	level, err := parseLevel(lvl)
	if err != nil {
		log.Fatalf("could not parse msg-level: %+v", err)
	}
	cmd.Level = level

	return cmd
}

func NewRunControl() config.RunCtl {
	var (
		cmd config.RunCtl
		lvl string
	)

	flag.StringVar(&cmd.Name, "id", "", "name of the tdaq process")
	flag.StringVar(&lvl, "lvl", "INFO", "msgstream level")
	flag.StringVar(&cmd.RunCtl, "rc-addr", ":44000", "[addr]:port of run-ctl process")
	flag.StringVar(&cmd.Web, "web", "", "[addr]:port of run-control web GUI")
	flag.BoolVar(&cmd.Interactive, "i", false, "enable interactive run-ctl shell")

	flag.Parse()

	cmd.Args = flag.Args()

	if cmd.Name == "" {
		cmd.Name = path.Base(os.Args[0])
	}

	level, err := parseLevel(lvl)
	if err != nil {
		log.Fatalf("could not parse msg-level: %+v", err)
	}
	cmd.Level = level

	return cmd
}

func parseLevel(lvl string) (log.Level, error) {
	lvl = strings.ToLower(lvl)
	switch {
	case strings.HasPrefix(lvl, "dbg"), strings.HasPrefix(lvl, "debug"):
		return log.LvlDebug, nil
	case strings.HasPrefix(lvl, "info"):
		return log.LvlInfo, nil
	case strings.HasPrefix(lvl, "warn"):
		return log.LvlWarning, nil
	case strings.HasPrefix(lvl, "err"):
		return log.LvlError, nil
	default:
		v, err := strconv.Atoi(lvl)
		if err != nil {
			return 0, xerrors.Errorf("unknown level value %q: %+v", lvl, err)
		}
		return log.Level(v), nil
	}
}