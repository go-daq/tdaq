// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package config
package config // import "github.com/go-daq/tdaq/config"

import (
	"github.com/go-daq/tdaq/log"
)

// Process describes how a TDAQ process should be configured.
type Process struct {
	Name   string    // name of the TDAQ process
	Level  log.Level // verbosity level of the TDAQ process
	RunCtl string    // address of the run-ctl of the flock of TDAQ processes

	Args []string // additional flag arguments
}

// RunCtl describes how a TDAQ RunControl process should be configured.
type RunCtl struct {
	Name   string    // name of the run-ctl process
	Level  log.Level // verbosity level of the run-ctl process
	RunCtl string    // address of the TCP run-ctl cmd server
	Log    string    // address of the TCP run-ctl log server
	Web    string    // address of the HTTP run-ctl web server

	Interactive bool // enable interactive shell commands for the run-ctl process

	LogFile string // path to logfile for run-ctl log server

	Args []string // additional flag arguments
}
