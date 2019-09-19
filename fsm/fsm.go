// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fsm // import "github.com/go-daq/tdaq/fsm"

//go:generate stringer -type Status -output z_status_string.go

// Status describes the current status of a tdaq process.
type Status uint8

const (
	UnConf Status = iota
	Conf
	Init
	Stopped
	Running
	Exiting
	Error
)
