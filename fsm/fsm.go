// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fsm // import "github.com/go-daq/tdaq/fsm"

import (
	"golang.org/x/xerrors"
)

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

func (st Status) String() string {
	switch st {
	case UnConf:
		return "unconfigured"
	case Conf:
		return "configured"
	case Init:
		return "initialized"
	case Stopped:
		return "stopped"
	case Running:
		return "running"
	case Exiting:
		return "exiting"
	case Error:
		return "error"
	default:
		panic(xerrors.Errorf("invalid status value %d", uint8(st)))
	}
}
