// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fsm // import "github.com/go-daq/tdaq/fsm"

import (
	"github.com/go-daq/tdaq/log"
)

//go:generate stringer -type StateKind -output z_statekind_string.go

type StateKind uint8

const (
	Error StateKind = iota
	UnConf
	UnInit
	Conf
	Stopped
	Running
)

type Status struct {
	Level log.Level
	State StateKind
	Msg   string
	Tags  map[string]string
}

func NewStatus() Status {
	return Status{
		Tags: make(map[string]string),
	}
}
