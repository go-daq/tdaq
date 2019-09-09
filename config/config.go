// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package config
package config // import "github.com/go-daq/tdaq/config"

import (
	"github.com/go-daq/tdaq/log"
)

type Process struct {
	Name   string
	Level  log.Level
	RunCtl string

	Args []string
}
