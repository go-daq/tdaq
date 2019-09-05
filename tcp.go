// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq // import "github.com/go-daq/tdaq"

import (
	"net"

	"github.com/go-daq/tdaq/log"
)

func setupTCPConn(conn *net.TCPConn) {
	var err error

	err = conn.SetKeepAlive(true)
	if err != nil {
		log.Warnf("could not set keep-alive: %v", err)
	}
	err = conn.SetLinger(1)
	if err != nil {
		log.Warnf("could not set linger: %v", err)
	}

	return
}
