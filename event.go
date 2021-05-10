// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq // import "github.com/go-daq/tdaq"

type Event struct {
	Type    uint32 // event type
	Version uint32 // event version
	Flags   uint32

	DeviceID uint32 // device or stream number
	RunNbr   uint32
	EvtNbr   uint32
	TrigNbr  uint32
	_        uint32 // reserved

	Timestamp struct {
		Beg uint64
		End uint64
	}

	Descr  string                 // description
	Tags   map[string]interface{} // tags associated with that event
	Blocks map[uint32][]byte      // blocks of raw data

	Subs []Event // sub events
}
