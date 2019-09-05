// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package coms regroups all the tdaq commands.
package coms // import "github.com/go-daq/tdaq/coms"

//go:generate stringer -type Type .

import (
	"encoding/binary"

	"github.com/go-daq/tdaq/iodaq"
)

type Type uint8

const (
	Init Type = iota
	Config
	Reset
	Start
	Stop
	Terminate
	Status
	Log
	Ok
	Err
)

type Command struct {
	Type Type
	Data []byte
}

func (cmd Command) MarshalTDAQ() ([]byte, error) {
	cur := 0
	buf := make([]byte, 4+1+len(cmd.Data))
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(buf)-4))
	cur += 4
	buf[cur] = uint8(cmd.Type)
	cur++
	copy(buf[cur:], cmd.Data)
	return buf, nil
}

func (cmd *Command) UnmarshalTDAQ(p []byte) error {
	n := binary.LittleEndian.Uint32(p[:4])
	p = p[4:]
	cmd.Type = Type(p[0])
	cmd.Data = append(cmd.Data[:0], p[1:n]...)
	return nil
}

var (
	_ iodaq.Marshaler   = (*Command)(nil)
	_ iodaq.Unmarshaler = (*Command)(nil)
)
