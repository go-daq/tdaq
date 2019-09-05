// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package iodaq // import "github.com/go-daq/tdaq/iodaq"

//go:generate stringer -type FrameType .

import (
	"context"
	"io"
)

type Frame interface {
	Header() Header
}

type FrameType uint8

const (
	FrameInvalid FrameType = 0x0
	FrameStatus  FrameType = 0x1
	FrameData    FrameType = 0x2
	FrameCmd     FrameType = 0x3
	FrameOK      FrameType = 0x4
	FrameErr     FrameType = 0x5
)

type Header struct {
	Len  uint32
	Type FrameType
}

type RawFrame struct {
	hdr Header
	raw []byte
}

func (rf RawFrame) Header() Header { return rf.hdr }

func RecvFrame(ctx context.Context, r io.Reader) (RawFrame, error) {
	data, err := Recv(ctx, r)
	if err != nil {
		return RawFrame{}, err
	}

	frame := RawFrame{
		hdr: Header{
			Len:  uint32(len(data) + 1),
			Type: FrameType(data[0]),
		},
		raw: data[1:],
	}

	return frame, nil
}

func SendFrame(ctx context.Context, w io.Writer, frame Frame) error {
	panic("not implemented")
}
