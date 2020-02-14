// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package tdaq is a minimal toolkit to implement a tiny data acquisition system.
package tdaq // import "github.com/go-daq/tdaq"

import (
	"bytes"
	"context"

	"github.com/go-daq/tdaq/log"
	"golang.org/x/xerrors"
)

type Context struct {
	Ctx context.Context
	Msg log.MsgStream
}

type Marshaler interface {
	MarshalTDAQ() ([]byte, error)
}

type Unmarshaler interface {
	UnmarshalTDAQ(p []byte) error
}

type Sender interface {
	Send(msg []byte) error
}

type Recver interface {
	Recv() ([]byte, error)
}

// Frame is the datum being exchanged between tdaq processes.
type Frame struct {
	Type FrameType // type of frame (cmd,data,err,ok)
	Path string    // end-point path
	Body []byte    // frame payload
}

func (f Frame) encode() []byte {
	psz := len(f.Path)
	bsz := len(f.Body)
	beg := 2
	end := beg + psz
	msg := make([]byte, 1+1+psz+bsz)
	msg[0] = byte(f.Type)
	msg[1] = byte(psz)
	copy(msg[beg:end], []byte(f.Path))
	copy(msg[end:], f.Body)

	return msg
}

// FrameType describes the type of a Frame.
type FrameType byte

const (
	FrameUnknown FrameType = iota
	FrameCmd
	FrameData
	FrameMsg
	FrameOK
	FrameEOF
	FrameErr
)

func (ft FrameType) String() string {
	switch ft {
	case FrameUnknown:
		return "unknown-frame"
	case FrameCmd:
		return "cmd-frame"
	case FrameData:
		return "data-frame"
	case FrameMsg:
		return "msg-frame"
	case FrameOK:
		return "ok-frame"
	case FrameEOF:
		return "eof-frame"
	case FrameErr:
		return "err-frame"
	default:
		panic(xerrors.Errorf("invalid frame-type %d", byte(ft)))
	}
}

var (
	eofFrame = []byte{byte(FrameEOF), 0}
)

func SendMsg(ctx context.Context, sck Sender, msg MsgFrame) error {
	raw, err := msg.MarshalTDAQ()
	if err != nil {
		return err
	}
	return sendFrame(ctx, sck, FrameMsg, []byte("/log"), raw)
}

func SendFrame(ctx context.Context, sck Sender, frame Frame) error {
	return sendFrame(ctx, sck, frame.Type, []byte(frame.Path), frame.Body)
}

func sendFrame(ctx context.Context, sck Sender, ftype FrameType, path, body []byte) error {

	psz := len(path)
	bsz := len(body)
	beg := 2
	end := beg + psz
	msg := make([]byte, 1+1+psz+bsz)
	msg[0] = byte(ftype)
	msg[1] = byte(psz)
	copy(msg[beg:end], []byte(path))
	copy(msg[end:], body)
	return sck.Send(msg)
}

func RecvFrame(ctx context.Context, sck Recver) (frame Frame, err error) {

	msg, err := sck.Recv()
	if err != nil {
		return frame, xerrors.Errorf("could not receive TDAQ frame: %w", err)
	}
	frame.Type = FrameType(msg[0])

	psz := int(msg[1])
	beg := 2
	end := beg + psz
	frame.Path = string(msg[beg:end])
	if len(msg[end:]) > 0 {
		frame.Body = msg[end:]
	}

	return frame, nil
}

type MsgFrame struct {
	Name  string
	Level log.Level
	Msg   string
}

func (frame MsgFrame) MarshalTDAQ() ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := NewEncoder(buf)
	enc.WriteStr(frame.Name)
	enc.WriteI8(int8(frame.Level))
	enc.WriteStr(frame.Msg)
	err := enc.Err()
	return buf.Bytes(), err
}

func (frame *MsgFrame) UnmarshalTDAQ(p []byte) error {
	dec := NewDecoder(bytes.NewReader(p))
	frame.Name = dec.ReadStr()
	frame.Level = log.Level(dec.ReadI8())
	frame.Msg = dec.ReadStr()
	return dec.Err()
}

type EndPoint struct {
	Name string
	Addr string
	Type string
}

func (ep EndPoint) MarshalTDAQ() ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := NewEncoder(buf)
	enc.WriteStr(ep.Name)
	enc.WriteStr(ep.Addr)
	enc.WriteStr(ep.Type)

	return buf.Bytes(), enc.err
}

func (ep *EndPoint) UnmarshalTDAQ(b []byte) error {
	dec := NewDecoder(bytes.NewReader(b))
	ep.Name = dec.ReadStr()
	ep.Addr = dec.ReadStr()
	ep.Type = dec.ReadStr()
	return dec.err
}

var (
	_ Marshaler   = (*MsgFrame)(nil)
	_ Unmarshaler = (*MsgFrame)(nil)
)
