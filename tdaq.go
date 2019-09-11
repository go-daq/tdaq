// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package tdaq is a minimal toolkit to implement a tiny data acquisition system.
package tdaq // import "github.com/go-daq/tdaq"

//go:generate stringer -type FrameType -output z_frametype_string.go .

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"math"

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

type Frame struct {
	Len  int32     // length of frame (Type+Path+Body)
	Type FrameType // type of frame (cmd,data,err,ok)
	Path string    // end-point path
	Body []byte    // frame payload
}

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

var (
	eofFrame []byte
)

func init() {
	buf := new(bytes.Buffer)
	err := sendFrame(context.Background(), buf, FrameEOF, nil, nil)
	if err != nil {
		panic(xerrors.Errorf("tdaq: could not pre-serialize eof-frame: %w", err))
	}
	eofFrame = buf.Bytes()
}

func SendData(ctx context.Context, w io.Writer, path, data []byte) error {
	return sendFrame(ctx, w, FrameData, path, data)
}

func SendMsg(ctx context.Context, w io.Writer, msg MsgFrame) error {
	raw, err := msg.MarshalTDAQ()
	if err != nil {
		return err
	}
	return sendFrame(ctx, w, FrameMsg, []byte("/log"), raw)
}

func SendFrame(ctx context.Context, w io.Writer, frame Frame) error {
	return sendFrame(ctx, w, frame.Type, []byte(frame.Path), frame.Body)
}

func sendFrame(ctx context.Context, w io.Writer, ftype FrameType, path, body []byte) error {

	hdr := make([]byte, 4+1+1)
	binary.LittleEndian.PutUint32(hdr, uint32(len(path))+uint32(len(body)))
	hdr[4] = byte(ftype)
	hdr[5] = byte(len(path))
	r := io.MultiReader(bytes.NewReader(hdr), bytes.NewReader(path), bytes.NewReader(body))
	_, err := io.Copy(w, r)
	return err
}

func RecvFrame(ctx context.Context, r io.Reader) (frame Frame, err error) {
	var hdr = make([]byte, 4+1+1)
	_, err = io.ReadFull(r, hdr)
	if err != nil {
		return frame, xerrors.Errorf("could not receive TDAQ frame header: %w", err)
	}
	size := binary.LittleEndian.Uint32(hdr[:4])
	frame.Len = int32(size)
	frame.Type = FrameType(hdr[4])
	if size == 0 {
		return frame, nil
	}

	if size < 0 || size >= math.MaxInt32 {
		return frame, xerrors.Errorf("corrupted frame (len=%d)", size)
	}

	if int(size) < int(hdr[5]) {
		return frame, xerrors.Errorf("corrupted frame (len=%d, hdr=%d)", size, hdr[5])
	}

	raw := make([]byte, size)
	_, err = io.ReadFull(r, raw)
	if err != nil {
		return frame, xerrors.Errorf("could not receive TDAQ frame body: %w", err)
	}

	frame.Path = string(raw[:int(hdr[5])])
	frame.Body = raw[int(hdr[5]):]

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
