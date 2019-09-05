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

	"golang.org/x/xerrors"
)

type Marshaler interface {
	MarshalTDAQ() ([]byte, error)
}

type Unmarshaler interface {
	UnmarshalTDAQ(p []byte) error
}

type Frame struct {
	Len  int64     // length of frame (Type+Path+Body)
	Type FrameType // type of frame (cmd,data,err,ok)
	Path string    // end-point path
	Body []byte    // frame payload
}

type FrameType byte

const (
	FrameUnknown FrameType = iota
	FrameCmd
	FrameData
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

func SendFrame(ctx context.Context, w io.Writer, frame Frame) error {
	return sendFrame(ctx, w, frame.Type, []byte(frame.Path), frame.Body)
}

func sendFrame(ctx context.Context, w io.Writer, ftype FrameType, path, body []byte) error {

	hdr := make([]byte, 8+1+1)
	binary.LittleEndian.PutUint64(hdr, uint64(len(path))+uint64(len(body)))
	hdr[8] = byte(ftype)
	hdr[9] = byte(len(path))
	r := io.MultiReader(bytes.NewReader(hdr), bytes.NewReader(path), bytes.NewReader(body))
	_, err := io.Copy(w, r)
	return err
}

func RecvFrame(ctx context.Context, r io.Reader) (frame Frame, err error) {
	var hdr = make([]byte, 8+1+1)
	_, err = io.ReadFull(r, hdr)
	if err != nil {
		return frame, xerrors.Errorf("could not receive TDAQ frame header: %w", err)
	}
	frame.Len = int64(binary.LittleEndian.Uint64(hdr[:8]))
	frame.Type = FrameType(hdr[8])

	raw := make([]byte, frame.Len)
	_, err = io.ReadFull(r, raw)
	if err != nil {
		return frame, xerrors.Errorf("could not receive TDAQ frame body: %w", err)
	}

	frame.Path = string(raw[:int(hdr[9])])
	frame.Body = raw[int(hdr[9]):]

	return frame, nil
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
