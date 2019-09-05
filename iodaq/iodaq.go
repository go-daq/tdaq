// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package iodaq defines the TDAQ wire protocol.
package iodaq // import "github.com/go-daq/tdaq/iodaq"

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

type Marshaler interface {
	MarshalTDAQ() ([]byte, error)
}

type Unmarshaler interface {
	UnmarshalTDAQ(p []byte) error
}

func Send(ctx context.Context, w io.Writer, v Marshaler) error {
	raw, err := v.MarshalTDAQ()
	if err != nil {
		return errors.Wrap(err, "could not marshal TDAQ data for sending")
	}

	hdr := make([]byte, 4)
	binary.LittleEndian.PutUint32(hdr, uint32(len(raw)))
	r := io.MultiReader(bytes.NewReader(hdr), bytes.NewReader(raw))

	_, err = io.Copy(w, r)
	return err
}

func Recv(ctx context.Context, r io.Reader) ([]byte, error) {
	hdr := make([]byte, 4)
	_, err := io.ReadFull(r, hdr)
	if err != nil {
		return nil, errors.Wrap(err, "could not receive TDAQ data header")
	}
	n := binary.LittleEndian.Uint32(hdr)

	data := make([]byte, n)
	_, err = io.ReadFull(r, data)
	return data, err
}

type Msg struct {
	Data []byte
}

func (msg Msg) MarshalTDAQ() ([]byte, error) { return msg.Data, nil }
func (msg *Msg) UnmarshalTDAQ(p []byte) error {
	msg.Data = make([]byte, len(p)) // FIXME(sbinet): reuse instead of re-alloc?
	copy(msg.Data, p)
	return nil
}

type Decoder struct {
	r   io.Reader
	err error
	buf []byte
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		r:   r,
		buf: make([]byte, 1024),
	}
}

func (dec *Decoder) Err() error { return dec.err }

func (dec *Decoder) ReadString() string {
	if dec.err != nil {
		return ""
	}
	_, dec.err = io.ReadFull(dec.r, dec.buf[:4])
	if dec.err != nil {
		return ""
	}

	n := binary.LittleEndian.Uint32(dec.buf[:4])
	if nn := uint32(len(dec.buf)); n > nn {
		dec.buf = append(dec.buf, make([]byte, n-nn)...)
	}
	_, dec.err = io.ReadFull(dec.r, dec.buf[:n])
	if dec.err != nil {
		return ""
	}
	return string(dec.buf[:n])
}

var (
	_ Marshaler   = (*Msg)(nil)
	_ Unmarshaler = (*Msg)(nil)
)
