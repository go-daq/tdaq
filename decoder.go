// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq // import "github.com/go-daq/tdaq"

import (
	"encoding/binary"
	"io"
	"math"
)

type creader struct {
	r io.Reader
	n int
}

func (r *creader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n += n
	return n, err
}

type Decoder struct {
	r   io.Reader
	err error
	buf []byte
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: r, buf: make([]byte, 8)}
}

func (dec *Decoder) Err() error { return dec.err }

func (dec *Decoder) load(n int) {
	if dec.err != nil {
		copy(dec.buf, []byte{0, 0, 0, 0, 0, 0, 0, 0})
		return
	}
	_, dec.err = io.ReadFull(dec.r, dec.buf[:n])
}

func (dec *Decoder) ReadI8() int8 {
	dec.load(1)
	return int8(dec.buf[0])
}

func (dec *Decoder) ReadI16() int16 {
	dec.load(2)
	return int16(binary.LittleEndian.Uint16(dec.buf[:2]))
}

func (dec *Decoder) ReadI32() int32 {
	dec.load(4)
	return int32(binary.LittleEndian.Uint32(dec.buf[:4]))
}

func (dec *Decoder) ReadI64() int64 {
	dec.load(8)
	return int64(binary.LittleEndian.Uint64(dec.buf[:8]))
}

func (dec *Decoder) ReadU8() uint8 {
	dec.load(1)
	return dec.buf[0]
}

func (dec *Decoder) ReadU16() uint16 {
	dec.load(2)
	return binary.LittleEndian.Uint16(dec.buf[:2])
}

func (dec *Decoder) ReadU32() uint32 {
	dec.load(4)
	return binary.LittleEndian.Uint32(dec.buf[:4])
}

func (dec *Decoder) ReadU64() uint64 {
	dec.load(8)
	return binary.LittleEndian.Uint64(dec.buf[:8])
}

func (dec *Decoder) ReadF32() float32 {
	dec.load(4)
	return math.Float32frombits(binary.LittleEndian.Uint32(dec.buf[:4]))
}

func (dec *Decoder) ReadF64() float64 {
	dec.load(8)
	return math.Float64frombits(binary.LittleEndian.Uint64(dec.buf[:8]))
}

func (dec *Decoder) ReadStr() string {
	n := dec.ReadU64()
	if n == 0 || dec.err != nil {
		return ""
	}
	str := make([]byte, n)
	_, dec.err = io.ReadFull(dec.r, str)
	return string(str)
}
