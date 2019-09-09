// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq // import "github.com/go-daq/tdaq"

import (
	"encoding/binary"
	"io"
	"math"

	"golang.org/x/xerrors"
)

type Decoder struct {
	r   io.Reader
	err error
	buf []byte
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: r, buf: make([]byte, 8)}
}

func (dec *Decoder) Decode(ptr interface{}) error {
	if dec.err != nil {
		return dec.err
	}

	if v, ok := ptr.(Unmarshaler); ok {
		n := dec.ReadU64()
		if dec.err != nil {
			return dec.err
		}
		if sz := uint64(len(dec.buf)); sz < n {
			dec.buf = append(dec.buf, make([]byte, n-sz)...)
		}
		dec.load(int(n))
		if dec.err != nil {
			return dec.err
		}
		dec.err = v.UnmarshalTDAQ(dec.buf)
		return dec.err
	}

	switch v := ptr.(type) {
	case *bool:
		*v = dec.ReadBool()
	case *uint8:
		*v = dec.ReadU8()
	case *uint16:
		*v = dec.ReadU16()
	case *uint32:
		*v = dec.ReadU32()
	case *uint64:
		*v = dec.ReadU64()
	case *int8:
		*v = dec.ReadI8()
	case *int16:
		*v = dec.ReadI16()
	case *int32:
		*v = dec.ReadI32()
	case *int64:
		*v = dec.ReadI64()
	case *float32:
		*v = dec.ReadF32()
	case *float64:
		*v = dec.ReadF64()
	case *string:
		*v = dec.ReadStr()
	default:
		return xerrors.Errorf("invalid value-type=%T", v)
	}
	return dec.err
}

func (dec *Decoder) Err() error { return dec.err }

func (dec *Decoder) load(n int) {
	if dec.err != nil {
		copy(dec.buf, []byte{0, 0, 0, 0, 0, 0, 0, 0})
		return
	}
	_, dec.err = io.ReadFull(dec.r, dec.buf[:n])
}

func (dec *Decoder) ReadBool() bool {
	v := dec.ReadU8()
	if v == 1 {
		return true
	}
	return false
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
