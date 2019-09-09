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

type Encoder struct {
	w   io.Writer
	err error

	buf []byte
}

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: w, buf: make([]byte, 8)}
}

func (enc *Encoder) Err() error { return enc.err }

func (enc *Encoder) Encode(v interface{}) error {
	if enc.err != nil {
		return enc.err
	}

	if v, ok := v.(Marshaler); ok {
		raw, err := v.MarshalTDAQ()
		if err != nil {
			enc.err = err
			return err
		}
		enc.WriteU64(uint64(len(raw)))
		_, enc.err = enc.w.Write(raw)
		return enc.err
	}

	switch v := v.(type) {
	case bool:
		enc.WriteBool(v)
	case int8:
		enc.WriteI8(v)
	case int16:
		enc.WriteI16(v)
	case int32:
		enc.WriteI32(v)
	case int64:
		enc.WriteI64(v)
	case uint8:
		enc.WriteU8(v)
	case uint16:
		enc.WriteU16(v)
	case uint32:
		enc.WriteU32(v)
	case uint64:
		enc.WriteU64(v)
	case float32:
		enc.WriteF32(v)
	case float64:
		enc.WriteF64(v)
	case string:
		enc.WriteStr(v)
	default:
		return xerrors.Errorf("value type=%T not supported", v)
	}

	return enc.err
}

func (enc *Encoder) WriteBool(v bool) {
	if enc.err != nil {
		return
	}
	switch v {
	case true:
		enc.buf[0] = 1
	default:
		enc.buf[0] = 0
	}
	_, enc.err = enc.w.Write(enc.buf[:1])
}

func (enc *Encoder) WriteI8(v int8) {
	if enc.err != nil {
		return
	}
	enc.buf[0] = uint8(v)
	_, enc.err = enc.w.Write(enc.buf[:1])
}

func (enc *Encoder) WriteI16(v int16) {
	if enc.err != nil {
		return
	}
	binary.LittleEndian.PutUint16(enc.buf[:2], uint16(v))
	_, enc.err = enc.w.Write(enc.buf[:2])
}

func (enc *Encoder) WriteI32(v int32) {
	if enc.err != nil {
		return
	}
	binary.LittleEndian.PutUint32(enc.buf[:4], uint32(v))
	_, enc.err = enc.w.Write(enc.buf[:4])
}

func (enc *Encoder) WriteI64(v int64) {
	if enc.err != nil {
		return
	}
	binary.LittleEndian.PutUint64(enc.buf[:8], uint64(v))
	_, enc.err = enc.w.Write(enc.buf[:8])
}

func (enc *Encoder) WriteU8(v uint8) {
	if enc.err != nil {
		return
	}
	enc.buf[0] = v
	_, enc.err = enc.w.Write(enc.buf[:1])
}

func (enc *Encoder) WriteU16(v uint16) {
	if enc.err != nil {
		return
	}
	binary.LittleEndian.PutUint16(enc.buf[:2], v)
	_, enc.err = enc.w.Write(enc.buf[:2])
}

func (enc *Encoder) WriteU32(v uint32) {
	if enc.err != nil {
		return
	}
	binary.LittleEndian.PutUint32(enc.buf[:4], v)
	_, enc.err = enc.w.Write(enc.buf[:4])
}

func (enc *Encoder) WriteU64(v uint64) {
	if enc.err != nil {
		return
	}
	binary.LittleEndian.PutUint64(enc.buf[:8], v)
	_, enc.err = enc.w.Write(enc.buf[:8])
}

func (enc *Encoder) WriteF32(v float32) {
	if enc.err != nil {
		return
	}
	binary.LittleEndian.PutUint32(enc.buf[:4], math.Float32bits(v))
	_, enc.err = enc.w.Write(enc.buf[:4])
}

func (enc *Encoder) WriteF64(v float64) {
	if enc.err != nil {
		return
	}
	binary.LittleEndian.PutUint64(enc.buf[:8], math.Float64bits(v))
	_, enc.err = enc.w.Write(enc.buf[:8])
}

func (enc *Encoder) WriteStr(v string) {
	enc.WriteU64(uint64(len(v)))

	if enc.err != nil {
		return
	}
	_, enc.err = enc.w.Write([]byte(v))
}
