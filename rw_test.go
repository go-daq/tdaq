// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq_test

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/go-daq/tdaq"
)

func TestTranscoder(t *testing.T) {
	for _, tt := range []struct {
		name string
		wfct func(io.Writer, interface{}) error
		rfct func(io.Reader) (interface{}, error)
		want interface{}
	}{
		{
			name: "bool-false",
			wfct: func(w io.Writer, v interface{}) error {
				enc := tdaq.NewEncoder(w)
				enc.WriteBool(v.(bool))
				return enc.Err()
			},
			rfct: func(r io.Reader) (interface{}, error) {
				dec := tdaq.NewDecoder(r)
				v := dec.ReadBool()
				return v, dec.Err()
			},
			want: false,
		},
		{
			name: "bool-true",
			wfct: func(w io.Writer, v interface{}) error {
				enc := tdaq.NewEncoder(w)
				enc.WriteBool(v.(bool))
				return enc.Err()
			},
			rfct: func(r io.Reader) (interface{}, error) {
				dec := tdaq.NewDecoder(r)
				v := dec.ReadBool()
				return v, dec.Err()
			},
			want: true,
		},
		{
			name: "u8",
			wfct: func(w io.Writer, v interface{}) error {
				enc := tdaq.NewEncoder(w)
				enc.WriteU8(v.(uint8))
				return enc.Err()
			},
			rfct: func(r io.Reader) (interface{}, error) {
				dec := tdaq.NewDecoder(r)
				v := dec.ReadU8()
				return v, dec.Err()
			},
			want: uint8(42),
		},
		{
			name: "u16",
			wfct: func(w io.Writer, v interface{}) error {
				enc := tdaq.NewEncoder(w)
				enc.WriteU16(v.(uint16))
				return enc.Err()
			},
			rfct: func(r io.Reader) (interface{}, error) {
				dec := tdaq.NewDecoder(r)
				v := dec.ReadU16()
				return v, dec.Err()
			},
			want: uint16(42),
		},
		{
			name: "uint32",
			wfct: func(w io.Writer, v interface{}) error {
				enc := tdaq.NewEncoder(w)
				enc.WriteU32(v.(uint32))
				return enc.Err()
			},
			rfct: func(r io.Reader) (interface{}, error) {
				dec := tdaq.NewDecoder(r)
				v := dec.ReadU32()
				return v, dec.Err()
			},
			want: uint32(42),
		},
		{
			name: "uint64",
			wfct: func(w io.Writer, v interface{}) error {
				enc := tdaq.NewEncoder(w)
				enc.WriteU64(v.(uint64))
				return enc.Err()
			},
			rfct: func(r io.Reader) (interface{}, error) {
				dec := tdaq.NewDecoder(r)
				v := dec.ReadU64()
				return v, dec.Err()
			},
			want: uint64(42),
		},
		{
			name: "i8",
			wfct: func(w io.Writer, v interface{}) error {
				enc := tdaq.NewEncoder(w)
				enc.WriteI8(v.(int8))
				return enc.Err()
			},
			rfct: func(r io.Reader) (interface{}, error) {
				dec := tdaq.NewDecoder(r)
				v := dec.ReadI8()
				return v, dec.Err()
			},
			want: int8(-42),
		},
		{
			name: "i16",
			wfct: func(w io.Writer, v interface{}) error {
				enc := tdaq.NewEncoder(w)
				enc.WriteI16(v.(int16))
				return enc.Err()
			},
			rfct: func(r io.Reader) (interface{}, error) {
				dec := tdaq.NewDecoder(r)
				v := dec.ReadI16()
				return v, dec.Err()
			},
			want: int16(-42),
		},
		{
			name: "int32",
			wfct: func(w io.Writer, v interface{}) error {
				enc := tdaq.NewEncoder(w)
				enc.WriteI32(v.(int32))
				return enc.Err()
			},
			rfct: func(r io.Reader) (interface{}, error) {
				dec := tdaq.NewDecoder(r)
				v := dec.ReadI32()
				return v, dec.Err()
			},
			want: int32(-42),
		},
		{
			name: "int64",
			wfct: func(w io.Writer, v interface{}) error {
				enc := tdaq.NewEncoder(w)
				enc.WriteI64(v.(int64))
				return enc.Err()
			},
			rfct: func(r io.Reader) (interface{}, error) {
				dec := tdaq.NewDecoder(r)
				v := dec.ReadI64()
				return v, dec.Err()
			},
			want: int64(-42),
		},
		{
			name: "float32",
			wfct: func(w io.Writer, v interface{}) error {
				enc := tdaq.NewEncoder(w)
				enc.WriteF32(v.(float32))
				return enc.Err()
			},
			rfct: func(r io.Reader) (interface{}, error) {
				dec := tdaq.NewDecoder(r)
				v := dec.ReadF32()
				return v, dec.Err()
			},
			want: float32(-42),
		},
		{
			name: "float64",
			wfct: func(w io.Writer, v interface{}) error {
				enc := tdaq.NewEncoder(w)
				enc.WriteF64(v.(float64))
				return enc.Err()
			},
			rfct: func(r io.Reader) (interface{}, error) {
				dec := tdaq.NewDecoder(r)
				v := dec.ReadF64()
				return v, dec.Err()
			},
			want: float64(-42),
		},
		{
			name: "string",
			wfct: func(w io.Writer, v interface{}) error {
				enc := tdaq.NewEncoder(w)
				enc.WriteStr(v.(string))
				return enc.Err()
			},
			rfct: func(r io.Reader) (interface{}, error) {
				dec := tdaq.NewDecoder(r)
				v := dec.ReadStr()
				return v, dec.Err()
			},
			want: "hello-tdaq",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			err := tt.wfct(buf, tt.want)
			if err != nil {
				t.Fatalf("could not encode value %v: %+v", tt.want, err)
			}

			got, err := tt.rfct(buf)
			if err != nil {
				t.Fatalf("could not decode value %v: %+v", tt.want, err)
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("r/w round trip failed:\ngot = %v\nwant= %v\n", got, tt.want)
			}

			{
				err := tt.wfct(failWriter{}, tt.want)
				switch err {
				case nil:
					t.Fatalf("expected an error")
				case io.EOF:
					// ok.
				default:
					t.Fatalf("expected io.EOF, got %+v", err)
				}

				_, err = tt.rfct(failReader{})
				switch err {
				case nil:
					t.Fatalf("expected an error")
				case io.EOF:
					// ok.
				default:
					t.Fatalf("expected io.EOF, got %+v", err)
				}
			}
			{
				buf := new(bytes.Buffer)
				enc := tdaq.NewEncoder(buf)
				err := enc.Encode(tt.want)
				if err != nil {
					t.Fatalf("could not encode value %v: %+v", tt.want, err)
				}

				dec := tdaq.NewDecoder(buf)
				got := reflect.New(reflect.TypeOf(tt.want)).Elem()
				err = dec.Decode(got.Addr().Interface())
				if err != nil {
					t.Fatalf("could not decode value %v: %+v", tt.want, err)
				}

				if got, want := got.Interface(), tt.want; !reflect.DeepEqual(got, want) {
					t.Fatalf("invalid r/w round-trip:\ngot = %v\nwant= %v\n", got, want)
				}
			}
			{
				enc := tdaq.NewEncoder(failWriter{})
				err := enc.Encode(tt.want)
				if err == nil {
					t.Fatalf("expected an error")
				}
				err = enc.Encode(tt.want)
				if err == nil {
					t.Fatalf("expected an error")
				}

				dec := tdaq.NewDecoder(failReader{})
				got := reflect.New(reflect.TypeOf(tt.want)).Elem()
				err = dec.Decode(got.Addr().Interface())
				if err == nil {
					t.Fatalf("expected an error")
				}
				err = dec.Decode(got.Addr().Interface())
				if err == nil {
					t.Fatalf("expected an error")
				}
			}
		})
	}
}
func TestMarshaler(t *testing.T) {
	want := testDataType{n: "hello", v: -42}
	buf := new(bytes.Buffer)
	enc := tdaq.NewEncoder(buf)
	err := enc.Encode(want)
	if err != nil {
		t.Fatalf("could not encode value %v: %+v", want, err)
	}

	var got testDataType
	dec := tdaq.NewDecoder(buf)
	err = dec.Decode(&got)
	if err != nil {
		t.Fatalf("could not decode value %v: %+v", want, err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("invalid r/w round-trop:\ngot = %#v\nwant= %#v\n", got, want)
	}
}

type testDataType struct {
	n string
	v int64
}

func (t testDataType) MarshalTDAQ() ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := tdaq.NewEncoder(buf)
	enc.WriteStr(t.n)
	enc.WriteI64(t.v)
	return buf.Bytes(), enc.Err()
}

func (t *testDataType) UnmarshalTDAQ(p []byte) error {
	dec := tdaq.NewDecoder(bytes.NewReader(p))
	t.n = dec.ReadStr()
	t.v = dec.ReadI64()
	return dec.Err()
}

type failReader struct{}

func (failReader) Read([]byte) (int, error) { return 0, io.EOF }

type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, io.EOF }

var (
	_ io.Reader = (*failReader)(nil)
	_ io.Writer = (*failWriter)(nil)

	_ tdaq.Marshaler   = (*testDataType)(nil)
	_ tdaq.Unmarshaler = (*testDataType)(nil)
)
