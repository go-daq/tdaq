// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package iomux provides simple goroutine safe I/O primitives.
package iomux // import "github.com/go-daq/tdaq/internal/iomux"

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
)

// Writer is a goroutine-safe io.Writer.
type Writer struct {
	mu sync.Mutex
	w  io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

func (w *Writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	n, err := w.w.Write(p)
	w.mu.Unlock()
	return n, err
}

func (w *Writer) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	o := new(strings.Builder)
	fmt.Fprintf(o, "%v", w.w)
	return o.String()
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var err error
	if wc, ok := w.w.(io.Closer); ok {
		err = wc.Close()
	}
	return err
}

var (
	_ io.Writer = (*Writer)(nil)
)

type Socket struct {
	buf net.Buffers
}

func (sck *Socket) Write(p []byte) (int, error) {
	sck.buf = append(sck.buf, p)
	return len(p), nil
}

func (sck *Socket) Read(p []byte) (int, error) {
	return sck.buf.Read(p)
}

func (sck *Socket) Send(p []byte) error {
	sck.buf = append(sck.buf, p)
	return nil
}

func (sck *Socket) Recv() ([]byte, error) {
	n := len(sck.buf)
	buf := sck.buf[n-1]
	sck.buf = sck.buf[:n]

	return buf, nil
}
