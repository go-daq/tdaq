// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package iomux provides simple goroutine safe I/O primitives.
package iomux // import "github.com/go-daq/tdaq/internal/iomux"

import (
	"fmt"
	"io"
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
	var o strings.Builder
	fmt.Fprintf(&o, "%v\n", w.w)
	return o.String()
}

var (
	_ io.Writer = (*Writer)(nil)
)
