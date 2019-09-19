// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package iomux // import "github.com/go-daq/tdaq/internal/iomux"

import (
	"bytes"
	"testing"
)

func TestStringer(t *testing.T) {
	want := "hello"

	o := NewWriter(new(bytes.Buffer))
	o.Write([]byte(want))

	got1 := o.String()
	if got1 != want {
		t.Fatalf("invalid stringer: got1=%q, want=%q", got1, want)
	}

	got2 := o.String()
	if got2 != want {
		t.Fatalf("invalid stringer: got2=%q, want=%q", got2, want)
	}
}
