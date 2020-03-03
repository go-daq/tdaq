// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dflow // import "github.com/go-daq/tdaq/internal/dflow"

import (
	"fmt"
	"reflect"
	"testing"
)

func TestGraph(t *testing.T) {
	g := New()
	for _, tt := range []struct {
		name string
		in   []string
		out  []string
		err  error
	}{
		{
			name: "n1",
			in:   []string{"A", "B"},
			out:  []string{"C"},
		},
		{
			name: "n2",
			out:  []string{"A"},
		},
		{
			name: "n3",
			out:  []string{"B", "E"},
		},
		{
			name: "n4",
			in:   []string{"C"},
			out:  []string{"D"},
		},
		{
			name: "n5",
			in:   []string{"D", "E"},
		},
	} {
		err := g.Add(tt.name, tt.in, tt.out)
		switch {
		case err == nil && tt.err == nil:
			// ok
		case err != nil && tt.err != nil:
			if got, want := err.Error(), tt.err.Error(); got != want {
				t.Fatalf("could not build node %q: %v -> %v\ngot = %v\nwant= %v\n", tt.name, tt.in, tt.out, got, want)
			}
		case err == nil && tt.err != nil:
			if got, want := "<nil>", tt.err.Error(); got != want {
				t.Fatalf("could not build node %q: %v -> %v\ngot = %v\nwant= %v\n", tt.name, tt.in, tt.out, got, want)
			}
		case err != nil && tt.err == nil:
			if got, want := err.Error(), "<nil>"; got != want {
				t.Fatalf("could not build node %q: %v -> %v\ngot = %v\nwant= %v\n", tt.name, tt.in, tt.out, got, want)
			}
		}
	}

	dg, err := g.build()
	if err != nil {
		t.Fatalf("could not build graph: %+v", err)
	}

	err = g.check(dg)
	if err != nil {
		t.Fatalf("cycle detected: %+v", err)
	}
}

func TestGraphWithCycle(t *testing.T) {
	g := New()
	for _, tt := range []struct {
		name string
		in   []string
		out  []string
		err  error
	}{
		{
			name: "n1",
			in:   []string{"A", "B"},
			out:  []string{"C"},
		},
		{
			name: "n2",
			out:  []string{"A"},
		},
		{
			name: "n3",
			in:   []string{"D"},
			out:  []string{"B"},
		},
		{
			name: "n4",
			in:   []string{"C"},
			out:  []string{"D"},
		},
	} {
		err := g.Add(tt.name, tt.in, tt.out)
		switch {
		case err == nil && tt.err == nil:
			// ok
		case err != nil && tt.err != nil:
			if got, want := err.Error(), tt.err.Error(); got != want {
				t.Fatalf("could not build node %q: %v -> %v\ngot = %v\nwant= %v\n", tt.name, tt.in, tt.out, got, want)
			}
		case err == nil && tt.err != nil:
			if got, want := "<nil>", tt.err.Error(); got != want {
				t.Fatalf("could not build node %q: %v -> %v\ngot = %v\nwant= %v\n", tt.name, tt.in, tt.out, got, want)
			}
		case err != nil && tt.err == nil:
			if got, want := err.Error(), "<nil>"; got != want {
				t.Fatalf("could not build node %q: %v -> %v\ngot = %v\nwant= %v\n", tt.name, tt.in, tt.out, got, want)
			}
		}
	}

	dg, err := g.build()
	if err != nil {
		t.Fatalf("could not build graph: %+v", err)
	}

	err = g.check(dg)
	if err == nil {
		t.Fatalf("should have detected a cycle")
	}
}

func TestGraphWithNonLocalDuplicateOutput(t *testing.T) {
	g := New()
	for _, tt := range []struct {
		name string
		in   []string
		out  []string
		err  error
	}{
		{
			name: "n1",
			in:   []string{"A", "B"},
			out:  []string{"C"},
		},
		{
			name: "n2",
			out:  []string{"A", "C"},
			//err:  fmt.Errorf(`duplicate output edge "C" (node "n1")`),
		},
		{
			name: "n3",
			out:  []string{"B"},
		},
	} {
		err := g.Add(tt.name, tt.in, tt.out)
		switch {
		case err == nil && tt.err == nil:
			// ok
		case err != nil && tt.err != nil:
			if got, want := err.Error(), tt.err.Error(); got != want {
				t.Fatalf("could not build node %q: %v -> %v\ngot = %v\nwant= %v\n", tt.name, tt.in, tt.out, got, want)
			}
		case err == nil && tt.err != nil:
			if got, want := "<nil>", tt.err.Error(); got != want {
				t.Fatalf("could not build node %q: %v -> %v\ngot = %v\nwant= %v\n", tt.name, tt.in, tt.out, got, want)
			}
		case err != nil && tt.err == nil:
			if got, want := err.Error(), "<nil>"; got != want {
				t.Fatalf("could not build node %q: %v -> %v\ngot = %v\nwant= %v\n", tt.name, tt.in, tt.out, got, want)
			}
		}
	}

	_, err := g.build()
	if err == nil {
		t.Fatalf("expected an error!!")
	}
}

func TestGraphWithLocalDuplicateOutput(t *testing.T) {
	g := New()
	for _, tt := range []struct {
		name string
		in   []string
		out  []string
		err  error
	}{
		{
			name: "n2",
			out:  []string{"A"},
		},
		{
			name: "n1",
			in:   []string{"A"},
			out:  []string{"C", "B", "C"},
			err:  fmt.Errorf(`duplicate outputs for node "n1": [C]`),
		},
	} {
		err := g.Add(tt.name, tt.in, tt.out)
		switch {
		case err == nil && tt.err == nil:
			// ok
		case err != nil && tt.err != nil:
			if got, want := err.Error(), tt.err.Error(); got != want {
				t.Fatalf("could not build node %q: %v -> %v\ngot = %v\nwant= %v\n", tt.name, tt.in, tt.out, got, want)
			}
		case err == nil && tt.err != nil:
			if got, want := "<nil>", tt.err.Error(); got != want {
				t.Fatalf("could not build node %q: %v -> %v\ngot = %v\nwant= %v\n", tt.name, tt.in, tt.out, got, want)
			}
		case err != nil && tt.err == nil:
			if got, want := err.Error(), "<nil>"; got != want {
				t.Fatalf("could not build node %q: %v -> %v\ngot = %v\nwant= %v\n", tt.name, tt.in, tt.out, got, want)
			}
		}
	}

	_, err := g.build()
	if err != nil {
		t.Fatalf("could not build graph: %+v", err)
	}
}

func TestGraphWithLocalDuplicateInput(t *testing.T) {
	g := New()
	for _, tt := range []struct {
		name string
		in   []string
		out  []string
		err  error
	}{
		{
			name: "n2",
			out:  []string{"A"},
		},
		{
			name: "n1",
			in:   []string{"A", "B", "A"},
			out:  []string{"C"},
			err:  fmt.Errorf(`duplicate inputs for node "n1": [A]`),
		},
	} {
		err := g.Add(tt.name, tt.in, tt.out)
		switch {
		case err == nil && tt.err == nil:
			// ok
		case err != nil && tt.err != nil:
			if got, want := err.Error(), tt.err.Error(); got != want {
				t.Fatalf("could not build node %q: %v -> %v\ngot = %v\nwant= %v\n", tt.name, tt.in, tt.out, got, want)
			}
		case err == nil && tt.err != nil:
			if got, want := "<nil>", tt.err.Error(); got != want {
				t.Fatalf("could not build node %q: %v -> %v\ngot = %v\nwant= %v\n", tt.name, tt.in, tt.out, got, want)
			}
		case err != nil && tt.err == nil:
			if got, want := err.Error(), "<nil>"; got != want {
				t.Fatalf("could not build node %q: %v -> %v\ngot = %v\nwant= %v\n", tt.name, tt.in, tt.out, got, want)
			}
		}
	}

	_, err := g.build()
	if err != nil {
		t.Fatalf("could not build graph: %+v", err)
	}
}

func TestGraphWithMissingInput(t *testing.T) {
	g := New()
	for _, tt := range []struct {
		name string
		in   []string
		out  []string
		err  error
	}{
		{
			name: "n2",
			out:  []string{"B"},
		},
		{
			name: "n1",
			in:   []string{"A", "B"},
			out:  []string{"C"},
		},
	} {
		err := g.Add(tt.name, tt.in, tt.out)
		switch {
		case err == nil && tt.err == nil:
			// ok
		case err != nil && tt.err != nil:
			if got, want := err.Error(), tt.err.Error(); got != want {
				t.Fatalf("could not build node %q: %v -> %v\ngot = %v\nwant= %v\n", tt.name, tt.in, tt.out, got, want)
			}
		case err == nil && tt.err != nil:
			if got, want := "<nil>", tt.err.Error(); got != want {
				t.Fatalf("could not build node %q: %v -> %v\ngot = %v\nwant= %v\n", tt.name, tt.in, tt.out, got, want)
			}
		case err != nil && tt.err == nil:
			if got, want := err.Error(), "<nil>"; got != want {
				t.Fatalf("could not build node %q: %v -> %v\ngot = %v\nwant= %v\n", tt.name, tt.in, tt.out, got, want)
			}
		}
	}

	_, err := g.build()
	if err == nil {
		t.Fatalf("expected an error")
	}

	want := fmt.Errorf(`node "n1" declared "A" as input but NO KNOWN produced for it`)
	if got, want := err.Error(), want.Error(); got != want {
		t.Fatalf("invalid error.\ngot = %v\nwant= %v\n", got, want)
	}
}

func TestGraphWithFanIn(t *testing.T) {
	g := New()
	for _, tt := range []struct {
		name string
		in   []string
		out  []string
		err  error
	}{
		{
			name: "n1",
			in:   []string{"A", "B"},
			out:  []string{"C"},
		},
		{
			name: "n2",
			out:  []string{"A"},
		},
		{
			name: "n3",
			out:  []string{"B"},
		},
		{
			name: "n4",
			in:   []string{"B"},
		},
	} {
		err := g.Add(tt.name, tt.in, tt.out)
		switch {
		case err == nil && tt.err == nil:
			// ok
		case err != nil && tt.err != nil:
			if got, want := err.Error(), tt.err.Error(); got != want {
				t.Fatalf("could not build node %q: %v -> %v\ngot = %v\nwant= %v\n", tt.name, tt.in, tt.out, got, want)
			}
		case err == nil && tt.err != nil:
			if got, want := "<nil>", tt.err.Error(); got != want {
				t.Fatalf("could not build node %q: %v -> %v\ngot = %v\nwant= %v\n", tt.name, tt.in, tt.out, got, want)
			}
		case err != nil && tt.err == nil:
			if got, want := err.Error(), "<nil>"; got != want {
				t.Fatalf("could not build node %q: %v -> %v\ngot = %v\nwant= %v\n", tt.name, tt.in, tt.out, got, want)
			}
		}
	}

	dg, err := g.build()
	if err != nil {
		t.Fatalf("could not build graph: %+v", err)
	}

	err = g.check(dg)
	if err != nil {
		t.Fatalf("cycle detected: %+v", err)
	}

	dg2, err := g.build()
	if err != nil {
		t.Fatalf("could not re-build graph: %+v", err)
	}

	if !reflect.DeepEqual(dg, dg2) {
		t.Fatalf("build not idempotent!")
	}
}

func TestGraphWithInconsistentEdge(t *testing.T) {
	g := New()
	for _, tt := range []struct {
		name string
		in   []string
		out  []string
		err  error
	}{
		{
			name: "n1",
			out:  []string{"A"},
		},
		{
			name: "n2",
			in:   []string{"A"},
		},
		{
			name: "n3",
			in:   []string{"A"},
		},
		{
			name: "n4",
			out:  []string{"A"},
		},
	} {
		err := g.Add(tt.name, tt.in, tt.out)
		switch {
		case err == nil && tt.err == nil:
			// ok
		case err != nil && tt.err != nil:
			if got, want := err.Error(), tt.err.Error(); got != want {
				t.Fatalf("could not build node %q: %v -> %v\ngot = %v\nwant= %v\n", tt.name, tt.in, tt.out, got, want)
			}
		case err == nil && tt.err != nil:
			if got, want := "<nil>", tt.err.Error(); got != want {
				t.Fatalf("could not build node %q: %v -> %v\ngot = %v\nwant= %v\n", tt.name, tt.in, tt.out, got, want)
			}
		case err != nil && tt.err == nil:
			if got, want := err.Error(), "<nil>"; got != want {
				t.Fatalf("could not build node %q: %v -> %v\ngot = %v\nwant= %v\n", tt.name, tt.in, tt.out, got, want)
			}
		}
	}

	_, err := g.build()
	if err == nil {
		t.Fatalf("expected an error")
	}

	want := fmt.Errorf(`node "n1" already declared "A" as its output (dup-node="n4")`)
	if got, want := err.Error(), want.Error(); got != want {
		t.Fatalf("invalid error.\ngot = %v\nwant= %v\n", got, want)
	}
}
