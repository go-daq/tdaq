// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xdaq_test // import "github.com/go-daq/tdaq/xdaq"

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/go-daq/tdaq"
	"github.com/go-daq/tdaq/internal/tcputil"
	"github.com/go-daq/tdaq/job"
	"github.com/go-daq/tdaq/log"
	"github.com/go-daq/tdaq/xdaq"
	"golang.org/x/xerrors"
)

type pstate struct {
	name string
	n    *int64
	v    *int64
}

func TestSequence(t *testing.T) {
	t.Parallel()

	const (
		rclvl   = log.LvlDebug
		proclvl = log.LvlDebug
	)

	port, err := tcputil.GetTCPPort()
	if err != nil {
		t.Fatalf("could not find a tcp port for run-ctl: %+v", err)
	}

	rcAddr := ":" + port

	port, err = tcputil.GetTCPPort()
	if err != nil {
		t.Fatalf("could not find a tcp port for run-ctl web server: %+v", err)
	}
	webAddr := ":" + port

	stdout := new(bytes.Buffer)
	app := job.New("tcp", stdout)
	defer func() {
		if err != nil {
			t.Logf("stdout:\n%v\n", stdout.String())
		}
	}()

	app.Cfg.RunCtl = rcAddr
	app.Cfg.Web = webAddr
	app.Cfg.Level = rclvl

	var (
		proc1 = pstate{name: "proc-1"}
		proc2 = pstate{name: "proc-2"}
		proc3 = pstate{name: "proc-3.1"}
		proc4 = pstate{name: "proc-3.2"}
	)

	app.Add(
		func() job.Proc {
			dev := new(xdaq.I64Gen)
			proc1.v = &dev.N
			return job.Proc{
				Dev:   dev,
				Name:  proc1.name,
				Level: proclvl,
				Outputs: job.OutputHandlers{
					"/i64-1": dev.Output,
				},
				Handlers: job.RunHandlers{dev.Loop},
			}
		}(),
		func() job.Proc {
			dev := new(xdaq.I64Processor)
			proc2.v = &dev.V
			return job.Proc{
				Dev:   dev,
				Name:  proc2.name,
				Level: proclvl,
				Inputs: job.InputHandlers{
					"/i64-1": dev.Input,
				},
				Outputs: job.OutputHandlers{
					"/i64-2": dev.Output,
				},
			}
		}(),
		func() job.Proc {
			dev := new(xdaq.I64Dumper)
			proc3.v = &dev.V
			return job.Proc{
				Dev:   dev,
				Name:  proc3.name,
				Level: proclvl,
				Inputs: job.InputHandlers{
					"/i64-2": dev.Input,
				},
			}

		}(),
		func() job.Proc {
			dev := new(xdaq.I64Dumper)
			proc4.v = &dev.V
			return job.Proc{
				Dev:   dev,
				Name:  proc4.name,
				Level: proclvl,
				Inputs: job.InputHandlers{
					"/i64-2": dev.Input,
				},
			}

		}(),
	)

	err = app.Start()
	if err != nil {
		t.Fatalf("could not start tdaq app: %+v", err)
	}

	for _, tt := range []struct {
		name string
		cmd  tdaq.CmdType
	}{
		{"/config", tdaq.CmdConfig},
		{"/init", tdaq.CmdInit},
		{"/reset", tdaq.CmdReset},
		{"/config", tdaq.CmdConfig},
		{"/init", tdaq.CmdInit},
		{"/start", tdaq.CmdStart},
		{"/stop", tdaq.CmdStop},
		{"/status", tdaq.CmdStatus},
		{"/start", tdaq.CmdStart},
		{"/stop", tdaq.CmdStop},
		{"/quit", tdaq.CmdQuit},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		func() {
			defer cancel()
			err = app.Do(ctx, tt.cmd)
			if err != nil {
				t.Fatalf("could not send command %v: %+v", tt.cmd, err)
			}
			if tt.name == "/start" {
				time.Sleep(100 * time.Millisecond)
			}
			if tt.name == "/stop" {
				switch {
				case *proc1.v-1 != *proc2.v:
					err = xerrors.Errorf("stage-1 error")
					t.Fatalf("stage-1 error: %q:%v, %q:%v", proc1.name, *proc1.v, proc2.name, *proc2.v)
				case *proc2.v*2 != *proc3.v:
					err = xerrors.Errorf("stage-2 error")
					t.Fatalf("stage-2 error: %q:%v, %q:%v", proc2.name, *proc2.v, proc3.name, *proc3.v)
				case *proc3.v != *proc4.v:
					err = xerrors.Errorf("stage-3 error")
					t.Fatalf("stage-3 error: %q:%v, %q:%v", proc3.name, *proc3.v, proc4.name, *proc4.v)
				}
			}
		}()
	}

	err = app.Wait()
	if err != nil {
		t.Fatalf("error shutting down tdaq app: %+v", err)
	}
}

func TestAdder(t *testing.T) {
	t.Parallel()

	const (
		rclvl   = log.LvlDebug
		proclvl = log.LvlDebug
	)

	port, err := tcputil.GetTCPPort()
	if err != nil {
		t.Fatalf("could not find a tcp port for run-ctl: %+v", err)
	}

	rcAddr := ":" + port

	port, err = tcputil.GetTCPPort()
	if err != nil {
		t.Fatalf("could not find a tcp port for run-ctl web server: %+v", err)
	}
	webAddr := ":" + port

	stdout := new(bytes.Buffer)
	app := job.New("tcp", stdout)
	defer func() {
		if err != nil {
			t.Logf("stdout:\n%v\n", stdout.String())
		}
	}()

	app.Cfg.RunCtl = rcAddr
	app.Cfg.Web = webAddr
	app.Cfg.Level = rclvl

	var (
		proc1 = pstate{name: "gen-1"}
		proc2 = pstate{name: "gen-2"}
		proc3 = pstate{name: "adder"}
		proc4 = pstate{name: "dumper"}
	)

	app.Add(
		func() job.Proc {
			dev := new(xdaq.I64Gen)
			proc1.v = &dev.N
			return job.Proc{
				Dev:   dev,
				Name:  proc1.name,
				Level: proclvl,
				Outputs: job.OutputHandlers{
					"/i64-1": dev.Output,
				},
				Handlers: job.RunHandlers{dev.Loop},
			}
		}(),
		func() job.Proc {
			dev := new(xdaq.I64Gen)
			proc2.v = &dev.N
			return job.Proc{
				Dev:   dev,
				Name:  proc2.name,
				Level: proclvl,
				Outputs: job.OutputHandlers{
					"/i64-2": dev.Output,
				},
				Handlers: job.RunHandlers{dev.Loop},
			}
		}(),
		func() job.Proc {
			dev := new(xdaq.I64Adder)
			proc3.n = &dev.N
			proc3.v = &dev.V
			return job.Proc{
				Dev:   dev,
				Name:  proc3.name,
				Level: proclvl,
				Inputs: job.InputHandlers{
					"/i64-1": dev.Left,
					"/i64-2": dev.Right,
				},
				Outputs: job.OutputHandlers{
					"/sum": dev.Output,
				},
			}
		}(),
		func() job.Proc {
			dev := new(xdaq.I64Dumper)
			proc4.v = &dev.V
			return job.Proc{
				Dev:   dev,
				Name:  proc4.name,
				Level: proclvl,
				Inputs: job.InputHandlers{
					"/sum": dev.Input,
				},
			}
		}(),
	)

	err = app.Start()
	if err != nil {
		t.Fatalf("could not start tdaq app: %+v", err)
	}

	for _, tt := range []struct {
		name string
		cmd  tdaq.CmdType
	}{
		{"/config", tdaq.CmdConfig},
		{"/init", tdaq.CmdInit},
		{"/reset", tdaq.CmdReset},
		{"/config", tdaq.CmdConfig},
		{"/init", tdaq.CmdInit},
		{"/start", tdaq.CmdStart},
		{"/status", tdaq.CmdStatus},
		{"/stop", tdaq.CmdStop},
		{"/quit", tdaq.CmdQuit},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		func() {
			defer cancel()
			err = app.Do(ctx, tt.cmd)
			if err != nil {
				t.Fatalf("could not send command %v: %+v", tt.cmd, err)
			}
			if tt.name == "/start" {
				time.Sleep(100 * time.Millisecond)
			}
			if tt.name == "/stop" {
				switch {
				case *proc3.v != 2*(*proc3.n-1):
					err = xerrors.Errorf("stage-2 error")
					t.Fatalf("stage-2 error: %q:%v, %q:%v", proc3.name+"-sum", *proc3.v, proc3.name+"-n", *proc3.n)
				case *proc3.v != *proc4.v:
					err = xerrors.Errorf("stage-3 error")
					t.Fatalf("stage-3 error: %q:%v, %q:%v", proc3.name, *proc3.v, proc4.name, *proc4.v)
				}
			}
		}()
	}

	err = app.Wait()
	if err != nil {
		t.Fatalf("error shutting down tdaq app: %+v", err)
	}
}

func TestScaler(t *testing.T) {
	t.Parallel()

	const (
		rclvl   = log.LvlDebug
		proclvl = log.LvlDebug
	)

	port, err := tcputil.GetTCPPort()
	if err != nil {
		t.Fatalf("could not find a tcp port for run-ctl: %+v", err)
	}

	rcAddr := ":" + port

	port, err = tcputil.GetTCPPort()
	if err != nil {
		t.Fatalf("could not find a tcp port for run-ctl web server: %+v", err)
	}
	webAddr := ":" + port

	stdout := new(bytes.Buffer)
	app := job.New("tcp", stdout)
	defer func() {
		if err != nil {
			t.Logf("stdout:\n%v\n", stdout.String())
		}
	}()

	app.Cfg.RunCtl = rcAddr
	app.Cfg.Web = webAddr
	app.Cfg.Level = rclvl

	var (
		proc1 = pstate{name: "proc-1"}
		proc2 = pstate{name: "proc-2"}
		proc3 = pstate{name: "proc-3"}
		proc4 = pstate{name: "proc-4"}
		proc5 = pstate{name: "proc-5"}
	)

	app.Add(
		func() job.Proc {
			dev := new(xdaq.I64Gen)
			proc1.n = &dev.N
			return job.Proc{
				Dev:   dev,
				Name:  proc1.name,
				Level: proclvl,
				Outputs: job.OutputHandlers{
					"/i64-1": dev.Output,
				},
				Handlers: job.RunHandlers{dev.Loop},
			}
		}(),
		func() job.Proc {
			var n int64
			dev := &xdaq.Scaler{
				Accept: func() bool {
					n++
					return n%2 == 0
				},
			}
			proc2.n = &n
			return job.Proc{
				Dev:   dev,
				Name:  proc2.name,
				Level: proclvl,
				Cmds: job.CmdHandlers{
					"/reset": func(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
						n = 0
						dev.Accept = func() bool {
							n++
							return n%2 == 0
						}
						return dev.OnReset(ctx, resp, req)
					},
				},
				Inputs: job.InputHandlers{
					"/i64-1": dev.Input,
				},
				Outputs: job.OutputHandlers{
					"/i64-2": dev.Output,
				},
			}
		}(),
		func() job.Proc {
			dev := new(xdaq.I64Dumper)
			proc3.n = &dev.N
			return job.Proc{
				Dev:   dev,
				Name:  proc3.name,
				Level: proclvl,
				Inputs: job.InputHandlers{
					"/i64-2": dev.Input,
				},
			}
		}(),
		func() job.Proc {
			dev := new(xdaq.Scaler)
			return job.Proc{
				Dev:   dev,
				Name:  proc4.name,
				Level: proclvl,
				Inputs: job.InputHandlers{
					"/i64-1": dev.Input,
				},
				Outputs: job.OutputHandlers{
					"/i64-3": dev.Output,
				},
			}
		}(),
		func() job.Proc {
			dev := new(xdaq.I64Dumper)
			proc5.n = &dev.N
			return job.Proc{
				Dev:   dev,
				Name:  proc5.name,
				Level: proclvl,
				Inputs: job.InputHandlers{
					"/i64-3": dev.Input,
				},
			}
		}(),
	)

	err = app.Start()
	if err != nil {
		t.Fatalf("could not start tdaq app: %+v", err)
	}

	for _, tt := range []struct {
		name string
		cmd  tdaq.CmdType
	}{
		{"/config", tdaq.CmdConfig},
		{"/init", tdaq.CmdInit},
		{"/reset", tdaq.CmdReset},
		{"/config", tdaq.CmdConfig},
		{"/init", tdaq.CmdInit},
		{"/start", tdaq.CmdStart},
		{"/status", tdaq.CmdStatus},
		{"/stop", tdaq.CmdStop},
		{"/quit", tdaq.CmdQuit},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		func() {
			defer cancel()
			err = app.Do(ctx, tt.cmd)
			if err != nil {
				t.Fatalf("could not send command %v: %+v", tt.cmd, err)
			}
			if tt.name == "/start" {
				time.Sleep(100 * time.Millisecond)
			}
			if tt.name == "/stop" {
				switch {
				case *proc1.n != *proc2.n:
					err = xerrors.Errorf("stage-1 error")
					t.Fatalf("stage-1: error: %q:%v, %q:%v", proc1.name, *proc1.n, proc2.name, *proc2.n)
				case int64(float64(*proc2.n)*0.5) != int64(float64(*proc3.n)):
					err = xerrors.Errorf("stage-2 error")
					t.Fatalf("stage-2: error: %q:%v, %q:%v", proc2.name, *proc2.n, proc3.name, *proc3.n)
				case *proc5.n != 0:
					err = xerrors.Errorf("stage-3 error")
					t.Fatalf("stage-3: error: %q:%v, %q:%v", proc1.name, *proc1.n, proc5.name, *proc5.n)
				}
			}
		}()
	}

	err = app.Wait()
	if err != nil {
		t.Fatalf("error shutting down tdaq app: %+v", err)
	}
}

func TestSplitter(t *testing.T) {
	t.Parallel()

	const (
		rclvl   = log.LvlDebug
		proclvl = log.LvlDebug
	)

	port, err := tcputil.GetTCPPort()
	if err != nil {
		t.Fatalf("could not find a tcp port for run-ctl: %+v", err)
	}

	rcAddr := ":" + port

	port, err = tcputil.GetTCPPort()
	if err != nil {
		t.Fatalf("could not find a tcp port for run-ctl web server: %+v", err)
	}
	webAddr := ":" + port

	stdout := new(bytes.Buffer)
	app := job.New("tcp", stdout)
	defer func() {
		if err != nil {
			t.Logf("stdout:\n%v\n", stdout.String())
		}
	}()

	app.Cfg.RunCtl = rcAddr
	app.Cfg.Web = webAddr
	app.Cfg.Level = rclvl

	var (
		proc1 = pstate{name: "proc-1"}
		proc2 = pstate{name: "proc-2"}
		proc3 = pstate{name: "proc-3"}
		proc4 = pstate{name: "proc-4"}
		proc5 = pstate{name: "proc-5"}
		proc6 = pstate{name: "proc-6"}
		proc7 = pstate{name: "proc-7"}
	)

	app.Add(
		func() job.Proc {
			dev := new(xdaq.I64Gen)
			proc1.n = &dev.N
			return job.Proc{
				Dev:   dev,
				Name:  proc1.name,
				Level: proclvl,
				Outputs: job.OutputHandlers{
					"/i64": dev.Output,
				},
				Handlers: job.RunHandlers{dev.Loop},
			}
		}(),
		func() job.Proc {
			var n int64
			dev := &xdaq.Splitter{
				Fct: func() int {
					n++
					switch {
					case n%2 == 0:
						return -1
					default:
						return +1
					}
				},
			}
			proc2.n = &n
			return job.Proc{
				Dev:   dev,
				Name:  proc2.name,
				Level: proclvl,
				Cmds: job.CmdHandlers{
					"/reset": func(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
						n = 0
						dev.Fct = func() int {
							n++
							switch {
							case n%2 == 0:
								return -1
							default:
								return +1
							}
						}
						return dev.OnReset(ctx, resp, req)
					},
				},
				Inputs: job.InputHandlers{
					"/i64": dev.Input,
				},
				Outputs: job.OutputHandlers{
					"/i64-1-left":  dev.Left,
					"/i64-1-right": dev.Right,
				},
			}
		}(),
		func() job.Proc {
			dev := new(xdaq.I64Dumper)
			proc3.n = &dev.N
			return job.Proc{
				Dev:   dev,
				Name:  proc3.name,
				Level: proclvl,
				Inputs: job.InputHandlers{
					"/i64-1-left": dev.Input,
				},
			}
		}(),
		func() job.Proc {
			dev := new(xdaq.I64Dumper)
			proc4.n = &dev.N
			return job.Proc{
				Dev:   dev,
				Name:  proc4.name,
				Level: proclvl,
				Inputs: job.InputHandlers{
					"/i64-1-right": dev.Input,
				},
			}
		}(),
		func() job.Proc {
			dev := &xdaq.Splitter{}
			return job.Proc{
				Dev:   dev,
				Name:  proc5.name,
				Level: proclvl,
				Inputs: job.InputHandlers{
					"/i64": dev.Input,
				},
				Outputs: job.OutputHandlers{
					"/i64-2-left":  dev.Left,
					"/i64-2-right": dev.Right,
				},
			}
		}(),
		func() job.Proc {
			dev := new(xdaq.I64Dumper)
			proc6.n = &dev.N
			return job.Proc{
				Dev:   dev,
				Name:  proc6.name,
				Level: proclvl,
				Inputs: job.InputHandlers{
					"/i64-2-left": dev.Input,
				},
			}
		}(),
		func() job.Proc {
			dev := new(xdaq.I64Dumper)
			proc7.n = &dev.N
			return job.Proc{
				Dev:   dev,
				Name:  proc7.name,
				Level: proclvl,
				Inputs: job.InputHandlers{
					"/i64-2-right": dev.Input,
				},
			}
		}(),
	)

	err = app.Start()
	if err != nil {
		t.Fatalf("could not start tdaq app: %+v", err)
	}

	for _, tt := range []struct {
		name string
		cmd  tdaq.CmdType
	}{
		{"/config", tdaq.CmdConfig},
		{"/init", tdaq.CmdInit},
		{"/reset", tdaq.CmdReset},
		{"/config", tdaq.CmdConfig},
		{"/init", tdaq.CmdInit},
		{"/start", tdaq.CmdStart},
		{"/stop", tdaq.CmdStop},
		{"/status", tdaq.CmdStatus},
		{"/start", tdaq.CmdStart},
		{"/stop", tdaq.CmdStop},
		{"/quit", tdaq.CmdQuit},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		func() {
			defer cancel()
			err = app.Do(ctx, tt.cmd)
			if err != nil {
				t.Fatalf("could not send command %v: %+v", tt.cmd, err)
			}
			if tt.name == "/start" {
				time.Sleep(100 * time.Millisecond)
			}
			if tt.name == "/stop" {
				switch {
				case *proc1.n != *proc2.n:
					err = xerrors.Errorf("stage-1 error")
					t.Fatalf("stage-1: error: %q:%v, %q:%v", proc1.name, *proc1.n, proc2.name, *proc2.n)
				case *proc2.n != *proc3.n+*proc4.n:
					err = xerrors.Errorf("stage-2 error")
					t.Fatalf("stage-2: error: %q:%v, %q:%v %q:%v", proc2.name, *proc2.n, proc3.name, *proc3.n, proc4.name, *proc4.n)
				case *proc1.n != *proc6.n+*proc7.n:
					err = xerrors.Errorf("stage-3 error")
					t.Fatalf("stage-3: error: %q:%v, %q:%v %q:%v", proc1.name, *proc1.n, proc6.name, *proc6.n, proc7.name, *proc7.n)
				case *proc7.n != 0:
					err = xerrors.Errorf("stage-4 error")
					t.Fatalf("stage-4: error: %q:%v, %q:%v", proc1.name, *proc1.n, proc7.name, *proc7.n)
				}
			}
		}()
	}

	err = app.Wait()
	if err != nil {
		t.Fatalf("error shutting down tdaq app: %+v", err)
	}
}
