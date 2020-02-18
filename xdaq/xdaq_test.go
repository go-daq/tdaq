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
	"github.com/go-daq/tdaq/internal/iomux"
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

	stdout := iomux.NewWriter(new(bytes.Buffer))
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

	var (
		gen  *testI64Gen
		dmp1 *testI64Dumper
		dmp2 *testI64Dumper
	)
	app.Add(
		func() job.Proc {
			gen = new(testI64Gen)
			proc1.n = &gen.N
			proc1.v = &gen.V
			return job.Proc{
				Dev:   gen,
				Name:  proc1.name,
				Level: proclvl,
				Outputs: job.OutputHandlers{
					"/i64-1": gen.Output,
				},
			}
		}(),
		func() job.Proc {
			dev := new(xdaq.I64Processor)
			proc2.n = &dev.N
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
			dmp1 = &testI64Dumper{
				drain: make(chan int),
			}
			proc3.n = &dmp1.N
			proc3.v = &dmp1.V
			return job.Proc{
				Dev:   dmp1,
				Name:  proc3.name,
				Level: proclvl,
				Inputs: job.InputHandlers{
					"/i64-2": dmp1.Input,
				},
			}

		}(),
		func() job.Proc {
			dmp2 = &testI64Dumper{
				drain: make(chan int),
			}
			proc4.n = &dmp2.N
			proc4.v = &dmp2.V
			return job.Proc{
				Dev:   dmp2,
				Name:  proc4.name,
				Level: proclvl,
				Inputs: job.InputHandlers{
					"/i64-2": dmp2.Input,
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
		{"/quit", tdaq.CmdQuit},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		const N = 15
		func() {
			defer cancel()
			if tt.name == "/start" {
				gen.mu.Lock()
				gen.N = 0
				gen.V = 0
				dmp1.want = N
				dmp2.want = N
				dmp1.timer = time.NewTimer(1 * time.Second)
				dmp2.timer = time.NewTimer(1 * time.Second)
				gen.mu.Unlock()
				go gen.loop(N)
			}
			err = app.Do(ctx, tt.cmd)
			if err != nil {
				t.Fatalf("could not send command %v: %+v", tt.cmd, err)
			}
			if tt.name == "/start" {
				<-dmp1.drain
				<-dmp2.drain
			}
			if tt.name == "/stop" {
				switch {
				case *proc1.n != *proc2.n:
					err = xerrors.Errorf("stage-1 error")
					t.Fatalf("stage-1 error: %q:%v, %q:%v", proc1.name, *proc1.n, proc2.name, *proc2.n)
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

	stdout := iomux.NewWriter(new(bytes.Buffer))
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

	var (
		gen1 *testI64Gen
		gen2 *testI64Gen
		dump *testI64Dumper
	)

	app.Add(
		func() job.Proc {
			gen1 = new(testI64Gen)
			proc1.v = &gen1.N
			return job.Proc{
				Dev:   gen1,
				Name:  proc1.name,
				Level: proclvl,
				Outputs: job.OutputHandlers{
					"/i64-1": gen1.Output,
				},
			}
		}(),
		func() job.Proc {
			gen2 = new(testI64Gen)
			proc2.v = &gen2.N
			return job.Proc{
				Dev:   gen2,
				Name:  proc2.name,
				Level: proclvl,
				Outputs: job.OutputHandlers{
					"/i64-2": gen2.Output,
				},
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
			dump = &testI64Dumper{
				drain: make(chan int),
			}
			proc4.n = &dump.N
			proc4.v = &dump.V
			return job.Proc{
				Dev:   dump,
				Name:  proc4.name,
				Level: proclvl,
				Inputs: job.InputHandlers{
					"/sum": dump.Input,
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
		const N = 15
		func() {
			defer cancel()
			if tt.name == "/start" {
				dump.want = N
				dump.timer = time.NewTimer(1 * time.Second)
				go gen1.loop(N)
				go gen2.loop(N)
			}
			err = app.Do(ctx, tt.cmd)
			if err != nil {
				t.Fatalf("could not send command %v: %+v", tt.cmd, err)
			}
			if tt.name == "/start" {
				<-dump.drain
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

	stdout := iomux.NewWriter(new(bytes.Buffer))
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

	var (
		gen  *testI64Gen
		dmp1 *testI64Dumper
		dmp2 *testI64Dumper
	)

	app.Add(
		func() job.Proc {
			gen = new(testI64Gen)
			proc1.n = &gen.N
			proc1.v = &gen.V
			return job.Proc{
				Dev:   gen,
				Name:  proc1.name,
				Level: proclvl,
				Outputs: job.OutputHandlers{
					"/i64-1": gen.Output,
				},
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
			dmp1 = &testI64Dumper{
				drain: make(chan int),
			}
			proc3.n = &dmp1.N
			proc3.v = &dmp1.V
			return job.Proc{
				Dev:   dmp1,
				Name:  proc3.name,
				Level: proclvl,
				Inputs: job.InputHandlers{
					"/i64-2": dmp1.Input,
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
			dmp2 = &testI64Dumper{
				drain: make(chan int),
			}
			proc5.n = &dmp2.N
			proc5.v = &dmp2.V
			return job.Proc{
				Dev:   dmp2,
				Name:  proc5.name,
				Level: proclvl,
				Inputs: job.InputHandlers{
					"/i64-3": dmp2.Input,
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
		const N = 16
		func() {
			defer cancel()
			if tt.name == "/start" {
				gen.mu.Lock()
				gen.N = 0
				gen.V = 0
				dmp1.want = N / 2
				dmp1.timer = time.NewTimer(1 * time.Second)
				dmp2.timer = time.NewTimer(1 * time.Second)
				gen.mu.Unlock()
				go gen.loop(N)
			}
			err = app.Do(ctx, tt.cmd)
			if err != nil {
				t.Fatalf("could not send command %v: %+v", tt.cmd, err)
			}
			if tt.name == "/start" {
				<-dmp1.drain
			}
			if tt.name == "/stop" {
				switch {
				case *proc1.n != *proc2.n:
					err = xerrors.Errorf("stage-1 error")
					t.Fatalf("stage-1: error: %q:%v, %q:%v", proc1.name, *proc1.n, proc2.name, *proc2.n)
				case *proc2.n/2 != *proc3.n:
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

	stdout := iomux.NewWriter(new(bytes.Buffer))
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

	var (
		gen  *testI64Gen
		dmp1 *testI64Dumper
		dmp2 *testI64Dumper
		dmp3 *testI64Dumper
		dmp4 *testI64Dumper
	)

	app.Add(
		func() job.Proc {
			gen = new(testI64Gen)
			proc1.n = &gen.N
			proc1.v = &gen.V
			return job.Proc{
				Dev:   gen,
				Name:  proc1.name,
				Level: proclvl,
				Outputs: job.OutputHandlers{
					"/i64": gen.Output,
				},
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
			dmp1 = &testI64Dumper{
				drain: make(chan int),
			}
			proc3.n = &dmp1.N
			proc3.v = &dmp1.V
			return job.Proc{
				Dev:   dmp1,
				Name:  proc3.name,
				Level: proclvl,
				Inputs: job.InputHandlers{
					"/i64-1-left": dmp1.Input,
				},
			}
		}(),
		func() job.Proc {
			dmp2 = &testI64Dumper{
				drain: make(chan int),
			}
			proc4.n = &dmp2.N
			proc4.v = &dmp2.V
			return job.Proc{
				Dev:   dmp2,
				Name:  proc4.name,
				Level: proclvl,
				Inputs: job.InputHandlers{
					"/i64-1-right": dmp2.Input,
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
			dmp3 = &testI64Dumper{
				drain: make(chan int),
			}
			proc6.n = &dmp3.N
			proc6.v = &dmp3.V
			return job.Proc{
				Dev:   dmp3,
				Name:  proc6.name,
				Level: proclvl,
				Inputs: job.InputHandlers{
					"/i64-2-left": dmp3.Input,
				},
			}
		}(),
		func() job.Proc {
			dmp4 = &testI64Dumper{
				drain: make(chan int),
			}
			proc7.n = &dmp4.N
			proc7.v = &dmp4.V
			return job.Proc{
				Dev:   dmp4,
				Name:  proc7.name,
				Level: proclvl,
				Inputs: job.InputHandlers{
					"/i64-2-right": dmp4.Input,
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
		{"/quit", tdaq.CmdQuit},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		const N = 16
		func() {
			defer cancel()
			if tt.name == "/start" {
				gen.mu.Lock()
				gen.N = 0
				gen.V = 0
				dmp1.want = N / 2
				dmp2.want = N / 2
				dmp3.want = N
				dmp4.want = 0
				dmp1.timer = time.NewTimer(1 * time.Second)
				dmp2.timer = time.NewTimer(1 * time.Second)
				dmp3.timer = time.NewTimer(1 * time.Second)
				dmp4.timer = time.NewTimer(1 * time.Second)
				gen.mu.Unlock()
				go gen.loop(N)
			}
			err = app.Do(ctx, tt.cmd)
			if err != nil {
				t.Fatalf("could not send command %v: %+v", tt.cmd, err)
			}
			if tt.name == "/start" {
				<-dmp1.drain
				<-dmp2.drain
				<-dmp3.drain
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

func TestTOF(t *testing.T) {
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

	stdout := iomux.NewWriter(new(bytes.Buffer))
	app := job.New("tcp", stdout)
	defer func() {
		if err != nil {
			t.Logf("stdout:\n%v\n", stdout.String())
		}
	}()

	app.Cfg.RunCtl = rcAddr
	app.Cfg.Web = webAddr
	app.Cfg.Level = rclvl

	type pstate struct {
		name   string
		n      *int64
		mean   *float64
		stddev *float64
	}

	var (
		proc1 = pstate{name: "proc-1"}
		proc2 = pstate{name: "proc-2"}
	)

	const (
		runTime = 100 * time.Millisecond
	)

	app.Add(
		func() job.Proc {
			dev := new(testI64GenUTC)
			dev.Freq = runTime / 50
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
			dev := new(testI64TOF)
			proc2.n = &dev.N
			proc2.mean = &dev.Mean
			proc2.stddev = &dev.StdDev
			return job.Proc{
				Dev:   dev,
				Name:  proc2.name,
				Level: proclvl,
				Inputs: job.InputHandlers{
					"/i64": dev.Input,
				},
			}

		}(),
	)

	err = app.Start()
	if err != nil {
		t.Fatalf("could not start tdaq app: %+v", err)
	}

	runF := func(f func(cmd tdaq.CmdType), cmds ...tdaq.CmdType) {
		for _, cmd := range cmds {
			f(cmd)
		}
	}

	runF(
		func(cmd tdaq.CmdType) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err = app.Do(ctx, cmd)
			if err != nil {
				t.Fatalf("could not send command %v: %+v", cmd, err)
			}
			if cmd == tdaq.CmdStart {
				time.Sleep(runTime)
			}
		},
		tdaq.CmdConfig, tdaq.CmdInit,
		tdaq.CmdStart, tdaq.CmdStop,
		tdaq.CmdQuit,
	)

	err = app.Wait()
	if err != nil {
		t.Fatalf("error shutting down tdaq app: %+v", err)
	}
}

func BenchmarkTOF(b *testing.B) {

	const (
		rclvl   = log.LvlError
		proclvl = log.LvlError
	)

	port, err := tcputil.GetTCPPort()
	if err != nil {
		b.Fatalf("could not find a tcp port for run-ctl: %+v", err)
	}

	rcAddr := ":" + port

	port, err = tcputil.GetTCPPort()
	if err != nil {
		b.Fatalf("could not find a tcp port for run-ctl web server: %+v", err)
	}
	webAddr := ":" + port

	stdout := iomux.NewWriter(new(bytes.Buffer))
	app := job.New("tcp", stdout)
	defer func() {
		if err != nil {
			b.Logf("stdout:\n%v\n", stdout.String())
		}
	}()

	app.Cfg.RunCtl = rcAddr
	app.Cfg.Web = webAddr
	app.Cfg.Level = rclvl

	type pstate struct {
		name   string
		n      *int64
		mean   *float64
		stddev *float64
	}

	var (
		proc1 = pstate{name: "proc-1"}
		proc2 = pstate{name: "proc-2"}
	)

	app.Add(
		func() job.Proc {
			dev := new(testI64GenUTC)
			dev.Freq = 1 * time.Microsecond
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
			dev := new(testI64TOF)
			proc2.n = &dev.N
			proc2.mean = &dev.Mean
			proc2.stddev = &dev.StdDev
			return job.Proc{
				Dev:   dev,
				Name:  proc2.name,
				Level: proclvl,
				Inputs: job.InputHandlers{
					"/i64": dev.Input,
				},
			}

		}(),
	)

	err = app.Start()
	if err != nil {
		b.Fatalf("could not start tdaq app: %+v", err)
	}

	runF := func(f func(cmd tdaq.CmdType), cmds ...tdaq.CmdType) {
		for _, cmd := range cmds {
			f(cmd)
		}
	}

	runF(
		func(cmd tdaq.CmdType) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err = app.Do(ctx, cmd)
			if err != nil {
				b.Fatalf("could not send command %v: %+v", cmd, err)
			}
		},
		tdaq.CmdConfig, tdaq.CmdInit,
	)

	const (
		runTime = 5 * time.Second
	)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		runF(
			func(cmd tdaq.CmdType) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				err = app.Do(ctx, cmd)
				if err != nil {
					b.Fatalf("could not send command %v: %+v", cmd, err)
				}
				if cmd == tdaq.CmdStart {
					time.Sleep(runTime)
				}
				if cmd == tdaq.CmdStop {
					b.ReportMetric(float64(*proc2.n)/float64(b.N)/float64(runTime/time.Second), "bench-01-tokens/op/s")
					b.ReportMetric(*proc2.mean, "bench-02-tof-ns")
					b.ReportMetric(*proc2.stddev, "bench-03-stddev-ns")
				}
			},
			tdaq.CmdStart, tdaq.CmdStop,
		)
	}

	runF(
		func(cmd tdaq.CmdType) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err = app.Do(ctx, cmd)
			if err != nil {
				b.Fatalf("could not send command %v: %+v", cmd, err)
			}
		},
		tdaq.CmdQuit,
	)

	err = app.Wait()
	if err != nil {
		b.Fatalf("error shutting down tdaq app: %+v", err)
	}
}
