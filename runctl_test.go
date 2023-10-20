// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq_test // import "github.com/go-daq/tdaq"

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-daq/tdaq"
	"github.com/go-daq/tdaq/config"
	"github.com/go-daq/tdaq/internal/iomux"
	"github.com/go-daq/tdaq/internal/tcputil"
	"github.com/go-daq/tdaq/job"
	"github.com/go-daq/tdaq/log"
	"github.com/go-daq/tdaq/xdaq"
	"golang.org/x/sync/errgroup"
)

func TestRunControlAPI(t *testing.T) {
	t.Parallel()

	const (
		rclvl   = log.LvlDebug
		proclvl = log.LvlInfo
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

	app.Add(
		func() job.Proc {
			dev := new(xdaq.I64Gen)
			return job.Proc{
				Dev:   dev,
				Level: proclvl,
				Name:  "data-src",
				Cmds: job.CmdHandlers{
					"/config": dev.OnConfig,
					"/init":   dev.OnInit,
					"/reset":  dev.OnReset,
					"/start":  dev.OnStart,
					"/stop":   dev.OnStop,
					"/quit":   dev.OnQuit,
				},
				Outputs: job.OutputHandlers{
					"/i64": dev.Output,
				},
				Handlers: job.RunHandlers{dev.Loop},
			}
		}(),
	)

	for _, i := range []int{1, 2, 3} {
		name := fmt.Sprintf("data-sink-%d", i)
		app.Add(
			func() job.Proc {
				dev := new(xdaq.I64Dumper)
				return job.Proc{
					Dev:  dev,
					Name: name,
					Inputs: job.InputHandlers{
						"/i64": dev.Input,
					},
				}
			}(),
		)
	}

	err = app.Start()
	if err != nil {
		t.Fatalf("could not start job: %+v", err)
	}

	for _, tt := range []struct {
		name string
		cmd  tdaq.CmdType
	}{
		{"config", tdaq.CmdConfig},
		{"init", tdaq.CmdInit},
		{"reset", tdaq.CmdReset},
		{"config", tdaq.CmdConfig},
		{"init", tdaq.CmdInit},
		{"start", tdaq.CmdStart},
		{"stop", tdaq.CmdStop},
		{"status", tdaq.CmdStatus},
		{"start", tdaq.CmdStart},
		{"stop", tdaq.CmdStop},
		{"quit", tdaq.CmdQuit},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		func() {
			defer cancel()
			err = app.Do(ctx, tt.cmd)
			if err != nil {
				t.Fatalf("could not send command %v: %+v", tt.cmd, err)
			}
		}()
	}

	err = app.Wait()
	if err != nil {
		t.Fatalf("could not run app: %+v", err)
	}
}

func TestRunControlWithDuplicateProc(t *testing.T) {
	t.Parallel()

	const (
		rclvl   = log.LvlDebug
		proclvl = log.LvlInfo
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

	fname, err := os.CreateTemp("", "tdaq-")
	if err != nil {
		t.Fatalf("could not create a temporary log file for run-ctl log server: %+v", err)
	}
	fname.Close()
	defer func() {
		if err != nil {
			raw, err := os.ReadFile(fname.Name())
			if err == nil {
				t.Logf("log-file:\n%v\n", string(raw))
			}
		}
		os.Remove(fname.Name())
	}()

	cfg := config.RunCtl{
		Name:      "run-ctl",
		Level:     rclvl,
		Trans:     "tcp",
		RunCtl:    rcAddr,
		Web:       webAddr,
		LogFile:   fname.Name(),
		HBeatFreq: 50 * time.Millisecond,
	}

	rc, err := tdaq.NewRunControl(cfg, stdout)
	if err != nil {
		t.Fatalf("could not create run-ctl: %+v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	grp, ctx := errgroup.WithContext(ctx)

	errc := make(chan error)
	go func() {
		errc <- rc.Run(ctx)
	}()

	grp.Go(func() error {
		dev := xdaq.I64Gen{}

		cfg := config.Process{
			Name:   "proc-1",
			Level:  proclvl,
			Trans:  "tcp",
			RunCtl: rcAddr,
		}
		srv := tdaq.New(cfg, stdout)
		srv.OutputHandle("/i64", dev.Output)

		srv.RunHandle(dev.Loop)

		err := srv.Run(ctx)
		return err
	})

	grp.Go(func() error {
		dev := xdaq.I64Dumper{}
		cfg := config.Process{
			Name:   "proc-1",
			Level:  proclvl,
			Trans:  "tcp",
			RunCtl: rcAddr,
		}
		srv := tdaq.New(cfg, stdout)
		srv.InputHandle("/i64", dev.Input)
		err := srv.Run(ctx)
		return err
	})

	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()

loop:
	for {
		select {
		case <-timeout.C:
			t.Fatalf("devices did not connect")
		case err := <-errc:
			if err == nil {
				t.Fatalf("expected an error!")
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("expected a canceled-context error, got: %+v", err)
			}
			break loop
		}
	}

	err = grp.Wait()
	if err == nil {
		t.Fatalf("expected an error!")
	}
	want := fmt.Errorf(`could not join run-ctl: received error /join-ack from run-ctl: duplicate tdaq process with name "proc-1"`)
	if got, want := err.Error(), want.Error(); !strings.HasPrefix(got, want) {
		t.Fatalf("invalid error.\ngot= %v\nwant=%v\n", got, want)
	}
	err = nil
}

func TestRunControlWithDuplicateOutput(t *testing.T) {
	t.Parallel()

	const (
		rclvl   = log.LvlDebug
		proclvl = log.LvlInfo
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

	fname, err := os.CreateTemp("", "tdaq-")
	if err != nil {
		t.Fatalf("could not create a temporary log file for run-ctl log server: %+v", err)
	}
	fname.Close()
	defer func() {
		if err != nil {
			raw, err := os.ReadFile(fname.Name())
			if err == nil {
				t.Logf("log-file:\n%v\n", string(raw))
			}
		}
		os.Remove(fname.Name())
	}()

	cfg := config.RunCtl{
		Name:      "run-ctl",
		Level:     rclvl,
		Trans:     "tcp",
		RunCtl:    rcAddr,
		Web:       webAddr,
		LogFile:   fname.Name(),
		HBeatFreq: 50 * time.Millisecond,
	}

	rc, err := tdaq.NewRunControl(cfg, stdout)
	if err != nil {
		t.Fatalf("could not create run-ctl: %+v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	grp, ctx := errgroup.WithContext(ctx)

	errc := make(chan error)
	go func() {
		errc <- rc.Run(ctx)
	}()

	grp.Go(func() error {
		dev := xdaq.I64Gen{}

		cfg := config.Process{
			Name:   "proc-1",
			Level:  proclvl,
			Trans:  "tcp",
			RunCtl: rcAddr,
		}
		srv := tdaq.New(cfg, stdout)
		srv.OutputHandle("/i64", dev.Output)

		srv.RunHandle(dev.Loop)

		err := srv.Run(ctx)
		return err
	})

	grp.Go(func() error {
		dev := xdaq.I64Gen{}
		cfg := config.Process{
			Name:   "proc-2",
			Level:  proclvl,
			Trans:  "tcp",
			RunCtl: rcAddr,
		}
		srv := tdaq.New(cfg, stdout)
		srv.OutputHandle("/i64", dev.Output)
		err := srv.Run(ctx)
		return err
	})

	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
loop:
	for {
		select {
		case <-timeout.C:
			t.Fatalf("devices did not connect")
		default:
			n := rc.NumClients()
			if n == 2 {
				break loop
			}
		}
	}

	for _, tt := range []struct {
		name string
		cmd  tdaq.CmdType
		err  error
	}{
		{"config", tdaq.CmdConfig, nil},
		{"init", tdaq.CmdInit, fmt.Errorf(`could not create DAG of data dependencies: could not build graph for analysis: node "proc-1" already declared "/i64" as its output (dup-node="proc-2")`)},
		{"quit", tdaq.CmdQuit, nil},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		func() {
			defer cancel()
			err = rc.Do(ctx, tt.cmd)
			switch {
			case err == nil && tt.err == nil:
				// ok
			case err != nil && tt.err != nil:
				if got, want := err.Error(), tt.err.Error(); got != want {
					t.Fatalf("sent command /%s\ngot = %+v\nwant= %+v\n", tt.cmd, err, tt.err)
				}
			case err != nil && tt.err == nil:
				t.Fatalf("could not send command %v: %+v", tt.cmd, err)
			case err == nil && tt.err != nil:
				t.Fatalf("sent command /%s. got=nil, want=%+v\n", tt.cmd, tt.err)
			default:
				t.Fatalf("err: %+v", err)
			}
		}()
	}

	err = grp.Wait()
	if err != nil {
		t.Fatalf("could not run device run-group: %+v", err)
	}

	err = <-errc
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("error shutting down run-ctl: %+v", err)
	}

}

func TestRunControlWithMissingInput(t *testing.T) {
	t.Parallel()

	const (
		rclvl   = log.LvlDebug
		proclvl = log.LvlInfo
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

	fname, err := os.CreateTemp("", "tdaq-")
	if err != nil {
		t.Fatalf("could not create a temporary log file for run-ctl log server: %+v", err)
	}
	fname.Close()
	defer func() {
		if err != nil {
			raw, err := os.ReadFile(fname.Name())
			if err == nil {
				t.Logf("log-file:\n%v\n", string(raw))
			}
		}
		os.Remove(fname.Name())
	}()

	cfg := config.RunCtl{
		Name:      "run-ctl",
		Level:     rclvl,
		Trans:     "tcp",
		RunCtl:    rcAddr,
		Web:       webAddr,
		LogFile:   fname.Name(),
		HBeatFreq: 50 * time.Millisecond,
	}

	rc, err := tdaq.NewRunControl(cfg, stdout)
	if err != nil {
		t.Fatalf("could not create run-ctl: %+v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	grp, ctx := errgroup.WithContext(ctx)

	errc := make(chan error)
	go func() {
		errc <- rc.Run(ctx)
	}()

	grp.Go(func() error {
		dev := xdaq.I64Dumper{}

		cfg := config.Process{
			Name:   "proc-1",
			Level:  proclvl,
			Trans:  "tcp",
			RunCtl: rcAddr,
		}
		srv := tdaq.New(cfg, stdout)
		srv.InputHandle("/i64", dev.Input)

		err := srv.Run(ctx)
		return err
	})

	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
loop:
	for {
		select {
		case <-timeout.C:
			t.Fatalf("devices did not connect")
		default:
			n := rc.NumClients()
			if n == 1 {
				break loop
			}
		}
	}

	for _, tt := range []struct {
		name string
		cmd  tdaq.CmdType
		err  error
	}{
		{"config", tdaq.CmdConfig, fmt.Errorf(`could not find a provider for input "/i64" for "proc-1"`)},
		{"quit", tdaq.CmdQuit, nil},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		func() {
			defer cancel()
			err = rc.Do(ctx, tt.cmd)
			switch {
			case err == nil && tt.err == nil:
				// ok
			case err != nil && tt.err != nil:
				if got, want := err.Error(), tt.err.Error(); got != want {
					t.Fatalf("sent command /%s\ngot = %+v\nwant= %+v\n", tt.cmd, err, tt.err)
				}
			case err != nil && tt.err == nil:
				t.Fatalf("could not send command %v: %+v", tt.cmd, err)
			case err == nil && tt.err != nil:
				t.Fatalf("sent command /%s. got=nil, want=%+v\n", tt.cmd, tt.err)
			default:
				t.Fatalf("err: %+v", err)
			}
		}()
	}

	err = grp.Wait()
	if err != nil {
		t.Fatalf("could not run device run-group: %+v", err)
	}

	err = <-errc
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("error shutting down run-ctl: %+v", err)
	}

}
