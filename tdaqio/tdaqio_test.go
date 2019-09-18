// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaqio_test // import "github.com/go-daq/tdaq/tdaqio"

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/go-daq/tdaq"
	"github.com/go-daq/tdaq/config"
	"github.com/go-daq/tdaq/internal/iomux"
	"github.com/go-daq/tdaq/internal/tcputil"
	"github.com/go-daq/tdaq/log"
	"github.com/go-daq/tdaq/tdaqio"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

func TestSequence(t *testing.T) {
	t.Parallel()

	const (
		rclvl   = log.LvlDebug
		proclvl = log.LvlDebug
		nprocs  = 4
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

	fname, err := ioutil.TempFile("", "tdaq-")
	if err != nil {
		t.Fatalf("could not create a temporary log file for run-ctl log server: %+v", err)
	}
	fname.Close()
	defer func() {
		if err != nil {
			raw, err := ioutil.ReadFile(fname.Name())
			if err == nil {
				t.Logf("log-file:\n%v\n", string(raw))
			}
		}
		os.Remove(fname.Name())
	}()

	cfg := config.RunCtl{
		Name:      "run-ctl",
		Level:     rclvl,
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

	type state struct {
		name string
		v    *int64
	}

	var (
		proc1 = state{name: "proc-1"}
		proc2 = state{name: "proc-2"}
		proc3 = state{name: "proc-3.1"}
		proc4 = state{name: "proc-3.2"}
	)

	grp.Go(func() error {
		dev := tdaqio.I64Gen{}
		proc1.v = &dev.N

		cfg := config.Process{
			Name:   proc1.name,
			Level:  proclvl,
			RunCtl: rcAddr,
		}
		srv := tdaq.New(cfg, stdout)
		srv.CmdHandle("/config", dev.OnConfig)
		srv.CmdHandle("/init", dev.OnInit)
		srv.CmdHandle("/start", dev.OnStart)
		srv.CmdHandle("/stop", dev.OnStop)
		srv.CmdHandle("/reset", dev.OnReset)
		srv.CmdHandle("/quit", dev.OnQuit)

		srv.OutputHandle("/i64-1", dev.Output)
		srv.RunHandle(dev.Loop)

		err := srv.Run(ctx)
		return err
	})

	grp.Go(func() error {
		dev := tdaqio.I64Processor{}
		proc2.v = &dev.V

		cfg := config.Process{
			Name:   proc2.name,
			Level:  proclvl,
			RunCtl: rcAddr,
		}
		srv := tdaq.New(cfg, stdout)
		srv.CmdHandle("/config", dev.OnConfig)
		srv.CmdHandle("/init", dev.OnInit)
		srv.CmdHandle("/start", dev.OnStart)
		srv.CmdHandle("/stop", dev.OnStop)
		srv.CmdHandle("/reset", dev.OnReset)
		srv.CmdHandle("/quit", dev.OnQuit)

		srv.InputHandle("/i64-1", dev.Input)
		srv.OutputHandle("/i64-2", dev.Output)

		err := srv.Run(ctx)
		return err
	})

	grp.Go(func() error {
		dev := tdaqio.I64Dumper{}
		proc3.v = &dev.V

		cfg := config.Process{
			Name:   proc3.name,
			Level:  proclvl,
			RunCtl: rcAddr,
		}
		srv := tdaq.New(cfg, stdout)
		srv.CmdHandle("/config", dev.OnConfig)
		srv.CmdHandle("/init", dev.OnInit)
		srv.CmdHandle("/start", dev.OnStart)
		srv.CmdHandle("/stop", dev.OnStop)
		srv.CmdHandle("/reset", dev.OnReset)
		srv.CmdHandle("/quit", dev.OnQuit)

		srv.InputHandle("/i64-2", dev.Input)

		err := srv.Run(ctx)
		return err
	})

	grp.Go(func() error {
		dev := tdaqio.I64Dumper{}
		proc4.v = &dev.V

		cfg := config.Process{
			Name:   proc4.name,
			Level:  proclvl,
			RunCtl: rcAddr,
		}
		srv := tdaq.New(cfg, stdout)
		srv.CmdHandle("/config", dev.OnConfig)
		srv.CmdHandle("/init", dev.OnInit)
		srv.CmdHandle("/start", dev.OnStart)
		srv.CmdHandle("/stop", dev.OnStop)
		srv.CmdHandle("/reset", dev.OnReset)
		srv.CmdHandle("/quit", dev.OnQuit)

		srv.InputHandle("/i64-2", dev.Input)

		err := srv.Run(ctx)
		return err
	})

	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
loop:
	for {
		select {
		case <-timeout.C:
			t.Logf("stdout:\n%v\n", stdout.String())
			t.Fatalf("devices did not connect")
		default:
			n := rc.NumClients()
			if n == nprocs {
				break loop
			}
		}
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
			err = rc.Do(ctx, tt.cmd)
			if err != nil {
				t.Fatalf("could not send command %v: %+v", tt.cmd, err)
			}
			if tt.name == "/start" {
				time.Sleep(100 * time.Millisecond)
			}
			if tt.name == "/stop" {
				switch {
				case *proc1.v-1 != *proc2.v:
					t.Fatalf("stage-1 error: %q:%v, %q:%v", proc1.name, *proc1.v, proc2.name, *proc2.v)
				case *proc2.v*2 != *proc3.v:
					t.Fatalf("stage-2 error: %q:%v, %q:%v", proc2.name, *proc2.v, proc3.name, *proc3.v)
				case *proc3.v != *proc4.v:
					t.Fatalf("stage-3 error: %q:%v, %q:%v", proc3.name, *proc3.v, proc4.name, *proc4.v)
				}
			}
		}()
	}

	err = grp.Wait()
	if err != nil {
		t.Logf("stdout:\n%v\n", stdout.String())
		t.Fatalf("could not run device run-group: %+v", err)
	}

	err = <-errc
	if err != nil && !xerrors.Is(err, context.Canceled) {
		t.Logf("stdout:\n%v\n", stdout.String())
		t.Fatalf("error shutting down run-ctl: %+v", err)
	}

	switch {
	case *proc1.v-1 != *proc2.v:
		t.Fatalf("stage-1 error: %q:%v, %q:%v", proc1.name, *proc1.v, proc2.name, *proc2.v)
	case *proc2.v*2 != *proc3.v:
		t.Fatalf("stage-2 error: %q:%v, %q:%v", proc2.name, *proc2.v, proc3.name, *proc3.v)
	case *proc3.v != *proc4.v:
		t.Fatalf("stage-3 error: %q:%v, %q:%v", proc3.name, *proc3.v, proc4.name, *proc4.v)
	}
}
