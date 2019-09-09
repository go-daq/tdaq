// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq // import "github.com/go-daq/tdaq"

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/go-daq/tdaq/config"
	"github.com/go-daq/tdaq/log"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

func TestRunControl(t *testing.T) {
	t.Parallel()

	port, err := getTCPPort()
	if err != nil {
		t.Fatalf("could not find a tcp port for run-ctl: %+v", err)
	}

	addr := ":" + port
	cmds := make(chan CmdType)
	stdout := new(bytes.Buffer)

	cfg := config.RunCtl{
		Name:   "run-ctl",
		Level:  log.LvlInfo,
		RunCtl: addr,
	}

	rc, err := NewRunControl(cfg, cmds, stdout)
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
		dev := testProducer{
			seed: 1234,
		}

		cfg := config.Process{
			Name:   "data-src",
			Level:  log.LvlInfo,
			RunCtl: addr,
		}
		srv := New(cfg)
		srv.CmdHandle("/config", dev.OnConfig)
		srv.CmdHandle("/init", dev.OnInit)
		srv.CmdHandle("/reset", dev.OnReset)
		srv.CmdHandle("/start", dev.OnStart)
		srv.CmdHandle("/stop", dev.OnStop)
		srv.CmdHandle("/term", dev.OnTerminate)

		srv.OutputHandle("/adc", dev.adc)

		srv.RunHandle(dev.run)

		err := srv.Run(ctx)
		return err
	})

	for _, i := range []int{1, 2, 3} {
		name := fmt.Sprintf("data-sink-%d", i)
		grp.Go(func() error {
			dev := testConsumer{}

			cfg := config.Process{
				Name:   name,
				Level:  log.LvlInfo,
				RunCtl: addr,
			}
			srv := New(cfg)
			srv.CmdHandle("/init", dev.OnInit)
			srv.CmdHandle("/reset", dev.OnReset)
			srv.CmdHandle("/stop", dev.OnStop)

			srv.InputHandle("/adc", dev.adc)

			err := srv.Run(context.Background())
			return err
		})
	}

	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
loop:
	for {
		select {
		case <-timeout.C:
			t.Logf("stdout:\n%v\n", stdout.String())
			t.Fatalf("devices did not connect")
		default:
			rc.mu.RLock()
			n := len(rc.conns)
			rc.mu.RUnlock()
			if n == 4 {
				break loop
			}
		}
	}

	for _, tt := range []struct {
		name string
		cmd  CmdType
		dt   time.Duration
	}{
		{"config", CmdConfig, 20 * time.Millisecond},
		{"init", CmdInit, 20 * time.Millisecond},
		{"reset", CmdReset, 10 * time.Millisecond},
		{"config", CmdConfig, 20 * time.Millisecond},
		{"init", CmdInit, 20 * time.Millisecond},
		{"start", CmdStart, 2 * time.Second},
		{"stop", CmdStop, 10 * time.Millisecond},
		{"start", CmdStart, 2 * time.Second},
		{"stop", CmdStop, 10 * time.Millisecond},
		{"term", CmdTerm, 1 * time.Second},
	} {
		cmds <- tt.cmd
		time.Sleep(tt.dt)
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
}

func getTCPPort() (string, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return "", err
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return "", err
	}
	defer l.Close()
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port), nil
}

type testProducer struct {
	seed int64
	rnd  *rand.Rand

	n    int
	data chan []byte
}

func (dev *testProducer) OnConfig(ctx Context, resp *Frame, req Frame) error {
	ctx.Msg.Debugf("received /config command...")
	return nil
}

func (dev *testProducer) OnInit(ctx Context, resp *Frame, req Frame) error {
	ctx.Msg.Debugf("received /init command...")
	dev.rnd = rand.New(rand.NewSource(dev.seed))
	dev.data = make(chan []byte, 1024)
	dev.n = 0
	return nil
}

func (dev *testProducer) OnReset(ctx Context, resp *Frame, req Frame) error {
	ctx.Msg.Debugf("received /reset command...")
	dev.rnd = rand.New(rand.NewSource(dev.seed))
	dev.data = make(chan []byte, 1024)
	dev.n = 0
	return nil
}

func (dev *testProducer) OnStart(ctx Context, resp *Frame, req Frame) error {
	ctx.Msg.Debugf("received /start command...")
	return nil
}

func (dev *testProducer) OnStop(ctx Context, resp *Frame, req Frame) error {
	n := dev.n
	ctx.Msg.Debugf("received /stop command... -> n=%d", n)
	return nil
}

func (dev *testProducer) OnTerminate(ctx Context, resp *Frame, req Frame) error {
	ctx.Msg.Debugf("received %q command...", req.Path)
	return nil
}

func (dev *testProducer) adc(ctx Context, dst *Frame) error {
	select {
	case <-ctx.Ctx.Done():
		dst.Body = nil
	case data := <-dev.data:
		dst.Body = data
	}
	return nil
}

func (dev *testProducer) run(ctx Context) error {
	for {
		select {
		case <-ctx.Ctx.Done():
			return nil
		default:
			raw := make([]byte, 1024)
			rand.Read(raw)
			select {
			case dev.data <- raw:
				dev.n++
			default:
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	return nil
}

type testConsumer struct {
	n int
}

func (dev *testConsumer) OnInit(ctx Context, resp *Frame, req Frame) error {
	ctx.Msg.Debugf("received /init command...")
	dev.n = 0
	return nil
}

func (dev *testConsumer) OnReset(ctx Context, resp *Frame, req Frame) error {
	ctx.Msg.Debugf("received /reset command...")
	dev.n = 0
	return nil
}

func (dev *testConsumer) OnStop(ctx Context, resp *Frame, req Frame) error {
	n := dev.n
	ctx.Msg.Debugf("received /stop command... -> n=%d", n)
	return nil
}

func (dev *testConsumer) adc(ctx Context, src Frame) error {
	dev.n++
	return nil
}
