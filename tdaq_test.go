// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq_test // import "github.com/go-daq/tdaq"

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/go-daq/tdaq"
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
	stdout := new(bytes.Buffer)

	cfg := config.RunCtl{
		Name:   "run-ctl",
		Level:  log.LvlDebug,
		RunCtl: addr,
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
		dev := testProducer{
			seed: 1234,
		}

		cfg := config.Process{
			Name:   "data-src",
			Level:  log.LvlInfo,
			RunCtl: addr,
		}
		srv := tdaq.New(cfg)
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
			srv := tdaq.New(cfg)
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
			n := rc.NumClients()
			if n == 4 {
				break loop
			}
		}
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
		{"term", tdaq.CmdTerm},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		func() {
			defer cancel()
			err = rc.Do(ctx, tt.cmd)
			if err != nil {
				t.Fatalf("could not send command %v: %+v\nstdout:\n%v", tt.cmd, err, stdout.String())
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

func (dev *testProducer) OnConfig(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /config command...")
	return nil
}

func (dev *testProducer) OnInit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /init command...")
	dev.rnd = rand.New(rand.NewSource(dev.seed))
	dev.data = make(chan []byte, 1024)
	dev.n = 0
	return nil
}

func (dev *testProducer) OnReset(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /reset command...")
	dev.rnd = rand.New(rand.NewSource(dev.seed))
	dev.data = make(chan []byte, 1024)
	dev.n = 0
	return nil
}

func (dev *testProducer) OnStart(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /start command...")
	return nil
}

func (dev *testProducer) OnStop(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	n := dev.n
	ctx.Msg.Debugf("received /stop command... -> n=%d", n)
	return nil
}

func (dev *testProducer) OnTerminate(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received %q command...", req.Path)
	return nil
}

func (dev *testProducer) adc(ctx tdaq.Context, dst *tdaq.Frame) error {
	select {
	case <-ctx.Ctx.Done():
		dst.Body = nil
	case data := <-dev.data:
		dst.Body = data
	}
	return nil
}

func (dev *testProducer) run(ctx tdaq.Context) error {
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

func (dev *testConsumer) OnInit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /init command...")
	dev.n = 0
	return nil
}

func (dev *testConsumer) OnReset(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	ctx.Msg.Debugf("received /reset command...")
	dev.n = 0
	return nil
}

func (dev *testConsumer) OnStop(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error {
	n := dev.n
	ctx.Msg.Debugf("received /stop command... -> n=%d", n)
	return nil
}

func (dev *testConsumer) adc(ctx tdaq.Context, src tdaq.Frame) error {
	dev.n++
	return nil
}
