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
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

type mtbuf struct {
	mu  sync.Mutex
	buf *bytes.Buffer
}

func (b *mtbuf) Sync() error { return nil }
func (b *mtbuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}
func (b *mtbuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestRunControl(t *testing.T) {
	port, err := getTCPPort()
	if err != nil {
		t.Fatalf("could not find a tcp port for run-ctl: %+v", err)
	}

	addr := ":" + port
	stdout := &mtbuf{buf: new(bytes.Buffer)}

	rc, err := NewRunControl(addr, os.Stdin, stdout)
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

		srv := New(addr, "data-src")
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

			srv := New(addr, name)
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
		fct  func(context.Context) error
		dt   time.Duration
	}{
		{"config", rc.doConfig, 20 * time.Millisecond},
		{"init", rc.doInit, 20 * time.Millisecond},
		{"reset", rc.doReset, 10 * time.Millisecond},
		{"config", rc.doConfig, 20 * time.Millisecond},
		{"init", rc.doInit, 20 * time.Millisecond},
		{"start", rc.doStart, 2 * time.Second},
		{"stop", rc.doStop, 10 * time.Millisecond},
		{"start", rc.doStart, 2 * time.Second},
		{"stop", rc.doStop, 10 * time.Millisecond},
		{"term", rc.doTerm, 1 * time.Second},
	} {
		err := tt.fct(ctx)
		if err != nil {
			t.Logf("stdout:\n%v\n", stdout.String())
			t.Fatalf("could not run %v: %+v", tt.name, err)
		}
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
