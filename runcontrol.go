// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq // import "github.com/go-daq/tdaq"

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/go-daq/tdaq/fsm"
	"github.com/go-daq/tdaq/log"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

type descr struct {
	name   string
	status fsm.Status
	ieps   []EndPoint
	oeps   []EndPoint
}

type RunControl struct {
	quit chan struct{}

	srv net.Listener

	stdin  io.Reader
	stdout log.WriteSyncer

	mu        sync.RWMutex
	msg       log.MsgStream
	conns     map[net.Conn]descr
	listening bool

	runNbr uint64
}

func NewRunControl(addr string, stdin io.Reader, stdout io.Writer) (*RunControl, error) {
	if stdin == nil {
		stdin = os.Stdin
	}
	if stdout == nil {
		stdout = os.Stdout
	}
	var out log.WriteSyncer
	switch stdout := stdout.(type) {
	case log.WriteSyncer:
		out = stdout
	default:
		out = newSyncWriter(stdout)
	}
	rc := &RunControl{
		quit:      make(chan struct{}),
		stdin:     stdin,
		stdout:    out,
		msg:       log.NewMsgStream("run-ctl", log.LvlInfo, out),
		conns:     make(map[net.Conn]descr),
		listening: true,
		runNbr:    uint64(time.Now().UTC().Unix()),
	}

	srv, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, xerrors.Errorf("could not create TCP cmd server: %w", err)
	}
	rc.srv = srv

	return rc, nil
}

func (rc *RunControl) Run(ctx context.Context) error {
	defer rc.stdout.Sync()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go rc.serve(ctx)
	go rc.input(ctx, rc.stdin)
	//	go rctl.cmdsLoop(ctx)
	//	go rctl.run(ctx)

	for {
		select {
		case <-rc.quit:
			return nil

		case <-ctx.Done():
			close(rc.quit)
			return ctx.Err()
		}
	}
	return nil
}

func (rc *RunControl) serve(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		select {
		case <-rc.quit:
			return
		case <-ctx.Done():
			err := ctx.Err()
			if err != nil {
				rc.msg.Errorf("context errored during run-ctl serve: %v", err)
			}
			return
		default:
			conn, err := rc.srv.Accept()
			if err != nil {
				rc.msg.Errorf("error accepting connection: %v", err)
				continue
			}
			go rc.handleConn(ctx, conn)
		}
	}
}

func (rc *RunControl) input(ctx context.Context, r io.Reader) {
	var (
		err  error
		sc   = bufio.NewScanner(r)
		quit = false
	)

	for sc.Scan() {
		err = sc.Err()
		if err != nil {
			rc.msg.Errorf("could not scan input command: %+v", err)
			return
		}
		buf := sc.Bytes()
		if len(buf) == 0 {
			continue
		}
		var fct func(context.Context) error
		switch buf[0] {
		case 'c':
			fct = rc.doConfig
		case 'i':
			fct = rc.doInit
		case 'x':
			fct = rc.doReset
		case 'r':
			fct = rc.doStart
		case 's':
			fct = rc.doStop
		case 'q':
			fct = rc.doTerm
			defer close(rc.quit)
			quit = true
		default:
			rc.msg.Warnf("unknown command %q", buf)
			continue
		}
		err = fct(ctx)
		if err != nil {
			rc.msg.Errorf("could not send command %q: %+v", buf[0], err)
			if quit {
				return
			}
			continue
		}
		if quit {
			return
		}
	}
}

func (rc *RunControl) handleConn(ctx context.Context, conn net.Conn) {
	setupTCPConn(conn.(*net.TCPConn))

	req, err := RecvFrame(ctx, conn)
	if err != nil {
		rc.msg.Errorf("could not receive JOIN cmd from conn %v: %v", conn.RemoteAddr(), err)
		sendFrame(ctx, conn, FrameErr, nil, []byte(err.Error()))
		return
	}
	cmd, err := CmdFrom(req)
	if err != nil {
		rc.msg.Errorf("could not receive JOIN cmd from conn %v: %v", conn.RemoteAddr(), err)
		sendFrame(ctx, conn, FrameErr, nil, []byte(err.Error()))
		return
	}
	if cmd.Type != CmdJoin {
		rc.msg.Errorf("received invalid cmd from conn %v: cmd=%v (want JOIN)", conn.RemoteAddr(), cmd.Type)
		sendFrame(ctx, conn, FrameErr, nil, []byte(fmt.Sprintf("invalid cmd %v, want JOIN", cmd.Type)))
		return
	}

	var join JoinCmd
	err = join.UnmarshalTDAQ(cmd.Body)
	if err != nil {
		rc.msg.Errorf("could not decode JOIN cmd payload: %v", err)
		sendFrame(ctx, conn, FrameErr, nil, []byte(err.Error()))
		return
	}

	rc.msg.Infof("received JOIN from conn %v: %v", conn.RemoteAddr(), join)

	err = sendFrame(ctx, conn, FrameOK, nil, nil)
	if err != nil {
		rc.msg.Errorf("could not send OK-frame to conn %v (%s): %v", conn.RemoteAddr(), join.Name, err)
		return
	}

	rc.mu.Lock()
	rc.conns[conn] = descr{
		name: join.Name,
		status: fsm.Status{
			State: fsm.UnConf,
		},
		ieps: join.InEndPoints,
		oeps: join.OutEndPoints,
	}
	rc.mu.Unlock()

}

func (rc *RunControl) cmdsLoop(ctx context.Context) {
	panic("not implemented")
}

func (rc *RunControl) run(ctx context.Context) {
	panic("not implemented")
}

func (rc *RunControl) dbgloop(ctx context.Context) {
	time.Sleep(8 * time.Second)

	for _, tt := range []struct {
		cmd CmdType
		dt  time.Duration
		f   func(context.Context) error
	}{
		{CmdConfig, 5 * time.Second, rc.doConfig},
		{CmdInit, 2 * time.Second, rc.doInit},
		{CmdReset, 2 * time.Second, rc.doReset},
		{CmdConfig, 2 * time.Second, rc.doConfig},
		{CmdInit, 2 * time.Second, rc.doInit},
		{CmdStart, 10 * time.Second, rc.doStart},
		{CmdStatus, 2 * time.Second, rc.doStatus},
		{CmdLog, 2 * time.Second, nil},
		{CmdStop, 2 * time.Second, rc.doStop},
		{CmdStart, 10 * time.Second, rc.doStart},
		{CmdStop, 2 * time.Second, rc.doStop},
		{CmdTerm, 2 * time.Second, rc.doTerm},
	} {
		rc.msg.Debugf("--- cmd %v...", tt.cmd)
		if tt.f != nil {
			err := tt.f(ctx)
			if err != nil {
				rc.msg.Errorf("--- cmd %v failed: %v", tt.cmd, err)
				continue
			}
		}
		switch tt.cmd {
		case CmdLog:
			rc.broadcast(ctx, tt.cmd)
		}
		rc.msg.Debugf("--- cmd %v... [done]", tt.cmd)
		time.Sleep(tt.dt)
	}

	close(rc.quit)
}

func (rc *RunControl) broadcast(ctx context.Context, cmd CmdType) error {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	var berr []error

	for conn, descr := range rc.conns {
		rc.msg.Debugf("proc %q...", descr.name)
		rc.msg.Debugf("sending cmd %v...", cmd)
		err := sendCmd(ctx, conn, cmd, nil)
		if err != nil {
			rc.msg.Errorf("could not send cmd %v to conn %v (%s): %v", cmd, conn.RemoteAddr(), descr.name, err)
			berr = append(berr, err)
			continue
		}
		rc.msg.Debugf("sending cmd %v... [ok]", cmd)
		rc.msg.Debugf("receiving ACK...")
		ack, err := RecvFrame(ctx, conn)
		if err != nil {
			rc.msg.Errorf("could not receive ACK: %v", err)
			berr = append(berr, err)
			continue
		}
		switch ack.Type {
		case FrameOK:
			rc.msg.Debugf("receiving ACK... [ok]")
		case FrameErr:
			rc.msg.Errorf("received ERR ACK: %v", string(ack.Body))
			berr = append(berr, xerrors.Errorf(string(ack.Body)))
		default:
			rc.msg.Errorf("received invalid frame type %v", ack.Type)
			berr = append(berr, xerrors.Errorf("received invalid frame type %v", ack.Type))
		}
	}

	// FIXME(sbinet): better handling
	if len(berr) > 0 {
		return berr[0]
	}

	return nil
}

func (rc *RunControl) doConfig(ctx context.Context) error {
	rc.msg.Infof("/config processes...")
	rc.mu.Lock()
	defer rc.mu.Unlock()

	conns := make([]net.Conn, 0, len(rc.conns))
	for conn, descr := range rc.conns {
		for i := range descr.ieps {
			iport := &descr.ieps[i]
			provider, ok := rc.providerOf(*iport)
			if !ok {
				return xerrors.Errorf("could not find a provider for input %q for %q", iport.Name, descr.name)
			}
			iport.Addr = provider
		}
		rc.conns[conn] = descr
		conns = append(conns, conn)
	}

	var grp errgroup.Group
	for i := range conns {
		conn := conns[i]
		descr := rc.conns[conn]
		cmd := ConfigCmd{
			Name:         descr.name,
			InEndPoints:  descr.ieps,
			OutEndPoints: descr.oeps,
		}
		grp.Go(func() error {
			err := SendCmd(ctx, conn, &cmd)
			if err != nil {
				rc.msg.Errorf("could not send %v to conn %v (%s): %v", cmd, conn.RemoteAddr(), descr.name, err)
				return err
			}

			ack, err := RecvFrame(ctx, conn)
			if err != nil {
				rc.msg.Errorf("could not receive ACK: %v", err)
				return err
			}
			switch ack.Type {
			case FrameOK:
				rc.msg.Debugf("receiving ACK... [ok]")
			case FrameErr:
				rc.msg.Errorf("received ERR ACK: %v", string(ack.Body))
				return xerrors.Errorf(string(ack.Body))
			default:
				rc.msg.Errorf("received invalid frame type %v", ack.Type)
				return xerrors.Errorf("received invalid frame type %v", ack.Type)
			}
			return nil
		})
	}

	err := grp.Wait()
	if err != nil {
		return xerrors.Errorf("failed to run errgroup: %w", err)
	}

	return nil
}

func (rc *RunControl) doInit(ctx context.Context) error {
	rc.msg.Infof("/init processes...")
	return rc.broadcast(ctx, CmdInit)
}

func (rc *RunControl) doReset(ctx context.Context) error {
	rc.msg.Infof("/reset processes...")
	return rc.broadcast(ctx, CmdReset)
}

func (rc *RunControl) doStart(ctx context.Context) error {
	rc.msg.Infof("/start processes...")
	return rc.broadcast(ctx, CmdStart)
}

func (rc *RunControl) doStop(ctx context.Context) error {
	rc.msg.Infof("/stop processes...")
	return rc.broadcast(ctx, CmdStop)
}

func (rc *RunControl) doTerm(ctx context.Context) error {
	rc.msg.Infof("/term processes...")
	return rc.broadcast(ctx, CmdTerm)
}

func (rc *RunControl) doStatus(ctx context.Context) error {
	rc.msg.Infof("/status processes...")
	return rc.broadcast(ctx, CmdStatus)
}

func (rc *RunControl) providerOf(p EndPoint) (string, bool) {
	for _, descr := range rc.conns {
		for _, oport := range descr.oeps {
			if oport.Name == p.Name {
				return oport.Addr, true
			}
		}
	}
	return "", false
}
