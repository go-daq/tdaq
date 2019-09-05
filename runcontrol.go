// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq // import "github.com/go-daq/tdaq"

import (
	"context"
	"fmt"
	"net"
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

	mu        sync.RWMutex
	conns     map[net.Conn]descr
	listening bool

	runNbr uint64
}

func NewRunControl(addr string) (*RunControl, error) {
	rc := &RunControl{
		quit:      make(chan struct{}),
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

func (rctl *RunControl) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go rctl.serve(ctx)
	//	go rctl.cmdsLoop(ctx)
	//	go rctl.run(ctx)

	// FIXME(sbinet): remove
	go rctl.dbgloop(ctx)

	for {
		select {
		case <-rctl.quit:
			return nil

		case <-ctx.Done():
			close(rctl.quit)
			return ctx.Err()
		}
	}
	return nil
}

func (rctl *RunControl) serve(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		select {
		case <-rctl.quit:
			return
		case <-ctx.Done():
			err := ctx.Err()
			if err != nil {
				log.Errorf("context errored during run-ctl serve: %v", err)
			}
			return
		default:
			conn, err := rctl.srv.Accept()
			if err != nil {
				log.Errorf("error accepting connection: %v", err)
				continue
			}
			go rctl.handleConn(ctx, conn)
		}
	}
}

func (rctl *RunControl) handleConn(ctx context.Context, conn net.Conn) {
	setupTCPConn(conn.(*net.TCPConn))

	req, err := RecvFrame(ctx, conn)
	if err != nil {
		log.Errorf("could not receive JOIN cmd from conn %v: %v", conn.RemoteAddr(), err)
		sendFrame(ctx, conn, FrameErr, nil, []byte(err.Error()))
		return
	}
	cmd, err := CmdFrom(req)
	if err != nil {
		log.Errorf("could not receive JOIN cmd from conn %v: %v", conn.RemoteAddr(), err)
		sendFrame(ctx, conn, FrameErr, nil, []byte(err.Error()))
		return
	}
	if cmd.Type != CmdJoin {
		log.Errorf("received invalid cmd from conn %v: cmd=%v (want JOIN)", conn.RemoteAddr(), cmd.Type)
		sendFrame(ctx, conn, FrameErr, nil, []byte(fmt.Sprintf("invalid cmd %v, want JOIN", cmd.Type)))
		return
	}

	var join JoinCmd
	err = join.UnmarshalTDAQ(cmd.Body)
	if err != nil {
		log.Errorf("could not decode JOIN cmd payload: %v", err)
		sendFrame(ctx, conn, FrameErr, nil, []byte(err.Error()))
		return
	}

	log.Infof("received JOIN from conn %v: %v", conn.RemoteAddr(), join)

	err = sendFrame(ctx, conn, FrameOK, nil, nil)
	if err != nil {
		log.Errorf("could not send OK-frame to conn %v (%s): %v", conn.RemoteAddr(), join.Name, err)
		return
	}

	rctl.mu.Lock()
	rctl.conns[conn] = descr{
		name: join.Name,
		status: fsm.Status{
			State: fsm.UnConf,
		},
		ieps: join.InEndPoints,
		oeps: join.OutEndPoints,
	}
	rctl.mu.Unlock()

}

func (rctl *RunControl) cmdsLoop(ctx context.Context) {
	panic("not implemented")
}

func (rctl *RunControl) run(ctx context.Context) {
	panic("not implemented")
}

func (rctl *RunControl) dbgloop(ctx context.Context) {
	time.Sleep(8 * time.Second)

	for _, tt := range []struct {
		cmd CmdType
		dt  time.Duration
		f   func(context.Context) error
	}{
		{CmdConfig, 5 * time.Second, rctl.doConfig},
		{CmdInit, 2 * time.Second, rctl.doInit},
		{CmdReset, 2 * time.Second, rctl.doReset},
		{CmdConfig, 2 * time.Second, rctl.doConfig},
		{CmdInit, 2 * time.Second, rctl.doInit},
		{CmdStart, 10 * time.Second, rctl.doStart},
		{CmdStatus, 2 * time.Second, rctl.doStatus},
		{CmdLog, 2 * time.Second, nil},
		{CmdStop, 2 * time.Second, rctl.doStop},
		{CmdStart, 10 * time.Second, rctl.doStart},
		{CmdStop, 2 * time.Second, rctl.doStop},
		{CmdTerm, 2 * time.Second, rctl.doTerm},
	} {
		log.Infof("--- cmd %v...", tt.cmd)
		if tt.f != nil {
			err := tt.f(ctx)
			if err != nil {
				log.Errorf("--- cmd %v failed: %v", tt.cmd, err)
				continue
			}
		}
		switch tt.cmd {
		case CmdLog:
			rctl.broadcast(ctx, tt.cmd)
		}
		log.Infof("--- cmd %v... [done]", tt.cmd)
		time.Sleep(tt.dt)
	}

	close(rctl.quit)
}

func (rctl *RunControl) broadcast(ctx context.Context, cmd CmdType) error {
	rctl.mu.RLock()
	defer rctl.mu.RUnlock()

	var berr []error

	for conn, descr := range rctl.conns {
		log.Infof("proc %q...", descr.name)
		log.Infof("sending cmd %v...", cmd)
		err := sendCmd(ctx, conn, cmd, nil)
		if err != nil {
			log.Errorf("could not send cmd %v to conn %v (%s): %v", cmd, conn.RemoteAddr(), descr.name, err)
			berr = append(berr, err)
			continue
		}
		log.Infof("sending cmd %v... [ok]", cmd)
		log.Infof("receiving ACK...")
		ack, err := RecvFrame(ctx, conn)
		if err != nil {
			log.Errorf("could not receive ACK: %v", err)
			berr = append(berr, err)
			continue
		}
		switch ack.Type {
		case FrameOK:
			log.Infof("receiving ACK... [ok]")
		case FrameErr:
			log.Errorf("received ERR ACK: %v", string(ack.Body))
			berr = append(berr, xerrors.Errorf(string(ack.Body)))
		default:
			log.Errorf("received invalid frame type %v", ack.Type)
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
	log.Infof("configuring processes...")
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
				log.Errorf("could not send %v to conn %v (%s): %v", cmd, conn.RemoteAddr(), descr.name, err)
				return err
			}

			ack, err := RecvFrame(ctx, conn)
			if err != nil {
				log.Errorf("could not receive ACK: %v", err)
				return err
			}
			switch ack.Type {
			case FrameOK:
				log.Infof("receiving ACK... [ok]")
			case FrameErr:
				log.Errorf("received ERR ACK: %v", string(ack.Body))
				return xerrors.Errorf(string(ack.Body))
			default:
				log.Errorf("received invalid frame type %v", ack.Type)
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
	return rc.broadcast(ctx, CmdInit)
}

func (rc *RunControl) doReset(ctx context.Context) error {
	return rc.broadcast(ctx, CmdReset)
}

func (rc *RunControl) doStart(ctx context.Context) error {
	return rc.broadcast(ctx, CmdStart)
}

func (rc *RunControl) doStop(ctx context.Context) error {
	return rc.broadcast(ctx, CmdStop)
}

func (rc *RunControl) doTerm(ctx context.Context) error {
	return rc.broadcast(ctx, CmdTerm)
}

func (rc *RunControl) doStatus(ctx context.Context) error {
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
