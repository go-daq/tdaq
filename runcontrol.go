// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq // import "github.com/go-daq/tdaq"

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/go-daq/tdaq/config"
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

	stdout log.WriteSyncer

	mu        sync.RWMutex
	msg       log.MsgStream
	conns     map[net.Conn]descr
	listening bool

	runNbr uint64
}

func NewRunControl(cfg config.RunCtl, stdout io.Writer) (*RunControl, error) {
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
		stdout:    out,
		msg:       log.NewMsgStream(cfg.Name, cfg.Level, out),
		conns:     make(map[net.Conn]descr),
		listening: true,
		runNbr:    uint64(time.Now().UTC().Unix()),
	}

	rc.msg.Infof("listening on %q...", cfg.RunCtl)
	srv, err := net.Listen("tcp", cfg.RunCtl)
	if err != nil {
		return nil, xerrors.Errorf("could not create TCP cmd server: %w", err)
	}
	rc.srv = srv

	return rc, nil
}

// NumClients returns the number of TDAQ processes connected to this run control.
func (rc *RunControl) NumClients() int {
	rc.mu.RLock()
	n := len(rc.conns)
	rc.mu.RUnlock()
	return n
}

func (rc *RunControl) Run(ctx context.Context) error {
	rc.msg.Infof("waiting for commands...")
	defer rc.stdout.Sync()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go rc.serve(ctx)

	var err error

loop:
	for {
		select {
		case <-rc.quit:
			rc.msg.Infof("shutting down...")
			break loop

		case <-ctx.Done():
			rc.msg.Infof("context done: shutting down...")
			close(rc.quit)
			err = ctx.Err()
			break loop
		}
	}

	if err != nil {
		rc.msg.Errorf("could not serve run-ctl commands: %+v", err)
	}

	rc.close()

	return err
}

func (rc *RunControl) close() {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	for conn, descr := range rc.conns {
		err := conn.Close()
		if err != nil {
			rc.msg.Errorf("could not close cmd-conn to %q: %+v", descr.name, err)
		}
		delete(rc.conns, conn)
	}

	err := rc.srv.Close()
	if err != nil {
		rc.msg.Errorf("could not close run-ctl cmd server: %+v", err)
	}
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
				select {
				case <-rc.quit:
					// ok, we are shutting down.
				default:
					rc.msg.Errorf("error accepting connection: %v", err)
				}
				continue
			}
			go rc.handleConn(ctx, conn)
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

	rc.msg.Infof("received JOIN from conn %v", conn.RemoteAddr())
	rc.msg.Infof("  proc: %q", join.Name)
	if len(join.InEndPoints) > 0 {
		rc.msg.Infof("   - inputs:")
		for _, p := range join.InEndPoints {
			rc.msg.Infof("     - name: %q", p.Name)
			if p.Addr != "" {
				rc.msg.Infof("       addr: %q", p.Addr)
			}
			if p.Type != "" {
				rc.msg.Infof("       type: %q", p.Type)
			}
		}
	}

	if len(join.OutEndPoints) > 0 {
		rc.msg.Infof("   - outputs:")
		for _, p := range join.OutEndPoints {
			rc.msg.Infof("     - name: %q", p.Name)
			if p.Addr != "" {
				rc.msg.Infof("       addr: %q", p.Addr)
			}
			if p.Type != "" {
				rc.msg.Infof("       type: %q", p.Type)
			}
		}
	}

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

// Do sends the provided command to all connected TDAQ processes.
func (rc *RunControl) Do(ctx context.Context, cmd CmdType) error {
	var fct func(context.Context) error
	switch cmd {
	case CmdConfig:
		fct = rc.doConfig
	case CmdInit:
		fct = rc.doInit
	case CmdReset:
		fct = rc.doReset
	case CmdStart:
		fct = rc.doStart
	case CmdStop:
		fct = rc.doStop
	case CmdTerm:
		fct = rc.doTerm
	case CmdStatus:
		fct = rc.doStatus
	default:
		return xerrors.Errorf("unknown command %#v", cmd)
	}

	return fct(ctx)
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
	err := rc.broadcast(ctx, CmdTerm)
	if err != nil {
		return err
	}
	close(rc.quit)
	return nil
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
