// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq // import "github.com/go-daq/tdaq"

import (
	"context"
	"io"
	"net"
	"time"

	"github.com/go-daq/tdaq/fsm"
	"github.com/go-daq/tdaq/log"
	"golang.org/x/xerrors"
)

type Process struct {
	quit chan struct{}

	rc   net.Conn
	srv  net.Listener
	recv map[net.Conn]fsm.Status
	send map[net.Conn]fsm.Status

	cmds chan Cmd

	handler struct {
		onConfig cmdHandlerFunc
		onInit   cmdHandlerFunc
		onReset  cmdHandlerFunc
		onStart  cmdHandlerFunc
		onStop   cmdHandlerFunc
		onTerm   cmdHandlerFunc

		onStatus cmdHandlerFunc
		onLog    cmdHandlerFunc

		onData handlerFunc
	}

	udev Device
	name string
}

func (proc *Process) Name() string {
	return proc.name
}

func (proc *Process) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	proc.initHandlers()

	go proc.cmdsLoop(ctx)
	go proc.run(ctx)

	errc := make(chan error)
	go func() {
		errc <- proc.udev.Run(ctx)
	}()

	for {
		select {
		case <-proc.quit:
			return nil

		case <-ctx.Done():
			close(proc.quit)
			return ctx.Err()
		case err := <-errc:
			close(proc.quit)
			return err
		}
	}
	return nil
}

func NewProcess(rcAddr, name string) (*Process, error) {
	conn, err := net.Dial("tcp", rcAddr)
	if err != nil {
		return nil, xerrors.Errorf("could not dial run-ctl: %w", err)
	}
	setupTCPConn(conn.(*net.TCPConn))

	srv, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, xerrors.Errorf("could not listen for incoming connections: %w", err)
	}

	proc := &Process{
		quit: make(chan struct{}),
		rc:   conn,
		srv:  srv,
		recv: make(map[net.Conn]fsm.Status),
		send: make(map[net.Conn]fsm.Status),

		cmds: make(chan Cmd),
		name: name,
	}

	err = proc.join()
	if err != nil {
		return nil, xerrors.Errorf("could not join run-ctl: %w", err)
	}

	return proc, nil
}

func (proc *Process) join() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	select {
	case <-proc.quit:
		return xerrors.Errorf("could not join run-ctl before term")
	case <-ctx.Done():
		return xerrors.Errorf("could not join run-ctl before timeout: %v", ctx.Err())
	default:
		err := sendCmd(ctx, proc.rc, CmdJoin, []byte(proc.name))
		if err != nil {
			return xerrors.Errorf("could not send JOIN frame to run-ctl: %w", err)
		}

		frame, err := RecvFrame(ctx, proc.rc)
		if err != nil {
			return xerrors.Errorf("could not recv JOIN-ACK frame from run-ctl: %w", err)
		}
		switch frame.Type {
		case FrameOK:
			return nil // OK
		case FrameErr:
			return xerrors.Errorf("received error JOIN-ACK frame from run-ctl: %s", frame.Body)
		default:
			return xerrors.Errorf("received invalid JOIN-ACK frame from run-ctl")
		}
	}

	return nil
}

func (proc *Process) initHandlers() {
	for _, hdlr := range []*cmdHandlerFunc{
		&proc.handler.onConfig,
		&proc.handler.onInit,
		&proc.handler.onReset,
		&proc.handler.onStart,
		&proc.handler.onStop,
		&proc.handler.onTerm,

		&proc.handler.onStatus,
		&proc.handler.onLog,
	} {
		*hdlr = func(ctx context.Context, resp *Frame, cmd Cmd) error {
			log.Debugf("received cmd %v", cmd.Type)
			return nil
		}
	}
}

func (proc *Process) cmdsLoop(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		select {
		case <-proc.quit:
			return
		default:
			cmd, err := recvCmd(ctx, proc.rc)
			switch err {
			case nil:
				// ok
			case io.EOF, io.ErrUnexpectedEOF:
				log.Errorf("could not receive cmds from run-ctl: %v", err)
				return
			default:
				log.Warnf("could not receive cmds from run-cli: %v", err)
				continue
			}

			go proc.handleCmd(ctx, proc.rc, cmd)
		}
	}
}

func (proc *Process) run(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		select {
		case <-proc.quit:
			return
		}
	}
}

func (proc *Process) handleCmd(ctx context.Context, w io.Writer, cmd Cmd) {
	var (
		resp = Frame{Type: FrameOK}
		err  error
	)
	switch cmd.Type {
	case CmdConfig:
		err = proc.handler.onConfig(ctx, &resp, cmd)
	case CmdInit:
		err = proc.handler.onInit(ctx, &resp, cmd)
	case CmdReset:
		err = proc.handler.onReset(ctx, &resp, cmd)
	case CmdStart:
		err = proc.handler.onStart(ctx, &resp, cmd)
	case CmdStop:
		err = proc.handler.onStop(ctx, &resp, cmd)
	case CmdTerm:
		err = proc.handler.onTerm(ctx, &resp, cmd)
		defer close(proc.quit)
	case CmdStatus:
		err = proc.handler.onStatus(ctx, &resp, cmd)
	case CmdLog:
		err = proc.handler.onLog(ctx, &resp, cmd)
	default:
		panic(xerrors.Errorf("unknown cmd type %#v", cmd))
	}

	if err != nil {
		log.Warnf("could not run on-%v handler: %v", cmd.Type, err)
		resp.Type = FrameErr
		resp.Body = []byte(err.Error())
	}

	err = SendFrame(ctx, w, resp)
	if err != nil {
		log.Warnf("could not send ack cmd: %v", err)
	}
}

var (
	_ Device = (*Process)(nil)
)
