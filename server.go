// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq // import "github.com/go-daq/tdaq"

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/go-daq/tdaq/log"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

type Server struct {
	rc   string // run-ctl address:port
	name string

	rctl net.Conn

	mu   sync.RWMutex
	imgr *imgr
	omgr *omgr
	cmgr *cmdmgr

	runctx  context.Context
	rundone context.CancelFunc

	quit chan struct{} // term channel
}

func New(rctl, name string) *Server {
	srv := &Server{
		rc:   rctl,
		name: name,
		imgr: newIMgr(),
		omgr: newOMgr(),
		cmgr: newCmdMgr(
			"/join",
			"/config", "/init", "/reset", "/start", "/stop",
			"/term",
			"/status", "/log",
		),

		quit: make(chan struct{}),
	}
	return srv
}

func (srv *Server) CmdHandle(name string, h CmdHandler) {
	srv.cmgr.Handle(name, h)
}

func (srv *Server) InputHandle(name string, h InputHandler) {
	srv.imgr.Handle(name, h)
}

func (srv *Server) OutputHandle(name string, h OutputHandler) {
	srv.omgr.Handle(name, h)
}

func (srv *Server) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	rctl, err := net.Dial("tcp", srv.rc)
	if err != nil {
		return xerrors.Errorf("could not dial run-ctl: %w", err)
	}
	defer rctl.Close()
	setupTCPConn(rctl.(*net.TCPConn))

	srv.rctl = rctl

	err = srv.omgr.init(srv)
	if err != nil {
		return xerrors.Errorf("could not setup i/o data ports: %w", err)
	}

	err = srv.join(ctx)
	if err != nil {
		return xerrors.Errorf("could not join run-ctl: %w", err)
	}

	srv.cmgr.init()

	go srv.cmdsLoop(ctx)

	select {
	case <-srv.quit:
		return nil
	case <-ctx.Done():
		close(srv.quit)
		return ctx.Err()
	}

	return err
}

func (srv *Server) join(ctx context.Context) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	select {
	case <-srv.quit:
		return xerrors.Errorf("could not join run-ctl before terminate")
	case <-ctx.Done():
		return xerrors.Errorf("could not join run-ctl before timeout: %w", ctx.Err())
	default:
		cmd := JoinCmd{
			Name:     srv.name,
			InPorts:  srv.imgr.ports(),
			OutPorts: srv.omgr.ports(),
		}

		err := SendCmd(ctx, srv.rctl, &cmd)
		if err != nil {
			return xerrors.Errorf("could not send JOIN frame to run-ctl: %w", err)
		}

		frame, err := RecvFrame(ctx, srv.rctl)
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

func (srv *Server) cmdsLoop(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		select {
		case <-srv.quit:
			return
		default:
			cmd, err := RecvFrame(ctx, srv.rctl)
			switch {
			case err == nil:
				// ok
			case xerrors.Is(err, io.EOF), xerrors.Is(err, io.ErrUnexpectedEOF):
				log.Errorf("could not receive cmds from run-ctl 1: %+v", err)
				return
			default:
				var nerr net.Error
				if xerrors.As(err, &nerr); !nerr.Temporary() {
					log.Warnf("connection to run-ctl: %+v", err)
					return
				}
				log.Warnf("could not receive cmds from run-ctl 2: %+v", err)
				continue
			}

			go srv.handleCmd(ctx, srv.rctl, cmd)
		}
	}
}

func (srv *Server) handleCmd(ctx context.Context, w io.Writer, req Frame) {
	var (
		resp = Frame{Type: FrameOK}
		err  error
	)

	name := req.Path

	h, ok := srv.cmgr.endpoint(name)
	if !ok {
		log.Warnf("invalid request path %q", name)
		resp.Type = FrameErr
		resp.Body = []byte(xerrors.Errorf("invalid request path %q", name).Error())

		err = SendFrame(ctx, w, resp)
		if err != nil {
			log.Warnf("could not send ack cmd: %v", err)
		}
	}

	var onCmd func(ctx context.Context, req Frame) error
	switch name {
	case "/config":
		onCmd = srv.onConfig
	case "/init":
		onCmd = srv.onInit
	case "/reset":
		onCmd = srv.onReset
	case "/start":
		onCmd = srv.onStart
	case "/stop":
		onCmd = srv.onStop
	case "/term":
		onCmd = srv.onTerm
		defer close(srv.quit)

	case "/status":
		onCmd = srv.onStatus
	case "/log":
		onCmd = srv.onLog

	default:
		log.Errorf("invalid cmd %q", name)
	}

	err = onCmd(ctx, req)
	if err != nil {
		log.Warnf("could not run %v pre-handler: %v", name, err)
		resp.Type = FrameErr
		resp.Body = []byte(err.Error())
	}

	err = h(ctx, &resp, req)
	if err != nil {
		log.Warnf("could not run %v handler: %v", name, err)
		resp.Type = FrameErr
		resp.Body = []byte(err.Error())
	}

	err = SendFrame(ctx, w, resp)
	if err != nil {
		log.Warnf("could not send ack cmd: %v", err)
	}
}

func (srv *Server) onConfig(ctx context.Context, req Frame) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	// FIXME(sbinet): validate FSM state transition here.

	cmd, err := newConfigCmd(req)
	if err != nil {
		return xerrors.Errorf("could not retrieve /config cmd: %w", err)
	}

	for _, iport := range cmd.InPorts {
		err = srv.imgr.dial(iport)
		if err != nil {
			return err
		}
	}

	return nil
}

func (srv *Server) onInit(ctx context.Context, req Frame) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	return nil
}

func (srv *Server) onReset(ctx context.Context, req Frame) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	ierr := srv.imgr.onReset(ctx)
	oerr := srv.omgr.onReset(ctx)

	switch {
	case ierr != nil:
		return ierr
	case oerr != nil:
		return oerr
	}

	return nil
}

func (srv *Server) onStart(ctx context.Context, req Frame) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	runctx, cancel := context.WithCancel(context.Background())
	srv.runctx = runctx
	srv.rundone = cancel

	ierr := srv.imgr.onStart(runctx)
	oerr := srv.omgr.onStart(runctx)

	switch {
	case ierr != nil:
		return ierr
	case oerr != nil:
		return oerr
	}

	return nil
}

func (srv *Server) onStop(ctx context.Context, req Frame) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	srv.rundone()
	<-srv.runctx.Done()

	return nil
}

func (srv *Server) onTerm(ctx context.Context, req Frame) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	return nil
}

func (srv *Server) onStatus(ctx context.Context, req Frame) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	return nil
}

func (srv *Server) onLog(ctx context.Context, req Frame) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	return nil
}

type oport struct {
	name  string
	srv   *Server
	l     net.Listener
	mu    sync.RWMutex
	conns []net.Conn
}

func (o *oport) accept() {
	for {
		conn, err := o.l.Accept()
		if err != nil {
			log.Errorf("could not accept conn for end-point %q: %v", o.name, err)
			if err.(net.Error).Temporary() {
				continue
			}
			return
		}
		setupTCPConn(conn.(*net.TCPConn))

		o.mu.Lock()
		o.conns = append(o.conns, conn)
		o.mu.Unlock()
	}
}

func (o *oport) onReset() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	var err error
	for _, conn := range o.conns {
		e := conn.Close()
		if e != nil {
			err = e
		}
	}
	o.conns = o.conns[:0]

	return err
}

func (o *oport) send(data []byte) error {
	o.mu.RLock()
	defer o.mu.RUnlock()

	var grp errgroup.Group
	for i := range o.conns {
		conn := o.conns[i]
		grp.Go(func() error {
			_, err := conn.Write(data)
			return err
		})
	}
	return grp.Wait()
}
