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
	"strings"
	"sync"
	"time"

	"github.com/go-daq/tdaq/config"
	"github.com/go-daq/tdaq/fsm"
	"github.com/go-daq/tdaq/log"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

type Server struct {
	rc   string // run-ctl address:port
	name string
	cfg  config.Process

	rctl  net.Conn
	hbeat net.Conn
	log   net.Conn

	mu   sync.RWMutex
	msg  *msgstream
	imgr *imgr
	omgr *omgr
	cmgr *cmdmgr

	state struct {
		cur  fsm.Status
		next fsm.Status
	}

	runctx  context.Context
	rundone context.CancelFunc
	rungrp  *errgroup.Group
	runfcts []func(Context) error

	quit chan struct{} // term channel
}

func New(cfg config.Process, stdout io.Writer) *Server {
	if stdout == nil {
		stdout = os.Stdout
	}

	srv := &Server{
		rc:   cfg.RunCtl,
		name: cfg.Name,
		cfg:  cfg,
		msg:  newMsgStream(cfg.Name, cfg.Level, stdout),
		cmgr: newCmdMgr(
			"/config", "/init", "/reset", "/start", "/stop",
			"/quit",
			"/status",
		),

		quit: make(chan struct{}),
	}
	srv.imgr = newIMgr(srv)
	srv.omgr = newOMgr(srv)

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

func (srv *Server) RunHandle(f RunHandler) {
	srv.mu.Lock()
	defer srv.mu.Unlock()
	srv.runfcts = append(srv.runfcts, f)
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

	defer srv.cmgr.close()
	defer srv.imgr.close()
	defer srv.omgr.close()

	err = srv.omgr.init(srv)
	if err != nil {
		return xerrors.Errorf("could not setup i/o data ports: %w", err)
	}

	err = srv.join(ctx)
	if err != nil {
		return xerrors.Errorf("could not join run-ctl: %w", err)
	}

	srv.cmgr.init()

	go srv.hbeatLoop(ctx)
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

func (srv *Server) setCurState(state fsm.Status) {
	srv.mu.Lock()
	srv.state.cur = state
	srv.state.next = state
	srv.mu.Unlock()
}

func (srv *Server) setNextState(state fsm.Status) {
	srv.mu.Lock()
	srv.state.next = state
	srv.mu.Unlock()
}

func (srv *Server) getCurState() fsm.Status {
	srv.mu.RLock()
	state := srv.state.cur
	srv.mu.RUnlock()
	return state
}

func (srv *Server) getNextState() fsm.Status {
	srv.mu.RLock()
	state := srv.state.next
	srv.mu.RUnlock()
	return state
}

func (srv *Server) join(ctx context.Context) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	select {
	case <-srv.quit:
		return xerrors.Errorf("could not /join run-ctl before exiting")
	case <-ctx.Done():
		return xerrors.Errorf("could not /join run-ctl before timeout: %w", ctx.Err())
	default:
		join := JoinCmd{
			Name:         srv.name,
			InEndPoints:  srv.imgr.endpoints(),
			OutEndPoints: srv.omgr.endpoints(),
		}

		err := SendCmd(ctx, srv.rctl, &join)
		if err != nil {
			return xerrors.Errorf("could not send /join cmd to run-ctl: %w", err)
		}

		frame, err := RecvFrame(ctx, srv.rctl)
		if err != nil {
			return xerrors.Errorf("could not recv /join-ack from run-ctl: %w", err)
		}
		switch frame.Type {
		case FrameOK:
			// OK
		case FrameErr:
			return xerrors.Errorf("received error /join-ack from run-ctl: %s", frame.Body)
		default:
			return xerrors.Errorf("received invalid /join-ack frame from run-ctl (frame=%#v)", frame)
		}

		err = srv.setupLogCmd(ctx)
		if err != nil {
			return xerrors.Errorf("could not setup /log cmd: %w", err)
		}

		err = srv.setupHBeatCmd(ctx)
		if err != nil {
			return xerrors.Errorf("could not setup /hbeat cmd: %w", err)
		}
	}

	return nil
}

func (srv *Server) setupLogCmd(ctx context.Context) error {
	frame, err := RecvFrame(ctx, srv.rctl)
	if err != nil {
		return xerrors.Errorf("could not recv /log cmd from run-ctl: %w", err)
	}
	if frame.Type != FrameCmd {
		return xerrors.Errorf("received invalid /log cmd from run-ctl: type=%v", frame.Type)
	}

	cmd, err := newLogCmd(frame)
	if err != nil {
		return xerrors.Errorf("received invalid /log cmd from run-ctl: %w", err)
	}

	ackOK := Frame{Type: FrameOK}
	err = SendFrame(ctx, srv.rctl, ackOK)
	if err != nil {
		return xerrors.Errorf("could not send /log-ack to run-ctl: %w", err)
	}

	conn, err := net.Dial("tcp", cmd.Addr)
	if err != nil {
		return xerrors.Errorf("could not dial run-ctl log server: %w", err)
	}
	setupTCPConn(conn.(*net.TCPConn))

	srv.log = conn
	srv.msg.setLog(srv.log)

	return nil
}

func (srv *Server) setupHBeatCmd(ctx context.Context) error {
	frame, err := RecvFrame(ctx, srv.rctl)
	if err != nil {
		return xerrors.Errorf("could not recv /hbeat cmd from run-ctl: %w", err)
	}
	if frame.Type != FrameCmd {
		return xerrors.Errorf("received invalid /hbeat cmd from run-ctl: type=%v", frame.Type)
	}

	cmd, err := newHBeatCmd(frame)
	if err != nil {
		return xerrors.Errorf("received invalid /hbeat cmd from run-ctl: %w", err)
	}

	ackOK := Frame{Type: FrameOK}
	err = SendFrame(ctx, srv.rctl, ackOK)
	if err != nil {
		return xerrors.Errorf("could not send /hbeat-ack to run-ctl: %w", err)
	}

	conn, err := net.Dial("tcp", cmd.Addr)
	if err != nil {
		return xerrors.Errorf("could not dial run-ctl hbeat server: %w", err)
	}
	setupTCPConn(conn.(*net.TCPConn))

	srv.hbeat = conn

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
			frame, err := RecvFrame(ctx, srv.rctl)
			switch {
			case err == nil:
				// ok
			case xerrors.Is(err, io.EOF):
				switch state := srv.getNextState(); state {
				case fsm.Exiting:
					// ok
				default:
					srv.msg.Warnf("connection to run-ctl: %+v", err)
				}
				return

			default:
				var nerr net.Error
				if xerrors.As(err, &nerr); nerr != nil && !nerr.Temporary() {
					switch state := srv.getNextState(); state {
					case fsm.Exiting:
						// ok
					default:
						srv.msg.Warnf("connection to run-ctl: %+v", err)
					}
					return
				}
				srv.msg.Warnf("could not receive cmds from run-ctl: %+v", err)
				continue
			}

			srv.handleCmd(ctx, srv.rctl, frame)
		}
	}
}

func (srv *Server) handleCmd(ctx context.Context, w io.Writer, req Frame) {
	var (
		resp = Frame{Type: FrameOK}
		next fsm.Status
		err  error
	)

	name := req.Path

	h, ok := srv.cmgr.endpoint(name)
	if !ok {
		srv.msg.Warnf("invalid request path %q", name)
		resp.Type = FrameErr
		resp.Body = []byte(xerrors.Errorf("invalid request path %q", name).Error())

		err = SendFrame(ctx, w, resp)
		if err != nil {
			srv.msg.Warnf("could not send ack cmd: %+v", err)
		}
	}

	var onCmd func(ctx Context, req Frame) error
	switch name {
	case "/config":
		onCmd = srv.onConfig
		next = fsm.Conf
	case "/init":
		onCmd = srv.onInit
		next = fsm.Init
	case "/reset":
		onCmd = srv.onReset
		next = fsm.Conf
	case "/start":
		onCmd = srv.onStart
		next = fsm.Running
		runctx, cancel := context.WithCancel(context.Background())
		rungrp, runctx := errgroup.WithContext(runctx)

		srv.runctx = runctx
		srv.rundone = cancel
		srv.rungrp = rungrp

		ctx = runctx
	case "/stop":
		onCmd = srv.onStop
		next = fsm.Stopped
	case "/quit":
		onCmd = srv.onQuit
		next = fsm.Exiting
		defer close(srv.quit)

	case "/status":
		onCmd = srv.onStatus
		next = srv.getCurState()

	default:
		srv.msg.Errorf("invalid cmd %q", name)
		return
	}

	srv.setNextState(next)

	tctx := Context{Ctx: ctx, Msg: srv.msg}
	errPre := onCmd(tctx, req)
	if errPre != nil {
		srv.msg.Warnf("could not run %v pre-handler: %+v", name, errPre)
		resp.Type = FrameErr
		resp.Body = []byte(errPre.Error())
		next = fsm.Error
	}

	errH := h(tctx, &resp, req)
	if errH != nil {
		srv.msg.Warnf("could not run %v handler: %+v", name, errH)
		resp.Type = FrameErr
		resp.Body = []byte(errH.Error())
		next = fsm.Error
	}

	srv.setCurState(next)

	switch name {
	case "/status":
		// ok. reply already sent.
	default:
		err = SendFrame(ctx, w, resp)
		if err != nil {
			srv.msg.Warnf("could not send ack cmd: %+v", err)
		}
	}
}

func (srv *Server) onConfig(ctx Context, req Frame) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	switch srv.state.cur {
	case fsm.UnConf, fsm.Conf, fsm.Error:
		// ok
	default:
		return xerrors.Errorf("%s: invalid state transition %v -> configured", srv.name, srv.state.cur)
	}

	ierr := srv.imgr.onConfig(ctx, req)
	if ierr != nil {
		return xerrors.Errorf("could not /config input-ports: %w", ierr)
	}

	return nil
}

func (srv *Server) onInit(ctx Context, req Frame) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	switch srv.state.cur {
	case fsm.Conf:
		// ok
	default:
		return xerrors.Errorf("%s: invalid state transition %v -> initialized", srv.name, srv.state.cur)
	}

	return nil
}

func (srv *Server) onReset(ctx Context, req Frame) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	switch srv.state.cur {
	case fsm.UnConf, fsm.Conf, fsm.Init, fsm.Stopped, fsm.Error:
		// ok
	default:
		return xerrors.Errorf("%s: invalid state transition %v -> reset", srv.name, srv.state.cur)
	}

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

func (srv *Server) onStart(runctx Context, req Frame) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	switch srv.state.cur {
	case fsm.Init, fsm.Stopped:
		// ok
	default:
		return xerrors.Errorf("%s: invalid state transition %v -> started", srv.name, srv.state.cur)
	}

	for i := range srv.runfcts {
		f := srv.runfcts[i]
		srv.rungrp.Go(func() error {
			return f(runctx)
		})
	}

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

func (srv *Server) onStop(ctx Context, req Frame) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	switch srv.state.cur {
	case fsm.Running:
		// ok
	default:
		return xerrors.Errorf("%s: invalid state transition %v -> stopped", srv.name, srv.state.cur)
	}

	srv.rundone()
	<-srv.runctx.Done()
	werr := srv.rungrp.Wait()

	ierr := srv.imgr.onStop(ctx)
	oerr := srv.omgr.onStop(ctx)

	switch {
	case werr != nil:
		return werr
	case ierr != nil:
		return ierr
	case oerr != nil:
		return oerr
	}

	return nil
}

func (srv *Server) onQuit(ctx Context, req Frame) error {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	return nil
}

func (srv *Server) onStatus(ctx Context, req Frame) error {
	srv.mu.RLock()
	defer srv.mu.RUnlock()

	state := srv.state.cur
	cmd := StatusCmd{
		Name:   srv.name,
		Status: state,
	}

	err := SendCmd(ctx.Ctx, srv.rctl, &cmd)
	if err != nil {
		return xerrors.Errorf("%s: could not send /status reply: %w", srv.name, err)
	}

	return nil
}

func (srv *Server) hbeatLoop(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	cmd := JoinCmd{Name: srv.name}
	err := SendCmd(ctx, srv.hbeat, &cmd)
	if err != nil {
		srv.msg.Errorf("%s: could not send /join to run-ctl hbeat: %+v", srv.name, err)
		return
	}

	ack, err := RecvFrame(ctx, srv.hbeat)
	switch ack.Type {
	case FrameOK:
		// OK
	case FrameErr:
		srv.msg.Errorf("received error /join-ack from run-ctl hbeat: %s", ack.Body)
		return
	default:
		srv.msg.Errorf("received invalid /join-ack frame from run-ctl hbeat (type=%#v)", ack.Type)
		return
	}

	for {
		select {
		case <-srv.quit:
			return
		default:
			frame, err := RecvFrame(ctx, srv.hbeat)
			switch {
			case err == nil:
				// ok
			case xerrors.Is(err, io.EOF):
				switch state := srv.getNextState(); state {
				case fsm.Exiting:
					// ok
				default:
					srv.msg.Warnf("connection to run-ctl: %+v", err)
				}
				return

			default:
				var nerr net.Error
				if xerrors.As(err, &nerr); nerr != nil && !nerr.Temporary() {
					switch state := srv.getNextState(); state {
					case fsm.Exiting:
						// ok
					default:
						srv.msg.Warnf("connection to run-ctl: %+v", err)
					}
					return
				}
				srv.msg.Warnf("could not receive cmds from run-ctl: %+v", err)
				continue
			}

			err = srv.handleHBeat(ctx, frame)
			if err != nil {
				srv.msg.Errorf("could not send /hbeat recv to run-ctl: %+v", err)
			}
		}
	}
}

func (srv *Server) handleHBeat(ctx context.Context, frame Frame) error {
	srv.mu.RLock()
	defer srv.mu.RUnlock()

	state := srv.state.cur
	cmd := StatusCmd{
		Name:   srv.name,
		Status: state,
	}

	err := SendCmd(ctx, srv.hbeat, &cmd)
	if err != nil {
		return xerrors.Errorf("%s: could not send /hbeat reply: %w", srv.name, err)
	}

	return nil
}

type msgstream struct {
	mu   sync.Mutex
	lvl  log.Level
	w    io.Writer
	conn net.Conn
	n    string
}

func newMsgStream(name string, lvl log.Level, w io.Writer) *msgstream {
	return &msgstream{
		lvl: lvl,
		w:   w,
		n:   fmt.Sprintf("%-20s ", name),
	}
}

// Debugf displays a (formated) DBG message
func (msg *msgstream) Debugf(format string, a ...interface{}) (int, error) {
	return msg.Msg(log.LvlDebug, format, a...)
}

// Infof displays a (formated) INFO message
func (msg *msgstream) Infof(format string, a ...interface{}) (int, error) {
	return msg.Msg(log.LvlInfo, format, a...)
}

// Warnf displays a (formated) WARN message
func (msg *msgstream) Warnf(format string, a ...interface{}) (int, error) {
	return msg.Msg(log.LvlWarning, format, a...)
}

// Errorf displays a (formated) ERR message
func (msg *msgstream) Errorf(format string, a ...interface{}) (int, error) {
	return msg.Msg(log.LvlError, format, a...)
}

// Msg displays a (formated) message with level lvl.
func (msg *msgstream) Msg(lvl log.Level, format string, a ...interface{}) (int, error) {
	msg.mu.Lock()
	defer msg.mu.Unlock()

	if lvl < msg.lvl {
		return 0, nil
	}
	eol := ""
	if !strings.HasSuffix(format, "\n") {
		eol = "\n"
	}
	format = msg.n + lvl.MsgString() + " " + format + eol
	str := []byte(fmt.Sprintf(format, a...))

	if msg.conn != nil {
		go SendMsg(context.Background(), msg.conn, MsgFrame{
			Name:  strings.TrimSpace(msg.n),
			Level: lvl,
			Msg:   string(str),
		})
	}

	return msg.w.Write(str)
}

func (msg *msgstream) setLog(conn net.Conn) {
	msg.mu.Lock()
	msg.conn = conn
	msg.mu.Unlock()
}

var (
	_ log.MsgStream = (*msgstream)(nil)
)
