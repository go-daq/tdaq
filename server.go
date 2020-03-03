// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq // import "github.com/go-daq/tdaq"

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-daq/tdaq/config"
	"github.com/go-daq/tdaq/fsm"
	"github.com/go-daq/tdaq/log"
	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/pub"
	"go.nanomsg.org/mangos/v3/protocol/rep"
	"go.nanomsg.org/mangos/v3/protocol/req"
	"golang.org/x/sync/errgroup"
)

type Server struct {
	rc   string // run-ctl address:port
	name string
	cfg  config.Process

	rctl struct {
		sck mangos.Socket
		lis mangos.Listener
	}
	hbeat struct {
		sck mangos.Socket
		lis mangos.Listener
	}
	log struct {
		sck mangos.Socket
		lis mangos.Listener
	}

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

	rpark chan int      // rctl parking signal
	hpark chan int      // hbeat parking signal
	done  chan struct{} // signal to prepare exiting
}

func New(cfg config.Process, stdout io.Writer) *Server {
	if stdout == nil {
		stdout = os.Stdout
	}

	srv := &Server{
		rc: config.RunCtl{
			Trans:  cfg.Trans,
			RunCtl: cfg.RunCtl,
		}.Addr(),
		name: cfg.Name,
		cfg:  cfg,
		msg:  newMsgStream(cfg.Name, cfg.Level, stdout),
		cmgr: newCmdMgr(
			"/config", "/init", "/reset", "/start", "/stop",
			"/quit",
			"/status",
		),

		rpark: make(chan int),
		hpark: make(chan int),
		done:  make(chan struct{}),
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

	rctl, rlis, err := makeListener(rep.NewSocket, makeAddr(srv.cfg))
	if err != nil {
		return fmt.Errorf("could not create run-ctl socket: %w", err)
	}
	srv.rctl.sck = rctl
	srv.rctl.lis = rlis

	hbeat, hlis, err := makeListener(rep.NewSocket, makeAddr(srv.cfg))
	if err != nil {
		return fmt.Errorf("could not create hbeat socket: %w", err)
	}
	srv.hbeat.sck = hbeat
	srv.hbeat.lis = hlis

	log, llis, err := makeListener(pub.NewSocket, makeAddr(srv.cfg))
	if err != nil {
		return fmt.Errorf("could not create log socket: %w", err)
	}
	srv.log.sck = log
	srv.log.lis = llis
	srv.msg.setLog(log)

	go srv.hbeatLoop(ctx)
	go srv.cmdsLoop(ctx)

	err = srv.omgr.init(srv)
	if err != nil {
		return fmt.Errorf("could not setup i/o data ports: %w", err)
	}

	srv.cmgr.init()

	err = srv.join(ctx)
	if err != nil {
		return fmt.Errorf("could not join run-ctl: %w", err)
	}

	defer srv.msg.Debugf("server closing...")

	select {
	case <-srv.done:
		srv.close()
		return nil
	case <-ctx.Done():
		close(srv.done)
		srv.close()
		return ctx.Err()
	}
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

	sck, err := req.NewSocket()
	if err != nil {
		return fmt.Errorf("could not create /join socket: %w", err)
	}
	defer sck.Close()

	err = sck.Dial(srv.rc)
	if err != nil {
		return fmt.Errorf(
			"could not dial /join socket %q: %w",
			srv.rc, err,
		)
	}

	select {
	case <-srv.done:
		return fmt.Errorf("could not /join run-ctl before exiting")
	case <-ctx.Done():
		return fmt.Errorf("could not /join run-ctl before timeout: %w", ctx.Err())
	default:
	}

	join := JoinCmd{
		Name:         srv.name,
		Ctl:          srv.rctl.lis.Address(),
		HBeat:        srv.hbeat.lis.Address(),
		Log:          srv.log.lis.Address(),
		InEndPoints:  srv.imgr.endpoints(),
		OutEndPoints: srv.omgr.endpoints(),
	}

	err = SendCmd(ctx, sck, &join)
	if err != nil {
		return fmt.Errorf("could not send /join cmd to run-ctl: %w", err)
	}

	frame, err := RecvFrame(ctx, sck)
	if err != nil {
		return fmt.Errorf("could not recv /join-ack from run-ctl: %w", err)
	}
	switch frame.Type {
	case FrameOK:
		// OK
		return nil
	case FrameErr:
		return fmt.Errorf("received error /join-ack from run-ctl: %s", frame.Body)
	default:
		return fmt.Errorf("received invalid /join-ack frame from run-ctl (frame=%#v)", frame)
	}
}

func (srv *Server) cmdsLoop(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		select {
		case <-srv.rpark:
			_ = srv.rctl.lis.Close()
			_ = srv.rctl.sck.Close()
			srv.rpark <- 1
			return
		case <-ctx.Done():
			return
		default:
			frame, err := RecvFrame(ctx, srv.rctl.sck)
			switch {
			case err == nil:
				// ok
			case errors.Is(err, mangos.ErrClosed):
				switch state := srv.getNextState(); state {
				case fsm.Exiting:
					// ok
				default:
					srv.msg.Warnf("connection to run-ctl: %+v", err)
				}
				return

			default:
				srv.msg.Warnf("could not receive cmds from run-ctl: %+v", err)
				continue
			}

			srv.handleCmd(ctx, frame)
		}
	}
}

func (srv *Server) handleCmd(ctx context.Context, req Frame) {
	var (
		sck  = srv.rctl.sck
		resp = Frame{Type: FrameOK}
		next fsm.Status
		err  error
	)

	name := req.Path

	h, ok := srv.cmgr.endpoint(name)
	if !ok {
		srv.msg.Warnf("invalid request path %q", name)
		resp.Type = FrameErr
		resp.Body = []byte(fmt.Errorf("invalid request path %q", name).Error())

		err = SendFrame(ctx, sck, resp)
		if err != nil {
			srv.msg.Warnf("could not send ack cmd: %+v", err)
		}
		return
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
		defer close(srv.done)

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
		err = SendFrame(ctx, sck, resp)
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
		return fmt.Errorf("%s: invalid state transition %v -> configured", srv.name, srv.state.cur)
	}

	ierr := srv.imgr.onConfig(ctx, req)
	if ierr != nil {
		return fmt.Errorf("could not /config input-ports: %w", ierr)
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
		return fmt.Errorf("%s: invalid state transition %v -> initialized", srv.name, srv.state.cur)
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
		return fmt.Errorf("%s: invalid state transition %v -> reset", srv.name, srv.state.cur)
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
		return fmt.Errorf("%s: invalid state transition %v -> started", srv.name, srv.state.cur)
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
		return fmt.Errorf("%s: invalid state transition %v -> stopped", srv.name, srv.state.cur)
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

	err := SendCmd(ctx.Ctx, srv.rctl.sck, &cmd)
	if err != nil {
		return fmt.Errorf("%s: could not send /status reply: %w", srv.name, err)
	}

	return nil
}

func (srv *Server) hbeatLoop(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		select {
		case <-srv.hpark:
			_ = srv.hbeat.lis.Close()
			_ = srv.hbeat.sck.Close()
			srv.hpark <- 1
			return
		case <-ctx.Done():
			return
		default:
			frame, err := RecvFrame(ctx, srv.hbeat.sck)
			switch {
			case err == nil:
				// ok
			case errors.Is(err, mangos.ErrClosed):
				switch state := srv.getNextState(); state {
				case fsm.Exiting:
					// ok
				default:
					srv.msg.Warnf("connection to run-ctl: %+v", err)
				}
				return

			default:
				srv.msg.Warnf("could not recv /hbeat from run-ctl: %+v", err)
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

	err := SendCmd(ctx, srv.hbeat.sck, &cmd)
	if err != nil {
		return fmt.Errorf("%s: could not send /hbeat reply: %w", srv.name, err)
	}

	return nil
}

func (srv *Server) close() {
	defer func() {
		go func() {
			srv.msg.setLog(nil)
			_ = srv.log.lis.Close()
			_ = srv.log.sck.Close()
		}()
	}()

	srv.msg.Debugf("server shutting down...")

	srv.park(srv.hpark)
	srv.park(srv.rpark)

	srv.imgr.close()
	srv.omgr.close()
	srv.cmgr.close()
	srv.msg.Debugf("server shutting down... [done]")
}

func (srv *Server) park(ch chan int) {
	select {
	case ch <- 1:
		<-ch
	default:
	}
}

type msgstream struct {
	mu  sync.Mutex
	lvl log.Level
	w   io.Writer
	sck mangos.Socket
	n   string
}

func newMsgStream(name string, lvl log.Level, w io.Writer) *msgstream {
	return &msgstream{
		lvl: lvl,
		w:   w,
		n:   fmt.Sprintf("%-20s ", name),
	}
}

// Debugf displays a (formated) DBG message
func (msg *msgstream) Debugf(format string, a ...interface{}) {
	msg.Msg(log.LvlDebug, format, a...)
}

// Infof displays a (formated) INFO message
func (msg *msgstream) Infof(format string, a ...interface{}) {
	msg.Msg(log.LvlInfo, format, a...)
}

// Warnf displays a (formated) WARN message
func (msg *msgstream) Warnf(format string, a ...interface{}) {
	msg.Msg(log.LvlWarning, format, a...)
}

// Errorf displays a (formated) ERR message
func (msg *msgstream) Errorf(format string, a ...interface{}) {
	msg.Msg(log.LvlError, format, a...)
}

// Msg displays a (formated) message with level lvl.
func (msg *msgstream) Msg(lvl log.Level, format string, a ...interface{}) {
	msg.mu.Lock()
	defer msg.mu.Unlock()

	if lvl < msg.lvl {
		return
	}
	eol := ""
	if !strings.HasSuffix(format, "\n") {
		eol = "\n"
	}
	format = msg.n + lvl.MsgString() + " " + format + eol
	str := []byte(fmt.Sprintf(format, a...))

	if msg.sck != nil {
		go func(sck mangos.Socket) {
			_ = SendMsg(context.Background(), sck, MsgFrame{
				Name:  strings.TrimSpace(msg.n),
				Level: lvl,
				Msg:   string(str),
			})
		}(msg.sck)
	}

	_, _ = msg.w.Write(str)
}

func (msg *msgstream) setLog(sck mangos.Socket) {
	msg.mu.Lock()
	msg.sck = sck
	msg.mu.Unlock()
}

var (
	_ log.MsgStream = (*msgstream)(nil)
)
