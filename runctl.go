// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq // import "github.com/go-daq/tdaq"

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/go-daq/tdaq/config"
	"github.com/go-daq/tdaq/fsm"
	"github.com/go-daq/tdaq/log"
	"golang.org/x/net/websocket"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

type proc struct {
	name   string
	status fsm.Status
	ieps   []EndPoint
	oeps   []EndPoint
	cmd    net.Conn
	hbeat  net.Conn
}

func (p *proc) close() error {
	var (
		err1 error
		err2 error
	)

	if p.cmd != nil {
		err1 = p.cmd.Close()
		if err1 == nil {
			p.cmd = nil
		}
	}

	if p.hbeat != nil {
		err2 = p.hbeat.Close()
		if err2 == nil {
			p.hbeat = nil
		}
	}

	if err1 != nil {
		return err1
	}

	if err2 != nil {
		return err2
	}

	return nil
}

type RunControl struct {
	quit chan struct{}
	cfg  config.RunCtl

	srv   net.Listener // ctl server
	hbeat net.Listener // hbeat server
	log   net.Listener // log server
	web   websrv       // web server

	stdout io.Writer

	mu        sync.RWMutex
	status    fsm.StateKind
	msg       log.MsgStream
	procs     map[string]*proc
	listening bool

	msgch chan MsgFrame // messages from log server
	flog  *os.File

	runNbr uint64
}

func NewRunControl(cfg config.RunCtl, stdout io.Writer) (*RunControl, error) {
	if stdout == nil {
		stdout = os.Stdout
	}
	fname := cfg.LogFile
	if fname == "" {
		fname = fmt.Sprintf("log-tdaq-runctl-%v.txt", time.Now().UTC().Format("2006-01-150405"))
	}

	flog, err := os.Create(fname)
	if err != nil {
		return nil, xerrors.Errorf("could not create run-ctl log file %q: %w", fname, err)
	}

	stdout = io.MultiWriter(stdout, flog)
	out := newSyncWriter(stdout)

	rc := &RunControl{
		quit:      make(chan struct{}),
		cfg:       cfg,
		stdout:    out,
		status:    fsm.UnConf,
		msg:       log.NewMsgStream(cfg.Name, cfg.Level, out),
		procs:     make(map[string]*proc),
		listening: true,
		msgch:     make(chan MsgFrame, 1024),
		flog:      flog,
		runNbr:    uint64(time.Now().UTC().Unix()),
	}

	rc.msg.Infof("listening on %q...", cfg.RunCtl)
	srv, err := net.Listen("tcp", cfg.RunCtl)
	if err != nil {
		return nil, xerrors.Errorf("could not create TCP cmd server: %w", err)
	}
	rc.srv = srv

	srv, err = net.Listen("tcp", cfg.Log)
	if err != nil {
		return nil, xerrors.Errorf("could not create TCP log server: %w", err)
	}
	rc.log = srv

	hbeat, err := net.Listen("tcp", ":0")
	if err != nil {
		return nil, xerrors.Errorf("could not create TCP hbeat server: %w", err)
	}
	rc.hbeat = hbeat

	if cfg.Web != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/", rc.webHome)
		mux.HandleFunc("/cmd", rc.webCmd)
		mux.Handle("/status", websocket.Handler(rc.webStatus))
		mux.Handle("/msg", websocket.Handler(rc.webMsg))
		rc.web = &http.Server{
			Addr:    cfg.Web,
			Handler: mux,
		}
	}

	return rc, nil
}

// NumClients returns the number of TDAQ processes connected to this run control.
func (rc *RunControl) NumClients() int {
	rc.mu.RLock()
	n := len(rc.procs)
	rc.mu.RUnlock()
	return n
}

func (rc *RunControl) Run(ctx context.Context) error {
	rc.msg.Infof("waiting for commands...")
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go rc.serveLog(ctx)
	go rc.serveHeartbeat(ctx)
	go rc.serveCtl(ctx)
	go rc.serveWeb(ctx)

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
	rc.msg.Infof("closing...")

	for _, proc := range rc.procs {
		err := proc.close()
		if err != nil {
			rc.msg.Errorf("could not close proc to %q: %+v", proc.name, err)
		}
		delete(rc.procs, proc.name)
	}
	rc.procs = nil

	if rc.log != nil {
		err := rc.log.Close()
		if err != nil {
			rc.msg.Errorf("could not close run-ctl log server: %+v", err)
		}
	}

	if rc.flog != nil {
		err := rc.flog.Close()
		if err != nil {
			rc.msg.Errorf("could not close run-ctl log file: %+v", err)
		}
	}

	if rc.hbeat != nil {
		err := rc.hbeat.Close()
		if err != nil {
			rc.msg.Errorf("could not close run-ctl heartbeat server: %+v", err)
		}
	}

	if rc.srv != nil {
		err := rc.srv.Close()
		if err != nil {
			rc.msg.Errorf("could not close run-ctl cmd server: %+v", err)
		}
	}

	if rc.web != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := rc.web.Shutdown(ctx)
		if err != nil {
			rc.msg.Errorf("could not close run-ctl web server: %+v", err)
		}
	}
}

func (rc *RunControl) serveCtl(ctx context.Context) {
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
			go rc.handleCtlConn(ctx, conn)
		}
	}
}

func (rc *RunControl) serveLog(ctx context.Context) {
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
			conn, err := rc.log.Accept()
			if err != nil {
				select {
				case <-rc.quit:
					// ok, we are shutting down.
				default:
					rc.msg.Errorf("error accepting connection: %v", err)
				}
				continue
			}
			go rc.handleLogConn(ctx, conn)
		}
	}
}

func (rc *RunControl) serveHeartbeat(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		select {
		case <-rc.quit:
			return
		case <-ctx.Done():
			err := ctx.Err()
			if err != nil {
				rc.msg.Errorf("context errored during run-ctl heartbeat serve: %v", err)
			}
			return
		default:
			conn, err := rc.hbeat.Accept()
			if err != nil {
				select {
				case <-rc.quit:
					// ok, we are shutting down.
				default:
					rc.msg.Errorf("error accepting connection: %v", err)
				}
				continue
			}
			go rc.handleHeartbeatConn(ctx, conn)
		}
	}
}

func (rc *RunControl) handleCtlConn(ctx context.Context, conn net.Conn) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	setupTCPConn(conn.(*net.TCPConn))

	req, err := RecvFrame(ctx, conn)
	if err != nil {
		rc.msg.Errorf("could not receive /join cmd from conn %v: %v", conn.RemoteAddr(), err)
		sendFrame(ctx, conn, FrameErr, nil, []byte(err.Error()))
		return
	}

	join, err := newJoinCmd(req)
	if err != nil {
		rc.msg.Errorf("could not receive /join cmd from conn %v: %v", conn.RemoteAddr(), err)
		sendFrame(ctx, conn, FrameErr, nil, []byte(err.Error()))
		return
	}

	rc.msg.Infof("received /join from conn %v", conn.RemoteAddr())
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

	rc.procs[join.Name] = &proc{
		name: join.Name,
		status: fsm.Status{
			State: fsm.UnConf,
		},
		ieps: join.InEndPoints,
		oeps: join.OutEndPoints,
		cmd:  conn,
	}

	ackOK := Frame{Type: FrameOK}
	err = SendFrame(ctx, conn, ackOK)
	if err != nil {
		rc.msg.Errorf("could not recv /join-ack from %q: %+", join.Name, err)
		return
	}

	err1 := rc.setupLogCmd(ctx, join.Name, conn)
	if err1 != nil {
		rc.msg.Errorf("could not setup /log cmd: %+v", err1)
		return
	}

	err2 := rc.setupHBeatCmd(ctx, join.Name, conn)
	if err2 != nil {
		rc.msg.Errorf("could not setup /hbeat cmd: %+v", err2)
		return
	}
}

func (rc *RunControl) setupLogCmd(ctx context.Context, name string, conn net.Conn) error {
	err := SendCmd(ctx, conn, &LogCmd{Name: name, Addr: rc.log.Addr().String()})
	if err != nil {
		return xerrors.Errorf("could not send /log cmd to %q: %w", name, err)
	}

	ack, err := RecvFrame(ctx, conn)
	if err != nil {
		err = xerrors.Errorf("could not receive /log cmd ack from %q: %w", name, err)
		sendFrame(ctx, conn, FrameErr, nil, []byte(err.Error()))
		return err
	}

	switch ack.Type {
	case FrameOK:
		return nil // ok
	case FrameErr:
		return xerrors.Errorf("received error /log-ack frame from conn %q: %s", name, string(ack.Body))
	default:
		return xerrors.Errorf("received invalid /log-ack frame from conn %q: %#v", name, ack)
	}
}

func (rc *RunControl) setupHBeatCmd(ctx context.Context, name string, conn net.Conn) error {
	err := SendCmd(ctx, conn, &HBeatCmd{Name: name, Addr: rc.hbeat.Addr().String()})
	if err != nil {
		return xerrors.Errorf("could not send /hbeat cmd to %q: %w", name, err)
	}

	ack, err := RecvFrame(ctx, conn)
	if err != nil {
		err = xerrors.Errorf("could not receive /hbeat cmd ack from %q: %w", name, err)
		sendFrame(ctx, conn, FrameErr, nil, []byte(err.Error()))
		return err
	}

	switch ack.Type {
	case FrameOK:
		return nil // ok
	case FrameErr:
		return xerrors.Errorf("received error /log-ack frame from conn %q: %s", name, string(ack.Body))
	default:
		return xerrors.Errorf("received invalid /log-ack frame from conn %q: %#v", name, ack)
	}
}

func (rc *RunControl) handleLogConn(ctx context.Context, conn net.Conn) {
	setupTCPConn(conn.(*net.TCPConn))
	defer conn.Close()

	for {
		select {
		case <-rc.quit:
			return
		case <-ctx.Done():
			return
		default:
			frame, err := RecvFrame(ctx, conn)
			if err != nil {
				select {
				case <-rc.quit:
					// ok, we're shutting down.
					return
				default:
				}
				rc.msg.Errorf("could not receive /log frame: %+v", err)
				var nerr net.Error
				if xerrors.Is(err, nerr); nerr != nil && !nerr.Temporary() {
					return
				}
				if xerrors.Is(err, io.EOF) {
					return
				}
			}
			var msg MsgFrame
			err = msg.UnmarshalTDAQ(frame.Body)
			if err != nil {
				rc.msg.Errorf("could not unmarshal /log frame: %+v", err)
			}
			go rc.processLog(msg)

			select {
			case rc.msgch <- msg:
			default:
				// ok to drop messages.
			}
		}
	}
}

func (rc *RunControl) handleHeartbeatConn(ctx context.Context, conn net.Conn) {
	setupTCPConn(conn.(*net.TCPConn))
	defer conn.Close()

	req, err := RecvFrame(ctx, conn)
	if err != nil {
		rc.msg.Errorf("could not receive /join cmd from conn %v: %+v", conn.RemoteAddr(), err)
		sendFrame(ctx, conn, FrameErr, nil, []byte(err.Error()))
		return
	}
	cmd, err := newJoinCmd(req)
	if err != nil {
		rc.msg.Errorf("could not receive /join cmd from conn %v: %+v", conn.RemoteAddr(), err)
		sendFrame(ctx, conn, FrameErr, nil, []byte(err.Error()))
		return
	}

	ackOK := Frame{Type: FrameOK}
	err = SendFrame(ctx, conn, ackOK)
	if err != nil {
		rc.msg.Errorf("could not send /join-ack hbeat to %q: %+v", cmd.Name, err)
		return
	}

	hbeat := time.NewTicker(5 * time.Second)
	defer hbeat.Stop()

	for {
		select {
		case <-rc.quit:
			return
		case <-ctx.Done():
			return
		case <-hbeat.C:
			err := rc.doHeartbeat(ctx)
			if err != nil {
				rc.msg.Warnf("could not process /status heartbeat: %+v", err)
			}
		}
	}
}

func (rc *RunControl) processLog(msg MsgFrame) {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	select {
	case <-rc.quit:
		// we are shutting down...
		return
	default:
	}

	_, err := rc.flog.Write([]byte(msg.Msg))

	if err != nil {
		rc.msg.Errorf("could not write msg to log file: %q\nerror: %+v", msg.Msg, err)
	}
}

func (rc *RunControl) broadcast(ctx context.Context, cmd CmdType) error {
	var berr []error

	for _, proc := range rc.procs {
		rc.msg.Debugf("sending cmd %v to %q...", cmd, proc.name)
		err := sendCmd(ctx, proc.cmd, cmd, nil)
		if err != nil {
			rc.msg.Errorf("could not send cmd %v to %q: %v", cmd, proc.name, err)
			berr = append(berr, err)
			continue
		}
		rc.msg.Debugf("sending cmd %v... [ok]", cmd)
		ack, err := RecvFrame(ctx, proc.cmd)
		if err != nil {
			rc.msg.Errorf("could not receive %v ACK from %q: %+v", cmd, proc.name, err)
			berr = append(berr, err)
			continue
		}
		switch ack.Type {
		case FrameOK:
			// ok
		case FrameErr:
			rc.msg.Errorf("received ERR ACK from %q: %v", proc.name, string(ack.Body))
			berr = append(berr, xerrors.Errorf(string(ack.Body)))
		default:
			rc.msg.Errorf("received invalid frame type %v from %q", ack.Type, proc.name)
			berr = append(berr, xerrors.Errorf("received invalid frame type %v from %q", ack.Type, proc.name))
		}
		rc.msg.Debugf("sending cmd %v to %q... [ok]", cmd, proc.name)
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

	procs := make([]string, 0, len(rc.procs))
	for _, proc := range rc.procs {
		for i := range proc.ieps {
			iport := &proc.ieps[i]
			provider, ok := rc.providerOf(*iport)
			if !ok {
				return xerrors.Errorf("could not find a provider for input %q for %q", iport.Name, proc.name)
			}
			iport.Addr = provider
		}
		procs = append(procs, proc.name)
	}

	var grp errgroup.Group
	for i := range procs {
		proc := rc.procs[procs[i]]
		cmd := ConfigCmd{
			Name:         proc.name,
			InEndPoints:  proc.ieps,
			OutEndPoints: proc.oeps,
		}
		grp.Go(func() error {
			rc.msg.Debugf("sending /config to %q...", proc.name)
			err := SendCmd(ctx, proc.cmd, &cmd)
			if err != nil {
				rc.msg.Errorf("could not send /config to %q: %v+", proc.name, err)
				return err
			}

			ack, err := RecvFrame(ctx, proc.cmd)
			if err != nil {
				rc.msg.Errorf("could not receive ACK from %q: %+v", proc.name, err)
				return err
			}
			switch ack.Type {
			case FrameOK:
				// ok
			case FrameErr:
				rc.msg.Errorf("received ERR ACK from %q: %v", proc.name, string(ack.Body))
				return xerrors.Errorf("received ERR ACK from %q: %v", proc.name, string(ack.Body))
			default:
				rc.msg.Errorf("received invalid frame type %v from %q", ack.Type, proc.name)
				return xerrors.Errorf("received invalid frame type %v from %q", ack.Type, proc.name)
			}
			rc.msg.Debugf("sending /config to %q... [ok]", proc.name)
			return nil
		})
	}

	err := grp.Wait()
	if err != nil {
		rc.status = fsm.Error
		return xerrors.Errorf("failed to run errgroup: %w", err)
	}

	rc.status = fsm.Conf
	for _, proc := range rc.procs {
		proc.status.State = rc.status
	}

	return nil
}

func (rc *RunControl) doInit(ctx context.Context) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.msg.Infof("/init processes...")

	err := rc.broadcast(ctx, CmdInit)
	if err != nil {
		rc.status = fsm.Error
		return err
	}

	rc.status = fsm.Init
	for _, proc := range rc.procs {
		proc.status.State = rc.status
	}

	return nil
}

func (rc *RunControl) doReset(ctx context.Context) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.msg.Infof("/reset processes...")

	err := rc.broadcast(ctx, CmdReset)
	if err != nil {
		rc.status = fsm.Error
		return err
	}

	rc.status = fsm.UnConf
	for _, proc := range rc.procs {
		proc.status.State = rc.status
	}

	return nil
}

func (rc *RunControl) doStart(ctx context.Context) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.msg.Infof("/start processes...")

	err := rc.broadcast(ctx, CmdStart)
	if err != nil {
		rc.status = fsm.Error
		return err
	}

	rc.status = fsm.Running
	for _, proc := range rc.procs {
		proc.status.State = rc.status
	}

	return nil
}

func (rc *RunControl) doStop(ctx context.Context) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.msg.Infof("/stop processes...")

	err := rc.broadcast(ctx, CmdStop)
	if err != nil {
		rc.status = fsm.Error
		return err
	}

	rc.status = fsm.Stopped
	for _, proc := range rc.procs {
		proc.status.State = rc.status
	}

	return nil
}

func (rc *RunControl) doTerm(ctx context.Context) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.msg.Infof("/term processes...")
	defer close(rc.quit)

	err := rc.broadcast(ctx, CmdTerm)
	if err != nil {
		rc.status = fsm.Error
		return err
	}

	rc.status = fsm.Exiting
	return nil
}

func (rc *RunControl) doStatus(ctx context.Context) error {
	rc.msg.Infof("/status processes...")

	rc.mu.RLock()
	procs := make([]string, 0, len(rc.procs))
	for name := range rc.procs {
		procs = append(procs, name)
	}
	rc.mu.RUnlock()

	var grp errgroup.Group
	for i := range procs {
		rc.mu.RLock()
		proc := rc.procs[procs[i]]
		rc.mu.RUnlock()
		cmd := StatusCmd{Name: proc.name}
		grp.Go(func() error {
			err := SendCmd(ctx, proc.cmd, &cmd)
			if err != nil {
				rc.msg.Errorf("could not send /status to %q: %+v", proc.name, err)
				return err
			}

			ack, err := RecvFrame(ctx, proc.cmd)
			if err != nil {
				rc.msg.Errorf("could not receive /status ACK from %q: %+v", proc.name, err)
				return err
			}
			switch ack.Type {
			case FrameCmd:
				cmd, err := newStatusCmd(ack)
				if err != nil {
					rc.msg.Errorf("could not receive /status reply for %q: %+v", proc.name, err)
					return xerrors.Errorf("could not receive /status reply for %q: %w", proc.name, err)
				}
				rc.mu.Lock()
				proc.status.State = cmd.Status
				rc.mu.Unlock()
				rc.msg.Infof("received /status = %v for %q", cmd.Status, proc.name)

			default:
				rc.msg.Errorf("received invalid frame type %v from %q", ack.Type, proc.name)
				return xerrors.Errorf("received invalid frame type %v from %q", ack.Type, proc.name)
			}
			return nil
		})
	}

	err := grp.Wait()
	if err != nil {
		return xerrors.Errorf("failed to run /status errgroup: %w", err)
	}

	return nil
}

func (rc *RunControl) doHeartbeat(ctx context.Context) error {
	rc.mu.RLock()
	procs := make([]string, 0, len(rc.procs))
	for name := range rc.procs {
		if rc.procs[name].hbeat == nil {
			// no heartbeat yet
			continue
		}
		procs = append(procs, name)
	}
	rc.mu.RUnlock()

	var grp errgroup.Group
	for i := range procs {
		rc.mu.RLock()
		proc := rc.procs[procs[i]]
		rc.mu.RUnlock()
		cmd := StatusCmd{Name: proc.name}
		grp.Go(func() error {
			err := SendCmd(ctx, proc.hbeat, &cmd)
			if err != nil {
				rc.msg.Errorf("could not send /status heartbeat to %s: %+v", proc.name, err)
				return err
			}

			ack, err := RecvFrame(ctx, proc.hbeat)
			if err != nil {
				rc.msg.Errorf("could not receive ACK: %v", err)
				return err
			}
			switch ack.Type {
			case FrameCmd:
				cmd, err := newStatusCmd(ack)
				if err != nil {
					rc.msg.Errorf("could not receive /status heartbeat reply for %q: %+v", proc.name, err)
					return xerrors.Errorf("could not receive /status heartbeat reply for %q: %w", proc.name, err)
				}
				rc.mu.Lock()
				proc.status.State = cmd.Status
				rc.mu.Unlock()

			default:
				rc.msg.Errorf("received invalid frame type %v from %q", ack.Type, proc.name)
				return xerrors.Errorf("received invalid frame type %v from %q", ack.Type, proc.name)
			}
			return nil
		})
	}

	err := grp.Wait()
	if err != nil {
		return xerrors.Errorf("failed to run /status heartbeat errgroup: %w", err)
	}

	return nil
}

func (rc *RunControl) providerOf(p EndPoint) (string, bool) {
	for _, proc := range rc.procs {
		for _, oport := range proc.oeps {
			if oport.Name == p.Name {
				return oport.Addr, true
			}
		}
	}
	return "", false
}
