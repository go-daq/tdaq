// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq // import "github.com/go-daq/tdaq"

import (
	"context"
	"fmt"
	"html/template"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
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

	srv   net.Listener // ctl server
	hbeat net.Listener // hbeat server
	log   net.Listener // log server
	web   *http.Server // web server

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
		fname = fmt.Sprintf("runctl-log-%v.txt", time.Now().UTC().Format("2006-01-150405"))
	}

	flog, err := os.Create(fname)
	if err != nil {
		return nil, xerrors.Errorf("could not create run-ctl log file %q: %w", fname, err)
	}

	stdout = io.MultiWriter(stdout, flog)
	out := newSyncWriter(stdout)

	rc := &RunControl{
		quit:      make(chan struct{}),
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
		rc.log = nil
	}

	if rc.flog != nil {
		err := rc.flog.Close()
		if err != nil {
			rc.msg.Errorf("could not close run-ctl log file: %+v", err)
		}
		rc.flog = nil
	}

	if rc.hbeat != nil {
		err := rc.hbeat.Close()
		if err != nil {
			rc.msg.Errorf("could not close run-ctl heartbeat server: %+v", err)
		}
		rc.hbeat = nil
	}

	if rc.srv != nil {
		err := rc.srv.Close()
		if err != nil {
			rc.msg.Errorf("could not close run-ctl cmd server: %+v", err)
		}
		rc.srv = nil
	}

	if rc.web != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := rc.web.Shutdown(ctx)
		if err != nil {
			rc.msg.Errorf("could not close run-ctl web server: %+v", err)
		}
		rc.web = nil
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

func (rc *RunControl) serveWeb(ctx context.Context) {
	if rc.web == nil {
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	rc.msg.Infof("starting web run-ctl server on %q...", rc.web.Addr)

	err := rc.web.ListenAndServe()
	if err != nil {
		if xerrors.Is(err, http.ErrServerClosed) {
			select {
			case <-rc.quit:
				// ok, we are shutting down.
				return
			default:
			}
		}
		rc.msg.Errorf("error running run-ctl web server: %+v", err)
	}
}

func (rc *RunControl) handleCtlConn(ctx context.Context, conn net.Conn) {
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

	join.HBeat = rc.hbeat.Addr().String()

	err = SendCmd(ctx, conn, &join)
	if err != nil {
		rc.msg.Errorf("could not send /join-ACK to conn %q: %v", join.Name, err)
		return
	}

	rc.mu.Lock()
	rc.procs[join.Name] = &proc{
		name: join.Name,
		status: fsm.Status{
			State: fsm.UnConf,
		},
		ieps: join.InEndPoints,
		oeps: join.OutEndPoints,
		cmd:  conn,
	}
	rc.mu.Unlock()

	err = SendCmd(ctx, conn, &LogCmd{Name: join.Name, Addr: rc.log.Addr().String()})
	if err != nil {
		rc.msg.Errorf("could not send /log cmd to %q: %+v", join.Name, err)
	}

	ack, err := RecvFrame(ctx, conn)
	if err != nil {
		rc.msg.Errorf("could not receive /log cmd ack from %q: %v", join.Name, err)
		sendFrame(ctx, conn, FrameErr, nil, []byte(err.Error()))
		return
	}

	switch ack.Type {
	case FrameOK:
		return // ok
	case FrameErr:
		rc.msg.Errorf("received error /log-ack frame from conn %q: %s", join.Name, string(ack.Body))
		return
	default:
		rc.msg.Errorf("received invalid /log-ack frame from conn %q: %#v", join.Name, ack)
		return
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

func (rc *RunControl) webHome(w http.ResponseWriter, r *http.Request) {
	t, err := template.New("tdaq-home").Parse(webHomePage)
	if err != nil {
		rc.msg.Errorf("error parsing web home-page: %+v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = t.Execute(w, nil)
	if err != nil {
		rc.msg.Errorf("error executing web home-page template: %+v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (rc *RunControl) webCmd(w http.ResponseWriter, r *http.Request) {
	err := r.ParseMultipartForm(500 << 20)
	if err != nil {
		rc.msg.Errorf("could not parse multipart form: %+v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ctx := r.Context()

	cmd := r.PostFormValue("cmd")
	switch cmd {
	case "/config":
		err = rc.doConfig(ctx)
	case "/init":
		err = rc.doInit(ctx)
	case "/start":
		err = rc.doStart(ctx)
	case "/stop":
		err = rc.doStop(ctx)
	case "/reset":
		err = rc.doReset(ctx)
	case "/term":
		err = rc.doTerm(ctx)
	case "/status":
		err = rc.doStatus(ctx)
	default:
		rc.msg.Errorf("received invalid cmd %q over web-gui", cmd)
		err = xerrors.Errorf("received invalid cmd %q", cmd)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err != nil {
		rc.msg.Errorf("could not run cmd %q: %+v", cmd, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	return
}

func (rc *RunControl) webStatus(ws *websocket.Conn) {
	tick := time.NewTicker(1 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-rc.quit:
			return
		case <-tick.C:
			var data struct {
				Status string `json:"status"`
				Procs  []struct {
					Name   string `json:"name"`
					Status string `json:"status"`
				} `json:"procs"`
				Timestamp string `json:"timestamp"`
			}
			rc.mu.RLock()
			data.Status = rc.status.String()
			data.Timestamp = time.Now().UTC().Format("2006-01-02 15:04:05") + " (UTC)"
			for _, proc := range rc.procs {
				data.Procs = append(data.Procs, struct {
					Name   string `json:"name"`
					Status string `json:"status"`
				}{proc.name, proc.status.State.String()})
			}
			rc.mu.RUnlock()
			sort.Slice(data.Procs, func(i, j int) bool {
				return data.Procs[i].Name < data.Procs[j].Name
			})
			err := websocket.JSON.Send(ws, data)
			if err != nil {
				rc.msg.Errorf("could not send /status report to websocket client: %+v", err)
				var nerr net.Error
				if xerrors.As(err, &nerr); nerr != nil && !nerr.Temporary() {
					return
				}
			}
		}
	}
}

func (rc *RunControl) webMsg(ws *websocket.Conn) {
	for {
		select {
		case <-rc.quit:
			return
		case msg := <-rc.msgch:
			var data struct {
				Name      string `json:"name"`
				Level     string `json:"level"`
				Msg       string `json:"msg"`
				Timestamp string `json:"timestamp"`
			}
			data.Name = msg.Name
			data.Level = msg.Level.String()
			data.Msg = msg.Msg
			data.Timestamp = time.Now().UTC().Format("2006-01-02 15:04:05") + " (UTC)"
			err := websocket.JSON.Send(ws, data)
			if err != nil {
				rc.msg.Errorf("could not send /msg report to websocket client: %+v", err)
				var nerr net.Error
				if xerrors.As(err, &nerr); nerr != nil && !nerr.Temporary() {
					return
				}
			}
		}
	}
}

const webHomePage = `<html>
<head>
    <title>TDAQ RunControl</title>

	<meta name="viewport" content="width=device-width, initial-scale=1">
	<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css" />
	<link rel="stylesheet" href="https://www.w3schools.com/w3css/3/w3.css">
	<script src="https://ajax.googleapis.com/ajax/libs/jquery/3.1.1/jquery.min.js"></script>

	<style>
	input[type=submit] {
		background-color: #F44336;
		padding:5px 15px;
		border:0 none;
		cursor:pointer;
		-webkit-border-radius: 5px;
		border-radius: 5px;
	}
	.flex-container {
		display: -webkit-flex;
		display: flex;
	}
	.flex-item {
		margin: 5px;
	}
	.app-file-upload {
		color: white;
		background-color: #0091EA;
		padding:5px 15px;
		border:0 none;
		cursor:pointer;
		-webkit-border-radius: 5px;
	}
	.msg-log {
		color: black;
		text-align: left;
		font-family: monospace;
	}

	.loader {
		border: 16px solid #f3f3f3;
		border-radius: 50%;
		border-top: 16px solid #3498db;
		width: 120px;
		height: 120px;
		-webkit-animation: spin 2s linear infinite; /* Safari */
		animation: spin 2s linear infinite;
	}

	/* Safari */
	@-webkit-keyframes spin {
		0% { -webkit-transform: rotate(0deg); }
		100% { -webkit-transform: rotate(360deg); }
	}

	@keyframes spin {
		0% { transform: rotate(0deg); }
		100% { transform: rotate(360deg); }
	}
	</style>

<script type="text/javascript">
	"use strict"
	
	var statusChan = null;
	var msgChan    = null;

	window.onload = function() {
		statusChan = new WebSocket("ws://"+location.host+"/status");
		
		statusChan.onmessage = function(event) {
			var data = JSON.parse(event.data);
			//console.log("data: "+JSON.stringify(data));
			updateStatus(data);
		};

		msgChan = new WebSocket("ws://"+location.host+"/msg");
		
		msgChan.onmessage = function(event) {
			var data = JSON.parse(event.data);
			//console.log("data: "+JSON.stringify(data));
			updateMsg(data);
		};
	};

	function updateStatus(data) {
		document.getElementById("rc-status").innerHTML = data.status;
		document.getElementById("rc-status-update").innerHTML = data.timestamp;

		var procs = document.getElementById("rc-procs-status");
		procs.innerHTML = "";
		if (data.procs != null) {
			data.procs.forEach(function(value) {
				var node = document.createElement("tr");
				node.innerHTML = "<th class=\"msg-log\">" + value.name +":</th>" +
					"<th class=\"msg-log\">"+value.status+"</th>";
				procs.appendChild(node);
			});
		}
	};

	function updateMsg(data) {
		var msgs = document.getElementById("rc-msg-log");
		msgs.innerText = msgs.innerText + data.timestamp + ": " + data.msg;
	};

	function cmdConfig() { sendCmd("/config"); };
	function cmdInit()   { sendCmd("/init"); };
	function cmdStart()  { sendCmd("/start"); };
	function cmdStop()   { sendCmd("/stop"); };
	function cmdReset()  { sendCmd("/reset"); };

	function cmdTerm()   { sendCmd("/term"); }; // FIXME(sbinet): add confirmation dialog

	function sendCmd(name) {
		var data = new FormData();
		data.append("cmd", name);
		$.ajax({
			url: "/cmd",
			method: "POST",
			data: data,
			processData: false,
			contentType: false,
			success: function(data, status) {
				// FIXME(sbinet): report?
			},
			error: function(e) {
				alert("could not send command ["+name+"]:\n"+e.responseText);
			}
		});
	};

</script>
</head>
<body>

<!-- Sidebar -->
<div id="app-sidebar" class="w3-sidebar w3-bar-block w3-card-4 w3-light-grey" style="width:25%">
	<div class="w3-bar-item w3-card-2 w3-black">
		<h2>TDAQ RunControl</h2>
	</div>
	<div class="w3-bar-item">

		<div>
			<table>
				<tbody>
					<tr>
						<th class="msg-log">RunControl Status:</th>
						<th><div id="rc-status" class="msg-log">N/A</div></th>
					</tr>
				</tbody>
			</table>
		</div>
		<br>

		<input type="button" onclick="cmdConfig()" value="Config">
		<input type="button" onclick="cmdInit()"   value="Init">
		<input type="button" onclick="cmdStart()"  value="Start">
		<input type="button" onclick="cmdStop()"   value="Stop">
		<input type="button" onclick="cmdReset()"  value="Reset">

		<br>
		<br>

		<div>
			<h4> TDAQ Processes:</h4>
			<table>
				<tbody id="rc-procs-status">
				</tbody>
			</table>
		</div>
		<br>

		<input type="button" onclick="cmdTerm()"  value="Terminate">
		<br>

		<span>---</span>
		Last status update:<br><span id="rc-status-update" class="msg-log">N/A</span><br>
		<br>
	</div>
</div>

<!-- Page Content -->
<div style="margin-left:25%; height:100%" class="w3-grey" id="app-container">
	<div class="w3-container w3-content w3-cell w3-cell-middle w3-cell-row w3-center w3-justify w3-grey" style="width:100%" id="app-display">
		<div>
			<pre id="rc-msg-log" class="msg-log"></pre>
		</div>
	</div>
</div>

</body>
</html>
`
