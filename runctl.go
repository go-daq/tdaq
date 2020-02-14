// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq // import "github.com/go-daq/tdaq"

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/go-daq/tdaq/config"
	"github.com/go-daq/tdaq/fsm"
	"github.com/go-daq/tdaq/internal/dflow"
	"github.com/go-daq/tdaq/internal/iomux"
	"github.com/go-daq/tdaq/log"
	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/rep"
	"go.nanomsg.org/mangos/v3/protocol/req"
	"go.nanomsg.org/mangos/v3/protocol/xsub"
	"golang.org/x/net/websocket"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

type RunControl struct {
	quit chan struct{}
	cfg  config.RunCtl

	srv *ctlsrv // ctl server
	web websrv  // web server

	stdout io.Writer

	mu        sync.RWMutex
	status    fsm.Status
	msg       log.MsgStream
	clients   map[string]*client
	dag       *dflow.Graph // DAG of data dependencies b/w processes
	deps      []string     // dep-ordered list of tdaq processes
	listening bool

	msgch chan MsgFrame // messages from log server
	flog  *iomux.Writer

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
	out := iomux.NewWriter(stdout)

	if cfg.HBeatFreq <= 0 {
		cfg.HBeatFreq = 5 * time.Second
	}

	rc := &RunControl{
		quit:      make(chan struct{}),
		cfg:       cfg,
		stdout:    out,
		status:    fsm.UnConf,
		msg:       log.NewMsgStream(cfg.Name, cfg.Level, out),
		clients:   make(map[string]*client),
		dag:       dflow.New(),
		listening: true,
		runNbr:    uint64(time.Now().UTC().Unix()),
		flog:      iomux.NewWriter(flog),
		msgch:     make(chan MsgFrame, 1024),
	}

	rc.msg.Infof("listening on %q...", cfg.RunCtl)
	rc.srv, err = newCtlSrv(makeAddr(cfg))
	if err != nil {
		return nil, xerrors.Errorf("could not start ctl-srv: %w", err)
	}

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
	n := len(rc.clients)
	rc.mu.RUnlock()
	return n
}

func (rc *RunControl) Run(ctx context.Context) error {
	rc.msg.Infof("waiting for commands...")
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

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
			select {
			case <-rc.quit:
				// ok
			default:
				close(rc.quit)
			}
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

	for _, c := range rc.clients {
		err := c.close()
		if err != nil {
			rc.msg.Errorf("could not close proc to %q: %+v", c.name, err)
		}
		delete(rc.clients, c.name)
	}
	rc.clients = nil

	err := rc.srv.Close()
	if err != nil {
		rc.msg.Errorf("could not close run-ctl cmd server: %+v", err)
	}

	err = rc.flog.Close()
	if err != nil {
		rc.msg.Errorf("could not close run-ctl log file: %+v", err)
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
				rc.msg.Errorf("context errored during run-ctl serve: %+v", err)
			}
			return
		default:
			rc.handleCtlConn(ctx)
		}
	}
}

func (rc *RunControl) handleCtlConn(ctx context.Context) {
	raw, err := RecvFrame(ctx, rc.srv.join)
	if err != nil {
		if xerrors.Is(err, mangos.ErrClosed) {
			select {
			case <-rc.quit:
				return // exiting
			case <-ctx.Done():
				return // exiting.
			default:
			}
		}
		rc.msg.Errorf("could not receive /join cmd: %+v", err)
		_ = sendFrame(ctx, rc.srv.join, FrameErr, nil, []byte(err.Error()))
		return
	}

	join, err := newJoinCmd(raw)
	if err != nil {
		rc.msg.Errorf("could not decode /join cmd: %+v", err)
		_ = sendFrame(ctx, rc.srv.join, FrameErr, nil, []byte(err.Error()))
		return
	}

	rc.msg.Infof("received /join cmd")
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

	rc.mu.Lock()
	defer rc.mu.Unlock()

	err = rc.checkDAG(ctx, join)
	if err != nil {
		rc.msg.Errorf("could not validate /join from %q: %+v", join.Name, err)
		err = SendFrame(ctx, rc.srv.join, Frame{Type: FrameErr, Body: []byte(err.Error())})
		if err != nil {
			rc.msg.Errorf("could not send /join-ack err to %q: %+v", join.Name, err)
		}
		return
	}

	ctl, err := req.NewSocket()
	if err != nil {
		rc.msg.Errorf("could not create /cmd socket for %q: %+v", join.Name, err)
		err = SendFrame(ctx, rc.srv.join, Frame{Type: FrameErr, Body: []byte(err.Error())})
		if err != nil {
			rc.msg.Errorf("could not send /join-ack err to %q: %+v", join.Name, err)
		}
		return
	}

	err = ctl.Dial(join.Ctl)
	if err != nil {
		rc.msg.Errorf("could not dial /cmd socket (%s) for %q: %+v", join.Ctl, join.Name, err)
		err = SendFrame(ctx, rc.srv.join, Frame{Type: FrameErr, Body: []byte(err.Error())})
		if err != nil {
			rc.msg.Errorf("could not send /join-ack err to %q: %+v", join.Name, err)
		}
		return
	}

	log, err := rc.setupLog(join.Name, join.Log)
	if err != nil {
		rc.msg.Errorf("could not setup /log cmd: %+v", err)
		_ = SendFrame(ctx, rc.srv.join, Frame{Type: FrameErr, Body: []byte(err.Error())})
		return
	}

	hbeat, err := rc.setupHBeat(join.Name, join.HBeat)
	if err != nil {
		rc.msg.Errorf("could not setup /hbeat cmd: %+v", err)
		_ = SendFrame(ctx, rc.srv.join, Frame{Type: FrameErr, Body: []byte(err.Error())})
		return
	}

	rc.clients[join.Name] = newClient(
		ctx, rc.msg, rc.cfg.HBeatFreq,
		join,
		ctl, hbeat, log,
		rc.msgch, rc.flog,
	)
	rc.deps = append(rc.deps, join.Name)

	ackOK := Frame{Type: FrameOK}
	err = SendFrame(ctx, rc.srv.join, ackOK)
	if err != nil {
		rc.msg.Errorf("could not send /join-ack to %q: %+v", join.Name, err)
		return
	}
}

func (rc *RunControl) checkDAG(ctx context.Context, cmd JoinCmd) error {
	if rc.dag.Has(cmd.Name) {
		return xerrors.Errorf("duplicate tdaq process with name %q", cmd.Name)
	}

	var (
		in  = make([]string, len(cmd.InEndPoints))
		out = make([]string, len(cmd.OutEndPoints))
	)

	for i, p := range cmd.InEndPoints {
		in[i] = p.Name
	}

	for i, p := range cmd.OutEndPoints {
		out[i] = p.Name
	}

	err := rc.dag.Add(cmd.Name, in, out)
	if err != nil {
		return xerrors.Errorf("could not add process %q to DAG: %w", cmd.Name, err)
	}

	return nil
}

func (rc *RunControl) setupLog(name, client string) (mangos.Socket, error) {
	sck, err := xsub.NewSocket()
	if err != nil {
		return nil, xerrors.Errorf(
			"could not create log-srv socket for client %s: %w",
			name, err,
		)
	}

	err = sck.Dial(client)
	if err != nil {
		return nil, xerrors.Errorf(
			"could not dial log-srv client %s: %w",
			name, err,
		)
	}

	return sck, nil
}

func (rc *RunControl) setupHBeat(name, client string) (mangos.Socket, error) {
	sck, err := req.NewSocket()
	if err != nil {
		return nil, xerrors.Errorf(
			"could not create hbeat-srv socket for client %s: %w",
			name, err,
		)
	}

	err = sck.Dial(client)
	if err != nil {
		return nil, xerrors.Errorf(
			"could not dial hbeat-srv client %s: %w",
			name, err,
		)
	}

	return sck, nil
}

func (rc *RunControl) broadcast(ctx context.Context, cmd CmdType) error {
	var berr []error

	for _, name := range rc.deps {
		cli := rc.clients[name]
		err := sendCmd(ctx, cli.cmd, cmd, nil)
		if err != nil {
			rc.msg.Errorf("could not send cmd %v to %q: %+v", cmd, cli.name, err)
			berr = append(berr, err)
			continue
		}
		ack, err := RecvFrame(ctx, cli.cmd)
		if err != nil {
			rc.msg.Errorf("could not receive %v ACK from %q: %+v", cmd, cli.name, err)
			berr = append(berr, err)
			continue
		}
		switch ack.Type {
		case FrameOK:
			if cmd == CmdQuit {
				cli.kill()
			}
		case FrameErr:
			rc.msg.Errorf("received ERR ACK from %q: %v", cli.name, string(ack.Body))
			berr = append(berr, xerrors.Errorf(string(ack.Body)))
		default:
			rc.msg.Errorf("received invalid frame type %v from %q", ack.Type, cli.name)
			berr = append(berr, xerrors.Errorf("received invalid frame type %v from %q", ack.Type, cli.name))
		}
		rc.msg.Debugf("sending cmd %v to %q... [ok]", cmd, cli.name)
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
	case CmdQuit:
		fct = rc.doQuit
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

	providers := make(map[string]string)
	for _, cli := range rc.clients {
		for _, oport := range cli.oeps {
			providers[oport.Name] = oport.Addr
		}
	}

	clients := make([]string, 0, len(rc.clients))
	for _, cli := range rc.clients {
		for i := range cli.ieps {
			iport := &cli.ieps[i]
			provider, ok := providers[iport.Name]
			if !ok {
				rc.msg.Errorf("could not find a provider for input %q for %q", iport.Name, cli.name)
				return xerrors.Errorf("could not find a provider for input %q for %q", iport.Name, cli.name)
			}
			cli.mu.Lock()
			iport.Addr = provider
			cli.mu.Unlock()
		}
		clients = append(clients, cli.name)
	}

	var grp errgroup.Group
	for i := range clients {
		cli := rc.clients[clients[i]]
		cmd := ConfigCmd{
			Name:         cli.name,
			InEndPoints:  cli.ieps,
			OutEndPoints: cli.oeps,
		}
		grp.Go(func() error {
			rc.msg.Debugf("sending /config to %q...", cli.name)
			err := SendCmd(ctx, cli.cmd, &cmd)
			if err != nil {
				rc.msg.Errorf("could not send /config to %q: %v+", cli.name, err)
				return err
			}

			ack, err := RecvFrame(ctx, cli.cmd)
			if err != nil {
				rc.msg.Errorf("could not receive ACK from %q: %+v", cli.name, err)
				return err
			}
			switch ack.Type {
			case FrameOK:
				// ok
			case FrameErr:
				rc.msg.Errorf("received ERR ACK from %q: %v", cli.name, string(ack.Body))
				return xerrors.Errorf("received ERR ACK from %q: %v", cli.name, string(ack.Body))
			default:
				rc.msg.Errorf("received invalid frame type %v from %q", ack.Type, cli.name)
				return xerrors.Errorf("received invalid frame type %v from %q", ack.Type, cli.name)
			}
			rc.msg.Debugf("sending /config to %q... [ok]", cli.name)
			return nil
		})
	}

	err := grp.Wait()
	if err != nil {
		rc.status = fsm.Error
		return xerrors.Errorf("failed to run errgroup: %w", err)
	}

	rc.status = fsm.Conf
	for _, cli := range rc.clients {
		cli.setStatus(rc.status)
	}

	return nil
}

func (rc *RunControl) doInit(ctx context.Context) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.msg.Infof("/init processes...")

	err := rc.dag.Analyze()
	if err != nil {
		rc.msg.Errorf("could not create DAG: %+v", err)
		return xerrors.Errorf("could not create DAG of data dependencies: %w", err)
	}

	rc.buildDeps()

	err = rc.broadcast(ctx, CmdInit)
	if err != nil {
		rc.status = fsm.Error
		return err
	}

	rc.status = fsm.Init
	for _, cli := range rc.clients {
		cli.setStatus(rc.status)
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
	for _, cli := range rc.clients {
		cli.setStatus(rc.status)
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
	for _, cli := range rc.clients {
		cli.setStatus(rc.status)
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
	for _, cli := range rc.clients {
		cli.setStatus(rc.status)
	}

	return nil
}

func (rc *RunControl) doQuit(ctx context.Context) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.msg.Infof("/quit processes...")
	defer close(rc.quit)

	err := rc.broadcast(ctx, CmdQuit)
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
	clients := make([]string, 0, len(rc.clients))
	for name := range rc.clients {
		clients = append(clients, name)
	}
	rc.mu.RUnlock()

	var grp errgroup.Group
	for i := range clients {
		rc.mu.RLock()
		cli := rc.clients[clients[i]]
		rc.mu.RUnlock()
		cmd := StatusCmd{Name: cli.name}
		grp.Go(func() error {
			err := SendCmd(ctx, cli.cmd, &cmd)
			if err != nil {
				rc.msg.Errorf("could not send /status to %q: %+v", cli.name, err)
				return err
			}

			ack, err := RecvFrame(ctx, cli.cmd)
			if err != nil {
				rc.msg.Errorf("could not receive /status ACK from %q: %+v", cli.name, err)
				return err
			}
			switch ack.Type {
			case FrameCmd:
				cmd, err := newStatusCmd(ack)
				if err != nil {
					rc.msg.Errorf("could not receive /status reply for %q: %+v", cli.name, err)
					return xerrors.Errorf("could not receive /status reply for %q: %w", cli.name, err)
				}
				cli.setStatus(cmd.Status)
				rc.msg.Infof("received /status = %v for %q", cmd.Status, cli.name)

			default:
				rc.msg.Errorf("received invalid frame type %v from %q", ack.Type, cli.name)
				return xerrors.Errorf("received invalid frame type %v from %q", ack.Type, cli.name)
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

func (rc *RunControl) buildDeps() {
	epts := make(map[string]struct{}, len(rc.clients))
	done := make([]string, 0, len(rc.clients))
	todo := make(map[string]struct{})
	for name := range rc.clients {
		todo[name] = struct{}{}
	}

	depsOf := func(name string) int {
		cli := rc.clients[name]
		n := len(cli.ieps)
		for _, ep := range cli.ieps {
			if _, ok := epts[ep.Name]; ok {
				n--
			}
		}
		return n
	}

loop:
	for {
		if len(todo) == 0 {
			break loop
		}
		for name := range todo {
			inputs := depsOf(name)
			if inputs == 0 {
				done = append(done, name)
				delete(todo, name)
				for _, ep := range rc.clients[name].oeps {
					epts[ep.Name] = struct{}{}
				}
			}
		}
	}

	rc.msg.Debugf("deps: %q", done)
	rc.deps = done
}

type ctlsrv struct {
	join mangos.Socket
	lis  mangos.Listener
}

func newCtlSrv(addr string) (*ctlsrv, error) {
	var srv ctlsrv
	sck, err := rep.NewSocket()
	if err != nil {
		return nil, xerrors.Errorf("could not create JOIN socket: %w", err)
	}

	lis, err := sck.NewListener(addr, nil)
	if err != nil {
		_ = sck.Close()
		return nil, xerrors.Errorf("could not create JOIN listener on %s: %w", addr, err)
	}

	err = lis.Listen()
	if err != nil {
		_ = lis.Close()
		_ = sck.Close()
		return nil, xerrors.Errorf("could not JOIN-listen on %s: %w", addr, err)
	}

	srv.join = sck
	srv.lis = lis

	return &srv, nil
}

func (srv *ctlsrv) Close() error {
	e1 := srv.lis.Close()
	e2 := srv.join.Close()
	switch {
	case e1 != nil:
		return e1
	case e2 != nil:
		return e2
	}
	return nil
}
