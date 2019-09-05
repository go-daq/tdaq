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
	"golang.org/x/xerrors"
)

type descr struct {
	name   string
	status fsm.Status
	iports []Port
	oports []Port
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
		iports: join.InPorts,
		oports: join.OutPorts,
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
	time.Sleep(5 * time.Second)

	for _, tt := range []struct {
		cmd CmdType
		dt  time.Duration
		f   func(context.Context) error
	}{
		{CmdConfig, 5 * time.Second, rctl.doConfig},
		{CmdInit, 2 * time.Second, rctl.doInit},
		{CmdReset, 5 * time.Second, rctl.doReset},
		{CmdConfig, 5 * time.Second, rctl.doConfig},
		{CmdInit, 2 * time.Second, rctl.doInit},
		{CmdStart, 10 * time.Second, rctl.doStart},
		{CmdStatus, 2 * time.Second, rctl.doStatus},
		{CmdLog, 2 * time.Second, nil},
		{CmdStop, 2 * time.Second, rctl.doStop},
		{CmdStart, 2 * time.Second, rctl.doStart},
		{CmdStop, 10 * time.Second, rctl.doStop},
		{CmdTerm, 2 * time.Second, rctl.doTerm},
	} {
		log.Infof("--- cmd %v...", tt.cmd)
		time.Sleep(tt.dt)
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
	}
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

	for conn, descr := range rc.conns {
		for i := range descr.iports {
			iport := &descr.iports[i]
			provider, ok := rc.providerOf(*iport)
			if !ok {
				return xerrors.Errorf("could not find a provider for input %q for %q", iport.Name, descr.name)
			}
			iport.Addr = provider
		}
		rc.conns[conn] = descr
	}

	var berr []error
	for conn, descr := range rc.conns {
		cmd := ConfigCmd{
			Name:     descr.name,
			InPorts:  descr.iports,
			OutPorts: descr.oports,
		}
		err := SendCmd(ctx, conn, &cmd)
		if err != nil {
			log.Errorf("could not send %v to conn %v (%s): %v", cmd, conn.RemoteAddr(), descr.name, err)
			berr = append(berr, err)
			continue
		}

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

func (rc *RunControl) providerOf(p Port) (string, bool) {
	for _, descr := range rc.conns {
		for _, oport := range descr.oports {
			if oport.Name == p.Name {
				return oport.Addr, true
			}
		}
	}
	return "", false
}

/*
func (rc *RunControl) Initialize(ctx context.Context) error {
	log.Infof("initialize command server...")

	rc.muConns.Lock()
	defer rc.muConns.Unlock()

	for conn, st := range rc.conns {
		switch st.State {
		case fsm.UnInit:
			err := rc.initConn(ctx, conn)
			if err != nil {
				return errors.Wrapf(err, "could not initialize connection %v", conn)
			}
		case fsm.UnConf:
			// ok.
		default:
			return errors.Errorf("tdaq: invalid %v -> %v state transition for conn %v", st.State, "Initialized", conn)
		}
	}

	return nil
}

func (rc *RunControl) initConn(ctx context.Context, conn *transport.Conn) error {
	log.Infof("intializing connection...")
	switch st := rc.conns[conn].State; st {
	case fsm.UnInit, fsm.UnConf:
		// ok
	default:
		return errors.Errorf("tdaq: invalid %v -> %v state transition for conn %v", st, "Initalized", conn)
	}

	var raw []byte // FIXME(sbinet)
	return rc.sendCmd(ctx, conn, coms.Command{Type: coms.Init, Data: raw})
}

func (rc *RunControl) Configure(ctx context.Context) error {
	log.Infof("configuring command server...")

	rc.muConns.Lock()
	defer rc.muConns.Unlock()
	rc.listening = false

	for conn, st := range rc.conns {
		switch st.State {
		default:
			return errors.Errorf("tdaq: invalid %v -> %v state transition for conn %v", st.State, "Configured", conn)
		case fsm.UnConf, fsm.Conf, fsm.Stopped:
			err := rc.configConn(ctx, conn)
			if err != nil {
				return errors.Wrapf(err, "could not configure connection %v", conn)
			}
		}
	}

	return nil
}

func (rc *RunControl) configConn(ctx context.Context, conn *transport.Conn) error {
	log.Infof("configuring connection...")

	switch st := rc.conns[conn].State; st {
	case fsm.UnConf, fsm.Conf, fsm.Stopped:
		// ok
	default:
		return errors.Errorf("tdaq: invalid %v -> %v state transition for conn %v", st, "Configured", conn)
	}

	var raw []byte // FIXME(sbinet)
	return rc.sendCmd(ctx, conn, coms.Command{Type: coms.Config, Data: raw})
}

func (rc *RunControl) Reset(ctx context.Context) error {
	log.Infof("processing Reset command...")
	rc.muConns.Lock()
	rc.listening = true
	rc.muConns.Unlock()
	return rc.sendCmd(ctx, transport.Broadcast, coms.Command{Type: coms.Reset, Data: nil})
}

func (rc *RunControl) StartRun(ctx context.Context) error {
	log.Infof("processing StartRun command for RUN #%v", rc.runNbr)

	runnbr := make([]byte, 8)
	binary.LittleEndian.PutUint64(runnbr, rc.runNbr)
	cmdStart := coms.Command{Type: coms.Start, Data: runnbr}

	conns, err := func() ([]*transport.Conn, error) {
		rc.muConns.RLock()
		defer rc.muConns.RUnlock()
		var conns []*transport.Conn
		for conn, st := range rc.conns {
			switch st.State {
			case fsm.Conf, fsm.Stopped:
				conns = append(conns, conn)

			default:
				return nil, errors.Errorf("tdaq: invalid %v -> %v state transition for conn %v", st.State, "Configured", conn)
			}
		}
		return conns, nil
	}()
	if err != nil {
		return err
	}

	grp1, _ := errgroup.WithContext(ctx)
	for i := range conns {
		conn := conns[i]
		switch conn.Type {
		case "Producer", "DataCollector":
			continue
		}
		grp1.Go(func() error {
			err := rc.sendCmd(ctx, conn, cmdStart)
			if err != nil {
				return errors.Wrapf(err, "could not start connection %v", conn)
			}

			for i := 0; i < 30; i++ {
				rc.muConns.RLock()
				st := rc.conns[conn]
				rc.muConns.RUnlock()
				if st.State == fsm.Running {
					// ok
					return nil
				}
				time.Sleep(100 * time.Millisecond)
			}
			return errors.Errorf("timeout waiting for running status for connection %v", conn)
		})
	}

	err = grp1.Wait()
	if err != nil {
		return errors.Wrap(err, "could not start first batch")
	}

	grp2, _ := errgroup.WithContext(ctx)
	for i := range conns {
		conn := conns[i]
		if conn.Type != "DataCollector" {
			continue
		}
		grp2.Go(func() error {
			err := rc.sendCmd(ctx, conn, cmdStart)
			if err != nil {
				return errors.Wrapf(err, "could not start connection %v", conn)
			}

			for i := 0; i < 30; i++ {
				rc.muConns.RLock()
				st := rc.conns[conn]
				rc.muConns.RUnlock()
				if st.State == fsm.Running {
					// ok
					return nil
				}
				time.Sleep(100 * time.Millisecond)
			}
			return errors.Errorf("timeout waiting for running status for connection %v", conn)
		})
	}

	err = grp2.Wait()
	if err != nil {
		return errors.Wrap(err, "could not start data collectors")
	}

	grp3, _ := errgroup.WithContext(ctx)
	for i := range conns {
		conn := conns[i]
		if conn.Type != "Producer" {
			continue
		}
		grp2.Go(func() error {
			err := rc.sendCmd(ctx, conn, cmdStart)
			if err != nil {
				return errors.Wrapf(err, "could not start connection %v", conn)
			}

			for i := 0; i < 30; i++ {
				rc.muConns.RLock()
				st := rc.conns[conn]
				rc.muConns.RUnlock()
				if st.State == fsm.Running {
					// ok
					return nil
				}
				time.Sleep(100 * time.Millisecond)
			}
			return errors.Errorf("timeout waiting for running status for connection %v", conn)
		})
	}

	err = grp3.Wait()
	if err != nil {
		return errors.Wrap(err, "could not start data producers")
	}

	return nil
}

func (rc *RunControl) StopRun(ctx context.Context) error {
	log.Infof("processing StopRun command for run #%v...", rc.runNbr)

	rc.muConns.Lock()
	rc.listening = true
	rc.muConns.Unlock()

	cmdStop := coms.Command{Type: coms.Stop}

	conns, err := func() ([]*transport.Conn, error) {
		rc.muConns.RLock()
		defer rc.muConns.RUnlock()
		var conns []*transport.Conn
		for conn, st := range rc.conns {
			switch st.State {
			case fsm.Running:
				// ok
				conns = append(conns, conn)

			default:
				return nil, errors.Errorf("tdaq: invalid %v -> %v state transition for conn %v", st.State, "Stopped", conn)
			}
		}
		return conns, nil
	}()
	if err != nil {
		return err
	}

	grp1, _ := errgroup.WithContext(ctx)
	for i := range conns {
		conn := conns[i]
		if conn.Type != "Producer" {
			continue
		}
		grp1.Go(func() error {
			err := rc.sendCmd(ctx, conn, cmdStop)
			if err != nil {
				return errors.Wrapf(err, "could not stop connection %v", conn)
			}
			ticks := time.NewTicker(100 * time.Second)
			defer ticks.Stop()
			timeout := time.NewTimer(60 * time.Second)
			defer timeout.Stop()
			for range ticks.C {
				select {
				case <-timeout.C:
					return errors.Errorf("timeout waiting for stopped status for connection %v", conn)
				default:
					rc.muConns.RLock()
					st := rc.conns[conn]
					rc.muConns.RUnlock()
					if st.State == fsm.Stopped {
						// ok
						return nil
					}
				}
			}
			panic("unreachable")
		})
	}

	if err := grp1.Wait(); err != nil {
		return errors.Wrap(err, "could not stop non-producers")
	}

	grp2, _ := errgroup.WithContext(ctx)
	for i := range conns {
		conn := conns[i]
		if conn.Type != "DataCollector" {
			continue
		}
		grp2.Go(func() error {
			return rc.sendCmd(ctx, conn, cmdStop)
		})
	}

	if err := grp2.Wait(); err != nil {
		return errors.Wrap(err, "could not stop data collectors")
	}

	grp3, _ := errgroup.WithContext(ctx)
	for i := range conns {
		conn := conns[i]
		switch conn.Type {
		case "DataCollector", "Producer":
			continue
		}
		grp3.Go(func() error {
			return rc.sendCmd(ctx, conn, cmdStop)
		})
	}

	if err := grp3.Wait(); err != nil {
		return errors.Wrap(err, "could not stop connections")
	}

	return nil
}

func (rc *RunControl) Terminate(ctx context.Context) error {
	log.Infof("processing Terminate command...")
	defer close(rc.quit)

	rc.muConns.Lock()
	rc.listening = false
	rc.muConns.Unlock()
	err := rc.sendCmd(ctx, transport.Broadcast, coms.Command{Type: coms.Terminate, Data: nil})
	if err != nil {
		return errors.Wrap(err, "could not terminate run control")
	}

	return nil
}

func (rc *RunControl) Exec(ctx context.Context) error {
	rc.quit = make(chan struct{})
	cmdStatus := coms.Command{Type: coms.Status}
	ticks := time.NewTicker(1 * time.Second)
	defer ticks.Stop()
	for {
		select {
		case <-ticks.C:
			err := rc.sendCmd(ctx, transport.Broadcast, cmdStatus)
			if err != nil {
				log.Infof("could not send %v command: %v", cmdStatus.Type, err)
			}
		case <-rc.quit:
			return nil
		}
	}
}

func (rc *RunControl) CmdHandler(ctx context.Context, evt transport.Event) error {
	rc.muConns.Lock()
	defer rc.muConns.Unlock()

	conn := evt.ID
	switch evt.Kind {
	case transport.ConnectEvent:
		switch {
		case rc.listening:
			log.Infof("ACCEPT connection: %v", conn.Name)
			err := rc.sendCmd(ctx, conn, coms.Command{Type: coms.Ok, Data: []byte("RunControl")})
			if err != nil {
				return errors.Wrapf(err, "could not send OK ACK-CONNECT to conn %v", conn)
			}
			rc.conns[conn] = fsm.Status{}
			return nil
		default:
			log.Infof("REFUSE connection: %v", conn.Name)
			err := rc.sendCmd(ctx, conn, coms.Command{Type: coms.Err, Data: []byte("RunControl not accepting new connections")})
			if err != nil {
				return errors.Wrapf(err, "could not send ERR ACK-CONNECT to conn %v", conn)
			}
			err = rc.cmdsrv.Close(ctx, conn)
			if err != nil {
				return errors.Wrapf(err, "could not close connection to %v", conn)
			}
			delete(rc.conns, conn)
			return nil
		}
	case transport.DisconnectEvent:
		delete(rc.conns, conn)
		return nil
	case transport.ReceiveEvent:
		switch conn.State {
		case 0:
			var cmd coms.Command
			err := cmd.UnmarshalTDAQ(evt.Msg)
			if err != nil {
				return errors.Wrapf(err, "could not unmarshal recv event message from conn %v", conn)
			}
			if cmd.Type != coms.Ok {
				return errors.Errorf("invalid command type %v (want %v)", cmd.Type, coms.Ok)
			}
			dec := iodaq.NewDecoder(bytes.NewReader(cmd.Data))
			conn.Type = dec.ReadString()
			conn.Name = dec.ReadString()
			if dec.Err() != nil {
				return errors.Wrapf(dec.Err(), "could not decode recv event message payload from conn %v", conn)
			}
			conn.State = 1 // identified.
			err = rc.sendCmd(ctx, conn, coms.Command{Type: coms.Ok})
			if err != nil {
				return errors.Wrapf(err, "could not send OK ACK-RECV to conn %v", conn)
			}
			// DoConnect(ctx, conn) // FIXME(sbinet)
		default:
			// DoStatus(ctx, conn) // FIXME(sbinet)
		}
	default:
		return errors.Errorf("unknown transport event %v", evt.Kind)
	}

	return nil
}

func (rc *RunControl) sendCmd(ctx context.Context, conn *transport.Conn, cmd coms.Command) error {
	rc.muCmdSrv.Lock()
	defer rc.muCmdSrv.Unlock()

	raw, err := cmd.MarshalTDAQ()
	if err != nil {
		return errors.Wrap(err, "could not marshal command")
	}

	return rc.cmdsrv.SendMsg(ctx, conn, iodaq.Msg{raw})
}
*/
