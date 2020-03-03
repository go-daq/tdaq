// Copyright 2020 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq // import "github.com/go-daq/tdaq"

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/go-daq/tdaq/fsm"
	"github.com/go-daq/tdaq/internal/iomux"
	"github.com/go-daq/tdaq/log"
	"go.nanomsg.org/mangos/v3"
)

// client is a proxy to a tdaq server from the RunControl standpoint.
type client struct {
	name string
	addr string
	msg  log.MsgStream

	quit chan int

	mu     sync.RWMutex
	status fsm.Status
	ieps   []EndPoint
	oeps   []EndPoint

	cmd   mangos.Socket
	hbeat mangos.Socket
	log   mangos.Socket
}

func newClient(ctx context.Context, msg log.MsgStream, freq time.Duration, join JoinCmd, ctl, hbeat, log mangos.Socket, msgs chan<- MsgFrame, flog *iomux.Writer) *client {
	cli := &client{
		name:   join.Name,
		addr:   join.Ctl,
		msg:    msg,
		quit:   make(chan int),
		status: fsm.UnConf,
		ieps:   join.InEndPoints,
		oeps:   join.OutEndPoints,
		cmd:    ctl,
		hbeat:  hbeat,
		log:    log,
	}
	go cli.hbeatLoop(ctx, freq)
	go cli.logLoop(ctx, flog, msgs)
	return cli
}

func (cli *client) getStatus() fsm.Status {
	cli.mu.RLock()
	defer cli.mu.RUnlock()
	return cli.status
}

func (cli *client) setStatus(status fsm.Status) {
	cli.mu.Lock()
	defer cli.mu.Unlock()
	cli.status = status
}

func (cli *client) hbeatLoop(ctx context.Context, freq time.Duration) {
	ticks := time.NewTicker(freq)
	defer ticks.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-cli.quit:
			return
		case <-ticks.C:
			cli.doHBeat(ctx)
		}
	}
}

func (cli *client) logLoop(ctx context.Context, flog *iomux.Writer, msgs chan<- MsgFrame) {
	for {
		frame, err := RecvFrame(ctx, cli.log)
		if err != nil {
			closed := errors.Is(err, mangos.ErrClosed)
			select {
			case <-cli.quit:
				if closed {
					// log-socket was closed.
					// but we are shutting down. no need to report.
					return
				}
			default:
			}
			cli.msg.Errorf("could not receive /log frame from (%s, %s): %+v", cli.name, cli.addr, err)
			if closed {
				return
			}
			continue
		}
		var msg MsgFrame
		err = msg.UnmarshalTDAQ(frame.Body)
		if err != nil {
			cli.msg.Errorf("could not unmarshal /log frame from (%s, %s): %+v", cli.name, cli.addr, err)
		}

		select {
		case msgs <- msg:
		case <-ctx.Done():
			return
		case <-cli.quit:
			return
		default:
			// ok to drop messages.
		}

		_, err = flog.Write([]byte(msg.Msg))
		if err != nil {
			cli.msg.Errorf("could not write msg (from (%s, %s)) to log file: %q\nerror: %+v", cli.name, cli.addr, msg.Msg, err)
		}
	}
}

func (cli *client) doHBeat(ctx context.Context) {
	cmd := StatusCmd{Name: cli.name}
	err := SendCmd(ctx, cli.hbeat, &cmd)
	if err != nil {
		cli.msg.Errorf("could not send /status heartbeat to %s: %+v", cli.name, err)
		return
	}

	ack, err := RecvFrame(ctx, cli.hbeat)
	if err != nil {
		cli.msg.Errorf("could not receive ACK: %+v", err)
		return
	}
	switch ack.Type {
	case FrameCmd:
		cmd, err := newStatusCmd(ack)
		if err != nil {
			cli.msg.Errorf("could not receive /status heartbeat reply for %q: %+v", cli.name, err)
			return
		}
		cli.setStatus(cmd.Status)

	default:
		cli.msg.Errorf("received invalid frame type %v from %q", ack.Type, cli.name)
		return
	}
}

func (cli *client) kill() {
	cli.mu.Lock()
	defer cli.mu.Unlock()
	close(cli.quit)
}

func (cli *client) close() error {
	cli.mu.Lock()
	defer cli.mu.Unlock()

	var (
		err1 = cli.cmd.Close()
		err2 = cli.hbeat.Close()
		err3 = cli.log.Close()
	)

	if err1 != nil {
		return err1
	}

	if err2 != nil {
		return err2
	}

	if err3 != nil {
		return err3
	}

	return nil
}
