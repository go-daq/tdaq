// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tcp // import "github.com/go-daq/tdaq/transport/tcp"

import (
	"context"
	"io"
	"net"
	"sync"

	"github.com/go-daq/tdaq/iodaq"
	"github.com/go-daq/tdaq/log"
	"github.com/go-daq/tdaq/transport"
	"github.com/pkg/errors"
)

type Server struct {
	l    *net.TCPListener
	evts chan transport.Event
	fct  transport.Func

	mu    sync.RWMutex
	conns []*transport.Conn

	quit chan struct{}
}

func NewServer(addr string, fct transport.Func) (*Server, error) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, errors.Wrap(err, "could not create TCP tdaq-server")
	}

	if fct == nil {
		fct = transport.DefaultFunc
	}

	srv := &Server{
		evts: make(chan transport.Event, 1024),
		fct:  fct,
		l:    l.(*net.TCPListener),
		quit: make(chan struct{}),
	}
	go srv.run()

	return srv, nil
}

func (srv *Server) run() {
	defer srv.l.Close()

	go func() {
		for {
			select {
			case <-srv.quit:
				return
			default:
				conn, err := srv.l.AcceptTCP()
				if err != nil {
					log.Errorf("error accepting a connection: %v", err)
					continue
				}
				setupTCPConn(conn)

				id := &transport.Conn{
					State: 0,
					Type:  "",
					Name:  "",
					Conn:  conn,
				}

				srv.mu.Lock()
				srv.conns = append(srv.conns, id)
				srv.mu.Unlock()
				go srv.handle(id)
			}
		}
	}()

	for {
		select {
		case <-srv.quit:
			return
		case evt := <-srv.evts:
			err := srv.fct(context.Background(), evt)
			if err != nil {
				log.Errorf("could not apply callback on evt %v: %v", evt, err)
			}
		}
	}
}

func (srv *Server) handle(conn *transport.Conn) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srv.evts <- transport.Event{Kind: transport.ConnectEvent, ID: conn}
	defer func() {
		srv.evts <- transport.Event{
			Kind: transport.DisconnectEvent,
			ID:   conn,
		}
	}()

	for {
		data, err := iodaq.Recv(ctx, conn.Conn)
		switch err {
		case io.EOF, io.ErrUnexpectedEOF:
			return
		}
		srv.evts <- transport.Event{
			Kind: transport.ReceiveEvent,
			ID:   conn,
			Msg:  data,
		}
	}
}

func (srv *Server) SendMsg(ctx context.Context, id *transport.Conn, msg iodaq.Msg) error {
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	for _, conn := range srv.conns {
		if !conn.Matches(id) {
			continue
		}
		if conn.State <= 0 {
			continue
		}
		err := iodaq.Send(ctx, conn, msg)
		if err != nil {
			return errors.Wrapf(err, "could not send message to %v", conn)
		}
	}
	return nil
}

func (srv *Server) RecvMsg(ctx context.Context, id *transport.Conn) (iodaq.Msg, error) {
	panic("not implemented")
}

func (srv *Server) Close(ctx context.Context, id *transport.Conn) error {
	if id == transport.Broadcast {
		close(srv.quit)
	}
	srv.mu.RLock()
	defer srv.mu.RUnlock()
	var err error
	for _, conn := range srv.conns {
		if !conn.Matches(id) {
			continue
		}
		errc := conn.Conn.Close()
		if errc != nil {
			err = errc
			log.Errorf("could not close conn %v: %v", conn, errc)
		}
	}
	return err
}

var (
	_ transport.Transporter = (*Server)(nil)
)
