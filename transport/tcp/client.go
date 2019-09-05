// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tcp // import "github.com/go-daq/tdaq/transport/tcp"

import (
	"context"
	"io"
	"net"

	"github.com/go-daq/tdaq/iodaq"
	"github.com/go-daq/tdaq/log"
	"github.com/go-daq/tdaq/transport"
	"github.com/pkg/errors"
)

type Client struct {
	evts chan transport.Event
	fct  transport.Func

	conn *transport.Conn
	quit chan struct{}
}

func NewClient(addr string, fct transport.Func) (*Client, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, errors.Wrap(err, "could not create TCP tdaq-client")
	}
	setupTCPConn(conn.(*net.TCPConn))

	if fct == nil {
		fct = transport.DefaultFunc
	}

	cli := &Client{
		evts: make(chan transport.Event, 1024),
		fct:  fct,
		conn: &transport.Conn{
			State: 0,
			Type:  "",
			Name:  "",
			Conn:  conn,
		},
		quit: make(chan struct{}),
	}

	go cli.run()

	return cli, nil
}

func (cli *Client) run() {
	defer cli.conn.Conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go cli.handle(ctx)

	for {
		select {
		case <-cli.quit:
			return
		case evt := <-cli.evts:
			err := cli.fct(ctx, evt)
			if err != nil {
				log.Errorf("could not apply callback on evt %v: %v", evt, err)
			}
		}
	}
}

func (cli *Client) handle(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		data, err := iodaq.Recv(ctx, cli.conn.Conn)
		switch err {
		case nil:
			// ok
		case io.EOF:
			log.Infof("connection EOF: %v", err)
			return

		case io.ErrUnexpectedEOF:
			log.Errorf("connection %v error: %v", cli.conn, err)
			return
		default:
			log.Infof("unknown error on connection %v: %v", cli.conn, err)
		}
		cli.evts <- transport.Event{
			Kind: transport.ReceiveEvent,
			ID:   cli.conn,
			Msg:  data,
		}
	}
}

func (cli *Client) SendMsg(ctx context.Context, id *transport.Conn, msg iodaq.Msg) error {
	if !id.Matches(cli.conn) {
		return nil
	}
	return iodaq.Send(ctx, cli.conn, msg)
}

func (cli *Client) RecvMsg(ctx context.Context, id *transport.Conn) (iodaq.Msg, error) {
	if !id.Matches(cli.conn) {
		return iodaq.Msg{}, nil
	}

	raw, err := iodaq.Recv(ctx, cli.conn)
	if err != nil {
		return iodaq.Msg{}, errors.Wrap(err, "could not receive data from connection")
	}
	return iodaq.Msg{Data: raw}, nil
}

func (cli *Client) Close(ctx context.Context, id *transport.Conn) error {
	panic("not implemented")
}

var (
	_ transport.Transporter = (*Client)(nil)
)
