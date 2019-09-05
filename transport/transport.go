// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package transport // import "github.com/go-daq/tdaq/transport"

import (
	"context"
	"io"
	"net"

	"github.com/go-daq/tdaq/iodaq"
)

type Transporter interface {
	// SendMsg sends data to the remote Transporter value.
	SendMsg(ctx context.Context, conn *Conn, msg iodaq.Msg) error

	// RecvMsg receives data from the remote Transporter value.
	RecvMsg(ctx context.Context, conn *Conn) (iodaq.Msg, error)

	// Close closes the connection to the remote Transporter value.
	Close(ctx context.Context, conn *Conn) error
}

// Conn describes an individual connection information on a Transporter.
type Conn struct {
	State int
	Type  string
	Name  string
	Conn  net.Conn
}

func (c *Conn) IsEnabled() bool { return c.State >= 0 }
func (c *Conn) Matches(o *Conn) bool {
	if o == Broadcast || c == Broadcast {
		return true
	}
	return c.Conn == o.Conn
}

func (c *Conn) Write(p []byte) (int, error) { return c.Conn.Write(p) }
func (c *Conn) Read(p []byte) (int, error)  { return c.Conn.Read(p) }

var (
	// Broadcast is a special Conn value used to send a message
	// to all peers of a Transporter.
	Broadcast = &Conn{Name: "Broadcast"}
)

// Event describes events that occur on a Transporter,
// such as a "connection" or a receipt of data.
type Event struct {
	Kind EventKind // kind of transport event
	ID   *Conn     // ID of the connection
	Msg  []byte    // data in case of a Receive event
}

type EventKind uint8

const (
	InvalidEvent    EventKind = 0
	ConnectEvent    EventKind = 1
	DisconnectEvent EventKind = 2
	ReceiveEvent    EventKind = 3
)

// Func is a callback function type used by Transporters.
type Func func(ctx context.Context, evt Event) error

func DefaultFunc(ctx context.Context, evt Event) error { return nil }

var (
	_ io.Reader = (*Conn)(nil)
	_ io.Writer = (*Conn)(nil)
)
