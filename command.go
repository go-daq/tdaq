// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq // import "github.com/go-daq/tdaq"

//go:generate stringer -type CmdType -output z_cmdtype_string.go .

import (
	"bytes"
	"context"
	"fmt"

	"github.com/go-daq/tdaq/fsm"
)

// CmdType describes the type of a command frame.
type CmdType byte

const (
	CmdUnknown CmdType = iota
	CmdJoin
	CmdConfig
	CmdInit
	CmdReset
	CmdStart
	CmdStop
	CmdQuit
	CmdStatus
)

func (cmd CmdType) String() string {
	switch cmd {
	case CmdUnknown:
		return "/unknown"
	case CmdJoin:
		return "/join"
	case CmdConfig:
		return "/config"
	case CmdInit:
		return "/init"
	case CmdReset:
		return "/reset"
	case CmdStart:
		return "/start"
	case CmdStop:
		return "/stop"
	case CmdQuit:
		return "/quit"
	case CmdStatus:
		return "/status"
	default:
		panic(fmt.Errorf("invalid cmd-type %d", byte(cmd)))
	}
}

var cmdNames = [...][]byte{
	CmdUnknown: []byte(CmdUnknown.String()),
	CmdJoin:    []byte(CmdJoin.String()),
	CmdConfig:  []byte(CmdConfig.String()),
	CmdInit:    []byte(CmdInit.String()),
	CmdReset:   []byte(CmdReset.String()),
	CmdStart:   []byte(CmdStart.String()),
	CmdStop:    []byte(CmdStop.String()),
	CmdQuit:    []byte(CmdQuit.String()),
	CmdStatus:  []byte(CmdStatus.String()),
}

func cmdTypeToPath(cmd CmdType) []byte {
	return cmdNames[cmd]
}

type Cmder interface {
	Marshaler
	Unmarshaler

	CmdType() CmdType
}

type Cmd struct {
	Type CmdType
	Body []byte
}

func cmdFrom(frame Frame) (Cmd, error) {
	if frame.Type != FrameCmd {
		return Cmd{}, fmt.Errorf("invalid frame type %v", frame.Type)
	}
	cmd := Cmd{
		Type: CmdType(frame.Body[0]),
		Body: frame.Body[1:],
	}
	return cmd, nil
}

func SendCmd(ctx context.Context, sck Sender, cmd Cmder) error {
	raw, err := cmd.MarshalTDAQ()
	if err != nil {
		return fmt.Errorf("could not marshal cmd: %w", err)
	}

	ctype := cmd.CmdType()
	path := cmdTypeToPath(cmd.CmdType())
	return sendFrame(ctx, sck, FrameCmd, path, append([]byte{byte(ctype)}, raw...))
}

func sendCmd(ctx context.Context, sck Sender, ctype CmdType, body []byte) error {
	path := cmdTypeToPath(ctype)
	return sendFrame(ctx, sck, FrameCmd, path, append([]byte{byte(ctype)}, body...))
}

type JoinCmd struct {
	Name         string // name of the process placing the /join command
	Ctl          string // address of ctl-REP socket of the process
	HBeat        string // address of hbeat-REP socket of the process
	Log          string // address of log-PUB socket of the process
	InEndPoints  []EndPoint
	OutEndPoints []EndPoint
}

func newJoinCmd(frame Frame) (JoinCmd, error) {
	var (
		cmd JoinCmd
		err error
	)

	raw, err := cmdFrom(frame)
	if err != nil {
		return cmd, fmt.Errorf("not a /join cmd: %w", err)
	}

	if raw.Type != CmdJoin {
		return cmd, fmt.Errorf("not a /join cmd")
	}

	err = cmd.UnmarshalTDAQ(raw.Body)
	return cmd, err
}

func (cmd JoinCmd) CmdType() CmdType { return CmdJoin }

func (cmd JoinCmd) MarshalTDAQ() ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := NewEncoder(buf)
	enc.WriteStr(cmd.Name)
	enc.WriteStr(cmd.Ctl)
	enc.WriteStr(cmd.HBeat)
	enc.WriteStr(cmd.Log)

	enc.WriteI32(int32(len(cmd.InEndPoints)))
	for _, ep := range cmd.InEndPoints {
		enc.WriteStr(ep.Name)
		enc.WriteStr(ep.Addr)
		enc.WriteStr(ep.Type)
	}

	enc.WriteI32(int32(len(cmd.OutEndPoints)))
	for _, ep := range cmd.OutEndPoints {
		enc.WriteStr(ep.Name)
		enc.WriteStr(ep.Addr)
		enc.WriteStr(ep.Type)
	}
	return buf.Bytes(), enc.err
}

func (cmd *JoinCmd) UnmarshalTDAQ(p []byte) error {
	dec := NewDecoder(bytes.NewReader(p))

	cmd.Name = dec.ReadStr()
	cmd.Ctl = dec.ReadStr()
	cmd.HBeat = dec.ReadStr()
	cmd.Log = dec.ReadStr()
	n := int(dec.ReadI32())
	cmd.InEndPoints = make([]EndPoint, n)
	for i := range cmd.InEndPoints {
		ep := &cmd.InEndPoints[i]
		ep.Name = dec.ReadStr()
		ep.Addr = dec.ReadStr()
		ep.Type = dec.ReadStr()
	}

	n = int(dec.ReadI32())
	cmd.OutEndPoints = make([]EndPoint, n)
	for i := range cmd.OutEndPoints {
		ep := &cmd.OutEndPoints[i]
		ep.Name = dec.ReadStr()
		ep.Addr = dec.ReadStr()
		ep.Type = dec.ReadStr()
	}

	return dec.err
}

type ConfigCmd struct {
	Name         string
	InEndPoints  []EndPoint
	OutEndPoints []EndPoint
}

func newConfigCmd(frame Frame) (ConfigCmd, error) {
	var (
		cmd ConfigCmd
		err error
	)

	raw, err := cmdFrom(frame)
	if err != nil {
		return cmd, fmt.Errorf("not a /config cmd: %w", err)
	}

	if raw.Type != CmdConfig {
		return cmd, fmt.Errorf("not a /config cmd")
	}

	err = cmd.UnmarshalTDAQ(raw.Body)
	return cmd, err
}

func (cmd ConfigCmd) CmdType() CmdType { return CmdConfig }

func (cmd ConfigCmd) MarshalTDAQ() ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := NewEncoder(buf)
	enc.WriteStr(cmd.Name)

	enc.WriteI32(int32(len(cmd.InEndPoints)))
	for _, ep := range cmd.InEndPoints {
		enc.WriteStr(ep.Name)
		enc.WriteStr(ep.Addr)
		enc.WriteStr(ep.Type)
	}

	enc.WriteI32(int32(len(cmd.OutEndPoints)))
	for _, ep := range cmd.OutEndPoints {
		enc.WriteStr(ep.Name)
		enc.WriteStr(ep.Addr)
		enc.WriteStr(ep.Type)
	}
	return buf.Bytes(), enc.err
}

func (cmd *ConfigCmd) UnmarshalTDAQ(p []byte) error {
	dec := NewDecoder(bytes.NewReader(p))

	cmd.Name = dec.ReadStr()
	n := int(dec.ReadI32())
	cmd.InEndPoints = make([]EndPoint, n)
	for i := range cmd.InEndPoints {
		ep := &cmd.InEndPoints[i]
		ep.Name = dec.ReadStr()
		ep.Addr = dec.ReadStr()
		ep.Type = dec.ReadStr()
	}

	n = int(dec.ReadI32())
	cmd.OutEndPoints = make([]EndPoint, n)
	for i := range cmd.OutEndPoints {
		ep := &cmd.OutEndPoints[i]
		ep.Name = dec.ReadStr()
		ep.Addr = dec.ReadStr()
		ep.Type = dec.ReadStr()
	}

	return dec.err
}

type StatusCmd struct {
	Name   string
	Status fsm.Status
}

func newStatusCmd(frame Frame) (StatusCmd, error) {
	var (
		cmd StatusCmd
		err error
	)

	raw, err := cmdFrom(frame)
	if err != nil {
		return cmd, fmt.Errorf("not a /status cmd: %w", err)
	}

	if raw.Type != CmdStatus {
		return cmd, fmt.Errorf("not a /status cmd")
	}

	err = cmd.UnmarshalTDAQ(raw.Body)
	return cmd, err
}

func (cmd StatusCmd) CmdType() CmdType { return CmdStatus }

func (cmd StatusCmd) MarshalTDAQ() ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := NewEncoder(buf)
	enc.WriteStr(cmd.Name)
	enc.WriteI8(int8(cmd.Status))
	return buf.Bytes(), enc.err
}

func (cmd *StatusCmd) UnmarshalTDAQ(p []byte) error {
	dec := NewDecoder(bytes.NewReader(p))
	cmd.Name = dec.ReadStr()
	cmd.Status = fsm.Status(dec.ReadI8())
	return dec.err
}

var (
	_ Cmder       = (*JoinCmd)(nil)
	_ Marshaler   = (*JoinCmd)(nil)
	_ Unmarshaler = (*JoinCmd)(nil)

	_ Cmder       = (*ConfigCmd)(nil)
	_ Marshaler   = (*ConfigCmd)(nil)
	_ Unmarshaler = (*ConfigCmd)(nil)

	_ Cmder       = (*StatusCmd)(nil)
	_ Marshaler   = (*StatusCmd)(nil)
	_ Unmarshaler = (*StatusCmd)(nil)
)
