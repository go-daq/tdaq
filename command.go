// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq // import "github.com/go-daq/tdaq"

//go:generate stringer -type CmdType -output z_cmdtype_string.go .

import (
	"bytes"
	"context"
	"io"

	"github.com/go-daq/tdaq/fsm"
	"golang.org/x/xerrors"
)

type CmdType byte

const (
	CmdUnknown CmdType = iota
	CmdJoin
	CmdHBeat
	CmdConfig
	CmdInit
	CmdReset
	CmdStart
	CmdStop
	CmdQuit
	CmdStatus
	CmdLog
)

var cmdNames = [...][]byte{
	CmdUnknown: []byte("/unknown"),
	CmdJoin:    []byte("/join"),
	CmdHBeat:   []byte("/hbeat"),
	CmdConfig:  []byte("/config"),
	CmdInit:    []byte("/init"),
	CmdReset:   []byte("/reset"),
	CmdStart:   []byte("/start"),
	CmdStop:    []byte("/stop"),
	CmdQuit:    []byte("/quit"),
	CmdStatus:  []byte("/status"),
	CmdLog:     []byte("/log"),
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
		return Cmd{}, xerrors.Errorf("invalid frame type %v", frame.Type)
	}
	cmd := Cmd{
		Type: CmdType(frame.Body[0]),
		Body: frame.Body[1:],
	}
	return cmd, nil
}

func SendCmd(ctx context.Context, w io.Writer, cmd Cmder) error {
	raw, err := cmd.MarshalTDAQ()
	if err != nil {
		return xerrors.Errorf("could not marshal cmd: %w", err)
	}

	ctype := cmd.CmdType()
	path := cmdTypeToPath(cmd.CmdType())
	return sendFrame(ctx, w, FrameCmd, path, append([]byte{byte(ctype)}, raw...))
}

func sendCmd(ctx context.Context, w io.Writer, ctype CmdType, body []byte) error {
	path := cmdTypeToPath(ctype)
	return sendFrame(ctx, w, FrameCmd, path, append([]byte{byte(ctype)}, body...))
}

func recvCmd(ctx context.Context, r io.Reader) (cmd Cmd, err error) {
	frame, err := RecvFrame(ctx, r)
	if err != nil {
		return cmd, xerrors.Errorf("could not receive TDAQ cmd: %w", err)
	}
	if frame.Type != FrameCmd {
		return cmd, xerrors.Errorf("did not receive a TDAQ cmd")
	}
	return Cmd{Type: CmdType(frame.Body[0]), Body: frame.Body[1:]}, nil
}

type JoinCmd struct {
	Name         string
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
		return cmd, xerrors.Errorf("not a /join cmd: %w", err)
	}

	if raw.Type != CmdJoin {
		return cmd, xerrors.Errorf("not a /join cmd")
	}

	err = cmd.UnmarshalTDAQ(raw.Body)
	return cmd, err
}

func (cmd JoinCmd) CmdType() CmdType { return CmdJoin }

func (cmd JoinCmd) MarshalTDAQ() ([]byte, error) {
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

func (cmd *JoinCmd) UnmarshalTDAQ(p []byte) error {
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

type HBeatCmd struct {
	Name string // name of the TDAQ process
	Addr string // address of the heartbeat server
}

func newHBeatCmd(frame Frame) (HBeatCmd, error) {
	var (
		cmd HBeatCmd
		err error
	)

	raw, err := cmdFrom(frame)
	if err != nil {
		return cmd, xerrors.Errorf("not a /hbeat cmd: %w", err)
	}

	if raw.Type != CmdHBeat {
		return cmd, xerrors.Errorf("not a /hbeat cmd")
	}

	err = cmd.UnmarshalTDAQ(raw.Body)
	return cmd, err
}

func (cmd HBeatCmd) CmdType() CmdType { return CmdHBeat }

func (cmd HBeatCmd) MarshalTDAQ() ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := NewEncoder(buf)
	enc.WriteStr(cmd.Name)
	enc.WriteStr(cmd.Addr)
	return buf.Bytes(), enc.err
}

func (cmd *HBeatCmd) UnmarshalTDAQ(p []byte) error {
	dec := NewDecoder(bytes.NewReader(p))
	cmd.Name = dec.ReadStr()
	cmd.Addr = dec.ReadStr()
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
		return cmd, xerrors.Errorf("not a /config cmd: %w", err)
	}

	if raw.Type != CmdConfig {
		return cmd, xerrors.Errorf("not a /config cmd")
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
	Status fsm.StateKind
}

func newStatusCmd(frame Frame) (StatusCmd, error) {
	var (
		cmd StatusCmd
		err error
	)

	raw, err := cmdFrom(frame)
	if err != nil {
		return cmd, xerrors.Errorf("not a /status cmd: %w", err)
	}

	if raw.Type != CmdStatus {
		return cmd, xerrors.Errorf("not a /status cmd")
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
	cmd.Status = fsm.StateKind(dec.ReadI8())
	return dec.err
}

type LogCmd struct {
	Name string // name of the TDAQ process
	Addr string // address of log server
}

func newLogCmd(frame Frame) (LogCmd, error) {
	var (
		cmd LogCmd
		err error
	)

	raw, err := cmdFrom(frame)
	if err != nil {
		return cmd, xerrors.Errorf("not a /log cmd: %w", err)
	}

	if raw.Type != CmdLog {
		return cmd, xerrors.Errorf("not a /log cmd")
	}

	err = cmd.UnmarshalTDAQ(raw.Body)
	return cmd, err
}

func (cmd LogCmd) CmdType() CmdType { return CmdLog }

func (cmd LogCmd) MarshalTDAQ() ([]byte, error) {
	buf := new(bytes.Buffer)
	enc := NewEncoder(buf)
	enc.WriteStr(cmd.Name)
	enc.WriteStr(cmd.Addr)
	return buf.Bytes(), enc.err
}

func (cmd *LogCmd) UnmarshalTDAQ(p []byte) error {
	dec := NewDecoder(bytes.NewReader(p))
	cmd.Name = dec.ReadStr()
	cmd.Addr = dec.ReadStr()
	return dec.err
}

var (
	_ Cmder       = (*JoinCmd)(nil)
	_ Marshaler   = (*JoinCmd)(nil)
	_ Unmarshaler = (*JoinCmd)(nil)

	_ Cmder       = (*HBeatCmd)(nil)
	_ Marshaler   = (*HBeatCmd)(nil)
	_ Unmarshaler = (*HBeatCmd)(nil)

	_ Cmder       = (*LogCmd)(nil)
	_ Marshaler   = (*LogCmd)(nil)
	_ Unmarshaler = (*LogCmd)(nil)

	_ Cmder       = (*ConfigCmd)(nil)
	_ Marshaler   = (*ConfigCmd)(nil)
	_ Unmarshaler = (*ConfigCmd)(nil)

	_ Cmder       = (*StatusCmd)(nil)
	_ Marshaler   = (*StatusCmd)(nil)
	_ Unmarshaler = (*StatusCmd)(nil)
)
