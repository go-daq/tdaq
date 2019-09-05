// Copyright 2019 The go-daq Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package log provides routines for logging messages.
package log // import "github.com/go-daq/tdaq/log"

import (
	"fmt"
	"io"
	"os"
	"strings"

	xerrors "golang.org/x/xerrors"
)

// Level regulates the verbosity level of a component.
type Level int

// Default verbosity levels.
const (
	LvlDebug   Level = -10 // LvlDebug defines the DBG verbosity level
	LvlInfo    Level = 0   // LvlInfo defines the INFO verbosity level
	LvlWarning Level = 10  // LvlWarning defines the WARN verbosity level
	LvlError   Level = 20  // LvlError defines the ERR verbosity level
)

func (lvl Level) msgstring() string {
	switch lvl {
	case LvlDebug:
		return "DBG "
	case LvlInfo:
		return "INFO"
	case LvlWarning:
		return "WARN"
	case LvlError:
		return "ERR "
	}
	panic(xerrors.Errorf("log: invalid log.Level value [%d]", int(lvl)))
}

// String prints the human-readable representation of a Level value.
func (lvl Level) String() string {
	switch lvl {
	case LvlDebug:
		return "DEBUG"
	case LvlInfo:
		return "INFO"
	case LvlWarning:
		return "WARN"
	case LvlError:
		return "ERROR"
	}
	panic(xerrors.Errorf("log: invalid log.Level value [%d]", int(lvl)))
}

// MsgStream provides access to verbosity-defined formated messages, a la fmt.Printf.
type MsgStream interface {
	Debugf(format string, a ...interface{}) (int, error)
	Infof(format string, a ...interface{}) (int, error)
	Warnf(format string, a ...interface{}) (int, error)
	Errorf(format string, a ...interface{}) (int, error)

	Msg(lvl Level, format string, a ...interface{}) (int, error)
}

// WriteSyncer is an io.Writer which can be sync'ed/flushed.
type WriteSyncer interface {
	io.Writer
	Sync() error
}

type msgstream struct {
	lvl Level
	w   WriteSyncer
	n   string
}

var (
	Default = newMsgStream("tdaq", LvlDebug, os.Stdout)
)

// Debugf displays a (formated) DBG message
func Debugf(format string, a ...interface{}) (int, error) {
	return Default.Debugf(format, a...)
}

// Infof displays a (formated) INFO message
func Infof(format string, a ...interface{}) (int, error) {
	return Default.Infof(format, a...)
}

// Warnf displays a (formated) WARN message
func Warnf(format string, a ...interface{}) (int, error) {
	return Default.Warnf(format, a...)
}

// Errorf displays a (formated) ERR message
func Errorf(format string, a ...interface{}) (int, error) {
	return Default.Errorf(format, a...)
}

// Panicf displays a (formated) ERR message and panics.
func Panicf(format string, a ...interface{}) (int, error) {
	Default.Errorf(format, a...)
	panic("tdaq panic")
}

// NewMsgStream creates a new MsgStream value with name name and minimum
// verbosity level lvl.
// This MsgStream will print messages into w.
func NewMsgStream(name string, lvl Level, w WriteSyncer) MsgStream {
	return newMsgStream(name, lvl, w)
}

func newMsgStream(name string, lvl Level, w WriteSyncer) msgstream {
	if w == nil {
		w = os.Stdout
	}

	return msgstream{
		lvl: lvl,
		w:   w,
		n:   fmt.Sprintf("%-20s ", name),
	}
}

// Debugf displays a (formated) DBG message
func (msg msgstream) Debugf(format string, a ...interface{}) (int, error) {
	return msg.Msg(LvlDebug, format, a...)
}

// Infof displays a (formated) INFO message
func (msg msgstream) Infof(format string, a ...interface{}) (int, error) {
	return msg.Msg(LvlInfo, format, a...)
}

// Warnf displays a (formated) WARN message
func (msg msgstream) Warnf(format string, a ...interface{}) (int, error) {
	defer msg.flush()
	return msg.Msg(LvlWarning, format, a...)
}

// Errorf displays a (formated) ERR message
func (msg msgstream) Errorf(format string, a ...interface{}) (int, error) {
	defer msg.flush()
	return msg.Msg(LvlError, format, a...)
}

// Msg displays a (formated) message with level lvl.
func (msg msgstream) Msg(lvl Level, format string, a ...interface{}) (int, error) {
	if lvl < msg.lvl {
		return 0, nil
	}
	eol := ""
	if !strings.HasSuffix(format, "\n") {
		eol = "\n"
	}
	format = msg.n + msg.lvl.msgstring() + " " + format + eol
	return fmt.Fprintf(msg.w, format, a...)
}

func (msg msgstream) flush() error {
	return msg.w.Sync()
}
