// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq // import "github.com/go-daq/tdaq"

import (
	"bytes"
	"context"
	"net"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/go-daq/tdaq/log"
)

func GetTCPPort() (string, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return "", err
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return "", err
	}
	defer l.Close()
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port), nil
}

func (rc *RunControl) SetWebSrv(srv websrv) {
	rc.web = srv
}

func (rc *RunControl) Web() websrv {
	return rc.web
}

func TestMsgFrame(t *testing.T) {
	ctx := context.Background()
	for _, tt := range []MsgFrame{
		MsgFrame{Name: "n1", Level: log.LvlDebug, Msg: strings.Repeat("0123456789", 80)},
		MsgFrame{Name: "n2", Level: log.LvlInfo, Msg: strings.Repeat("0123456789", 80)},
		MsgFrame{Name: "n3", Level: log.LvlWarning, Msg: strings.Repeat("0123456789", 80)},
		MsgFrame{Name: "n4", Level: log.LvlError, Msg: strings.Repeat("0123456789", 80)},
	} {
		t.Run(tt.Name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			err := SendMsg(ctx, buf, tt)
			if err != nil {
				t.Fatalf("could not send msg-frame: %+v", err)
			}
			frame, err := RecvFrame(ctx, buf)
			if err != nil {
				t.Fatalf("could not recv msg-frame: %+v", err)
			}
			var got MsgFrame
			err = got.UnmarshalTDAQ(frame.Body)
			if err != nil {
				t.Fatalf("could not unmarshal msg-frame: %+v", err)
			}

			if got, want := got, tt; !reflect.DeepEqual(got, want) {
				t.Fatalf("invalid r/w round-trip for msg-frame:\ngot = %#v\nwant= %#v\n", got, want)
			}
		})
	}
}

func TestFrame(t *testing.T) {
	ctx := context.Background()
	for _, tt := range []struct {
		name  string
		frame Frame
	}{
		{
			name: "unknown",
			frame: Frame{
				Type: FrameUnknown,
			},
		},
		{
			name:  "cmd",
			frame: Frame{Type: FrameCmd},
		},
		{
			name: "data",
			frame: Frame{
				Len:  12,
				Type: FrameData,
				Path: "/adc",
				Body: []byte("ADC DATA"),
			},
		},
		{
			name:  "msg",
			frame: Frame{Type: FrameMsg},
		},
		{
			name:  "ok",
			frame: Frame{Type: FrameOK},
		},
		{
			name:  "err",
			frame: Frame{Type: FrameErr},
		},
		{
			name:  "eof",
			frame: Frame{Type: FrameEOF},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			err := SendFrame(ctx, buf, tt.frame)
			if err != nil {
				t.Fatalf("could not send frame: %+v", err)
			}

			got, err := RecvFrame(ctx, buf)
			if err != nil {
				t.Fatalf("could not recv frame: %+v", err)
			}

			if got, want := got, tt.frame; !reflect.DeepEqual(got, want) {
				t.Fatalf("invalid r/w round-trip for frame %q:\ngot = %#v\nwant= %#v\n", tt.name, got, want)
			}
			if got, want := got.Type.String(), tt.frame.Type.String(); got != want {
				t.Fatalf("invalid frame type: got=%q, want=%q", got, want)
			}
		})
	}
}
