// Copyright 2020 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq // import "github.com/go-daq/tdaq"

import (
	"strings"

	"github.com/go-daq/tdaq/config"
	"go.nanomsg.org/mangos/v3"
	"golang.org/x/xerrors"

	_ "go.nanomsg.org/mangos/v3/transport/ipc"
	_ "go.nanomsg.org/mangos/v3/transport/tcp"
)

type addrer interface {
	Addr() string
}

func makeAddr(cfg addrer) string {
	switch cfg := cfg.(type) {
	case config.RunCtl:
		return cfg.Addr()
	case config.Process:
		addr := cfg.Addr()
		switch {
		case strings.HasPrefix(addr, "tcp://"):
			return "tcp://:0"
		case strings.HasPrefix(addr, "unix://"), strings.HasPrefix(addr, "ipc://"):
			panic("unix:// not implemented")
		default:
			panic("scheme [" + addr + "] not implemented")
		}
	default:
		panic(xerrors.Errorf("invalid addrer type %T", cfg))
	}
}

func makeListener(fun func() (mangos.Socket, error), ep string) (mangos.Socket, mangos.Listener, error) {
	sck, err := fun()
	if err != nil {
		return nil, nil, xerrors.Errorf("could not create socket %q: %w", ep, err)
	}

	lis, err := sck.NewListener(ep, nil)
	if err != nil {
		_ = sck.Close()
		return nil, nil, xerrors.Errorf("could not create listener %q: %w", ep, err)
	}

	err = lis.Listen()
	if err != nil {
		_ = lis.Close()
		_ = sck.Close()
		return nil, nil, xerrors.Errorf("could not listen on %q: %w", ep, err)
	}

	return sck, lis, nil
}
