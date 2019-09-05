// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq // import "github.com/go-daq/tdaq"

import (
	"context"
	"flag"

	"github.com/pkg/errors"
)

type Device interface {
	Run(ctx context.Context) error
}

type OnConfiger interface {
	OnConfig(ctx context.Context, resp *Frame, cmd Cmd) error
}

type OnIniter interface {
	OnInit(ctx context.Context, resp *Frame, cmd Cmd) error
}

type OnReseter interface {
	OnReset(ctx context.Context, resp *Frame, cmd Cmd) error
}

type OnStarter interface {
	OnStart(ctx context.Context, resp *Frame, cmd Cmd) error
}

type OnStoper interface {
	OnStop(ctx context.Context, resp *Frame, cmd Cmd) error
}

type OnTermer interface {
	OnTerm(ctx context.Context, resp *Frame, cmd Cmd) error
}

type OnStatuser interface {
	OnStatus(ctx context.Context, resp *Frame, cmd Cmd) error
}

type OnLoger interface {
	OnLog(ctx context.Context, resp *Frame, cmd Cmd) error
}

// Main configures and runs a device execution, managing its state.
func Main(dev Device) error {
	var (
		rctl = flag.String("runctl", ":44000", "[addr]:port of run-ctl server")
		name = flag.String("id", "", "name of the device")
	)
	flag.Parse()

	p, err := NewProcess(*rctl, *name)
	if err != nil {
		return errors.Wrapf(err, "could not create device %q (type=%T)", *name, dev)
	}
	p.udev = dev

	if dev, ok := dev.(OnConfiger); ok {
		p.handler.onConfig = dev.OnConfig
	}

	if dev, ok := dev.(OnIniter); ok {
		p.handler.onInit = dev.OnInit
	}

	if dev, ok := dev.(OnReseter); ok {
		p.handler.onReset = dev.OnReset
	}

	if dev, ok := dev.(OnStarter); ok {
		p.handler.onStart = dev.OnStart
	}

	if dev, ok := dev.(OnStoper); ok {
		p.handler.onStop = dev.OnStop
	}

	if dev, ok := dev.(OnTermer); ok {
		p.handler.onTerm = dev.OnTerm
	}

	if dev, ok := dev.(OnStatuser); ok {
		p.handler.onStatus = dev.OnStatus
	}

	if dev, ok := dev.(OnLoger); ok {
		p.handler.onLog = dev.OnLog
	}

	ctx := context.Background()
	return p.Run(ctx)
}
