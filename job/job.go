// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package job provides a high-level API to create and schedule a pool of run-ctl
// and tdaq servers together.
package job // import "github.com/go-daq/tdaq/job"

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/go-daq/tdaq"
	"github.com/go-daq/tdaq/config"
	"github.com/go-daq/tdaq/internal/iomux"
	"github.com/go-daq/tdaq/log"
	"golang.org/x/sync/errgroup"
)

// Proc describes a tdaq process.
type Proc struct {
	Dev      interface{} // tdaq device value
	Name     string      // name of the process
	Level    log.Level
	Cmds     CmdHandlers    // command handlers
	Inputs   InputHandlers  // input handlers
	Outputs  OutputHandlers // output handlers
	Handlers RunHandlers    // run-handlers
}

// CmdHandlers is a map of tdaq command handlers.
type CmdHandlers map[string]tdaq.CmdHandler

// InputHandlers is a map of tdaq input handlers.
type InputHandlers map[string]tdaq.InputHandler

// OutputHandlers is a map of tdaq output handlers.
type OutputHandlers map[string]tdaq.OutputHandler

// RunHandlers is a collection of tdaq run handlers.
type RunHandlers []tdaq.RunHandler

// App models a complete tdaq application, with a run-ctl and its
// flock of tdaq processes.
type App struct {
	Cfg config.RunCtl

	Timeout time.Duration // timeout for starting the app

	stdout *iomux.Writer
	tmplog bool

	names map[string]struct{} // processes' names
	procs []Proc              // collection of tdaq processes

	rctl *tdaq.RunControl // run-ctl for the tdaq application
	ctx  context.Context  // context of the whole tdaq application
	grp  errgroup.Group   // run-group of the tdaq processes
	errc chan error
}

// New creates a new tdaq application.
//
// No process is started nor scheduled yet and the tdaq application
// configuration can be further customized or modified.
func New(network string, stdout io.Writer) *App {
	var w *iomux.Writer

	if stdout == nil {
		stdout = os.Stdout
	}

	switch stdout := stdout.(type) {
	case *iomux.Writer:
		w = stdout
	default:
		w = iomux.NewWriter(stdout)
	}

	app := &App{
		Cfg: config.RunCtl{
			Name:      "run-ctl",
			Level:     log.LvlInfo,
			Trans:     network,
			RunCtl:    ":44000",
			Web:       ":8080",
			HBeatFreq: 50 * time.Millisecond,
		},
		Timeout: 5 * time.Second,
		stdout:  w,
		names:   make(map[string]struct{}),
		ctx:     context.Background(),
		errc:    make(chan error),
	}

	return app
}

// Add adds a collection of tdaq processes to the tdaq application.
//
// Add panics if duplicate processes (identified by name) are added.
func (app *App) Add(ps ...Proc) {
	for _, p := range ps {
		if _, dup := app.names[p.Name]; dup {
			panic(fmt.Errorf("duplicate process w/ name %q", p.Name))
		}
		if p.Cmds == nil {
			p.Cmds = make(CmdHandlers)
		}

		app.procs = append(app.procs, p)
		app.names[p.Name] = struct{}{}
	}
}

// Start starts the whole tdaq application.
// Start first starts the run-ctl and then all the tdaq processes.
// Start waits for all the tdaq processes to be correctly connected
// to the run-ctl (or bails out if the timeout is reached) before returning
// an error if any.
func (app *App) Start() error {
	if app.Cfg.LogFile == "" {
		f, err := os.CreateTemp("", "tdaq-")
		if err != nil {
			return fmt.Errorf("could not create log-file: %w", err)
		}
		f.Close()
		app.Cfg.LogFile = f.Name()
		app.tmplog = true
	}

	rc, err := tdaq.NewRunControl(app.Cfg, app.stdout)
	if err != nil {
		return fmt.Errorf("could not create run-ctl: %w", err)
	}
	app.rctl = rc

	go func() {
		app.errc <- app.rctl.Run(app.ctx)
	}()

	for i := range app.procs {
		p := app.procs[i]
		cfg := config.Process{
			Name:   p.Name,
			Level:  p.Level,
			Trans:  app.Cfg.Trans,
			RunCtl: app.Cfg.RunCtl,
		}

		srv := tdaq.New(cfg, app.stdout)

		cmd := ""

		cmd = "/config"
		switch h, ok := p.Cmds[cmd]; {
		case ok:
			srv.CmdHandle(cmd, h)
		default:
			if dev, ok := p.Dev.(onConfiger); ok {
				srv.CmdHandle(cmd, dev.OnConfig)
			}
		}

		cmd = "/init"
		switch h, ok := p.Cmds[cmd]; {
		case ok:
			srv.CmdHandle(cmd, h)
		default:
			if dev, ok := p.Dev.(onIniter); ok {
				srv.CmdHandle(cmd, dev.OnInit)
			}
		}

		cmd = "/reset"
		switch h, ok := p.Cmds[cmd]; {
		case ok:
			srv.CmdHandle(cmd, h)
		default:
			if dev, ok := p.Dev.(onReseter); ok {
				srv.CmdHandle(cmd, dev.OnReset)
			}
		}

		cmd = "/start"
		switch h, ok := p.Cmds[cmd]; {
		case ok:
			srv.CmdHandle(cmd, h)
		default:
			if dev, ok := p.Dev.(onStarter); ok {
				srv.CmdHandle(cmd, dev.OnStart)
			}
		}

		cmd = "/stop"
		switch h, ok := p.Cmds[cmd]; {
		case ok:
			srv.CmdHandle(cmd, h)
		default:
			if dev, ok := p.Dev.(onStoper); ok {
				srv.CmdHandle(cmd, dev.OnStop)
			}
		}

		cmd = "/quit"
		switch h, ok := p.Cmds[cmd]; {
		case ok:
			srv.CmdHandle(cmd, h)
		default:
			if dev, ok := p.Dev.(onQuiter); ok {
				srv.CmdHandle(cmd, dev.OnQuit)
			}
		}

		for n, h := range p.Inputs {
			srv.InputHandle(n, h)
		}
		for n, h := range p.Outputs {
			srv.OutputHandle(n, h)
		}
		for _, h := range p.Handlers {
			srv.RunHandle(h)
		}

		app.grp.Go(func() error {
			return srv.Run(app.ctx)
		})
	}

	timeout := time.NewTimer(app.Timeout)
	defer timeout.Stop()

loop:
	for {
		select {
		case <-timeout.C:
			return fmt.Errorf("devices did not connect before timeout (%v)", app.Timeout)
		default:
			n := app.rctl.NumClients()
			if n == len(app.procs) {
				break loop
			}
		}
	}

	return nil
}

// Wait waits for the run-ctl to shut down.
func (app *App) Wait() error {
	err1 := app.grp.Wait()
	err2 := <-app.errc

	if app.tmplog {
		os.Remove(app.Cfg.LogFile)
	}

	if err1 != nil {
		return fmt.Errorf("error while running devices: %w", err1)
	}

	if err2 != nil && !errors.Is(err2, context.Canceled) {
		return fmt.Errorf("error shutting down run-ctl: %w", err2)
	}

	return nil
}

// Do sends the given command to the underlying run-ctl.
func (app *App) Do(ctx context.Context, cmd tdaq.CmdType) error {
	return app.rctl.Do(ctx, cmd)
}

type onConfiger interface {
	OnConfig(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error
}
type onIniter interface {
	OnInit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error
}
type onReseter interface {
	OnReset(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error
}
type onStarter interface {
	OnStart(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error
}
type onStoper interface {
	OnStop(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error
}
type onQuiter interface {
	OnQuit(ctx tdaq.Context, resp *tdaq.Frame, req tdaq.Frame) error
}
