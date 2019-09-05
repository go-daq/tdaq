// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq // import "github.com/go-daq/tdaq"

import (
	"bytes"
	"context"
	"io"
	"net"
	"sort"
	"sync"

	"github.com/go-daq/tdaq/log"
	"golang.org/x/xerrors"
)

type CmdHandler func(ctx context.Context, resp *Frame, req Frame) error
type InputHandler func(ctx context.Context, src Frame) error
type OutputHandler func(ctx context.Context, dst *Frame) error

type imgr struct {
	mu sync.RWMutex
	ps map[string]net.Conn
	ep map[string]InputHandler
}

func newIMgr() *imgr {
	return &imgr{
		ps: make(map[string]net.Conn),
		ep: make(map[string]InputHandler),
	}
}

func (mgr *imgr) Handle(name string, h InputHandler) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	_, dup := mgr.ep[name]
	if dup {
		panic(xerrors.Errorf("duplicate input handler for %q", name))
	}

	mgr.ep[name] = h
}

func (mgr *imgr) ports() []Port {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	ps := make([]Port, 0, len(mgr.ep))
	for k := range mgr.ep {
		ps = append(ps, Port{
			Name: k,
			Type: "", // FIXME(sbinet)
		})
	}
	return ps
}

func (mgr *imgr) dial(p Port) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	conn, err := net.Dial("tcp", p.Addr)
	if err != nil {
		return xerrors.Errorf("could not dial %q end-point (ep=%q): %w", p.Addr, p.Name, err)
	}
	setupTCPConn(conn.(*net.TCPConn))
	mgr.ps[p.Name] = conn

	return nil
}

func (mgr *imgr) onReset(ctx context.Context) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	var err error

	for k, conn := range mgr.ps {
		mgr.ps[k] = nil
		if conn == nil {
			continue
		}
		e := conn.Close()
		if e != nil {
			err = e
			log.Errorf("could not close incoming end-point %q: %v", k, err)
			continue
		}
	}

	return err
}

func (mgr *imgr) onStart(ctx context.Context) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	for k := range mgr.ps {
		conn := mgr.ps[k]
		fct := mgr.ep[k]
		go mgr.run(ctx, k, conn, fct)
	}

	return nil
}

func (mgr *imgr) run(ctx context.Context, ep string, conn net.Conn, f InputHandler) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			frame, err := RecvFrame(ctx, conn)
			switch {
			default:
				log.Errorf("could not retrieve data frame for %q: %v", ep, err)
				return
			case xerrors.Is(err, io.EOF):
				return
			case err == nil:
				err = f(ctx, frame)
				if err != nil {
					log.Errorf("could not process data frame for %q: %v", ep, err)
				}
			}
		}
	}
}

type omgr struct {
	mu sync.RWMutex
	ps map[string]*oport
	ep map[string]OutputHandler
}

func newOMgr() *omgr {
	return &omgr{
		ps: make(map[string]*oport),
		ep: make(map[string]OutputHandler),
	}
}

func (mgr *omgr) Handle(name string, h OutputHandler) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	_, dup := mgr.ep[name]
	if dup {
		panic(xerrors.Errorf("duplicate output handler for %q", name))
	}

	mgr.ep[name] = h
}

func (mgr *omgr) init(srv *Server) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	for ep := range mgr.ep {
		l, err := net.Listen("tcp", ":0")
		if err != nil {
			return xerrors.Errorf("could not setup output port %q: %w", ep, err)
		}
		o := &oport{name: ep, l: l, srv: srv}
		go o.accept()
		mgr.ps[ep] = o
	}

	return nil
}

func (mgr *omgr) ports() []Port {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	ps := make([]Port, 0, len(mgr.ep))
	for k := range mgr.ep {
		l, ok := mgr.ps[k]
		if !ok {
			panic(xerrors.Errorf("could not find a listener for end-point %q", k))
		}

		ps = append(ps, Port{
			Name: k,
			Addr: l.l.Addr().String(),
			Type: "", // FIXME(sbinet)
		})
	}

	return ps
}

func (mgr *omgr) onReset(ctx context.Context) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	var err error

	for k, op := range mgr.ps {
		e := op.onReset()
		if e != nil {
			err = e
			log.Errorf("could not /reset outgoing end-point %q: %v", k, err)
		}
	}

	return err
}

func (mgr *omgr) onStart(ctx context.Context) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	for k := range mgr.ps {
		out := mgr.ps[k]
		fct := mgr.ep[k]
		go mgr.run(ctx, k, out, fct)
	}

	return nil
}

func (mgr *omgr) run(ctx context.Context, ep string, op *oport, f OutputHandler) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			var resp Frame
			err := f(ctx, &resp)
			if err != nil {
				log.Errorf("could not process data frame for %q: %v", ep, err)
				continue
			}

			buf := new(bytes.Buffer)
			err = SendFrame(ctx, buf, resp)
			if err != nil {
				log.Errorf("could not serialize data frame for %q: %v", ep, err)
				continue
			}

			err = op.send(buf.Bytes())
			if err != nil {
				log.Errorf("could not send data frame for %q: %v", ep, err)
				if err, ok := err.(net.Error); ok && !err.Temporary() {
					return
				}
				continue
			}
		}
	}
}

type cmdmgr struct {
	mu  sync.RWMutex
	ep  map[string]CmdHandler
	set map[string]struct{} // allowed set of handle names
}

func newCmdMgr(cmds ...string) *cmdmgr {
	set := make(map[string]struct{}, len(cmds))
	for _, cmd := range cmds {
		set[cmd] = struct{}{}
	}
	return &cmdmgr{
		ep:  make(map[string]CmdHandler),
		set: set,
	}
}

func (mgr *cmdmgr) Handle(name string, handler CmdHandler) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	if len(mgr.set) != 0 {
		if _, ok := mgr.set[name]; !ok {
			allowed := make([]string, 0, len(mgr.set))
			for k := range mgr.set {
				allowed = append(allowed, k)
			}
			sort.Strings(allowed)
			panic(xerrors.Errorf("handle %q is not in the allowed set of handles %v", name, allowed))
		}
	}

	_, dup := mgr.ep[name]
	if dup {
		panic(xerrors.Errorf("duplicate cmd handler for %q", name))
	}
	mgr.ep[name] = handler
}

func (mgr *cmdmgr) init() {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	for name := range mgr.set {
		_, ok := mgr.ep[name]
		if ok {
			continue
		}
		hdlr := func(ctx context.Context, resp *Frame, req Frame) error {
			cmd, err := CmdFrom(req)
			if err != nil {
				return err
			}
			log.Debugf("received cmd %v", cmd.Type)
			return nil
		}
		mgr.ep[name] = hdlr
	}
}

func (mgr *cmdmgr) endpoint(name string) (CmdHandler, bool) {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()
	h, ok := mgr.ep[name]
	return h, ok
}
