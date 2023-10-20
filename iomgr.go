// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq // import "github.com/go-daq/tdaq"

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"

	"github.com/go-daq/tdaq/fsm"
	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/pub"
	"go.nanomsg.org/mangos/v3/protocol/xsub"
	"golang.org/x/sync/errgroup"
)

type RunHandler func(ctx Context) error
type CmdHandler func(ctx Context, resp *Frame, req Frame) error
type InputHandler func(ctx Context, src Frame) error
type OutputHandler func(ctx Context, dst *Frame) error

type imgr struct {
	srv *Server
	mu  sync.RWMutex
	ps  map[string]mangos.Socket
	ep  map[string]InputHandler
	cfg ConfigCmd

	grp  *errgroup.Group
	done chan error
}

func newIMgr(srv *Server) *imgr {
	return &imgr{
		srv: srv,
		ps:  make(map[string]mangos.Socket),
		ep:  make(map[string]InputHandler),
	}
}

func (mgr *imgr) close() {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	for _, conn := range mgr.ps {
		_ = conn.Close()
	}
}

func (mgr *imgr) Handle(name string, h InputHandler) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	_, dup := mgr.ep[name]
	if dup {
		panic(fmt.Errorf("duplicate input handler for %q", name))
	}

	mgr.ep[name] = h
}

func (mgr *imgr) endpoints() []EndPoint {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	ps := make([]EndPoint, 0, len(mgr.ep))
	for k := range mgr.ep {
		ps = append(ps, EndPoint{
			Name: k,
			Type: "", // FIXME(sbinet)
		})
	}
	return ps
}

func (mgr *imgr) onConfig(ctx Context, src Frame) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	cmd, err := newConfigCmd(src)
	if err != nil {
		return fmt.Errorf("could not retrieve /config cmd: %w", err)
	}
	mgr.cfg = cmd

	for _, ep := range cmd.InEndPoints {
		err = mgr.dial(ep)
		if err != nil {
			return err
		}
	}

	return nil
}

func (mgr *imgr) dial(ep EndPoint) error {
	sck, err := xsub.NewSocket()
	if err != nil {
		return fmt.Errorf("could not create XSUB socket for ep=%q: %w",
			ep.Name, err,
		)
	}
	err = sck.Dial(ep.Addr)
	if err != nil {
		return fmt.Errorf("could not dial %q end-point (ep=%q): %w", ep.Addr, ep.Name, err)
	}
	mgr.ps[ep.Name] = sck

	return nil
}

func (mgr *imgr) onReset(ctx Context) error {
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
			ctx.Msg.Errorf("could not close incoming end-point %q: %+v", k, err)
			continue
		}
	}

	return err
}

func (mgr *imgr) onStart(ctx Context) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	mgr.done = make(chan error)

	if len(mgr.ps) == 0 {
		close(mgr.done)
		return nil
	}

	mgr.grp = new(errgroup.Group)
	for k := range mgr.ps {
		ept := k
		src := mgr.ps[k]
		fct := mgr.ep[k]
		mgr.grp.Go(func() error {
			return mgr.run(ctx, ept, src, fct)
		})
	}

	go func() {
		err := mgr.grp.Wait()
		if err != nil {
			ctx.Msg.Errorf("error during run: %+v", err)
		}
		mgr.done <- err
	}()

	return nil
}

func (mgr *imgr) onStop(ctx Context) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	select {
	case <-mgr.done:
		return nil

	case <-ctx.Ctx.Done():
		return fmt.Errorf("on-stop failed: %w", ctx.Ctx.Err())
	}
}

func (mgr *imgr) run(ctx Context, ep string, sck mangos.Socket, f InputHandler) error {
	for {
		select {
		case <-ctx.Ctx.Done():
			return nil
		default:
			frame, err := RecvFrame(ctx.Ctx, sck)
			switch {
			default:
				switch state := mgr.srv.getNextState(); state {
				case fsm.Stopped:
					// ok.
				default:
					ctx.Msg.Errorf("could not retrieve data frame for %q (state=%v): %+v", ep, state, err)
				}
				return nil
			case err == nil:
				if frame.Type == FrameEOF {
					// no more data
					return nil
				}

				err = f(ctx, frame)
				if err != nil {
					ctx.Msg.Errorf("could not process data frame for %q: %+v", ep, err)
					continue
				}
			}
		}
	}
}

type omgr struct {
	srv *Server
	mu  sync.RWMutex
	ps  map[string]*oport
	ep  map[string]OutputHandler

	grp  *errgroup.Group
	done chan error
}

func newOMgr(srv *Server) *omgr {
	return &omgr{
		srv: srv,
		ps:  make(map[string]*oport),
		ep:  make(map[string]OutputHandler),
	}
}

func (mgr *omgr) close() {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	for _, op := range mgr.ps {
		op.close()
	}
}

func (mgr *omgr) Handle(name string, h OutputHandler) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	_, dup := mgr.ep[name]
	if dup {
		panic(fmt.Errorf("duplicate output handler for %q", name))
	}

	mgr.ep[name] = h
}

func (mgr *omgr) init(srv *Server) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	return mgr.makeListeners(srv)
}

func (mgr *omgr) makeListeners(srv *Server) error {
	for ep := range mgr.ep {
		sck, lis, err := makeListener(pub.NewSocket, func() string {
			switch p, ok := mgr.ps[ep]; {
			case ok:
				return p.addr // re-use previous run's address
			default:
				return makeAddr(mgr.srv.cfg)
			}
		}())
		if err != nil {
			return fmt.Errorf("could not setup output port %q: %w", ep, err)
		}
		o := &oport{name: ep, addr: lis.Address(), srv: srv, l: lis, pub: sck}
		mgr.ps[ep] = o
	}

	return nil
}

func (mgr *omgr) endpoints() []EndPoint {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	eps := make([]EndPoint, 0, len(mgr.ep))
	for k := range mgr.ep {
		l, ok := mgr.ps[k]
		if !ok {
			panic(fmt.Errorf("could not find a listener for end-point %q", k))
		}

		eps = append(eps, EndPoint{
			Name: k,
			Addr: l.l.Address(),
			Type: "", // FIXME(sbinet)
		})
	}

	return eps
}

func (mgr *omgr) onReset(ctx Context) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	var err error

	for k, op := range mgr.ps {
		e := op.onReset()
		if e != nil {
			err = e
			ctx.Msg.Errorf("could not /reset outgoing end-point %q: %+v", k, err)
		}
	}

	if err != nil {
		return fmt.Errorf("could not /reset outgoing end-points: %w", err)
	}

	return mgr.makeListeners(mgr.srv)
}

func (mgr *omgr) onStart(ctx Context) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	mgr.done = make(chan error)

	if len(mgr.ps) == 0 {
		close(mgr.done)
		return nil
	}

	mgr.grp = new(errgroup.Group)
	for k := range mgr.ps {
		ept := k
		out := mgr.ps[k]
		fct := mgr.ep[k]
		mgr.grp.Go(func() error {
			return mgr.run(ctx, ept, out, fct)
		})
	}

	go func() {
		err := mgr.grp.Wait()
		if err != nil {
			ctx.Msg.Errorf("error during run: %+v", err)
		}
		mgr.done <- err
	}()

	return nil
}

func (mgr *omgr) onStop(ctx Context) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	select {
	case <-mgr.done:
		return nil
	case <-ctx.Ctx.Done():
		return fmt.Errorf("on-stop failed: %w", ctx.Ctx.Err())
	}
}

func (mgr *omgr) run(ctx Context, ep string, op *oport, f OutputHandler) error {
	for {
		select {
		case <-ctx.Ctx.Done():
			// send downstream clients the eof-frame poison pill
			err := op.send(eofFrame)
			if err != nil {
				ctx.Msg.Errorf("could not send eof-frame for %q (state=%v->%v): %+v", ep, mgr.srv.getCurState(), mgr.srv.getNextState(), err)

			}
			return nil
		default:
			resp := Frame{Type: FrameData, Path: ep}
			err := f(ctx, &resp)
			if err != nil {
				ctx.Msg.Errorf("could not process data frame for %q: %+v", ep, err)
				continue
			}
			if err := ctx.Ctx.Err(); err != nil && errors.Is(err, context.Canceled) {
				continue
			}

			msg := resp.encode()
			err = op.send(msg)
			if err != nil {
				switch state := mgr.srv.getNextState(); state {
				case fsm.Stopped:
					// ok
				default:
					ctx.Msg.Errorf("could not send data frame for %q (state=%v): %+v", ep, state, err)
				}
				if err, ok := err.(net.Error); ok && err.Timeout() {
					return fmt.Errorf("could not send data frame for %q: %w", ep, err)
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

func (mgr *cmdmgr) close() {}

func (mgr *cmdmgr) Handle(name string, handler CmdHandler) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	if name == "/status" {
		panic(fmt.Errorf("handle %q is not allowed", name))
	}

	if len(mgr.set) != 0 {
		if _, ok := mgr.set[name]; !ok {
			allowed := make([]string, 0, len(mgr.set))
			for k := range mgr.set {
				allowed = append(allowed, k)
			}
			sort.Strings(allowed)
			panic(fmt.Errorf("handle %q is not in the allowed set of handles %v", name, allowed))
		}
	}

	_, dup := mgr.ep[name]
	if dup {
		panic(fmt.Errorf("duplicate cmd handler for %q", name))
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
		hdlr := func(ctx Context, resp *Frame, req Frame) error {
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

type oport struct {
	name string
	addr string
	srv  *Server
	l    mangos.Listener
	pub  mangos.Socket
}

func (o *oport) close() {
	_ = o.l.Close()
	_ = o.pub.Close()
}

func (o *oport) onReset() error {
	e1 := o.l.Close()
	e2 := o.pub.Close()
	if e1 != nil {
		return e1
	}
	if e2 != nil {
		return e2
	}
	return nil
}

func (o *oport) send(data []byte) error {
	return o.pub.Send(data)
}
