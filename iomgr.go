// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq // import "github.com/go-daq/tdaq"

import (
	"bytes"
	"context"
	"net"
	"sort"
	"sync"

	"github.com/go-daq/tdaq/fsm"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

type RunHandler func(ctx Context) error
type CmdHandler func(ctx Context, resp *Frame, req Frame) error
type InputHandler func(ctx Context, src Frame) error
type OutputHandler func(ctx Context, dst *Frame) error

type imgr struct {
	srv *Server
	mu  sync.RWMutex
	ps  map[string]net.Conn
	ep  map[string]InputHandler
	cfg ConfigCmd

	grp  *errgroup.Group
	done chan error
}

func newIMgr(srv *Server) *imgr {
	return &imgr{
		srv: srv,
		ps:  make(map[string]net.Conn),
		ep:  make(map[string]InputHandler),
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
		return xerrors.Errorf("could not retrieve /config cmd: %w", err)
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
	conn, err := net.Dial("tcp", ep.Addr)
	if err != nil {
		return xerrors.Errorf("could not dial %q end-point (ep=%q): %w", ep.Addr, ep.Name, err)
	}
	setupTCPConn(conn.(*net.TCPConn))
	mgr.ps[ep.Name] = conn

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
			ctx.Msg.Errorf("could not close incoming end-point %q: %v", k, err)
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
		mgr.done <- mgr.grp.Wait()
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
		return xerrors.Errorf("on-stop failed: %w", ctx.Ctx.Err())
	}

	return xerrors.Errorf("impossible")
}

func (mgr *imgr) run(ctx Context, ep string, conn net.Conn, f InputHandler) error {
	for {
		select {
		case <-ctx.Ctx.Done():
			return nil
		default:
			frame, err := RecvFrame(ctx.Ctx, conn)
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
					ctx.Msg.Errorf("could not process data frame for %q: %v", ep, err)
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

func (mgr *omgr) endpoints() []EndPoint {
	mgr.mu.RLock()
	defer mgr.mu.RUnlock()

	eps := make([]EndPoint, 0, len(mgr.ep))
	for k := range mgr.ep {
		l, ok := mgr.ps[k]
		if !ok {
			panic(xerrors.Errorf("could not find a listener for end-point %q", k))
		}

		eps = append(eps, EndPoint{
			Name: k,
			Addr: l.l.Addr().String(),
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
			ctx.Msg.Errorf("could not /reset outgoing end-point %q: %v", k, err)
		}
	}

	return err
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
		mgr.done <- mgr.grp.Wait()
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
		return xerrors.Errorf("on-stop failed: %w", ctx.Ctx.Err())
	}

	return xerrors.Errorf("impossible")
}

func (mgr *omgr) run(ctx Context, ep string, op *oport, f OutputHandler) error {
	buf := new(bytes.Buffer)

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
			if err := ctx.Ctx.Err(); err != nil && xerrors.Is(err, context.Canceled) {
				continue
			}

			buf.Reset()
			err = SendFrame(ctx.Ctx, buf, resp)
			if err != nil {
				ctx.Msg.Errorf("could not serialize data frame for %q: %+v", ep, err)
				continue
			}

			err = op.send(buf.Bytes())
			if err != nil {
				switch state := mgr.srv.getNextState(); state {
				case fsm.Stopped:
					// ok
				default:
					ctx.Msg.Errorf("could not send data frame for %q (state=%v): %+v", ep, state, err)
				}
				if err, ok := err.(net.Error); ok && !err.Temporary() {
					return xerrors.Errorf("could not send data frame for %q: %w", ep, err)
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

	if name == "/status" {
		panic(xerrors.Errorf("handle %q is not allowed", name))
	}

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
	name  string
	srv   *Server
	l     net.Listener
	mu    sync.RWMutex
	conns []net.Conn
}

func (o *oport) accept() {
	for {
		conn, err := o.l.Accept()
		if err != nil {
			o.srv.msg.Errorf("could not accept conn for end-point %q: %v", o.name, err)
			if err.(net.Error).Temporary() {
				continue
			}
			return
		}
		setupTCPConn(conn.(*net.TCPConn))

		o.mu.Lock()
		o.conns = append(o.conns, conn)
		o.mu.Unlock()
	}
}

func (o *oport) onReset() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	var err error
	for _, conn := range o.conns {
		e := conn.Close()
		if e != nil {
			err = e
		}
	}
	o.conns = o.conns[:0]

	return err
}

func (o *oport) send(data []byte) error {
	o.mu.RLock()
	defer o.mu.RUnlock()

	var grp errgroup.Group
	for i := range o.conns {
		conn := o.conns[i]
		grp.Go(func() error {
			_, err := conn.Write(data)
			return err
		})
	}
	return grp.Wait()
}
