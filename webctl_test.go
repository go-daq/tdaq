// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq_test // import "github.com/go-daq/tdaq"

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-daq/tdaq"
	"github.com/go-daq/tdaq/config"
	"github.com/go-daq/tdaq/fsm"
	"github.com/go-daq/tdaq/internal/tcputil"
	"github.com/go-daq/tdaq/log"
	"github.com/go-daq/tdaq/tdaqio"
	"golang.org/x/net/websocket"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

func TestRunControlWebAPI(t *testing.T) {
	t.Parallel()

	const (
		rclvl   = log.LvlDebug
		proclvl = log.LvlDebug
	)

	port, err := tcputil.GetTCPPort()
	if err != nil {
		t.Fatalf("could not find a tcp port for run-ctl: %+v", err)
	}

	rcAddr := ":" + port

	port, err = tcputil.GetTCPPort()
	if err != nil {
		t.Fatalf("could not find a tcp port for run-ctl web server: %+v", err)
	}
	webAddr := ":" + port

	stdout := new(bytes.Buffer)

	fname, err := ioutil.TempFile("", "tdaq-")
	if err != nil {
		t.Fatalf("could not create a temporary log file for run-ctl log server: %+v", err)
	}
	fname.Close()
	defer func() {
		if err != nil {
			raw, err := ioutil.ReadFile(fname.Name())
			if err == nil {
				t.Logf("log-file:\n%v\n", string(raw))
			}
		}
		os.Remove(fname.Name())
	}()

	cfg := config.RunCtl{
		Name:      "run-ctl",
		Level:     rclvl,
		RunCtl:    rcAddr,
		Web:       webAddr,
		LogFile:   fname.Name(),
		HBeatFreq: 50 * time.Millisecond,
	}

	rc, err := tdaq.NewRunControl(cfg, stdout)
	if err != nil {
		t.Fatalf("could not create run-ctl: %+v", err)
	}
	tsrv := httptest.NewUnstartedServer(rc.Web().(*http.Server).Handler)
	tsrv.Config.ReadTimeout = 5 * time.Second
	tsrv.Config.WriteTimeout = 5 * time.Second
	tsrv.Start()
	defer tsrv.Close()

	cli := tsrv.Client()
	rc.SetWebSrv(newWebSrvTest(tsrv))
	tcli := &testCli{tsrv}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	grp, ctx := errgroup.WithContext(ctx)

	errc := make(chan error)
	go func() {
		errc <- rc.Run(ctx)
	}()

	grp.Go(func() error {
		dev := tdaqio.I64Gen{}

		cfg := config.Process{
			Name:   "data-src",
			Level:  proclvl,
			RunCtl: rcAddr,
		}
		srv := tdaq.New(cfg, ioutil.Discard)
		srv.CmdHandle("/config", dev.OnConfig)
		srv.CmdHandle("/init", dev.OnInit)
		srv.CmdHandle("/reset", dev.OnReset)
		srv.CmdHandle("/start", dev.OnStart)
		srv.CmdHandle("/stop", dev.OnStop)
		srv.CmdHandle("/quit", dev.OnQuit)

		srv.OutputHandle("/i64", dev.Output)

		srv.RunHandle(dev.Loop)

		err := srv.Run(ctx)
		return err
	})

	for _, i := range []int{1, 2, 3} {
		name := fmt.Sprintf("data-sink-%d", i)
		grp.Go(func() error {
			dev := tdaqio.I64Dumper{}

			cfg := config.Process{
				Name:   name,
				Level:  proclvl,
				RunCtl: rcAddr,
			}
			srv := tdaq.New(cfg, ioutil.Discard)
			srv.CmdHandle("/init", dev.OnInit)
			srv.CmdHandle("/reset", dev.OnReset)
			srv.CmdHandle("/stop", dev.OnStop)

			srv.InputHandle("/i64", dev.Input)

			err := srv.Run(context.Background())
			return err
		})
	}

	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
loop:
	for {
		select {
		case <-timeout.C:
			t.Logf("stdout:\n%v\n", stdout.String())
			t.Fatalf("devices did not connect")
		default:
			n := rc.NumClients()
			if n == 4 {
				break loop
			}
		}
	}

	func() {
		req, err := http.NewRequest(http.MethodGet, tsrv.URL+"/", nil)
		if err != nil {
			t.Fatalf("could not create http request for %q: %+v", "/", err)
		}
		resp, err := cli.Do(req)
		if err != nil {
			t.Fatalf("could not get /: %+v", err)
		}
		defer resp.Body.Close()
	}()

	ws, err := newTestWS(tsrv)
	if err != nil {
		t.Fatalf("could not create test websocket: %+v", err)
	}
	defer ws.Close()

	status, err := ws.readStatus()
	if err != nil {
		t.Fatalf("could not get /status: %+v", err)
	}
	if got, want := status, fsm.UnConf.String(); got != want {
		t.Fatalf("invalid status: got=%q, want=%q", got, want)
	}

	func() {
		// test invalid command
		cmd := "invalid-command"
		req, err := tcli.cmd(cmd)
		if err != nil {
			t.Fatalf("could not prepare invalid command /%s: %+v", cmd, err)
		}
		resp, err := cli.Do(req)
		if err != nil {
			t.Fatalf("could not send invalid command /%s: %+v", cmd, err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("invalid status code for invalid command: %v", resp.StatusCode)
		}
	}()

	for _, tt := range []struct {
		name string
		want string
	}{
		{name: "config", want: fsm.Conf.String()},
		{name: "init", want: fsm.Init.String()},
		{name: "reset", want: fsm.UnConf.String()},
		{name: "config", want: fsm.Conf.String()},
		{name: "init", want: fsm.Init.String()},
		{name: "start", want: fsm.Running.String()},
		{name: "stop", want: fsm.Stopped.String()},
		{name: "status", want: fsm.Stopped.String()},
		{name: "start", want: fsm.Running.String()},
		{name: "stop", want: fsm.Stopped.String()},
		{name: "quit", want: fsm.Exiting.String()},
	} {
		req, err := tcli.cmd(tt.name)
		if err != nil {
			t.Fatalf("could not prepare cmd /%s: %+v", tt.name, err)
		}
		func() {
			var resp *http.Response
			resp, err = cli.Do(req)
			if err != nil {
				t.Fatalf("could not send command /%s: %+v", tt.name, err)
			}
			defer resp.Body.Close()

			if tt.name == "quit" {
				return
			}

			status, err := ws.readStatus()
			if err != nil {
				t.Fatalf("could not get /status after /%s: %+v", tt.name, err)
			}

			if got, want := status, tt.want; got != want {
				t.Fatalf("invalid status after /%s: got=%q, want=%q", tt.name, got, want)
			}
		}()
	}

	err = grp.Wait()
	if err != nil {
		t.Logf("stdout:\n%v\n", stdout.String())
		t.Fatalf("could not run device run-group: %+v", err)
	}

	err = <-errc
	if err != nil && !xerrors.Is(err, context.Canceled) {
		t.Logf("stdout:\n%v\n", stdout.String())
		t.Fatalf("error shutting down run-ctl: %+v", err)
	}
}

type websrvTest struct {
	srv  *httptest.Server
	quit chan error
}

func newWebSrvTest(srv *httptest.Server) *websrvTest {
	return &websrvTest{
		srv:  srv,
		quit: make(chan error),
	}
}

func (srv *websrvTest) Close() {
	srv.srv.Close()
}

func (srv *websrvTest) ListenAndServe() error {
	select {
	case <-srv.quit:
		return nil
	}
}

func (srv *websrvTest) Shutdown(ctx context.Context) error {
	close(srv.quit)
	return nil
}

type testCli struct {
	srv *httptest.Server
}

func (cli *testCli) cmd(name string) (*http.Request, error) {
	buf := new(bytes.Buffer)
	w := multipart.NewWriter(buf)
	err := w.WriteField("cmd", "/"+name)
	if err != nil {
		return nil, xerrors.Errorf("could not create mime/multipart field: %w", err)
	}
	err = w.Close()
	if err != nil {
		return nil, xerrors.Errorf("could not create mime/multipart form: %w", err)
	}

	req, err := http.NewRequest(http.MethodPost, cli.srv.URL+"/cmd", buf)
	if err != nil {
		return nil, xerrors.Errorf("could not create http request for %q: %w", name, err)
	}
	req.Header.Set("Content-Type", w.FormDataContentType())

	return req, nil
}

type testWS struct {
	status *websocket.Conn
	msg    *websocket.Conn
	buf    *bytes.Buffer
}

func newTestWS(tsrv *httptest.Server) (*testWS, error) {
	origin := tsrv.URL + "/"
	urlStatus := "ws://" + strings.Replace(tsrv.URL, "http://", "", 1) + "/status"

	status, err := websocket.Dial(urlStatus, "", origin)
	if err != nil {
		return nil, xerrors.Errorf("could not dial /status websocket: %w", err)
	}

	urlMsg := "ws://" + strings.Replace(tsrv.URL, "http://", "", 1) + "/msg"
	msg, err := websocket.Dial(urlMsg, "", origin)
	if err != nil {
		return nil, xerrors.Errorf("could not dial /status websocket: %w", err)
	}

	buf := new(bytes.Buffer)
	go io.Copy(buf, msg)

	return &testWS{
		status: status,
		msg:    msg,
		buf:    buf,
	}, nil
}

func (ws *testWS) Close() error {
	err1 := ws.status.Close()
	err2 := ws.msg.Close()

	if err1 != nil {
		return xerrors.Errorf("could not close /status websocket: %w", err1)
	}

	if err2 != nil {
		return xerrors.Errorf("could not close /msg websocket: %w", err2)
	}

	return nil
}

func (ws *testWS) readStatus() (string, error) {
	buf := make([]byte, 1024)
	n, err := ws.status.Read(buf)
	if err != nil {
		return "", xerrors.Errorf("could not read /status response: %w", err)
	}

	var data struct {
		Status string `json:"status"`
	}
	err = json.NewDecoder(bytes.NewReader(buf[:n])).Decode(&data)
	if err != nil {
		return "", xerrors.Errorf("could not decode /status JSON response: %w", err)
	}

	return data.Status, nil
}
