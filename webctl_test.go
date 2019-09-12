// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq // import "github.com/go-daq/tdaq"

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/go-daq/tdaq/config"
	"github.com/go-daq/tdaq/log"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
)

func TestRunControlWebAPI(t *testing.T) {
	// t.Parallel()

	const (
		rclvl   = log.LvlDebug
		proclvl = log.LvlInfo
	)

	port, err := GetTCPPort()
	if err != nil {
		t.Fatalf("could not find a tcp port for run-ctl: %+v", err)
	}

	rcAddr := ":" + port

	port, err = GetTCPPort()
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
		Name:    "run-ctl",
		Level:   rclvl,
		RunCtl:  rcAddr,
		Web:     webAddr,
		LogFile: fname.Name(),
	}

	rc, err := NewRunControl(cfg, stdout)
	if err != nil {
		t.Fatalf("could not create run-ctl: %+v", err)
	}
	tsrv := httptest.NewUnstartedServer(rc.web.(*http.Server).Handler)
	tsrv.Config.ReadTimeout = 5 * time.Second
	tsrv.Config.WriteTimeout = 5 * time.Second
	tsrv.Start()
	defer tsrv.Close()

	cli := tsrv.Client()
	rc.web = newWebSrvTest(tsrv)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	grp, ctx := errgroup.WithContext(ctx)

	errc := make(chan error)
	go func() {
		errc <- rc.Run(ctx)
	}()

	grp.Go(func() error {
		dev := TestProducer{
			Seed: 1234,
		}

		cfg := config.Process{
			Name:   "data-src",
			Level:  proclvl,
			RunCtl: rcAddr,
		}
		srv := New(cfg)
		srv.CmdHandle("/config", dev.OnConfig)
		srv.CmdHandle("/init", dev.OnInit)
		srv.CmdHandle("/reset", dev.OnReset)
		srv.CmdHandle("/start", dev.OnStart)
		srv.CmdHandle("/stop", dev.OnStop)
		srv.CmdHandle("/term", dev.OnTerminate)

		srv.OutputHandle("/adc", dev.ADC)

		srv.RunHandle(dev.Loop)

		err := srv.Run(ctx)
		return err
	})

	for _, i := range []int{1, 2, 3} {
		name := fmt.Sprintf("data-sink-%d", i)
		grp.Go(func() error {
			dev := TestConsumer{}

			cfg := config.Process{
				Name:   name,
				Level:  proclvl,
				RunCtl: rcAddr,
			}
			srv := New(cfg)
			srv.CmdHandle("/init", dev.OnInit)
			srv.CmdHandle("/reset", dev.OnReset)
			srv.CmdHandle("/stop", dev.OnStop)

			srv.InputHandle("/adc", dev.ADC)

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

	for _, tt := range []string{
		"config",
		"init",
		"reset",
		"config",
		"init",
		"start",
		"stop",
		"status",
		"start",
		"stop",
		"term",
	} {
		buf := new(bytes.Buffer)
		w := multipart.NewWriter(buf)
		err := w.WriteField("cmd", "/"+tt)
		if err != nil {
			t.Fatalf("could not create mime/multipart field: %+v", err)
		}
		err = w.Close()
		if err != nil {
			t.Fatalf("could not create mime/multipart form: %+v", err)
		}

		req, err := http.NewRequest(http.MethodPost, tsrv.URL+"/cmd", buf)
		if err != nil {
			t.Fatalf("could not create http request for %q: %+v", tt, err)
		}
		req.Header.Set("Content-Type", w.FormDataContentType())

		func() {
			var resp *http.Response
			resp, err = cli.Do(req)
			if err != nil {
				t.Fatalf("could not send command /%s: %+v", tt, err)
			}
			defer resp.Body.Close()
			io.Copy(os.Stderr, resp.Body)
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
