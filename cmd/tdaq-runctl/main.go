// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main // import "github.com/go-daq/tdaq/cmd/tdaq-runctl"

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/go-daq/tdaq"
	"github.com/go-daq/tdaq/config"
	"github.com/go-daq/tdaq/flags"
	"github.com/go-daq/tdaq/log"
	"github.com/peterh/liner"
)

func main() {
	cmd := flags.NewRunControl()

	run(cmd, os.Stdout)
}

func run(cfg config.RunCtl, stdout io.Writer) {
	rc, err := tdaq.NewRunControl(cfg, os.Stdout)
	if err != nil {
		log.Errorf("could not create run control: %+v", err)
		os.Exit(1)
	}

	if cfg.Interactive {
		term := newShell(cfg, rc)
		defer term.Close()
	}

	err = rc.Run(context.Background())
	if err != nil {
		log.Errorf("could not run run-ctl: %+v", err)
		os.Exit(1)
	}
}

func newShell(cfg config.RunCtl, rc *tdaq.RunControl) *liner.State {

	fmt.Printf(`
::::::::::::::::::::::::::
:::  RunControl shell  :::
::::::::::::::::::::::::::

- /config -> configure tdaq processes
- /init   -> initialize tdaq processes
- /run    -> start a new run
- /stop   -> stop current run
- /reset  -> reset tdaq processes
- /status -> display status of all tdaq processes
- /quit   -> terminate tdaq processes (and quit)

`)

	ps1 := cfg.Name + ">> "
	term := liner.NewLiner()
	term.SetWordCompleter(shellCompleter)
	term.SetTabCompletionStyle(liner.TabPrints)

	go func() {
		quit := make(chan struct{})
		ctx := context.Background()
		defer func() {
			select {
			case <-quit:
			default:
				go rc.Do(ctx, tdaq.CmdQuit)
			}
		}()

		for {
			o, err := term.Prompt(ps1)
			if err != nil {
				os.Stdout.Write([]byte("\n"))
				if err != io.EOF {
					log.Fatalf("error: %+v", err)
				}
				return
			}
			words := strings.Split(strings.TrimSpace(o), " ")
			if len(words) == 0 {
				continue
			}
			switch words[0] {
			case "":
				continue
			case "/config":
				term.AppendHistory(o)
				err = rc.Do(ctx, tdaq.CmdConfig)
				if err != nil {
					log.Errorf("could not run /config: %+v", err)
					continue
				}
			case "/init":
				term.AppendHistory(o)
				err = rc.Do(ctx, tdaq.CmdInit)
				if err != nil {
					log.Errorf("could not run /init: %+v", err)
					continue
				}
			case "/reset":
				term.AppendHistory(o)
				err = rc.Do(ctx, tdaq.CmdReset)
				if err != nil {
					log.Errorf("could not run /reset: %+v", err)
					continue
				}
			case "/run":
				term.AppendHistory(o)
				err = rc.Do(ctx, tdaq.CmdStart)
				if err != nil {
					log.Errorf("could not run /start: %+v", err)
					continue
				}
			case "/stop":
				term.AppendHistory(o)
				err = rc.Do(ctx, tdaq.CmdStop)
				if err != nil {
					log.Errorf("could not run /stop: %+v", err)
					continue
				}
			case "/quit":
				term.AppendHistory(o)
				err = rc.Do(ctx, tdaq.CmdQuit)
				if err != nil {
					log.Errorf("could not run /quit: %+v", err)
					continue
				}
				close(quit)
				return
			case "/status":
				term.AppendHistory(o)
				err = rc.Do(ctx, tdaq.CmdStatus)
				if err != nil {
					log.Errorf("could not run /status: %+v", err)
					continue
				}
			default:
				log.Errorf("invalid tdaq command %q", o)
				continue
			}
		}
	}()

	return term
}

func shellCompleter(line string, pos int) (prefix string, completions []string, suffix string) {
	if pos != len(line) {
		// TODO(sbinet): better mid-line matching...
		prefix, completions, suffix = shellCompleter(line[:pos], pos)
		return prefix, completions, suffix + line[pos:]
	}

	if strings.TrimSpace(line) == "" {
		return line, nil, ""
	}

	cmds := []string{
		"/config", "/init", "/reset",
		"/run", "/stop",
		"/quit",
		"/status",
	}

	for _, cmd := range cmds {
		if strings.HasPrefix(cmd, line) {
			completions = append(completions, cmd[pos:])
		}
	}

	return line, completions, ""
}
