// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main // import "github.com/go-daq/tdaq/cmd/tdaq-runctl"

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/go-daq/tdaq"
	"github.com/go-daq/tdaq/config"
	"github.com/go-daq/tdaq/flags"
	"github.com/go-daq/tdaq/log"
	"github.com/peterh/liner"
)

func main() {
	cmd := flags.NewRunControl()

	var (
		cmds   = make(chan tdaq.CmdType)
		stdout = os.Stdout
	)

	if cmd.Interactive {
		term := newShell(cmd, cmds)
		defer term.Close()
	}

	run(cmd, cmds, stdout)
}

func newShell(cfg config.RunCtl, cmds chan tdaq.CmdType) *liner.State {

	fmt.Printf(`
::::::::::::::::::::::::::
:::  RunControl shell  :::
::::::::::::::::::::::::::

- /config -> configure tdaq processes
- /init   -> initialize tdaq processes
- /start  -> start a new run
- /stop   -> stop current run
- /reset  -> reset tdaq processes
- /term   -> terminate tdaq processes

`)

	ps1 := cfg.Name + ">> "
	term := liner.NewLiner()
	go func() {
		for {
			o, err := term.Prompt(ps1)
			if err != nil {
				os.Stdout.Write([]byte("\n"))
				if err != io.EOF {
					log.Fatalf("error: %+v", err)
				}
				return
			}
			switch o {
			case "":
				continue
			case "/config":
				cmds <- tdaq.CmdConfig
			case "/init":
				cmds <- tdaq.CmdInit
			case "/reset":
				cmds <- tdaq.CmdReset
			case "/start":
				cmds <- tdaq.CmdStart
			case "/stop":
				cmds <- tdaq.CmdStop
			case "/term":
				cmds <- tdaq.CmdTerm
			default:
				log.Errorf("invalid tdaq command %q", o)
				continue
			}
		}
	}()

	return term
}

func run(cfg config.RunCtl, cmds <-chan tdaq.CmdType, stdout io.Writer) {
	rc, err := tdaq.NewRunControl(cfg, cmds, os.Stdout)
	if err != nil {
		log.Errorf("could not create run control: %v", err)
		os.Exit(1)
	}

	err = rc.Run(context.Background())
	if err != nil {
		log.Errorf("could not run run-ctl: %v", err)
		os.Exit(1)
	}
}
