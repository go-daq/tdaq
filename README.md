# tdaq

[![GitHub release](https://img.shields.io/github/release/go-daq/tdaq.svg)](https://github.com/go-daq/tdaq/releases)
[![Build Status](https://travis-ci.org/go-daq/tdaq.svg?branch=master)](https://travis-ci.org/go-daq/tdaq)
[![codecov](https://codecov.io/gh/go-daq/tdaq/branch/master/graph/badge.svg)](https://codecov.io/gh/go-daq/tdaq)
[![Go Report Card](https://goreportcard.com/badge/github.com/go-daq/tdaq)](https://goreportcard.com/report/github.com/go-daq/tdaq)
[![GoDoc](https://godoc.org/github.com/go-daq/tdaq?status.svg)](https://godoc.org/github.com/go-daq/tdaq)
[![License](https://img.shields.io/badge/License-BSD--3-blue.svg)](https://github.com/go-daq/tdaq/license)
[![DOI](https://zenodo.org/badge/206621458.svg)](https://zenodo.org/badge/latestdoi/206621458)


`tdaq` is a toolkit to create distributed DAQ systems, over TCP/IP.

## Installation

```
$> go get github.com/go-daq/tdaq/...
```

## Example

In a terminal, launch the `run-control`:

```
$> tdaq-runctl -web=:8080 -i -lvl dbg
tdaq-runctl          INFO listening on ":44000"...

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

tdaq-runctl          INFO waiting for commands...
tdaq-runctl>>
tdaq-runctl          INFO starting web run-ctl server on ":8080"...
tdaq-runctl          INFO received /join from conn 127.0.0.1:44066
tdaq-runctl          INFO   proc: "tdaq-datasrc"
tdaq-runctl          INFO    - outputs:
tdaq-runctl          INFO      - name: "/adc"
tdaq-runctl          INFO        addr: "[::]:34373"
tdaq-runctl          INFO received /join from conn 127.0.0.1:44112
tdaq-runctl          INFO   proc: "tdaq-datasink"
tdaq-runctl          INFO    - inputs:
tdaq-runctl          INFO      - name: "/adc"
tdaq-runctl>> /config
tdaq-runctl          INFO /config processes...
tdaq-runctl          DBG  sending /config to "tdaq-datasrc"...
tdaq-runctl          DBG  sending /config to "tdaq-datasink"...
tdaq-runctl          DBG  sending /config to "tdaq-datasrc"... [ok]
tdaq-runctl          DBG  sending /config to "tdaq-datasink"... [ok]
tdaq-runctl>> /init
tdaq-runctl          INFO /init processes...
tdaq-runctl          DBG  sending cmd CmdInit to "tdaq-datasrc"...
tdaq-runctl          DBG  sending cmd CmdInit... [ok]
tdaq-runctl          DBG  sending cmd CmdInit to "tdaq-datasrc"... [ok]
tdaq-runctl          DBG  sending cmd CmdInit to "tdaq-datasink"...
tdaq-runctl          DBG  sending cmd CmdInit... [ok]
tdaq-runctl          DBG  sending cmd CmdInit to "tdaq-datasink"... [ok]
tdaq-runctl>> /run
tdaq-runctl          INFO /start processes...
tdaq-runctl          DBG  sending cmd CmdStart to "tdaq-datasrc"...
tdaq-runctl          DBG  sending cmd CmdStart... [ok]
tdaq-runctl          DBG  sending cmd CmdStart to "tdaq-datasrc"... [ok]
tdaq-runctl          DBG  sending cmd CmdStart to "tdaq-datasink"...
tdaq-runctl          DBG  sending cmd CmdStart... [ok]
tdaq-runctl          DBG  sending cmd CmdStart to "tdaq-datasink"... [ok]
tdaq-runctl>> /stop
tdaq-runctl          INFO /stop processes...
tdaq-runctl          DBG  sending cmd CmdStop to "tdaq-datasrc"...
tdaq-runctl          DBG  sending cmd CmdStop... [ok]
tdaq-runctl          DBG  sending cmd CmdStop to "tdaq-datasrc"... [ok]
tdaq-runctl          DBG  sending cmd CmdStop to "tdaq-datasink"...
tdaq-runctl          DBG  sending cmd CmdStop... [ok]
tdaq-runctl          DBG  sending cmd CmdStop to "tdaq-datasink"... [ok]
tdaq-runctl>> /run
tdaq-runctl          INFO /start processes...
tdaq-runctl          DBG  sending cmd CmdStart to "tdaq-datasrc"...
tdaq-runctl          DBG  sending cmd CmdStart... [ok]
tdaq-runctl          DBG  sending cmd CmdStart to "tdaq-datasrc"... [ok]
tdaq-runctl          DBG  sending cmd CmdStart to "tdaq-datasink"...
tdaq-runctl          DBG  sending cmd CmdStart... [ok]
tdaq-runctl          DBG  sending cmd CmdStart to "tdaq-datasink"... [ok]
tdaq-runctl>> /stop
tdaq-runctl          INFO /stop processes...
tdaq-runctl          DBG  sending cmd CmdStop to "tdaq-datasink"...
tdaq-runctl          DBG  sending cmd CmdStop... [ok]
tdaq-runctl          DBG  sending cmd CmdStop to "tdaq-datasink"... [ok]
tdaq-runctl          DBG  sending cmd CmdStop to "tdaq-datasrc"...
tdaq-runctl          DBG  sending cmd CmdStop... [ok]
tdaq-runctl          DBG  sending cmd CmdStop to "tdaq-datasrc"... [ok]
tdaq-runctl>> /quit
tdaq-runctl          INFO /quit processes...
tdaq-runctl          DBG  sending cmd CmdQuit to "tdaq-datasrc"...
tdaq-runctl          DBG  sending cmd CmdQuit... [ok]
tdaq-runctl          DBG  sending cmd CmdQuit to "tdaq-datasrc"... [ok]
tdaq-runctl          DBG  sending cmd CmdQuit to "tdaq-datasink"...
tdaq-runctl          DBG  sending cmd CmdQuit... [ok]
tdaq-runctl          DBG  sending cmd CmdQuit to "tdaq-datasink"... [ok]
tdaq-runctl          INFO shutting down...
tdaq-runctl          INFO closing...
```

In a second terminal, launch the `tdaq-datasrc` data producer application:

```
$> tdaq-datasrc -lvl dbg
tdaq-datasrc         DBG  received /config command...
tdaq-datasrc         DBG  received /init command...
tdaq-datasrc         DBG  received /start command...
tdaq-datasrc         DBG  received /stop command... -> n=57
tdaq-datasrc         DBG  received /start command...
tdaq-datasrc         DBG  received /stop command... -> n=457
tdaq-datasrc         DBG  received "/quit" command...
```

In a third terminal, launch the `tdaq-datasink` data consumer application:

```
$> tdaq-datasink -lvl dbg
tdaq-datasink        DBG  received /config command...
tdaq-datasink        DBG  received /init command...
tdaq-datasink        DBG  received /start command...
tdaq-datasink        DBG  received /stop command... -> n=57
tdaq-datasink        DBG  received /start command...
tdaq-datasink        DBG  received /stop command... -> n=457
tdaq-datasink        DBG  received "/quit" command...
```

One has also access to a web-based control UI for the run-ctl:

![web-ui](https://github.com/go-daq/tdaq/raw/master/testdata/webui_golden.png)
