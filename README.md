# tdaq

[![GitHub release](https://img.shields.io/github/release/go-daq/tdaq.svg)](https://github.com/go-daq/tdaq/releases)
[![Build Status](https://travis-ci.org/go-daq/tdaq.svg?branch=master)](https://travis-ci.org/go-daq/tdaq)
[![codecov](https://codecov.io/gh/go-daq/tdaq/branch/master/graph/badge.svg)](https://codecov.io/gh/go-daq/tdaq)
[![Go Report Card](https://goreportcard.com/badge/github.com/go-daq/tdaq)](https://goreportcard.com/report/github.com/go-daq/tdaq)
[![GoDoc](https://godoc.org/github.com/go-daq/tdaq?status.svg)](https://godoc.org/github.com/go-daq/tdaq)
[![License](https://img.shields.io/badge/License-BSD--3-blue.svg)](https://github.com/go-daq/tdaq/license)

`tdaq` is a toolkit to create distributed DAQ systems, over TCP/IP.

## Installation

```
$> go get github.com/go-daq/tdaq/...
```

## Example

In a terminal, launch the `run-control`:

```
$> tdaq-runctl
tdaq-runctl 
tdaq-runctl          INFO listening on ":44000"...
tdaq-runctl          INFO waiting for commands...
tdaq-runctl          INFO received JOIN from conn 127.0.0.1:49752
tdaq-runctl          INFO   proc: "tdaq-datasrc"
tdaq-runctl          INFO    - outputs:
tdaq-runctl          INFO      - name: "/adc"
tdaq-runctl          INFO        addr: "[::]:38713"
tdaq-runctl          INFO received JOIN from conn 127.0.0.1:49754
tdaq-runctl          INFO   proc: "tdaq-datasink"
tdaq-runctl          INFO    - inputs:
tdaq-runctl          INFO      - name: "/adc"
tdaq-runctl> /config
tdaq-runctl          INFO /config processes...
tdaq-runctl> /init
tdaq-runctl          INFO /init processes...
tdaq-runctl> /start
tdaq-runctl          INFO /start processes...
tdaq-runctl> /stop
tdaq-runctl          INFO /stop processes...
tdaq-runctl> /start
tdaq-runctl          INFO /start processes...
tdaq-runctl> /stop
tdaq-runctl          INFO /stop processes...
tdaq-runctl> /term
tdaq-runctl          INFO /term processes...
```

In a second terminal, launch the `tdaq-datasrc` data producer application:

```
$> tdaq-datasrc
tdaq-datasrc -lvl dbg
tdaq-datasrc         DBG  received /config command...
tdaq-datasrc         DBG  received /init command...
tdaq-datasrc         DBG  received /start command...
tdaq-datasrc         DBG  received /stop command... -> n=57
tdaq-datasrc         DBG  received /start command...
tdaq-datasrc         DBG  received /stop command... -> n=457
tdaq-datasrc         DBG  received "/term" command...
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
tdaq-datasink        DBG  received "/term" command...
```
