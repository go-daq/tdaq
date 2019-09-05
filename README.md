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
```

In a second terminal, launch the `tdaq-datasrc` data producer application:

```
$> tdaq-datasrc
tdaq                 DBG  received /config command... (data-src)
tdaq                 DBG  received /init command... (data-src)
tdaq                 DBG  received /reset command... (data-src)
tdaq                 DBG  received /config command... (data-src)
tdaq                 DBG  received /init command... (data-src)
tdaq                 DBG  received /start command... (data-src)
tdaq                 DBG  received cmd CmdStatus
tdaq                 DBG  received cmd CmdLog
tdaq                 DBG  run-done
tdaq                 DBG  received /stop command... (data-src) -> n=140
tdaq                 DBG  received /start command... (data-src)
tdaq                 DBG  run-done
tdaq                 DBG  received /stop command... (data-src) -> n=240
tdaq                 DBG  received "/term" command... (data-src)
```

In a third terminal, launch the `tdaq-datasink` data consumer application:

```
$> tdaq-datasink
tdaq                 DBG  received /config command... (data-sink)
tdaq                 DBG  received /init command... (data-sink)
tdaq                 DBG  received /reset command... (data-sink)
tdaq                 DBG  received /config command... (data-sink)
tdaq                 DBG  received /init command... (data-sink)
tdaq                 DBG  received /start command... (data-sink)
tdaq                 DBG  received cmd CmdStatus
tdaq                 DBG  received cmd CmdLog
tdaq                 DBG  received /stop command... (data-sink) -> n=140
tdaq                 DBG  received /start command... (data-sink)
tdaq                 DBG  received /stop command... (data-sink) -> n=240
tdaq                 DBG  received "/term" command... (data-sink)
```
