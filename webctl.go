// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package tdaq // import "github.com/go-daq/tdaq"

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"net"
	"net/http"
	"sort"
	"time"

	"golang.org/x/net/websocket"
)

type websrv interface {
	ListenAndServe() error
	Shutdown(context.Context) error
}

func (rc *RunControl) serveWeb(ctx context.Context) {
	if rc.web == nil {
		return
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	rc.msg.Infof("starting web run-ctl server on %q...", rc.cfg.Web)

	err := rc.web.ListenAndServe()
	if err != nil {
		if errors.Is(err, http.ErrServerClosed) {
			select {
			case <-rc.quit:
				// ok, we are shutting down.
				return
			case <-ctx.Done():
				// ok, we are shutting down.
				return
			default:
			}
		}
		rc.msg.Errorf("error running run-ctl web server: %+v", err)
	}
}

func (rc *RunControl) webHome(w http.ResponseWriter, r *http.Request) {
	t, err := template.New("tdaq-home").Parse(webHomePage)
	if err != nil {
		rc.msg.Errorf("error parsing web home-page: %+v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = t.Execute(w, nil)
	if err != nil {
		rc.msg.Errorf("error executing web home-page template: %+v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (rc *RunControl) webCmd(w http.ResponseWriter, r *http.Request) {
	err := r.ParseMultipartForm(500 << 20)
	if err != nil {
		rc.msg.Errorf("could not parse multipart form: %+v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	ctx := r.Context()

	cmd := r.PostFormValue("cmd")
	switch cmd {
	case "/config":
		err = rc.doConfig(ctx)
	case "/init":
		err = rc.doInit(ctx)
	case "/start":
		err = rc.doStart(ctx)
	case "/stop":
		err = rc.doStop(ctx)
	case "/reset":
		err = rc.doReset(ctx)
	case "/quit":
		err = rc.doQuit(ctx)
	case "/status":
		err = rc.doStatus(ctx)
	default:
		rc.msg.Errorf("received invalid cmd %q over web-gui", cmd)
		err = fmt.Errorf("received invalid cmd %q", cmd)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err != nil {
		rc.msg.Errorf("could not run cmd %q: %+v", cmd, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (rc *RunControl) webStatus(ws *websocket.Conn) {
	defer ws.Close()

	rc.mu.RLock()
	freq := rc.cfg.HBeatFreq
	rc.mu.RUnlock()

	if freq > 1*time.Second {
		freq = 1 * time.Second
	}

	tick := time.NewTicker(freq)
	defer tick.Stop()

	for {
		select {
		case <-rc.quit:
			return
		case <-tick.C:
			var data struct {
				Status string `json:"status"`
				Procs  []struct {
					Name   string `json:"name"`
					Status string `json:"status"`
				} `json:"procs"`
				Timestamp string `json:"timestamp"`
			}
			rc.mu.RLock()
			data.Status = rc.status.String()
			data.Timestamp = time.Now().UTC().Format("2006-01-02 15:04:05") + " (UTC)"
			for _, proc := range rc.clients {
				data.Procs = append(data.Procs, struct {
					Name   string `json:"name"`
					Status string `json:"status"`
				}{proc.name, proc.getStatus().String()})
			}
			rc.mu.RUnlock()
			sort.Slice(data.Procs, func(i, j int) bool {
				return data.Procs[i].Name < data.Procs[j].Name
			})
			err := websocket.JSON.Send(ws, data)
			if err != nil {
				rc.msg.Errorf("could not send /status report to websocket client: %+v", err)
				var nerr net.Error
				if errors.As(err, &nerr); nerr != nil && nerr.Timeout() {
					return
				}
			}
		}
	}
}

func (rc *RunControl) webMsg(ws *websocket.Conn) {
	defer ws.Close()

	for {
		select {
		case <-rc.quit:
			return
		case msg := <-rc.msgch:
			var data struct {
				Name      string `json:"name"`
				Level     string `json:"level"`
				Msg       string `json:"msg"`
				Timestamp string `json:"timestamp"`
			}
			data.Name = msg.Name
			data.Level = msg.Level.String()
			data.Msg = msg.Msg
			data.Timestamp = time.Now().UTC().Format("2006-01-02 15:04:05") + " (UTC)"
			err := websocket.JSON.Send(ws, data)
			if err != nil {
				rc.msg.Errorf("could not send /msg report to websocket client: %+v", err)
				var nerr net.Error
				if errors.As(err, &nerr); nerr != nil && nerr.Timeout() {
					return
				}
			}
		}
	}
}

const webHomePage = `<html>
<head>
    <title>TDAQ RunControl</title>

	<meta name="viewport" content="width=device-width, initial-scale=1">
	<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/4.7.0/css/font-awesome.min.css" />
	<link rel="stylesheet" href="https://www.w3schools.com/w3css/3/w3.css">
	<script src="https://ajax.googleapis.com/ajax/libs/jquery/3.1.1/jquery.min.js"></script>

	<style>
	input[type=submit] {
		background-color: #F44336;
		padding:5px 15px;
		border:0 none;
		cursor:pointer;
		-webkit-border-radius: 5px;
		border-radius: 5px;
	}
	.flex-container {
		display: -webkit-flex;
		display: flex;
	}
	.flex-item {
		margin: 5px;
	}
	.app-file-upload {
		color: white;
		background-color: #0091EA;
		padding:5px 15px;
		border:0 none;
		cursor:pointer;
		-webkit-border-radius: 5px;
	}
	.msg-log {
		color: black;
		text-align: left;
		font-family: monospace;
	}

	.loader {
		border: 16px solid #f3f3f3;
		border-radius: 50%;
		border-top: 16px solid #3498db;
		width: 120px;
		height: 120px;
		-webkit-animation: spin 2s linear infinite; /* Safari */
		animation: spin 2s linear infinite;
	}

	/* Safari */
	@-webkit-keyframes spin {
		0% { -webkit-transform: rotate(0deg); }
		100% { -webkit-transform: rotate(360deg); }
	}

	@keyframes spin {
		0% { transform: rotate(0deg); }
		100% { transform: rotate(360deg); }
	}
	</style>

<script type="text/javascript">
	"use strict"
	
	var statusChan = null;
	var msgChan    = null;

	window.onload = function() {
		statusChan = new WebSocket("ws://"+location.host+"/status");
		
		statusChan.onmessage = function(event) {
			var data = JSON.parse(event.data);
			//console.log("data: "+JSON.stringify(data));
			updateStatus(data);
		};

		msgChan = new WebSocket("ws://"+location.host+"/msg");
		
		msgChan.onmessage = function(event) {
			var data = JSON.parse(event.data);
			//console.log("data: "+JSON.stringify(data));
			updateMsg(data);
		};
	};

	function updateStatus(data) {
		document.getElementById("rc-status").innerHTML = data.status;
		document.getElementById("rc-status-update").innerHTML = data.timestamp;

		var procs = document.getElementById("rc-procs-status");
		procs.innerHTML = "";
		if (data.procs != null) {
			data.procs.forEach(function(value) {
				var node = document.createElement("tr");
				node.innerHTML = "<th class=\"msg-log\">" + value.name +":</th>" +
					"<th class=\"msg-log\">"+value.status+"</th>";
				procs.appendChild(node);
			});
		}
	};

	function updateMsg(data) {
		var msgs = document.getElementById("rc-msg-log");
		msgs.innerText = msgs.innerText + data.timestamp + ": " + data.msg;
	};

	function cmdConfig() { sendCmd("/config"); };
	function cmdInit()   { sendCmd("/init"); };
	function cmdStart()  { sendCmd("/start"); };
	function cmdStop()   { sendCmd("/stop"); };
	function cmdReset()  { sendCmd("/reset"); };

	function cmdQuit()   { sendCmd("/quit"); }; // FIXME(sbinet): add confirmation dialog

	function sendCmd(name) {
		var data = new FormData();
		data.append("cmd", name);
		$.ajax({
			url: "/cmd",
			method: "POST",
			data: data,
			processData: false,
			contentType: false,
			success: function(data, status) {
				// FIXME(sbinet): report?
			},
			error: function(e) {
				alert("could not send command ["+name+"]:\n"+e.responseText);
			}
		});
	};

</script>
</head>
<body>

<!-- Sidebar -->
<div id="app-sidebar" class="w3-sidebar w3-bar-block w3-card-4 w3-light-grey" style="width:25%">
	<div class="w3-bar-item w3-card-2 w3-black">
		<h2>TDAQ RunControl</h2>
	</div>
	<div class="w3-bar-item">

		<div>
			<table>
				<tbody>
					<tr>
						<th class="msg-log">RunControl Status:</th>
						<th><div id="rc-status" class="msg-log">N/A</div></th>
					</tr>
				</tbody>
			</table>
		</div>
		<br>

		<input type="button" onclick="cmdConfig()" value="Config">
		<input type="button" onclick="cmdInit()"   value="Init">
		<input type="button" onclick="cmdStart()"  value="Start">
		<input type="button" onclick="cmdStop()"   value="Stop">
		<input type="button" onclick="cmdReset()"  value="Reset">

		<br>
		<br>

		<div>
			<h4> TDAQ Processes:</h4>
			<table>
				<tbody id="rc-procs-status">
				</tbody>
			</table>
		</div>
		<br>

		<input type="button" onclick="cmdQuit()"  value="Quit">
		<br>

		<span>---</span>
		Last status update:<br><span id="rc-status-update" class="msg-log">N/A</span><br>
		<br>
	</div>
</div>

<!-- Page Content -->
<div style="margin-left:25%; height:100%" class="w3-grey" id="app-container">
	<div class="w3-container w3-content w3-cell w3-cell-middle w3-cell-row w3-center w3-justify w3-grey" style="width:100%" id="app-display">
		<div>
			<pre id="rc-msg-log" class="msg-log"></pre>
		</div>
	</div>
</div>

</body>
</html>
`
