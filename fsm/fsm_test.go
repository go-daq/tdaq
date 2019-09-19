// Copyright 2019 The go-daq Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fsm // import "github.com/go-daq/tdaq/fsm"

import "testing"

func TestStatus(t *testing.T) {
	for _, tt := range []struct {
		status Status
		want   string
		panics bool
	}{
		{status: UnConf, want: "unconfigured"},
		{status: Conf, want: "configured"},
		{status: Init, want: "initialized"},
		{status: Running, want: "running"},
		{status: Stopped, want: "stopped"},
		{status: Exiting, want: "exiting"},
		{status: Error, want: "error"},
		{status: Status(255), panics: true},
	} {
		t.Run("", func(t *testing.T) {
			if tt.panics {
				defer func() {
					err := recover()
					if err == nil {
						t.Fatalf("expected a panic")
					}
					if got, want := err.(error).Error(), "invalid status value 255"; got != want {
						t.Fatalf("invalid panic string.\ngot = %q\nwant= %q\n", got, want)
					}
				}()
			}

			got := tt.status.String()
			if got != tt.want {
				t.Fatalf("invalid stringer value.\ngot = %q\nwant= %q\n", got, tt.want)
			}
		})
	}
}
