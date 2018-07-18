// Copyright (c) 2016 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package spec_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/intel-hpdd/go-lustre/fs/spec"

	. "github.com/smartystreets/goconvey/convey"
)

func err2str(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}

func TestClientDeviceFromString(t *testing.T) {
	Convey("ClientDeviceFromString() should attempt to parse a string into a lustre client device", t, func() {
		var tests = []struct {
			in  string
			out string
			err string
		}{
			{
				in:  `0@lo:/fsname`,
				out: `0@lo:/fsname`,
			},
			{
				in:  `127.0.0.1@tcp:/fsname`,
				out: `127.0.0.1@tcp0:/fsname`,
			},
			{
				in:  `127.0.0.2@tcp,127.0.0.1@tcp:/fsname`,
				out: `127.0.0.2@tcp0,127.0.0.1@tcp0:/fsname`,
			},
			{
				in:  `10.0.1.2@tcp3:127.0.0.2@tcp,127.0.0.1@tcp:/fsname`,
				out: `10.0.1.2@tcp3:127.0.0.2@tcp0,127.0.0.1@tcp0:/fsname`,
			},
			{
				in:  `10.0.1.2@tcp3:localhost@tcp,localhost@tcp1:/fsname`,
				out: `10.0.1.2@tcp3:127.0.0.1@tcp0,127.0.0.1@tcp1:/fsname`,
			},

			{
				in:  `127.0.0.1@tcp:/`,
				err: `Cannot parse client mount device from "127.0.0.1@tcp:/"`,
			},
			{
				in:  `/fsname`,
				err: `Cannot parse client mount device from "/fsname"`,
			},
			{
				in:  `101@gni:/fsname`,
				err: `parsing nid failed: Unsupported LND: gni`,
			},
		}

		for _, tc := range tests {
			Convey(tc.in, func() {
				d, err := spec.ClientDeviceFromString(tc.in)
				So(err2str(err), ShouldEqual, tc.err)

				if d != nil {
					So(d.String(), ShouldEqual, tc.out)
				}
			})
		}
	})
}

func TestClientDeviceJSON(t *testing.T) {
	Convey("A ClientDevice should correctly serialize to/from JSON", t, func() {
		devString := "10.0.1.2@tcp3:127.0.0.2@tcp0,127.0.0.1@tcp0:/fsname"
		dev, err := spec.ClientDeviceFromString(devString)
		if err != nil {
			t.Fatal(err)
		}

		data, err := json.Marshal(dev)
		if err != nil {
			t.Fatal(err)
		}
		So(string(data), ShouldEqual, fmt.Sprintf("%q", devString))

		newDev := &spec.ClientDevice{}
		if err := json.Unmarshal(data, newDev); err != nil {
			t.Fatal(err)
		}
		So(newDev.String(), ShouldEqual, devString)
	})
}
