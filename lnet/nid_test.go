package lnet_test

import (
	"testing"

	"github.intel.com/hpdd/ce-tools/resources/lustre/lnet"

	. "github.com/smartystreets/goconvey/convey"
)

func TestNidFromString(t *testing.T) {
	Convey("NidFromString() should attempt to parse a string into a Nid", t, func() {
		var tests = []struct {
			in  string
			out string
			err string
		}{
			{
				in:  `127.0.0.1@tcp`,
				out: `127.0.0.1@tcp0`,
			},
			{
				in:  `127.0.0.2@tcp42`,
				out: `127.0.0.2@tcp42`,
			},
			{
				in:  `101@gni`,
				err: `Unsupported LND: gni`,
			},
			{
				in:  `101`,
				err: `Cannot parse NID from "101"`,
			},
			{
				in:  `@tcp`,
				err: `"" is not a valid IP address`,
			},
		}

		for _, tc := range tests {
			Convey(tc.in, func() {
				n, err := lnet.NidFromString(tc.in)
				So(err2str(err), ShouldEqual, tc.err)

				if n != nil {
					So(tc.out, ShouldEqual, n.String())
				}
			})
		}
	})
}

func err2str(err error) string {
	if err != nil {
		return err.Error()
	}
	return ""
}
