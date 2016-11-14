package llapi

import (
	"math"
	"testing"

	lustre "github.com/intel-hpdd/go-lustre"
)

func TestSafeInt64(t *testing.T) {
	var tests = []struct {
		in     uint64
		out    int64
		errStr string
	}{
		{
			in:     math.MaxUint64,
			out:    lustre.MaxExtentLength,
			errStr: "",
		},
		{
			in:     math.MaxUint64 - 1,
			out:    -2,
			errStr: "18446744073709551614 overflows int64",
		},
		{
			in:     math.MaxInt64,
			out:    math.MaxInt64,
			errStr: "",
		},
		{
			in:     0,
			out:    0,
			errStr: "",
		},
	}

	for _, tc := range tests {
		out, err := safeInt64(tc.in)
		if err2Str(err) != tc.errStr {
			t.Fatalf("Got error %s, expected %s", err, tc.errStr)
		}
		if out != tc.out {
			t.Fatalf("Got output %d, expected %d", out, tc.out)
		}
	}
}

func err2Str(err error) (str string) {
	if err != nil {
		str = err.Error()
	}
	return
}
