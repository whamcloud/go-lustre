package lustre

import (
	"flag"
	"io/ioutil"
	"testing"
	//    . "gopkg.in/check.v1"
)

var mnt = flag.String("mnt", "", "Lustre mountpoint")

func TestFid(t *testing.T) {
	if *mnt == "" {
		t.Fatal("use --mnt <path> to run lustre tests")
	}
	f, err := ioutil.TempFile(*mnt, "test")
	if err != nil {
		t.Errorf("Unable to create file in %s", mnt)
		return
	}
	name := f.Name()
	if name == "" {
		t.Error("Unable to get file name")
		return
	}
	fid, err := LookupFid(name)
	if err != nil {
		t.Error("Unable to get fid", err)
		return
	}
	fids, err := fid.Paths(*mnt)
	if err != nil {
		t.Error("fid.Paths: ", err, fids)
	}

}

func TestParseFid(t *testing.T) {
	str := "[0x123:0x456:0x0]"
	fid, err := ParseFid("[0x123:0x456:0x0]")
	if err != nil {
		t.Error("Unable to parse fid:", err)
	} else {
		if fid.f_seq != 0x123 || fid.f_oid != 0x456 || fid.f_ver != 0 {
			t.Errorf("Parse failure: %v", fid)
		}
	}
	if fid.String() != str {
		t.Error("Did not convert back to string ")
	}
}

func TestParseBadFid(t *testing.T) {
	fid2, err := ParseFid("[0x123:0x456:bad]")
	if err == nil {
		t.Error("Failed to detect bad FID string (%v)", fid2)
	}

}

func TestParseZeroFid(t *testing.T) {
	fid, err := ParseFid("[0x0:0x0:0x0]")
	if err != nil {
		t.Error("Unable to parse fid:", err)
	} else {
		if !fid.IsZero() {
			t.Errorf("fid should be zero: %v", fid)
		}
	}

}

/*
func Test(t *testing.T) {TestingT(t)}

func (s *SuiteType) SetUpSuite(c *C) {
    fmt.PrintLn("setup suite")a

type MySuite struct{}

var _ = Suite(&MySuite{})

func (s *MySuite) TestHelloWorld(c *C) {
    c.Assert(42, Equals, 42)
}

*/
