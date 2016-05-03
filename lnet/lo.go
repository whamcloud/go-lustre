package lnet

import (
	"fmt"
	"net"
)

const loDriverString = "lo"

func init() {
	drivers[loDriverString] = newLoopbackNid
}

// LoopbackNid is a Loopback LND NID
type LoopbackNid struct {
	IPAddress      *net.IP
	driverInstance int
}

// Address returns the underlying *net.IP
func (t *LoopbackNid) Address() interface{} {
	return t.IPAddress
}

// Driver returns the LND name
func (t *LoopbackNid) Driver() string {
	return loDriverString
}

// LNet returns a string representation of the driver name and instance
func (t *LoopbackNid) LNet() string {
	return fmt.Sprintf("%s%d", t.Driver(), t.driverInstance)
}

func newLoopbackNid(address string, driverInstance int) (RawNid, error) {
	ip := net.ParseIP("127.0.0.1")

	return &LoopbackNid{
		IPAddress:      &ip,
		driverInstance: driverInstance,
	}, nil
}
