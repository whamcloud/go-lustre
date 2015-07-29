package lnet

import (
	"fmt"
	"net"
)

type IbNid struct {
	IPAddress      *net.IP
	driverInstance int
}

func (t *IbNid) Address() interface{} {
	// Not a intended to be used as real IP address, so just return as string
	return t.IPAddress.String()
}

func (t *IbNid) Driver() string {
	return "o2ib"
}

func (t *IbNid) LNet() string {
	return fmt.Sprintf("%s%d", t.Driver(), t.driverInstance)
}

func newIbNid(address string, driverInstance int) (*IbNid, error) {
	ip := net.ParseIP(address)
	if ip == nil {
		return nil, fmt.Errorf("%q is not a valid IP address", address)
	}
	return &IbNid{
		IPAddress:      &ip,
		driverInstance: driverInstance,
	}, nil
}
