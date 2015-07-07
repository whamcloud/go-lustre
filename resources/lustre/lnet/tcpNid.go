package lnet

import (
	"fmt"
	"net"
)

type tcpNid struct {
	ipAddress      net.IP
	driverInstance int
}

func (t *tcpNid) String() string {
	return fmt.Sprintf("%s@%s%d", t.ipAddress, t.Driver(), t.driverInstance)
}

func (t *tcpNid) Driver() string {
	return "tcp"
}

func newTcpNid(address string, driverInstance int) (Nid, error) {
	ip := net.ParseIP(address)
	if ip == nil {
		return nil, fmt.Errorf("%q is not a valid IP address", address)
	}
	return &tcpNid{
		ipAddress:      ip,
		driverInstance: driverInstance,
	}, nil
}
