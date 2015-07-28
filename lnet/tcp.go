package lnet

import (
	"fmt"
	"net"
)

type TcpNid struct {
	IPAddress      *net.IP
	driverInstance int
}

func (t *TcpNid) String() string {
	return fmt.Sprintf("%s@%s%d", t.IPAddress, t.Driver(), t.driverInstance)
}

func (t *TcpNid) Address() string {
	return t.IPAddress.String()
}

func (t *TcpNid) Driver() string {
	return "tcp"
}

func newTcpNid(address string, driverInstance int) (Nid, error) {
	ip := net.ParseIP(address)
	if ip == nil {
		return nil, fmt.Errorf("%q is not a valid IP address", address)
	}
	return &TcpNid{
		IPAddress:      &ip,
		driverInstance: driverInstance,
	}, nil
}
