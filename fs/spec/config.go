package spec

import (
	"bytes"
	"fmt"
	"net"
)

// Config contains information needed to mount a Lustre client
type Config struct {
	ClientDevice  *ClientDevice
	Mountpoint    string
	PackageSource string
	SkipPackages  bool
	Encrypted     bool
	IPSecKey      string
	IPSecPeers    map[string]*net.IP
}

func (cfg *Config) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("Package Source: %s\n", cfg.PackageSource))
	buf.WriteString(fmt.Sprintf("Device: %s\n", cfg.ClientDevice))
	buf.WriteString(fmt.Sprintf("Mountpoint: %s\n", cfg.Mountpoint))
	buf.WriteString(fmt.Sprintf("Skip Package Installation? %t\n", cfg.SkipPackages))
	if cfg.Encrypted {
		buf.WriteString(fmt.Sprintf("PSK: %s\n", cfg.IPSecKey))
		buf.WriteString(fmt.Sprintf("Peers: %s\n", cfg.IPSecPeers))
	}
	return buf.String()
}

// FsName returns the name of the filesystem to be mounted
func (cfg *Config) FsName() string {
	return cfg.ClientDevice.FsName
}

// Mountspec returns a string representation of the client mount device
func (cfg *Config) Mountspec() string {
	return cfg.ClientDevice.String()
}
