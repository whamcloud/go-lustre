package lnet

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type RawNid interface {
	Driver() string
	Address() interface{}
	LNet() string
}

type Nid struct {
	raw RawNid
}

func (nid *Nid) MarshalJSON() ([]byte, error) {
	return json.Marshal(nid.String())
}

func (nid *Nid) UnmarshalJSON(b []byte) error {
	if b[0] == '"' {
		b = b[1 : len(b)-1]
	}
	n, err := NidFromString(string(b))
	if err != nil {
		return err
	}
	*nid = *n
	return nil
}

func (nid *Nid) String() string {
	return fmt.Sprintf("%s@%s", nid.raw.Address(), nid.raw.LNet())
}

func (nid *Nid) Address() interface{} {
	return nid.raw.Address()
}

func (nid *Nid) Driver() string {
	return nid.raw.Driver()
}

func NidFromString(inString string) (*Nid, error) {
	nidRe := regexp.MustCompile(`^(.*)@(\w+[^\d*])(\d*)$`)
	matches := nidRe.FindStringSubmatch(inString)
	if len(matches) < 3 {
		return nil, fmt.Errorf("Cannot parse NID from %q", inString)
	}

	address := matches[1]
	driver := matches[2]
	var driverInstance int
	if matches[3] != "" {
		var err error
		driverInstance, err = strconv.Atoi(matches[3])
		if err != nil {
			return nil, err
		}
	}

	switch strings.ToLower(driver) {
	case "tcp":
		raw, err := newTcpNid(address, driverInstance)
		if err != nil {
			return nil, err
		}
		return &Nid{raw: raw}, nil
	case "o2ib":
		raw, err := newIbNid(address, driverInstance)
		if err != nil {
			return nil, err
		}
		return &Nid{raw: raw}, nil
	default:
		return nil, fmt.Errorf("Unsupported LND: %s", driver)
	}
}
