package lnet

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type Nid interface {
	String() string
	Driver() string
}

func NidFromString(inString string) (Nid, error) {
	nidRe := regexp.MustCompile(`^(.*)@([A-z]+)(\d*)$`)
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
		return newTcpNid(address, driverInstance)
	default:
		return nil, fmt.Errorf("Unsupported LND: %s", driver)
	}
}
