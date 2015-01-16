package status

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	yaml "gopkg.in/yaml.v2"
)

const (
	procBase = "/proc/fs/lustre"
)

// LustreClient is a local client
type LustreClient struct {
	FsName   string
	ClientID string
}

func (c *LustreClient) String() string {
	return c.FsName + "-" + c.ClientID
}

func (c *LustreClient) getClientDevices(module string, cli string) []string {
	var ret []string
	nameGlob := fmt.Sprintf("%s*-%s-%s", c.FsName, cli, c.ClientID)
	p := filepath.Join(procBase, module, nameGlob)
	matches, _ := filepath.Glob(p)
	for _, c := range matches {
		ret = append(ret, clientName(c))
	}
	return ret
}

func (c *LustreClient) ClientPath(module string, cli string) string {
	name := fmt.Sprintf("%s-%s-%s", cli, module, c.ClientID)
	p := filepath.Join(procBase, module, name)
	return p
}

func clientName(path string) string {
	imp, _ := ReadImport(path)
	t := imp.Target
	if strings.HasSuffix(t, "_UUID") {
		t = t[0 : len(t)-5]
	}
	return t
}

// LOVTargets retuns list of OSC devices in the LOV
func (c *LustreClient) LOVTargets() []string {
	return c.getClientDevices("osc", "osc")
}

// LMVTargets retuns list of MDC devices in the LMV
func (c *LustreClient) LMVTargets() []string {
	return c.getClientDevices("mdc", "mdc")
}

type (
	Wrapper struct {
		Import Import
	}
	Import struct {
		Name         string
		State        string
		Target       string
		ConnectFlags []string `yaml:"connect_flags"`
		ImportFlags  []string `yaml:"import_flags"`
		Connection   ConnectionStatus
		// OSC only
		Averages WriteDataAverages `yaml:"write_data_averages"`
	}
	WriteDataAverages struct {
		BytesPerRpc     int     `yaml:"bytes_per_rpc"`
		MicrosendPerRpc int     `yaml:"usec_per_rpc"`
		MegabytesPerSec float64 `yaml:"MB_per_sec"`
	}
	ConnectionStatus struct {
		FailoverNids            []string `yaml:"failover_nids"`
		CurrentConnection       string   `yaml:"current_connection"`
		ConnectionAttempts      int      `yaml:"connection_attempts"`
		Generation              int      `yaml:"generation"`
		InProgressInvalidations int      `yaml:"in-progress_invalidations"`
	}
)

func removeZeros(b []byte) []byte {
	var copy = make([]byte, 0)
	for _, n := range b {
		if n != 0 {
			copy = append(copy, n)
		}
	}
	return copy
}

func ReadImport(path string) (*Import, error) {
	result := Wrapper{}
	b := make([]byte, 8192)
	importPath := filepath.Join(path, "import")

	// ioutil.ReadFile chokes on the binary data embedded in import (LU-5567)
	fp, err := os.Open(importPath)
	if err != nil {
		return nil, err
	}
	_, err = fp.Read(b)
	if err != nil {
		return nil, err
	}
	b = removeZeros(b) // sanitize
	e := yaml.Unmarshal(b, &result)
	if e != nil {
		return nil, e
	}
	return &result.Import, nil
}
