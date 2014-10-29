package lustre_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "hpdd/testlib"

	"path"
	"testing"
)

func TestLustre(t *testing.T) {
	targets := []LustreTarget{
		{"mgs", path.Join(TestPrefix, "mgsLoopFile"), 128 * 1e6, MGS_PRI},
		{"mdt00", path.Join(TestPrefix, "mdt00LoopFile"), 512 * 1e6, MDT_PRI},
		{"ost00", path.Join(TestPrefix, "ost00LoopFile"), 1024 * 1e6, OST_PRI},
	}
	var activeMounts []*MountPoint

	BeforeSuite(func() {
		AddDebugLogger(&ClosingGinkgoWriter{GinkgoWriter})
		LoadModules()
		activeMounts = DoLustreSetup(targets)
	})

	AfterSuite(func() {
		DoLustreTeardown(activeMounts)
		UnloadModules()
	})

	RegisterFailHandler(Fail)
	RunSpecs(t, "Lustre Suite")
}
