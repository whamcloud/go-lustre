package fs_test

import (
	"testing"

	"github.intel.com/hpdd/test/harness"
	"github.intel.com/hpdd/test/log"
	"github.intel.com/hpdd/test/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestLustre(t *testing.T) {
	BeforeSuite(func() {
		log.AddDebugLogger(&log.ClosingGinkgoWriter{GinkgoWriter})
		if err := harness.Setup(); err != nil {
			panic(err)
		}
	})

	AfterSuite(func() {
		if err := harness.Teardown(); err != nil {
			panic(err)
		}
	})

	RegisterFailHandler(Fail)
	utils.RunSpecs(t, "Lustre Utils Suite")
}
