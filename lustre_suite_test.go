package lustre_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"hpdd/test/harness"
	"hpdd/test/log"
	"testing"
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
	RunSpecs(t, "Lustre Suite")
}
