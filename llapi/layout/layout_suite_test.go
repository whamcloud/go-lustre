// Copyright (c) 2018 DDN. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package layout_test

import (
	"testing"

	"github.com/intel-hpdd/test/harness"
	"github.com/intel-hpdd/test/log"
	"github.com/intel-hpdd/test/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestLayout(t *testing.T) {
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
	utils.RunSpecs(t, "Layout Suite")
}
