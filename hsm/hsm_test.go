// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package hsm_test

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/wastore/go-lustre/hsm"
)

// Basic test to ensure that the test API implements all interfaces
func TestActionSource(t *testing.T) {
	src := hsm.NewTestSource()
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	src.Start(ctx)

	go src.GenerateRandomAction()

	next := <-src.Actions()
	handle, err := next.Begin(0, false)
	if err != nil {
		t.Fatalf("err: %s", err)
	}

	if handle.Action() != next.Action() {
		t.Fatalf("err: huh?")
	}
}
