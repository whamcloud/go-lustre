// Copyright (c) 2016 Intel Corporation. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package pool_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/intel-hpdd/go-lustre/pkg/pool"
)

func TestPool(t *testing.T) {
	alloc := func() (interface{}, error) {
		return "hello there", nil
	}
	p, err := pool.New("test", 2, 10, alloc)
	if err != nil {
		t.Fatal(err)
	}
	o, err := p.Get()
	if err != nil {
		t.Fatal(err)
	}
	if o.(string) != "hello there" {
		t.Fatal("message not equal")
	}
	p.Put(o)
	p.Close()
}

type Counter int

func (c Counter) Close() error {
	fmt.Printf("CLOSE counter %v\n", c)
	return nil
}

func TestConcurrency(t *testing.T) {
	var counter Counter
	var wg sync.WaitGroup
	alloc := func() (interface{}, error) {
		defer func() { counter++ }()
		return counter, nil
	}

	max := 16
	p, err := pool.New("counter", 1, max, alloc)
	if err != nil {
		t.Fatal(err)

	}

	for i := 0; i < max; i++ {
		wg.Add(1)
		go func(i int) {
			for j := 0; j < 100; j++ {
				o, err := p.Get()
				if err != nil {
					if err == pool.ErrClosed {
						break
					}
					t.Fatal(err)
				}
				fmt.Printf("thread-%d: got counter: %v\n", i, o)
				p.Put(o)
				time.Sleep(1)
			}
			wg.Done()
			fmt.Printf("thread-%d: done\n", i)

		}(i)
	}
	time.Sleep(1 * time.Millisecond)
	p.Close()
	wg.Wait()
	if p.Allocated() != 0 {
		t.Fatalf("Pool not empty: %d", p.Allocated())
	}

}
