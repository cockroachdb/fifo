// Copyright 2024 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package fifo

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// dt is a small unit of time that should be small but significantly higher than
// expected scheduling delays in the test environment.
const dt = 10 * time.Millisecond

func TestRateLimiterBasic(t *testing.T) {
	// refill rate: 1 token / dt
	rl := NewRateLimiter(1.0/dt.Seconds(), 1.0/dt.Seconds())
	ctx := context.Background()
	c := goAcquire(ctx, rl, 10)
	noRecv(t, c)
	time.Sleep(5 * dt)
	noRecv(t, c)
	time.Sleep(5 * dt)
	wait(t, c, dt)

	// Test fast path.
	time.Sleep(5 * dt)
	c = goAcquire(ctx, rl, 5)
	wait(t, c, dt)

	require.Equal(t, 0, rl.numWaitersLocked())
	c1 := goAcquire(ctx, rl, 100)
	expectWaiters(t, rl, 1, dt)
	c2 := goAcquire(ctx, rl, 100)
	c3 := goAcquire(ctx, rl, 100)
	expectWaiters(t, rl, 3, dt)
	noRecv(t, c1)
	noRecv(t, c2)
	noRecv(t, c3)
	rl.Return(100)
	wait(t, c1, dt)
	rl.Return(200)
	wait(t, c2, dt)
	wait(t, c3, dt)
}

func expectWaiters(t *testing.T, rl *RateLimiter, num int, timeout time.Duration) {
	t.Helper()
	const numTries = 20
	for i := 0; ; i++ {
		actual := func() int {
			rl.mu.Lock()
			defer rl.mu.Unlock()
			return rl.numWaitersLocked()
		}()
		if actual == num {
			return
		}
		if i == numTries {
			t.Fatalf("timed out expecting %d waiters (got %d)", num, actual)
		}
		time.Sleep(timeout / numTries)
	}
}

func goAcquire(ctx context.Context, rl *RateLimiter, n float64) chan error {
	c := make(chan error)
	go func() {
		c <- rl.Acquire(ctx, n)
	}()
	return c
}

func wait(t *testing.T, ch chan error, timeout time.Duration) {
	t.Helper()
	select {
	case err := <-ch:
		if err != nil {
			t.Fatalf("received error: %v", err)
		}
	case <-time.After(timeout):
		t.Fatal("did not receive notification")
	}
}

func noRecv(t *testing.T, ch chan error) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("received unexpected notification")
	case <-time.After(dt / 10):
	}
}

func TestRateLimiterTimeToFulfill(t *testing.T) {
	for _, tc := range []struct {
		rate, n  float64
		expected time.Duration
	}{
		{
			rate:     1,
			n:        100,
			expected: 100 * time.Second,
		},
		{
			rate:     100,
			n:        100,
			expected: 1 * time.Second,
		},
		{
			rate:     100,
			n:        1,
			expected: time.Second / 100,
		},
		{
			rate:     Inf(),
			n:        1,
			expected: 0,
		},
	} {
		var rl RateLimiter
		rl.mu.rate = tc.rate
		rl.mu.current = rand.Float64() * 100
		res := rl.timeToFulfillLocked(rl.mu.current + tc.n)
		if res < tc.expected-1 || res > tc.expected+1 {
			t.Fatalf("rate: %v  n: %v  expected: %v  got: %v", tc.rate, tc.n, tc.expected, res)
		}
	}
}
