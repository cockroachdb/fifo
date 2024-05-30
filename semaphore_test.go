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
	"errors"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSemaphoreAPI(t *testing.T) {
	s := NewSemaphore(10)
	require.True(t, s.TryAcquire(5))
	require.False(t, s.TryAcquire(10))
	require.Error(t, ErrRequestExceedsCapacity, s.Acquire(context.Background(), 20))
	require.Equal(t, "capacity: 10, outstanding: 5, num waiters: 0", s.Stats().String())

	ch := make(chan struct{}, 10)
	go func() {
		require.NoError(t, s.Acquire(context.Background(), 8))
		ch <- struct{}{}
		require.NoError(t, s.Acquire(context.Background(), 1))
		ch <- struct{}{}
		require.NoError(t, s.Acquire(context.Background(), 5))
		ch <- struct{}{}
	}()
	assertNoRecv(t, ch)
	s.Release(5)
	assertRecv(t, ch)
	assertRecv(t, ch)
	assertNoRecv(t, ch)
	s.Release(1)
	assertNoRecv(t, ch)
	s.Release(8)
	assertRecv(t, ch)

	// Test UpdateCapacity.
	go func() {
		require.NoError(t, s.Acquire(context.Background(), 8))
		ch <- struct{}{}
		require.NoError(t, s.Acquire(context.Background(), 1))
		ch <- struct{}{}
		require.Error(t, ErrRequestExceedsCapacity, s.Acquire(context.Background(), 5))
		ch <- struct{}{}
	}()
	assertNoRecv(t, ch)
	s.UpdateCapacity(15)
	assertRecv(t, ch)
	assertRecv(t, ch)
	assertNoRecv(t, ch)
	s.UpdateCapacity(2)
	assertRecv(t, ch)
}

// TestSemaphoreBasic is a test with multiple goroutines acquiring a unit and
// releasing it right after.
func TestSemaphoreBasic(t *testing.T) {
	capacities := []int64{1, 5, 10, 50, 100}
	goroutineCounts := []int{1, 10, 100}

	for _, capacity := range capacities {
		for _, numGoroutines := range goroutineCounts {
			s := NewSemaphore(capacity)
			ctx := context.Background()
			resCh := make(chan error, numGoroutines)

			for i := 0; i < numGoroutines; i++ {
				go func() {
					err := s.Acquire(ctx, 1)
					if err != nil {
						resCh <- err
						return
					}
					s.Release(1)
					resCh <- nil
				}()
			}

			for i := 0; i < numGoroutines; i++ {
				if err := assertRecv(t, resCh); err != nil {
					t.Fatal(err)
				}
			}

			if stats := s.Stats(); stats.Outstanding != 0 {
				t.Fatalf("expected nothing outstanding; got %s", stats)
			}
		}
	}
}

// TestSemaphoreContextCancellation tests the behavior that for an ongoing
// blocked acquisition, if the context passed in gets canceled the acquisition
// gets canceled too with an error indicating so.
func TestSemaphoreContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := NewSemaphore(1)
	require.NoError(t, s.Acquire(ctx, 1))

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Acquire(ctx, 1)
	}()

	cancel()

	err := assertRecv(t, errCh)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation error, got %v", err)
	}

	require.Equal(t, "capacity: 1, outstanding: 1, num waiters: 0", s.Stats().String())
}

// TestSemaphoreCanceledAcquisitions tests the behavior where we enqueue
// multiple acquisitions with canceled contexts and expect any subsequent
// acquisition with a valid context to proceed without error.
func TestSemaphoreCanceledAcquisitions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := NewSemaphore(1)
	require.NoError(t, s.Acquire(ctx, 1))

	cancel()
	const numGoroutines = 5

	errCh := make(chan error)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			errCh <- s.Acquire(ctx, 1)
		}()
	}

	for i := 0; i < numGoroutines; i++ {
		if err := assertRecv(t, errCh); !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context cancellation error, got %v", err)
		}
	}
	s.Release(1)

	go func() {
		errCh <- s.Acquire(context.Background(), 1)
	}()

	require.NoError(t, assertRecv(t, errCh))
}

// TestSemaphoreNumWaiters verifies that the Stats().NumWaiters is as expected.
func TestSemaphoreNumWaiters(t *testing.T) {
	s := NewSemaphore(1)
	ctx := context.Background()
	doneCh := make(chan struct{}, 10)
	doAcquire := func(ctx context.Context) {
		err := s.Acquire(ctx, 1)
		if ctx.Err() == nil {
			require.NoError(t, err)
			doneCh <- struct{}{}
		}
	}

	assertNumWaitersSoon := func(exp int) {
		for i := 0; ; i++ {
			got := s.Stats().NumWaiters
			if got == exp {
				return
			}
			if i >= 20 {
				t.Fatalf("expected num waiters to be %d, got %d", got, exp)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	// Initially s should have no waiters.
	assert.Equal(t, 0, s.Stats().NumWaiters)
	// Acquire all of the quota from the pool.
	require.NoError(t, s.Acquire(ctx, 1))
	// Still no waiters.
	assert.Equal(t, 0, s.Stats().NumWaiters)
	// Launch a goroutine to acquire quota, ensure that the length increases.
	go doAcquire(ctx)
	assertNumWaitersSoon(1)
	// Create more goroutines which will block to be canceled later in order to
	// ensure that cancelations deduct from the length.
	const numToCancel = 12 // an arbitrary number
	ctxToCancel, cancel := context.WithCancel(ctx)
	for i := 0; i < numToCancel; i++ {
		go doAcquire(ctxToCancel)
	}
	// Ensure that all of the new goroutines are reflected in the length.
	assertNumWaitersSoon(numToCancel + 1)
	// Launch another goroutine with the default context.
	go doAcquire(ctx)
	assertNumWaitersSoon(numToCancel + 2)
	// Cancel some of the goroutines.
	cancel()
	// Ensure that they are soon not reflected in the length.
	assertNumWaitersSoon(2)
	// Unblock the first goroutine.
	s.Release(1)
	assertRecv(t, doneCh)
	assert.Equal(t, 1, s.Stats().NumWaiters)
	// Unblock the second goroutine.
	s.Release(1)
	assertRecv(t, doneCh)
	assert.Equal(t, 0, s.Stats().NumWaiters)
}

func TestConcurrentUpdatesAndAcquisitions(t *testing.T) {
	ctx := context.Background()
	var wg sync.WaitGroup
	const maxCap = 100
	s := NewSemaphore(maxCap)
	const N = 100
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runtime.Gosched()
			newCap := rand.Int63n(maxCap-1) + 1
			s.UpdateCapacity(newCap)
		}()
	}
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runtime.Gosched()
			n := rand.Int63n(maxCap)
			err := s.Acquire(ctx, n)
			runtime.Gosched()
			if err == nil {
				s.Release(n)
			}
		}()
	}
	wg.Wait()
	s.UpdateCapacity(maxCap)
	assert.Equal(t, "capacity: 100, outstanding: 0, num waiters: 0", s.Stats().String())
}

func assertRecv[T any](t *testing.T, ch chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(time.Second):
		t.Fatal("did not receive notification")
		panic("unreachable")
	}
}

func assertNoRecv[T any](t *testing.T, ch chan T) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("received unexpected notification")
	case <-time.After(10 * time.Millisecond):
	}
}
