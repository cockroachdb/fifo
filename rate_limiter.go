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
	"math"
	"sync"
	"time"
)

// RateLimiter implements a token-bucket style rate limiter.
//
// A token bucket has a refill rate (tokens / second) and a burst size (maximum
// number of unused tokens that can accumulate and be used without waiting).
//
// RateLimiter implements a FIFO policy where requests are queued and served in
// order. This policy provides fairness and prevents starvation but is
// susceptible to head-of-line blocking, where a large request that can't be
// satisfied blocks many other small requests that could be.
type RateLimiter struct {
	mu struct {
		sync.Mutex

		// rate is the refill rate, in tokens/second.
		rate float64
		// burst is the maximum number of unused tokens that can accumulate.
		burst float64

		// current is the number of tokens in the bucket (at the lastUpdated time).
		// It includes tokens which are "earmarked" to go to an existing waiter; the
		// value can be larger than burst if we have requests waiting that need more
		// than the burst.
		current     float64
		lastUpdated time.Time

		// waiters is the queue of Acquire requests waiting for quota.
		//
		// When a waiter becomes the head of the queue, its channel is notified. The
		// head of the queue's channel can be notified again if the quota changes
		// (e.g. reconfiguration or Return).
		waiters Queue[rateLimiterWaiter]

		// numCanceled is the number of waiters in the waiters queue which have been
		// canceled. It is used to determine the current number of active waiters in
		// the queue which is waiters.Len() minus this value.
		numCanceled int
	}

	// timer is used by the current head of the waiters queue.
	timer *time.Timer
}

// Inf can be used for the refill rate and burst limit to disable rate limiting.
func Inf() float64 {
	return math.Inf(1)
}

func NewRateLimiter(refillRate, burstLimit float64) *RateLimiter {
	if refillRate <= 0 || burstLimit < 0 {
		panic("invalid refill rate or burst limit")
	}
	rl := &RateLimiter{}
	rl.mu.rate = refillRate
	rl.mu.burst = burstLimit
	rl.mu.lastUpdated = time.Now()
	rl.mu.waiters = MakeQueue[rateLimiterWaiter](&rateLimiterQueuePool)

	// Create a timer that we will reuse for each head of the waiters queue.
	rl.timer = time.NewTimer(0)
	<-rl.timer.C

	return rl
}

func (rl *RateLimiter) Acquire(ctx context.Context, n float64) error {
	rl.mu.Lock()
	rl.updateLocked(time.Now())

	// Fast path.
	if rl.numWaitersLocked() == 0 && rl.mu.current >= n {
		rl.mu.current -= n
		rl.mu.Unlock()
		return nil
	}

	c := chanNoValSyncPool.Get().(chan struct{})
	defer chanNoValSyncPool.Put(c)
	if invariants {
		defer assertChanDrained(c)
	}

	headOfQueue := rl.mu.waiters.Len() == 0
	w := rl.mu.waiters.PushBack(rateLimiterWaiter{c: c})
	timeToFulfill := rl.timeToFulfillLocked(n)
	rl.mu.Unlock()

	// There are two cases:
	//  - if we are the first in the queue, we calculate the amount of time we
	//    need to wait for enough quota to accumulate and start a timer. We also
	//    wait on c in case the quota changes (e.g. reconfiguration or quota being
	//    returned).
	//  - if there are other requests waiting, we just wait on c until we become
	//    the head of the queue.
	var timer *time.Timer
	var timerC <-chan time.Time
	if headOfQueue {
		timer = rl.getTimer()
		timer.Reset(timeToFulfill)
		timerC = timer.C
	}

	for {
		var now time.Time
		select {
		case <-ctx.Done():
			rl.mu.Lock()
			defer rl.mu.Unlock()

			cancelTimer(timer)
			rl.returnTimer(timer)

			// We need to check if we raced with a channel notify. Note that the
			// channel send happens under the lock, so there is no race here.
			select {
			case <-c:
				headOfQueue = true
			default:
			}

			if headOfQueue {
				if invariants && rl.mu.waiters.PeekFront().c != c {
					panic("unexpected head of queue")
				}
				rl.popHeadWaiterLocked()
			} else {
				if invariants && rl.mu.waiters.PeekFront().c == c {
					panic("head of queue was not notified")
				}
				// Mark the request as canceled (since it's not at the head of the
				// queue). It will get cleaned up in popHeadWaiterLocked().
				w.c = nil
				rl.mu.numCanceled++
			}
			return ctx.Err()

		case now = <-timerC:

		case <-c:
			now = time.Now()
			cancelTimer(timer)
			headOfQueue = true
		}

		rl.mu.Lock()
		rl.updateLocked(now)
		// If we got here, we must be the head of the queue.
		if invariants && (!headOfQueue || rl.mu.waiters.PeekFront().c != c) {
			panic("expected to be head of the queue")
		}

		if rl.mu.current >= n {
			rl.mu.current -= n
			rl.returnTimer(timer)
			rl.popHeadWaiterLocked()
			rl.mu.Unlock()
			// Drain the channel in case there was a race (so that we can reuse it).
			select {
			case <-c:
			default:
			}
			return nil
		}
		timeToFulfill = rl.timeToFulfillLocked(n)
		rl.mu.Unlock()

		if timer == nil {
			timer = rl.getTimer()
			timerC = timer.C
		}
		timer.Reset(timeToFulfill)
	}
}

// Return quota that was unused.
func (rl *RateLimiter) Return(n float64) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.updateLocked(time.Now())

	rl.mu.current += n
	rl.notifyHeadLocked()
}

// Reconfigure the refill rate and burst limit.
func (rl *RateLimiter) Reconfigure(refillRate, burstLimit float64) {
	if refillRate <= 0 || burstLimit < 0 {
		panic("invalid refill rate or burst limit")
	}
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.updateLocked(time.Now())

	rl.mu.rate = refillRate
	rl.mu.burst = burstLimit
	// The head of the queue may need to wait less now.
	rl.notifyHeadLocked()
}

func (rl *RateLimiter) Stats() RateLimiterStats {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.updateLocked(time.Now())
	stats := RateLimiterStats{
		RefillRate: rl.mu.rate,
		BurstLimit: rl.mu.burst,
	}
	if rl.numWaitersLocked() == 0 {
		stats.Available = rl.mu.current
	}
	return stats
}

// RateLimiterStats contains information about the current state of the rate
// limiter.
type RateLimiterStats struct {
	RefillRate float64
	BurstLimit float64

	Available float64

	// TODO(radu): consider keeping track of the total amount of time the
	// rate limiter was exhausted (i.e. there were waiters queued).
}

type rateLimiterWaiter struct {
	// c is the channel on which Acquire is blocked. It gets notified only when a
	// waiter is the head of the queue.
	c chan struct{}
}

// numWaitersLocked returns how many requests (that have not been canceled) are
// waiting in the queue.
func (rl *RateLimiter) numWaitersLocked() int {
	return rl.mu.waiters.Len() - rl.mu.numCanceled
}

// timeToFulfillLocked returns the amount of time we need to wait (at the
// current quota refill rate) until we have enough quota to fulfill a request
// of n.
//
// The method should only be called when the request can't be fulfilled. The
// returned duration is non-zero.
func (rl *RateLimiter) timeToFulfillLocked(n float64) time.Duration {
	delta := n - rl.mu.current
	if invariants && delta < 0 {
		panic("timeToFulfill called with enough quota")
	}
	// Compute the time it will take to get to the needed capacity.
	timeDelta := time.Duration(float64(delta) * float64(time.Second) / float64(rl.mu.rate))
	if timeDelta < time.Nanosecond {
		timeDelta = time.Nanosecond
	}
	return timeDelta
}

// updateLocked updates the current quota based on the refill rate and the time
// that has passed since the last update.
func (rl *RateLimiter) updateLocked(now time.Time) {
	d := now.Sub(rl.mu.lastUpdated).Seconds()
	if d < 0 {
		return
	}
	rl.mu.current += rl.mu.rate * d
	rl.mu.lastUpdated = now
	// If there are waiters, we may need more than the burst to satisfy a waiter.
	// In this case, the limiting to burst happens when the waiter queue is empty
	// again (in popHeadWaiterLocked).
	if rl.mu.current > rl.mu.burst && rl.numWaitersLocked() == 0 {
		rl.mu.current = rl.mu.burst
	}
}

// popHeadWaiterLocked pops the head of the waiters queue, and notifies the next
// one (cleaning up any canceled waiters in-between).
func (rl *RateLimiter) popHeadWaiterLocked() {
	for rl.mu.waiters.PopFront(); rl.mu.waiters.Len() > 0; rl.mu.waiters.PopFront() {
		w := rl.mu.waiters.PeekFront()
		if w.c == nil {
			// This request was canceled, we can just clean it up.
			rl.mu.numCanceled--
			if invariants && rl.mu.numCanceled < 0 {
				panic("negative numCanceled")
			}
			continue
		}
		// Notify the new head of the queue.
		w.c <- struct{}{}
		return
	}
	// Queue is now empty.
	if rl.mu.current > rl.mu.burst {
		rl.mu.current = rl.mu.burst
	}
}

// notifyHeadLocked notifies the head of the queue if there is one, or applies
// the burst limit if there are no waiters. Used when the current tokens or
// refill rate changes due to Return() or Reconfigure().
func (rl *RateLimiter) notifyHeadLocked() {
	if rl.mu.waiters.Len() > 0 {
		select {
		case rl.mu.waiters.PeekFront().c <- struct{}{}:
		default:
			// There is already a pending notification; do nothing.
		}
	} else if rl.mu.current > rl.mu.burst {
		rl.mu.current = rl.mu.burst
	}
}

// chanNoValSyncPool is used to pool allocations of the channels used to notify
// goroutines waiting in Acquire.
var chanNoValSyncPool = sync.Pool{
	New: func() interface{} { return make(chan struct{}, 1) },
}

var rateLimiterQueuePool = MakeQueueBackingPool[rateLimiterWaiter]()

func (rl *RateLimiter) getTimer() *time.Timer {
	timer := rl.timer
	if invariants {
		if timer == nil {
			panic("multiple timer uses")
		}
		rl.timer = nil
	}
	return timer
}

// returnTimerLocked is called when the timer is no longer needed. The timer or
// its channel must not be used again by the caller.
//
// The timer must be stopped or its channel must have been received from since
// the last Reset.
//
// Does nothing if timer is nil.
func (rl *RateLimiter) returnTimer(timer *time.Timer) {
	if timer != nil && invariants {
		assertChanDrained(timer.C)
		rl.timer = timer
	}
}

// cancelTimer stops the timer and drains the channel if the timer has already
// fired. Must not be called concurrently with a receive from the timer channel.
//
// It is safe to call timer.Reset after this function returns.
//
// Does nothing if timer is nil.
func cancelTimer(timer *time.Timer) {
	if timer != nil && !timer.Stop() {
		<-timer.C
	}
}

func assertChanDrained[T any](c <-chan T) {
	select {
	case <-c:
		panic("channel not drained")
	default:
	}
}
