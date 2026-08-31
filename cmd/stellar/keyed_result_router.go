package main

import (
	"context"
	"errors"
	"sync"
)

// keyedResultRouter fans a shared stream of fetchResult values out to one
// independent worker goroutine per key (a channel ID, or a channel+framing
// pair), so that processing (dedup bookkeeping, ordering, and disk I/O) for
// one key can never block delivery or processing for any other key. This is
// the structural guarantee behind "no channel blocks another channel's
// processing": without it, every channel's telemetry funneled through a single
// shared consumer goroutine that wrote to disk synchronously, so one slow or
// stalled write delayed every other channel queued behind it.
//
// A worker is created lazily, on first sight of its key, and runs for the
// lifetime of the router; it is never torn down early. A channel marked
// "done" mid-pass may still receive further messages (an END marker must never
// stop a channel from being checked, in any mode), so a worker that has reached
// its own completion keeps draining its queue rather than exiting.
type keyedResultRouter[K comparable] struct {
	mu      sync.Mutex
	queues  map[K]chan fetchResult
	bufSize int
	ctx     context.Context
	process func(ctx context.Context, key K, in <-chan fetchResult) error

	errOnce sync.Once
	errCh   chan error
}

// newKeyedResultRouter creates a router that starts one goroutine per key
// (running process) the first time that key is routed to. bufSize sizes each
// key's own inbound queue; a full queue only ever backs up that key's own
// producers, never any other key's.
func newKeyedResultRouter[K comparable](
	ctx context.Context,
	bufSize int,
	process func(ctx context.Context, key K, in <-chan fetchResult) error,
) *keyedResultRouter[K] {
	return &keyedResultRouter[K]{
		queues:  make(map[K]chan fetchResult),
		bufSize: bufSize,
		ctx:     ctx,
		process: process,
		errCh:   make(chan error, 1),
	}
}

// Route enqueues res for key's worker, starting the worker on first sight of
// key. The lookup-or-create step is a brief, I/O-free critical section; the
// actual enqueue happens outside the lock, so a full or slow queue for one key
// never blocks routing (or any other key's delivery), while this call may
// itself block if key's own queue is momentarily full (deliberate backpressure,
// scoped to that key alone).
func (r *keyedResultRouter[K]) Route(key K, res fetchResult) {
	r.mu.Lock()
	q, ok := r.queues[key]
	if !ok {
		q = make(chan fetchResult, r.bufSize)
		r.queues[key] = q
		go r.runWorker(key, q)
	}
	r.mu.Unlock()

	select {
	case q <- res:
	case <-r.ctx.Done():
	}
}

// Fail reports a fatal error not associated with any single key (e.g. a
// message decode failure with no channel ID yet known). Equivalent to a
// worker's process returning that error. Only the first reported error is
// kept; cancellation errors are ignored (matching ordinary shutdown).
func (r *keyedResultRouter[K]) Fail(err error) {
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	r.errOnce.Do(func() { r.errCh <- err })
}

func (r *keyedResultRouter[K]) runWorker(key K, q <-chan fetchResult) {
	if err := r.process(r.ctx, key, q); err != nil && !errors.Is(err, context.Canceled) {
		r.errOnce.Do(func() { r.errCh <- err })
	}
}

// Wait blocks until the router's context is done or any worker (or Fail)
// reports a fatal error. On context cancellation it returns ctx.Err() rather
// than nil; callers rely on that value, even though ordinary shutdown paths
// typically discard it.
func (r *keyedResultRouter[K]) Wait() error {
	select {
	case <-r.ctx.Done():
		return r.ctx.Err()
	case err := <-r.errCh:
		return err
	}
}

// keyedDoneSet records which keys (out of some set the caller understands) have
// reached completion, and answers cross-key "is everything done" questions
// without any one key's worker needing direct access to another's state.
// markDone is idempotent (marking an already-done key again is a harmless
// no-op re-check), so callers may call it every time they re-evaluate their own
// completion (e.g. after an auto-close grace period).
type keyedDoneSet[K comparable] struct {
	mu   sync.Mutex
	done map[K]bool
}

func newKeyedDoneSet[K comparable]() *keyedDoneSet[K] {
	return &keyedDoneSet[K]{done: make(map[K]bool)}
}

// markDone records key as done.
func (d *keyedDoneSet[K]) markDone(key K) {
	d.mu.Lock()
	d.done[key] = true
	d.mu.Unlock()
}

// countDone returns how many distinct keys have been marked done; for a
// fixed-size expected set, compare this against the expected total.
func (d *keyedDoneSet[K]) countDone() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.done)
}

// markDoneAndCheckAll records key as done (a no-op, returning false, if key was
// already done, so it never double-fires for the same key) and reports
// whether this call brought the done-set up to total. Marking and the
// completeness check happen under one lock, so if multiple keys finish around
// the same time, exactly one of them observes the transition to "all done".
func (d *keyedDoneSet[K]) markDoneAndCheckAll(key K, total int) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.done[key] {
		return false
	}
	d.done[key] = true
	return len(d.done) >= total
}

// allDoneAmong reports whether every key present in keys has been marked done;
// for a dynamically-discovered expected set (e.g. channel+framing pairs
// discovered as S3 objects are found).
func (d *keyedDoneSet[K]) allDoneAmong(keys map[K]bool) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	for k := range keys {
		if !d.done[k] {
			return false
		}
	}
	return true
}

// keyedSet is a concurrency-safe set, used to track which keys (e.g.
// channel+framing pairs) a scheduler goroutine has discovered as active, so
// worker goroutines can consult it (via Snapshot) without racing the scheduler.
type keyedSet[K comparable] struct {
	mu  sync.Mutex
	set map[K]bool
}

func newKeyedSet[K comparable]() *keyedSet[K] {
	return &keyedSet[K]{set: make(map[K]bool)}
}

func (s *keyedSet[K]) add(key K) {
	s.mu.Lock()
	s.set[key] = true
	s.mu.Unlock()
}

func (s *keyedSet[K]) has(key K) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.set[key]
}

// snapshot returns a shallow copy of the current set, safe for the caller to
// range over without holding the set's lock.
func (s *keyedSet[K]) snapshot() map[K]bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[K]bool, len(s.set))
	for k := range s.set {
		out[k] = true
	}
	return out
}

// sharedCounter tracks a per-key int shared between a scheduler goroutine and
// per-key worker goroutines, e.g. in-flight fetch counts (scheduler
// increments on spawn, worker decrements on receipt) or a worker-owned
// "next write index" the scheduler needs to read to respect its fetch window.
// Safe under concurrent access from both sides.
type sharedCounter[K comparable] struct {
	mu    sync.Mutex
	count map[K]int
}

func newSharedCounter[K comparable]() *sharedCounter[K] {
	return &sharedCounter[K]{count: make(map[K]int)}
}

func (c *sharedCounter[K]) inc(key K) {
	c.mu.Lock()
	c.count[key]++
	c.mu.Unlock()
}

func (c *sharedCounter[K]) dec(key K) {
	c.mu.Lock()
	c.count[key]--
	c.mu.Unlock()
}

func (c *sharedCounter[K]) set(key K, val int) {
	c.mu.Lock()
	c.count[key] = val
	c.mu.Unlock()
}

func (c *sharedCounter[K]) get(key K) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.count[key]
}
