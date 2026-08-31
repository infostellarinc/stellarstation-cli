package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestKeyedResultRouter_RoutesToIndependentWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var mu sync.Mutex
	received := map[string][]int{}

	router := newKeyedResultRouter(ctx, 10, func(ctx context.Context, key string, in <-chan fetchResult) error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case res := <-in:
				mu.Lock()
				received[key] = append(received[key], res.Index)
				mu.Unlock()
			}
		}
	})

	for i := 1; i <= 3; i++ {
		router.Route("a", fetchResult{ChannelID: "a", Index: i})
	}
	for i := 1; i <= 2; i++ {
		router.Route("b", fetchResult{ChannelID: "b", Index: i})
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		done := len(received["a"]) == 3 && len(received["b"]) == 2
		mu.Unlock()
		if done {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if got := received["a"]; len(got) != 3 {
		t.Errorf("key a received %v, want 3 results", got)
	}
	if got := received["b"]; len(got) != 2 {
		t.Errorf("key b received %v, want 2 results", got)
	}
}

// TestKeyedResultRouter_OneKeyBlockingDoesNotBlockAnother is the core guarantee
// this type exists for: a key whose worker never drains its queue (simulating a
// stalled disk write) must never prevent routing or processing for any other
// key. Without per-key queues, a single shared channel drained by one consumer
// would stall every key behind the blocked one.
func TestKeyedResultRouter_OneKeyBlockingDoesNotBlockAnother(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	blockA := make(chan struct{})
	var mu sync.Mutex
	var bCount int

	router := newKeyedResultRouter(ctx, 1, func(ctx context.Context, key string, in <-chan fetchResult) error {
		if key == "a" {
			// Never drains its queue until the test releases it.
			select {
			case <-blockA:
			case <-ctx.Done():
			}
			return ctx.Err()
		}
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-in:
				mu.Lock()
				bCount++
				mu.Unlock()
			}
		}
	})

	// Fill key A's queue (bufSize=1) and one more to guarantee its worker's
	// goroutine is blocked on the stalled key, then route many results for key B
	// from a separate goroutine (Route may legitimately block on A's full queue,
	// so it must run concurrently, not inline in the test body).
	router.Route("a", fetchResult{ChannelID: "a", Index: 1})
	go func() {
		for i := 2; i <= 5; i++ {
			router.Route("a", fetchResult{ChannelID: "a", Index: i})
		}
	}()

	const wantB = 50
	done := make(chan struct{})
	go func() {
		for i := 1; i <= wantB; i++ {
			router.Route("b", fetchResult{ChannelID: "b", Index: i})
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("routing to key b did not complete; key a's stalled worker blocked key b")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := bCount
		mu.Unlock()
		if got == wantB {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if bCount != wantB {
		t.Errorf("key b processed %d results, want %d; key a's stall affected key b's processing", bCount, wantB)
	}

	close(blockA)
}

func TestKeyedResultRouter_FailReturnsErrorFromWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	router := newKeyedResultRouter(ctx, 1, func(ctx context.Context, key string, in <-chan fetchResult) error {
		<-ctx.Done()
		return ctx.Err()
	})

	wantErr := errors.New("boom")
	router.Fail(wantErr)

	if err := router.Wait(); !errors.Is(err, wantErr) {
		t.Errorf("Wait() = %v, want %v", err, wantErr)
	}
}

func TestKeyedResultRouter_FailIgnoresCancellationAndNil(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	router := newKeyedResultRouter(ctx, 1, func(ctx context.Context, key string, in <-chan fetchResult) error {
		<-ctx.Done()
		return ctx.Err()
	})

	router.Fail(nil)
	router.Fail(context.Canceled)

	cancel()
	if err := router.Wait(); !errors.Is(err, context.Canceled) {
		t.Errorf("Wait() = %v, want context.Canceled (from ctx.Done(), not a Fail call)", err)
	}
}

func TestKeyedResultRouter_WorkerErrorPropagatesToWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	wantErr := errors.New("write failed")
	router := newKeyedResultRouter(ctx, 1, func(ctx context.Context, key string, in <-chan fetchResult) error {
		<-in
		return wantErr
	})

	router.Route("x", fetchResult{ChannelID: "x", Index: 1})

	if err := router.Wait(); !errors.Is(err, wantErr) {
		t.Errorf("Wait() = %v, want %v", err, wantErr)
	}
}

func TestKeyedResultRouter_ContextCancellationReturnsCtxErr(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	router := newKeyedResultRouter(ctx, 1, func(ctx context.Context, key string, in <-chan fetchResult) error {
		<-ctx.Done()
		return ctx.Err()
	})

	cancel()
	if err := router.Wait(); !errors.Is(err, context.Canceled) {
		t.Errorf("Wait() = %v, want context.Canceled", err)
	}
}

func TestKeyedDoneSet_MarkDoneAndCheckAll(t *testing.T) {
	d := newKeyedDoneSet[string]()

	if got := d.markDoneAndCheckAll("a", 2); got {
		t.Error("markDoneAndCheckAll(a, 2) with only 1 of 2 done = true, want false")
	}
	if got := d.markDoneAndCheckAll("a", 2); got {
		t.Error("re-marking an already-done key must not report a fresh completion")
	}
	if got := d.markDoneAndCheckAll("b", 2); !got {
		t.Error("markDoneAndCheckAll(b, 2) with 2 of 2 done = false, want true")
	}
	if got := d.countDone(); got != 2 {
		t.Errorf("countDone() = %d, want 2", got)
	}
}

func TestKeyedDoneSet_AllDoneAmong(t *testing.T) {
	d := newKeyedDoneSet[string]()
	d.markDone("a")

	if d.allDoneAmong(map[string]bool{"a": true, "b": true}) {
		t.Error("allDoneAmong must be false when b is not done")
	}
	d.markDone("b")
	if !d.allDoneAmong(map[string]bool{"a": true, "b": true}) {
		t.Error("allDoneAmong must be true once every key in the set is done")
	}
	if !d.allDoneAmong(map[string]bool{}) {
		t.Error("allDoneAmong of an empty set must be vacuously true")
	}
}

func TestKeyedSet_AddHasSnapshot(t *testing.T) {
	s := newKeyedSet[string]()
	if s.has("a") {
		t.Error("has(a) on empty set = true, want false")
	}
	s.add("a")
	s.add("b")
	if !s.has("a") || !s.has("b") {
		t.Error("has() must be true for added keys")
	}
	if s.has("c") {
		t.Error("has(c) = true for a key never added")
	}
	snap := s.snapshot()
	if len(snap) != 2 || !snap["a"] || !snap["b"] {
		t.Errorf("snapshot() = %v, want {a:true, b:true}", snap)
	}
	// Mutating the snapshot must not affect the set.
	snap["c"] = true
	if s.has("c") {
		t.Error("mutating a snapshot leaked back into the set")
	}
}

func TestSharedCounter_IncDecSetGet(t *testing.T) {
	c := newSharedCounter[string]()
	if got := c.get("x"); got != 0 {
		t.Errorf("get on unseen key = %d, want 0", got)
	}
	c.inc("x")
	c.inc("x")
	if got := c.get("x"); got != 2 {
		t.Errorf("after two inc, get = %d, want 2", got)
	}
	c.dec("x")
	if got := c.get("x"); got != 1 {
		t.Errorf("after dec, get = %d, want 1", got)
	}
	c.set("x", 42)
	if got := c.get("x"); got != 42 {
		t.Errorf("after set(42), get = %d, want 42", got)
	}
	// A different key is independent.
	if got := c.get("y"); got != 0 {
		t.Errorf("unrelated key y = %d, want 0", got)
	}
}
