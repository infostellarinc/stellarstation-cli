// Copyright (c) 2026 InfoStellar, Inc. All Rights Reserved.

package commandseq

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	streaming "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/satellitestreamer"
)

type recorder struct {
	mu        sync.Mutex
	acks      int
	released  []string
	discarded []string
	skipped   []string
}

func (r *recorder) Ack(m Message[string], v Verdict, reason string) []byte {
	r.acks++
	return fmt.Appendf(nil, "%s/%d/%d/%s/#%d", m.StreamID, m.Index, v, reason, r.acks)
}

func (r *recorder) Release(m Message[string]) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.released = append(r.released, m.Item)
}

func (r *recorder) Discard(m Message[string], failure streaming.CommandFailure) {
	r.discarded = append(r.discarded, m.Item+":"+failure.String())
}

func (r *recorder) Skip(streamID string, first, last uint64) {
	r.skipped = append(r.skipped, fmt.Sprintf("%s:%d-%d", streamID, first, last))
}

func (r *recorder) releasedItems() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.released...)
}

func newTestLane(t *testing.T, maxHeld int) (*Lane[string], *recorder, *clock.Mock) {
	t.Helper()
	mock := clock.NewMock()
	rec := &recorder{}
	return NewLane[string](Options{GapWait: WaitFor(2 * time.Second), MaxHeld: maxHeld, Clock: mock}, rec), rec, mock
}

func msg(streamID string, index uint64) Message[string] {
	item := fmt.Sprintf("%s%d", streamID, index)
	return Message[string]{StreamID: streamID, MessageID: "id-" + item, Index: index, Payload: []byte(item), Item: item}
}

// offer asserts the verdict and returns the ack.
func offer(t *testing.T, l *Lane[string], m Message[string], want Verdict) string {
	t.Helper()
	v, ack := l.Offer(m)
	require.Equal(t, want, v, "verdict for %s", m.Item)
	return string(ack)
}

func TestLane_InOrderIsSentImmediately(t *testing.T) {
	l, rec, _ := newTestLane(t, 0)
	for i := uint64(1); i <= 3; i++ {
		offer(t, l, msg("a", i), Accepted)
	}
	assert.Equal(t, []string{"a1", "a2", "a3"}, rec.released)
	assert.Empty(t, rec.skipped)
}

// The rule as written: 1, 2, 3, 5 arrive; 1-3 go at once, 5 after the wait,
// and a late 4 is refused.
func TestLane_GapIsSkippedAfterWaitAndLateArrivalRefused(t *testing.T) {
	l, rec, mock := newTestLane(t, 0)
	for _, i := range []uint64{1, 2, 3} {
		offer(t, l, msg("a", i), Accepted)
	}
	mock.Add(300 * time.Millisecond)
	offer(t, l, msg("a", 5), Accepted)
	mock.Add(600 * time.Millisecond)
	offer(t, l, msg("a", 6), Accepted)
	assert.Equal(t, []string{"a1", "a2", "a3"}, rec.released)

	mock.Add(1399 * time.Millisecond)
	l.Expire()
	assert.Len(t, rec.released, 3, "5 has not waited 2s yet")

	mock.Add(time.Millisecond)
	l.Expire()
	assert.Equal(t, []string{"a1", "a2", "a3", "a5", "a6"}, rec.released)
	assert.Equal(t, []string{"a:4-4"}, rec.skipped)

	nack := offer(t, l, msg("a", 4), Refused)
	assert.Contains(t, nack, ReasonArrivedLate)
	assert.Equal(t, nack, offer(t, l, msg("a", 4), Duplicate), "a late duplicate replays the NACK")
	assert.Len(t, rec.released, 5)
}

func TestLane_GapFilledWithinWait(t *testing.T) {
	l, rec, mock := newTestLane(t, 0)
	offer(t, l, msg("a", 1), Accepted)
	offer(t, l, msg("a", 3), Accepted)
	mock.Add(1500 * time.Millisecond)
	offer(t, l, msg("a", 2), Accepted)
	assert.Equal(t, []string{"a1", "a2", "a3"}, rec.released)

	mock.Add(time.Minute)
	l.Expire()
	assert.Empty(t, rec.skipped)
}

func TestLane_TrailingGapHoldsNothing(t *testing.T) {
	l, rec, mock := newTestLane(t, 0)
	offer(t, l, msg("a", 1), Accepted)
	mock.Add(time.Minute)
	l.Expire()
	offer(t, l, msg("a", 2), Accepted)
	assert.Equal(t, []string{"a1", "a2"}, rec.released)
	assert.Empty(t, rec.skipped)
}

// Each gap waits for the message right above it.
func TestLane_EachGapWaitsForTheMessageAboveIt(t *testing.T) {
	l, rec, mock := newTestLane(t, 0)
	offer(t, l, msg("a", 1), Accepted)
	offer(t, l, msg("a", 5), Accepted)
	mock.Add(1500 * time.Millisecond)
	offer(t, l, msg("a", 3), Accepted)

	mock.Add(500 * time.Millisecond)
	l.Expire()
	assert.Equal(t, []string{"a1"}, rec.released, "3 has waited only 0.5s")

	mock.Add(1500 * time.Millisecond)
	l.Expire()
	assert.Equal(t, []string{"a1", "a3", "a5"}, rec.released)
	assert.Equal(t, []string{"a:2-2", "a:4-4"}, rec.skipped)
}

func TestLane_Duplicates(t *testing.T) {
	l, rec, _ := newTestLane(t, 0)
	first := offer(t, l, msg("a", 1), Accepted)

	assert.Equal(t, first, offer(t, l, msg("a", 1), Duplicate), "the replay is byte-identical")

	otherPayload := msg("a", 1)
	otherPayload.Payload = []byte("different")
	assert.Contains(t, offer(t, l, otherPayload, Refused), ReasonIndexReused)

	otherID := msg("a", 1)
	otherID.MessageID = "other"
	assert.Contains(t, offer(t, l, otherID, Refused), ReasonIndexReused)

	assert.Equal(t, first, offer(t, l, msg("a", 1), Duplicate), "a reuse NACK does not replace the first answer")
	assert.Equal(t, []string{"a1"}, rec.released)
}

func TestLane_HeldDuplicateIsReplayed(t *testing.T) {
	l, rec, _ := newTestLane(t, 0)
	ack := offer(t, l, msg("a", 2), Accepted)
	assert.Equal(t, ack, offer(t, l, msg("a", 2), Duplicate))
	offer(t, l, msg("a", 1), Accepted)
	assert.Equal(t, []string{"a1", "a2"}, rec.released)
}

func TestLane_IndexZeroIsRefused(t *testing.T) {
	l, rec, _ := newTestLane(t, 0)
	first := offer(t, l, msg("a", 0), Refused)
	assert.Contains(t, first, ReasonIndexRequired)
	assert.NotEqual(t, first, offer(t, l, msg("a", 0), Refused), "index 0 is not recorded")
	assert.Empty(t, rec.released)
}

func TestLane_HoldBufferFull(t *testing.T) {
	l, rec, mock := newTestLane(t, 2)
	offer(t, l, msg("a", 2), Accepted)
	offer(t, l, msg("a", 3), Accepted)
	assert.Contains(t, offer(t, l, msg("a", 4), Refused), ReasonHoldBufferFull)

	mock.Add(2 * time.Second)
	l.Expire()
	assert.Equal(t, []string{"a2", "a3"}, rec.released)
	offer(t, l, msg("a", 4), Accepted)
	assert.Equal(t, []string{"a2", "a3", "a4"}, rec.released, "an unchanged resend is a fresh arrival")
}

func TestLane_NewStreamSupersedesOld(t *testing.T) {
	l, rec, _ := newTestLane(t, 0)
	offer(t, l, msg("a", 1), Accepted)
	heldAck := offer(t, l, msg("a", 3), Accepted)

	offer(t, l, msg("b", 1), Accepted)
	assert.Equal(t, []string{"a3:COMMAND_FAILURE_SUPERSEDED"}, rec.discarded)
	assert.Contains(t, offer(t, l, msg("a", 2), Refused), ReasonSuperseded)
	assert.Equal(t, heldAck, offer(t, l, msg("a", 3), Duplicate), "answered messages are still replayed")

	offer(t, l, msg("b", 2), Accepted)
	assert.Equal(t, []string{"a1", "b1", "b2"}, rec.released)
}

// Two clients send 1, 2, 3 and A, B, C, arriving as 1, A, 2, B, C, 3: stream
// A takes command authority at its first message, so 2 and 3 are refused.
func TestLane_InterleavedStreamsLatestHasAuthority(t *testing.T) {
	l, rec, _ := newTestLane(t, 0)
	steps := []struct {
		m    Message[string]
		want Verdict
	}{
		{msg("x", 1), Accepted},
		{msg("y", 1), Accepted},
		{msg("x", 2), Refused},
		{msg("y", 2), Accepted},
		{msg("y", 3), Accepted},
		{msg("x", 3), Refused},
	}
	for _, s := range steps {
		ack := offer(t, l, s.m, s.want)
		if s.want == Refused {
			assert.Contains(t, ack, ReasonSuperseded)
		}
	}
	assert.Equal(t, []string{"x1", "y1", "y2", "y3"}, rec.released, "each accepted command is sent at once")
	assert.Empty(t, rec.discarded, "stream x held nothing")
}

func TestLane_RefusalUsesUpItsIndex(t *testing.T) {
	l, rec, _ := newTestLane(t, 0)
	refused := msg("a", 1)
	refused.Refusal = ReasonNoCommands
	assert.Contains(t, offer(t, l, refused, Refused), ReasonNoCommands)
	offer(t, l, msg("a", 2), Accepted)

	heldRefusal := msg("a", 4)
	heldRefusal.Refusal = ReasonNoCommands
	offer(t, l, heldRefusal, Refused)
	offer(t, l, msg("a", 3), Accepted)
	offer(t, l, msg("a", 5), Accepted)

	assert.Equal(t, []string{"a2", "a3", "a5"}, rec.released)
	assert.Empty(t, rec.skipped)
}

func TestLane_CloseDiscardsHeldAndRefusesLater(t *testing.T) {
	l, rec, _ := newTestLane(t, 0)
	ack := offer(t, l, msg("a", 1), Accepted)
	offer(t, l, msg("a", 3), Accepted)

	l.Close(streaming.CommandFailure_COMMAND_FAILURE_PLAN_ENDED)
	assert.Equal(t, []string{"a3:COMMAND_FAILURE_PLAN_ENDED"}, rec.discarded)
	assert.Contains(t, offer(t, l, msg("a", 2), Refused), ReasonOutsideContact)
	assert.Equal(t, ack, offer(t, l, msg("a", 1), Duplicate))
	assert.Equal(t, []string{"a1"}, rec.released)
}

func TestLane_RunExpiresOnTheClock(t *testing.T) {
	l, rec, mock := newTestLane(t, 0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		l.Run(ctx)
		close(done)
	}()

	offer(t, l, msg("a", 1), Accepted)
	offer(t, l, msg("a", 3), Accepted)
	require.Eventually(t, func() bool {
		mock.Add(100 * time.Millisecond)
		return len(rec.releasedItems()) == 2
	}, 5*time.Second, time.Millisecond)
	assert.Equal(t, []string{"a1", "a3"}, rec.releasedItems())

	cancel()
	<-done
}

// Filling a gap re-arms Run on what is still held. Run waits on the lowest
// held index, so once that message goes, the timer must move to the next one
// rather than stay on a deadline that no longer matters.
func TestLane_FillingAGapReArmsRunOnTheRemainingHold(t *testing.T) {
	l, rec, mock := newTestLane(t, 0)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		l.Run(ctx)
		close(done)
	}()

	start := mock.Now()
	offer(t, l, msg("a", 1), Accepted)
	offer(t, l, msg("a", 5), Accepted) // waits for 2 to 4 until t=2s
	mock.Add(time.Second)
	offer(t, l, msg("a", 3), Accepted) // waits for 2 until t=3s, and is now the lowest held
	mock.Add(500 * time.Millisecond)
	offer(t, l, msg("a", 2), Accepted) // releases 2 and 3; 5 is the lowest held again
	require.Eventually(t, func() bool { return len(rec.releasedItems()) == 3 }, time.Second, time.Millisecond)

	// 5 goes at t=2s, not at the t=3s deadline 3 had.
	require.Eventually(t, func() bool {
		mock.Add(100 * time.Millisecond)
		return len(rec.releasedItems()) == 4
	}, 5*time.Second, time.Millisecond)
	assert.Less(t, mock.Since(start), 2*time.Second+200*time.Millisecond)
	assert.Equal(t, []string{"a1", "a2", "a3", "a5"}, rec.releasedItems())

	cancel()
	<-done
}

func TestLane_ConcurrentArrivalsAreReleasedInOrderOnce(t *testing.T) {
	l, rec, _ := newTestLane(t, 1000)
	const n = 500
	order := rand.New(rand.NewSource(1)).Perm(n)

	var wg sync.WaitGroup
	for _, i := range order {
		for copies := 0; copies < 2; copies++ {
			wg.Add(1)
			go func(index uint64) {
				defer wg.Done()
				l.Offer(msg("a", index))
			}(uint64(i + 1))
		}
	}
	wg.Wait()

	want := make([]string, n)
	for i := range want {
		want[i] = fmt.Sprintf("a%d", i+1)
	}
	assert.Equal(t, want, rec.releasedItems())
}

func TestVerdictString(t *testing.T) {
	assert.Equal(t, "accepted", Accepted.String())
	assert.Equal(t, "refused", Refused.String())
	assert.Equal(t, "duplicate", Duplicate.String())
	assert.Equal(t, "unknown", Verdict(9).String())
}

func TestLane_NextIndexIsAcceptedWhenHoldIsFull(t *testing.T) {
	l, rec, _ := newTestLane(t, 1)
	offer(t, l, msg("a", 3), Accepted)
	assert.Contains(t, offer(t, l, msg("a", 2), Refused), ReasonHoldBufferFull)
	offer(t, l, msg("a", 1), Accepted)
	assert.Equal(t, []string{"a1"}, rec.released, "3 still waits for 2")
}

func TestReasonTextCoversEveryCode(t *testing.T) {
	for _, code := range []string{
		ReasonMalformedPayload, ReasonWrongPlan, ReasonNoCommands, ReasonNoConfiguration, ReasonNoTransmitter,
		ReasonIndexRequired, ReasonIndexReused, ReasonArrivedLate, ReasonHoldBufferFull, ReasonSuperseded,
	} {
		assert.NotEmpty(t, ReasonText(code), code)
	}
	assert.Empty(t, ReasonText("unknown"))
}

// Reason codes are a public wire contract: clients, SCL rows and the pass API
// carry them verbatim, so a rename is a breaking change.
func TestReasonCodesArePinned(t *testing.T) {
	for code, want := range map[string]string{
		ReasonMalformedPayload: "malformed_payload",
		ReasonWrongPlan:        "wrong_plan",
		ReasonNoCommands:       "no_commands",
		ReasonNoConfiguration:  "no_configuration",
		ReasonNoTransmitter:    "no_transmitter",
		ReasonIndexRequired:    "index_required",
		ReasonIndexReused:      "index_reused",
		ReasonArrivedLate:      "arrived_late",
		ReasonHoldBufferFull:   "hold_buffer_full",
		ReasonSuperseded:       "superseded",
	} {
		assert.Equal(t, want, code)
	}
}

func TestLane_ZeroOptionsUseTheDefaultGapWait(t *testing.T) {
	mock := clock.NewMock()
	rec := &recorder{}
	l := NewLane[string](Options{Clock: mock}, rec)
	l.Expire()
	assert.Empty(t, rec.skipped, "an idle lane has nothing to expire")

	offer(t, l, msg("a", 1), Accepted)
	offer(t, l, msg("a", 3), Accepted)
	mock.Add(DefaultGapWait - time.Millisecond)
	l.Expire()
	assert.Equal(t, []string{"a1"}, rec.released)
	mock.Add(time.Millisecond)
	l.Expire()
	assert.Equal(t, []string{"a1", "a3"}, rec.released)
	assert.Equal(t, []string{"a:2-2"}, rec.skipped)
}

// "Not allowed to wait": 1, 2, 3, 5 arrive; 5 goes at once, skipping 4, and a
// late 4 is refused.
func TestLane_ZeroGapWaitSkipsAGapAtOnce(t *testing.T) {
	mock := clock.NewMock()
	rec := &recorder{}
	l := NewLane[string](Options{GapWait: WaitFor(0), Clock: mock}, rec)
	for _, i := range []uint64{1, 2, 3, 5} {
		offer(t, l, msg("a", i), Accepted)
	}
	assert.Equal(t, []string{"a1", "a2", "a3", "a5"}, rec.released, "sent without waiting for the clock")
	assert.Equal(t, []string{"a:4-4"}, rec.skipped)
	_, ok := l.nextDeadline()
	assert.False(t, ok, "nothing is left to expire")

	assert.Contains(t, offer(t, l, msg("a", 4), Refused), ReasonArrivedLate)
	assert.Len(t, rec.released, 4)
}

// "Not allowed to skip": a held message waits for the missing index however
// long it takes, and is reported not sent if the pass ends first.
func TestLane_NeverSkipHoldsUntilTheGapFills(t *testing.T) {
	mock := clock.NewMock()
	rec := &recorder{}
	l := NewLane[string](Options{GapWait: NeverSkip, Clock: mock}, rec)
	offer(t, l, msg("a", 1), Accepted)
	offer(t, l, msg("a", 3), Accepted)
	_, ok := l.nextDeadline()
	assert.False(t, ok, "Run has no deadline to wait for")

	mock.Add(24 * time.Hour)
	l.Expire()
	assert.Equal(t, []string{"a1"}, rec.released)
	assert.Empty(t, rec.skipped)

	offer(t, l, msg("a", 2), Accepted)
	assert.Equal(t, []string{"a1", "a2", "a3"}, rec.released)

	offer(t, l, msg("a", 5), Accepted)
	l.Close(streaming.CommandFailure_COMMAND_FAILURE_PLAN_ENDED)
	assert.Equal(t, []string{"a5:COMMAND_FAILURE_PLAN_ENDED"}, rec.discarded)
	assert.Empty(t, rec.skipped)
}

// A never-skipping lane still bounds what it holds.
func TestLane_NeverSkipStillRefusesPastTheHoldLimit(t *testing.T) {
	rec := &recorder{}
	l := NewLane[string](Options{GapWait: NeverSkip, MaxHeld: 1, Clock: clock.NewMock()}, rec)
	offer(t, l, msg("a", 2), Accepted)
	assert.Contains(t, offer(t, l, msg("a", 3), Refused), ReasonHoldBufferFull)
}

// A message refused on its content does not take command authority.
func TestLane_RefusedFirstMessageDoesNotSupersede(t *testing.T) {
	l, rec, _ := newTestLane(t, 8)
	offer(t, l, msg("x", 1), Accepted)
	offer(t, l, msg("x", 3), Accepted)
	y1 := msg("y", 1)
	y1.Refusal = ReasonNoCommands
	assert.Contains(t, offer(t, l, y1, Refused), ReasonNoCommands)
	offer(t, l, msg("x", 2), Accepted)
	assert.Equal(t, []string{"x1", "x2", "x3"}, rec.releasedItems())
	assert.Empty(t, rec.discarded)

	offer(t, l, msg("y", 2), Accepted)
	offer(t, l, msg("x", 4), Refused)
	assert.Equal(t, []string{"x1", "x2", "x3", "y2"}, rec.releasedItems())
}

// A message refused on content is answered but not held: it starts no wait,
// so a valid lower index arriving later is still sent, and its own index is
// consumed in passing.
func TestLane_RefusalAheadOfAGapStartsNoWait(t *testing.T) {
	l, rec, mock := newTestLane(t, 0)
	offer(t, l, msg("a", 1), Accepted)
	bad := msg("a", 3)
	bad.Refusal = ReasonUnsupported
	offer(t, l, bad, Refused)
	mock.Add(10 * time.Second)
	l.Expire()
	assert.Equal(t, []string{"a1"}, rec.released)
	assert.Empty(t, rec.skipped, "a refusal declares nothing missing")

	offer(t, l, msg("a", 2), Accepted)
	offer(t, l, msg("a", 4), Accepted)
	assert.Equal(t, []string{"a1", "a2", "a4"}, rec.released, "2 is sent, 3 was refused, 4 follows at once")
}

// A refusal arriving after a later message was held neither releases nor
// delays it: the held message still waits for the real gap.
func TestLane_RefusalDoesNotFillAGapForAHeldMessage(t *testing.T) {
	l, rec, mock := newTestLane(t, 0)
	offer(t, l, msg("a", 3), Accepted)
	bad := msg("a", 2)
	bad.Refusal = ReasonUnsupported
	offer(t, l, bad, Refused)
	assert.Empty(t, rec.released)

	offer(t, l, msg("a", 1), Accepted)
	assert.Equal(t, []string{"a1", "a3"}, rec.released, "1 sent, 2 consumed, 3 released")
	mock.Add(5 * time.Second)
	l.Expire()
	assert.Empty(t, rec.skipped)
}

// Refused messages from a stream without authority are never held, so they
// cannot fill its hold buffer.
func TestLane_RefusedMessagesDoNotFillTheHoldBuffer(t *testing.T) {
	l, rec, _ := newTestLane(t, 2)
	offer(t, l, msg("a", 1), Accepted)
	for i := uint64(2); i <= 10; i++ {
		bad := msg("b", i)
		bad.Refusal = ReasonUnsupported
		ack := offer(t, l, bad, Refused)
		assert.Contains(t, ack, ReasonUnsupported)
	}
	offer(t, l, msg("b", 12), Accepted)
	assert.Equal(t, []string{"a1"}, rec.released, "b12 is held for b1 and b11")
	assert.Empty(t, rec.discarded)
}
