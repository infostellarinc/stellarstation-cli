// Copyright (c) 2026 InfoStellar, Inc. All Rights Reserved.

package commandstatus

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	streaming "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/satellitestreamer"
)

const testTopic = "pass/p/channel/c/uplink/status"

type published struct {
	topic string
	batch *streaming.CommandStatusBatch
}

type recordingPublisher struct {
	mu  sync.Mutex
	got []published
	// failures is how many publishes fail before they start succeeding.
	failures int
}

func (p *recordingPublisher) Publish(topic string, payload []byte) error {
	b := &streaming.CommandStatusBatch{}
	if err := proto.Unmarshal(payload, b); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failures > 0 {
		p.failures--
		return errors.New("offline")
	}
	p.got = append(p.got, published{topic: topic, batch: b})
	return nil
}

func (p *recordingPublisher) statuses() []*streaming.CommandStatus {
	p.mu.Lock()
	defer p.mu.Unlock()
	var all []*streaming.CommandStatus
	for _, pub := range p.got {
		all = append(all, pub.batch.GetStatuses()...)
	}
	return all
}

func newTestTracker(maxBatch int) (*Tracker, *recordingPublisher, *clock.Mock) {
	pub := &recordingPublisher{}
	mock := clock.NewMock()
	return NewTracker(Options{MaxBatch: maxBatch, Clock: mock}, pub, slog.New(slog.DiscardHandler)), pub, mock
}

func testRef(frames uint32, final streaming.CommandStage) Ref {
	return Ref{
		StatusTopic: testTopic,
		MessageType: MessageTypeUplink,
		PassID:      "p",
		ChannelID:   "c",
		StreamID:    "s",
		MessageID:   "m",
		Index:       7,
		FrameCount:  frames,
		FinalStage:  final,
	}
}

type statusSummary struct {
	frame        uint32
	stage        streaming.CommandStage
	outcome      streaming.CommandOutcome
	final        bool
	messageFinal bool
}

func summarize(statuses []*streaming.CommandStatus) []statusSummary {
	out := make([]statusSummary, len(statuses))
	for i, s := range statuses {
		out[i] = statusSummary{s.GetFrameIndex(), s.GetStage(), s.GetOutcome(), s.GetFinal(), s.GetMessageFinal()}
	}
	return out
}

var (
	dispatched = streaming.CommandStage_COMMAND_STAGE_DISPATCHED
	radiated   = streaming.CommandStage_COMMAND_STAGE_RADIATED
	unset      = streaming.CommandStage_COMMAND_STAGE_UNSPECIFIED
	inProgress = streaming.CommandOutcome_COMMAND_OUTCOME_IN_PROGRESS
	succeeded  = streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED
	failed     = streaming.CommandOutcome_COMMAND_OUTCOME_FAILED
	unknown    = streaming.CommandOutcome_COMMAND_OUTCOME_UNKNOWN
)

func TestTracker_DispatchedThenRadiated(t *testing.T) {
	tr, pub, mock := newTestTracker(0)
	ref := testRef(1, radiated)
	tr.Register(ref)
	r := tr.Frames(ref, 0, 0)

	r.Dispatched(nil)
	at := mock.Now().Add(-time.Second)
	r.Radiated(at)
	tr.Flush()

	got := pub.statuses()
	assert.Equal(t, []statusSummary{
		{0, dispatched, inProgress, false, false},
		{0, radiated, succeeded, true, true},
	}, summarize(got))
	s := got[1]
	assert.Equal(t, at.UTC(), s.GetRadiatedAt().AsTime())
	assert.Equal(t, succeeded, s.GetMessageOutcome())
	assert.Equal(
		t,
		streaming.CommandOutcome_COMMAND_OUTCOME_UNSPECIFIED,
		got[0].GetMessageOutcome(),
		"unset until the message is final",
	)
	assert.Equal(t, "m", s.GetAckedMessageId())
	assert.Equal(t, "s", s.GetStreamId())
	assert.Equal(t, "p", s.GetPassId())
	assert.Equal(t, uint64(7), s.GetIndex())
	assert.Equal(t, "c", s.GetChannelId())
	assert.Equal(t, MessageTypeUplink, s.GetMessageType())
	assert.Equal(t, uint32(1), s.GetFrameCount())
	assert.NotEmpty(t, s.GetMessageId())
	assert.NotEqual(t, got[0].GetMessageId(), s.GetMessageId())
	assert.Equal(t, testTopic, pub.got[0].topic)
}

// The report before the device call never counts as the final stage, so a
// device whose last stage is DISPATCHED can still fail.
func TestTracker_DispatchingIsNotTheFinalStage(t *testing.T) {
	tr, pub, _ := newTestTracker(0)
	ref := testRef(1, dispatched)
	tr.Register(ref)
	r := tr.Frames(ref, 0, 0)
	r.Dispatching()
	r.Dispatched(errors.New("modem offline"))
	tr.Flush()
	got := pub.statuses()
	assert.Equal(t, []statusSummary{
		{0, dispatched, inProgress, false, false},
		{0, dispatched, failed, true, true},
	}, summarize(got))
	assert.Equal(t, streaming.CommandFailure_COMMAND_FAILURE_STATION_ERROR, got[1].GetFailure())
}

func TestTracker_DeviceErrorDefaultsToDispatchedFailed(t *testing.T) {
	tr, pub, _ := newTestTracker(0)
	ref := testRef(1, radiated)
	tr.Register(ref)
	tr.Frames(ref, 0, 0).Dispatched(&Error{Reason: "rejected"})
	tr.Flush()
	got := pub.statuses()
	require.Len(t, got, 1)
	assert.Equal(t, []statusSummary{{0, dispatched, failed, true, true}}, summarize(got))
	assert.Equal(t, "rejected", got[0].GetReason())
}

func TestTracker_RegisterAfterCloseIsIgnored(t *testing.T) {
	tr, pub, _ := newTestTracker(0)
	tr.Close()
	ref := testRef(1, radiated)
	tr.Register(ref)
	tr.Frames(ref, 0, 0).Dispatched(nil)
	tr.Flush()
	assert.Empty(t, pub.statuses())
}

// A skip notice after Close has no flush left to carry it, so it is dropped
// rather than queued for good.
func TestTracker_SkipAfterCloseIsIgnored(t *testing.T) {
	tr, pub, _ := newTestTracker(0)
	tr.Close()
	tr.Skip(testTopic, &streaming.SkippedRange{StreamId: "s", FirstIndex: 4, LastIndex: 5})
	tr.Flush()
	assert.Empty(t, pub.got)
	tr.mu.Lock()
	defer tr.mu.Unlock()
	assert.Empty(t, tr.pending)
}

func TestTracker_ReachingFinalStageSucceeds(t *testing.T) {
	tr, pub, _ := newTestTracker(0)
	ref := testRef(1, dispatched)
	tr.Register(ref)
	tr.Frames(ref, 0, 0).Dispatched(nil)
	tr.Flush()
	assert.Equal(t, []statusSummary{{0, dispatched, succeeded, true, true}}, summarize(pub.statuses()))
}

func TestTracker_StagesOnlyMoveForwardAndFirstFinalWins(t *testing.T) {
	tr, pub, _ := newTestTracker(0)
	ref := testRef(1, radiated)
	tr.Register(ref)
	r := tr.Frames(ref, 0, 0)

	r.Dispatched(nil)
	r.Dispatched(nil)
	r.Radiated(time.Now())
	r.Dispatched(nil)
	r.NotSent(streaming.CommandFailure_COMMAND_FAILURE_PLAN_ENDED, "late")
	tr.Flush()

	assert.Equal(t, []statusSummary{
		{0, dispatched, inProgress, false, false},
		{0, radiated, succeeded, true, true},
	}, summarize(pub.statuses()))
}

// Each frame gets its own status, and a report for several frames moves only
// those that can move.
func TestTracker_FramesAreReportedSeparately(t *testing.T) {
	tr, pub, _ := newTestTracker(0)
	ref := testRef(4, radiated)
	tr.Register(ref)

	tr.Frames(ref, 1, 1).Radiated(time.Now())
	tr.Frames(ref, 0, 3).Radiated(time.Now())
	tr.Flush()

	assert.Equal(t, []statusSummary{
		{1, radiated, succeeded, true, false},
		{0, radiated, succeeded, true, false},
		{2, radiated, succeeded, true, false},
		{3, radiated, succeeded, true, true},
	}, summarize(pub.statuses()), "frame 1 was already final")
}

func TestTracker_MessageOutcome(t *testing.T) {
	partial := streaming.CommandOutcome_COMMAND_OUTCOME_PARTIAL
	tests := []struct {
		name     string
		outcomes []streaming.CommandOutcome
		want     streaming.CommandOutcome
	}{
		{"all succeeded", []streaming.CommandOutcome{succeeded, succeeded}, succeeded},
		{"all failed", []streaming.CommandOutcome{failed, failed}, failed},
		{"unknown beats succeeded", []streaming.CommandOutcome{succeeded, unknown}, unknown},
		{
			"unknown beats failed: the frame may have been transmitted",
			[]streaming.CommandOutcome{failed, unknown},
			unknown,
		},
		{"some succeeded and the rest failed", []streaming.CommandOutcome{succeeded, failed}, partial},
		{"a partly applied configuration", []streaming.CommandOutcome{partial}, partial},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr, pub, _ := newTestTracker(0)
			ref := testRef(uint32(len(tt.outcomes)), radiated)
			tr.Register(ref)
			for i, o := range tt.outcomes {
				tr.Frames(ref, uint32(i), uint32(i)).Report(Update{Stage: dispatched, Outcome: o})
			}
			tr.Flush()
			got := pub.statuses()
			assert.Equal(t, tt.want, got[len(got)-1].GetMessageOutcome())
		})
	}
}

func TestTracker_DispatchedErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want *streaming.CommandStatus
	}{
		{
			name: "device error",
			err: &Error{
				Stage:        dispatched,
				Outcome:      failed,
				Failure:      streaming.CommandFailure_COMMAND_FAILURE_INVALID_COMMAND,
				ProviderCode: "cortex-tcu:2",
				Reason:       "invalid checksum",
			},
			want: &streaming.CommandStatus{
				Stage:        dispatched,
				Outcome:      failed,
				Failure:      streaming.CommandFailure_COMMAND_FAILURE_INVALID_COMMAND,
				ProviderCode: "cortex-tcu:2",
				Reason:       "invalid checksum",
			},
		},
		{
			name: "wrapped device error",
			err:  errors.Join(errors.New("context"), &Error{Stage: dispatched, Outcome: unknown}),
			want: &streaming.CommandStatus{Stage: dispatched, Outcome: unknown},
		},
		{
			name: "other error",
			err:  errors.New("socket closed"),
			want: &streaming.CommandStatus{
				Stage:   dispatched,
				Outcome: failed,
				Failure: streaming.CommandFailure_COMMAND_FAILURE_STATION_ERROR,
				Reason:  "socket closed",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr, pub, _ := newTestTracker(0)
			ref := testRef(1, radiated)
			tr.Register(ref)
			tr.Frames(ref, 0, 0).Dispatched(tt.err)
			tr.Flush()

			got := pub.statuses()
			require.Len(t, got, 1)
			assert.Equal(t, tt.want.GetStage(), got[0].GetStage())
			assert.Equal(t, tt.want.GetOutcome(), got[0].GetOutcome())
			assert.Equal(t, tt.want.GetFailure(), got[0].GetFailure())
			assert.Equal(t, tt.want.GetProviderCode(), got[0].GetProviderCode())
			assert.Equal(t, tt.want.GetReason(), got[0].GetReason())
			assert.True(t, got[0].GetFinal())
		})
	}
}

// The pass ends with frames 0 and 1 radiated, frame 2 in flight at the
// device and frames 3 and 4 still queued. Close finalizes each open frame in
// order, and only its last status completes the message.
func TestTracker_CloseFinalizesUnfinishedFrames(t *testing.T) {
	tr, pub, _ := newTestTracker(0)
	ref := testRef(5, radiated)
	tr.Register(ref)
	tr.Frames(ref, 0, 1).Radiated(time.Now())
	tr.Frames(ref, 2, 2).Dispatching()
	tr.Flush()
	pub.got = nil

	tr.Close()
	got := pub.statuses()
	assert.Equal(t, []statusSummary{
		{2, dispatched, unknown, true, false},
		{3, unset, failed, true, false},
		{4, unset, failed, true, true},
	}, summarize(got))
	assert.Equal(t, streaming.CommandFailure_COMMAND_FAILURE_NO_DEVICE_ACK, got[0].GetFailure())
	assert.Equal(t, streaming.CommandFailure_COMMAND_FAILURE_PLAN_ENDED, got[1].GetFailure())
	assert.Equal(t, unknown, got[2].GetMessageOutcome(), "frame 2 may have been transmitted")

	pub.got = nil
	tr.Frames(ref, 2, 2).Radiated(time.Now())
	tr.Flush()
	assert.Empty(t, pub.statuses(), "reports after Close are ignored")
}

// A final message is forgotten, and later reports for it change nothing.
func TestTracker_ForgetsFinalMessages(t *testing.T) {
	tr, pub, _ := newTestTracker(0)
	ref := testRef(2, radiated)
	tr.Register(ref)
	r := tr.Frames(ref, 0, 1)
	r.Dispatched(nil)
	assert.Len(t, tr.messages, 1)
	r.Radiated(time.Now())
	assert.Empty(t, tr.messages)

	r.NotSent(streaming.CommandFailure_COMMAND_FAILURE_PLAN_ENDED, "late")
	tr.Close()
	got := pub.statuses()
	require.Len(t, got, 4)
	assert.True(t, got[3].GetMessageFinal())
}

func TestTracker_SkipIsPublishedWithStatuses(t *testing.T) {
	tr, pub, _ := newTestTracker(0)
	tr.Skip(testTopic, &streaming.SkippedRange{StreamId: "s", FirstIndex: 4, LastIndex: 5})
	tr.Flush()
	require.Len(t, pub.got, 1)
	assert.Equal(t, uint64(4), pub.got[0].batch.GetSkipped()[0].GetFirstIndex())
	assert.Equal(t, uint64(5), pub.got[0].batch.GetSkipped()[0].GetLastIndex())
}

// A full batch is published without waiting for the interval, by Run rather
// than the caller, which never blocks on publishing.
func TestTracker_FullBatchFlushesWithoutWaitingForTheInterval(t *testing.T) {
	tr, pub, _ := newTestTracker(2)
	ref := testRef(2, radiated)
	tr.Register(ref)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		tr.Run(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()

	tr.Frames(ref, 0, 0).Dispatched(nil)
	assert.Never(t, func() bool { return len(pub.statuses()) > 0 }, 20*time.Millisecond, time.Millisecond)
	tr.Frames(ref, 1, 1).Dispatched(nil)
	require.Eventually(t, func() bool { return len(pub.statuses()) == 2 }, 5*time.Second, time.Millisecond)
	pub.mu.Lock()
	defer pub.mu.Unlock()
	require.Len(t, pub.got, 1)
}

func TestTracker_RunFlushesOnInterval(t *testing.T) {
	tr, pub, mock := newTestTracker(0)
	ref := testRef(1, radiated)
	tr.Register(ref)
	tr.Frames(ref, 0, 0).Dispatched(nil)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		tr.Run(ctx)
		close(done)
	}()
	require.Eventually(t, func() bool {
		mock.Add(DefaultFlushInterval)
		return len(pub.statuses()) == 1
	}, 5*time.Second, time.Millisecond)
	cancel()
	<-done
}

func TestTracker_FailedBatchIsRetriedAheadOfNewerStatuses(t *testing.T) {
	tr, pub, _ := newTestTracker(0)
	pub.failures = 1
	ref := testRef(1, radiated)
	tr.Register(ref)
	r := tr.Frames(ref, 0, 0)
	r.Dispatched(nil)
	tr.Flush()
	assert.Empty(t, pub.statuses(), "the first publish failed")

	r.Radiated(time.Now())
	tr.Flush()
	assert.Equal(t, []statusSummary{
		{0, dispatched, inProgress, false, false},
		{0, radiated, succeeded, true, true},
	}, summarize(pub.statuses()))
}

func TestTracker_CloseRetriesTheFinalFlush(t *testing.T) {
	tr, pub, mock := newTestTracker(0)
	pub.failures = 2
	ref := testRef(1, radiated)
	tr.Register(ref)

	done := make(chan struct{})
	go func() {
		tr.Close()
		close(done)
	}()
	require.Eventually(t, func() bool {
		mock.Add(closeFlushRetryDelay)
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, 5*time.Second, time.Millisecond)
	require.Len(t, pub.statuses(), 1, "published on the third attempt")
}

func TestTracker_IgnoresUnregisteredAndNil(t *testing.T) {
	tr, pub, _ := newTestTracker(0)
	tr.Frames(testRef(1, radiated), 0, 0).Dispatched(nil)
	tr.Flush()
	assert.Empty(t, pub.got)

	var nilTracker *Tracker
	nilTracker.Register(testRef(1, radiated))
	nilTracker.Frames(testRef(1, radiated), 0, 0).Radiated(time.Now())
	nilTracker.Skip(testTopic, &streaming.SkippedRange{})
	nilTracker.Flush()
	nilTracker.Close()
	nilTracker.Run(context.Background())
	Reporter{}.Dispatched(nil)
}

// gatedPublisher blocks its first publish until gate is closed.
type gatedPublisher struct {
	recordingPublisher
	gate    chan struct{}
	entered chan struct{}
	calls   int
}

func (p *gatedPublisher) Publish(topic string, payload []byte) error {
	p.mu.Lock()
	p.calls++
	first := p.calls == 1
	p.mu.Unlock()
	if first {
		close(p.entered)
		<-p.gate
	}
	return p.recordingPublisher.Publish(topic, payload)
}

func TestTracker_ConcurrentFlushesPublishInOrder(t *testing.T) {
	pub := &gatedPublisher{gate: make(chan struct{}), entered: make(chan struct{})}
	tr := NewTracker(Options{Clock: clock.NewMock()}, pub, slog.New(slog.DiscardHandler))
	ref := testRef(1, radiated)
	tr.Register(ref)
	r := tr.Frames(ref, 0, 0)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		r.Dispatched(nil)
		tr.Flush()
	}()
	<-pub.entered
	go func() {
		defer wg.Done()
		r.Radiated(time.Now())
		tr.Flush()
	}()
	time.Sleep(20 * time.Millisecond)
	close(pub.gate)
	wg.Wait()

	assert.Equal(t, []statusSummary{
		{0, dispatched, inProgress, false, false},
		{0, radiated, succeeded, true, true},
	}, summarize(pub.statuses()))
}

func TestNotSentReason(t *testing.T) {
	assert.Equal(t, "the pass ended before the command was sent",
		NotSentReason(streaming.CommandFailure_COMMAND_FAILURE_PLAN_ENDED))
	assert.Equal(t, "a newer stream took over the channel before the command was sent",
		NotSentReason(streaming.CommandFailure_COMMAND_FAILURE_SUPERSEDED))
}
