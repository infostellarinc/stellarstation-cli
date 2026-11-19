// Copyright (c) 2026 InfoStellar, Inc. All Rights Reserved.

// Package commandstatus tracks ACKed commands and configuration requests
// through the station and publishes their progress as CommandStatusBatch
// messages on the request topic's "/status" topic.
package commandstatus

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	streaming "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/satellitestreamer"
)

// Message types, as carried in Ack.message_type and CommandStatus.message_type.
const (
	MessageTypeUplink = "uplink"
	MessageTypeConfig = "gs_config_request"
)

const (
	DefaultFlushInterval = 100 * time.Millisecond
	DefaultMaxBatch      = 50

	// Close retries a failed final flush, since nothing flushes after it.
	closeFlushAttempts   = 3
	closeFlushRetryDelay = time.Second
)

// Ref identifies one tracked message.
type Ref struct {
	StatusTopic string
	MessageType string
	PassID      string
	ChannelID   string
	StreamID    string
	MessageID   string
	Index       uint64
	FrameCount  uint32
	// FinalStage is the furthest stage the station reports; reaching it
	// succeeds.
	FinalStage streaming.CommandStage
}

// Update is one report about a frame.
type Update struct {
	Stage        streaming.CommandStage
	Outcome      streaming.CommandOutcome
	Failure      streaming.CommandFailure
	ProviderCode string
	Reason       string
	RadiatedAt   time.Time
	ConfigFields []*streaming.ConfigFieldResult
}

func (u Update) final() bool {
	return u.Outcome != streaming.CommandOutcome_COMMAND_OUTCOME_IN_PROGRESS
}

// Error is a device outcome returned from a transmit handler.
type Error struct {
	Stage        streaming.CommandStage
	Outcome      streaming.CommandOutcome
	Failure      streaming.CommandFailure
	ProviderCode string
	Reason       string
}

func (e *Error) Error() string { return e.Reason }

type Publisher interface {
	Publish(topic string, payload []byte) error
}

type Options struct {
	FlushInterval time.Duration
	MaxBatch      int
	Clock         clock.Clock
}

type frame struct {
	stage   streaming.CommandStage
	outcome streaming.CommandOutcome
	final   bool
}

// moves reports whether u moves f forward: to a later stage, or to its final
// outcome at the same stage. A final frame never moves.
func (f frame) moves(u Update) bool {
	return !f.final && (u.Stage > f.stage || (u.Stage == f.stage && u.final()))
}

type tracked struct {
	ref    Ref
	frames []frame
	// open counts the frames that are not final yet.
	open int
}

// outcome summarizes a final message. UNKNOWN wins, since any unknown frame
// may have been transmitted; then a mix of successes and failures is
// PARTIAL, as only the failed frames are safe to resend.
func (m *tracked) outcome() streaming.CommandOutcome {
	var succeeded, failed, partial bool
	for _, f := range m.frames {
		switch f.outcome {
		case streaming.CommandOutcome_COMMAND_OUTCOME_UNKNOWN:
			return f.outcome
		case streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED:
			succeeded = true
		case streaming.CommandOutcome_COMMAND_OUTCOME_FAILED:
			failed = true
		case streaming.CommandOutcome_COMMAND_OUTCOME_PARTIAL:
			partial = true
		}
	}
	switch {
	case partial || (succeeded && failed):
		return streaming.CommandOutcome_COMMAND_OUTCOME_PARTIAL
	case failed:
		return streaming.CommandOutcome_COMMAND_OUTCOME_FAILED
	default:
		return streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED
	}
}

type key struct {
	topic    string
	streamID string
	index    uint64
}

// Tracker publishes one status each time a frame of a tracked message moves
// forward, so the first final status for a frame wins. It forgets a message
// once every frame is final. A nil *Tracker ignores every call.
type Tracker struct {
	opts Options
	pub  Publisher
	log  *slog.Logger
	// full asks Run to flush a full batch now.
	full chan struct{}

	// flushMu keeps concurrent flushes from publishing batches out of order.
	flushMu sync.Mutex

	mu       sync.Mutex
	messages map[key]*tracked
	pending  map[string]*streaming.CommandStatusBatch
	queued   int
	closed   bool
}

func NewTracker(opts Options, pub Publisher, log *slog.Logger) *Tracker {
	if opts.FlushInterval <= 0 {
		opts.FlushInterval = DefaultFlushInterval
	}
	if opts.MaxBatch <= 0 {
		opts.MaxBatch = DefaultMaxBatch
	}
	if opts.Clock == nil {
		opts.Clock = clock.New()
	}
	return &Tracker{
		opts:     opts,
		pub:      pub,
		log:      log,
		full:     make(chan struct{}, 1),
		messages: make(map[key]*tracked),
		pending:  make(map[string]*streaming.CommandStatusBatch),
	}
}

func keyOf(ref Ref) key { return key{topic: ref.StatusTopic, streamID: ref.StreamID, index: ref.Index} }

// Register starts tracking an ACKed message.
func (t *Tracker) Register(ref Ref) {
	if t == nil {
		return
	}
	if ref.FrameCount == 0 {
		ref.FrameCount = 1
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	t.messages[keyOf(ref)] = &tracked{ref: ref, frames: make([]frame, ref.FrameCount), open: int(ref.FrameCount)}
}

// report applies u to frames [first, last] of ref in order, and queues a
// status for each frame it moves. It never publishes: a full batch wakes Run.
func (t *Tracker) report(ref Ref, first, last uint32, u Update) {
	if t == nil {
		return
	}
	t.mu.Lock()
	k := keyOf(ref)
	m, ok := t.messages[k]
	if ok && !t.closed {
		for i := first; i <= min(last, uint32(len(m.frames))-1); i++ {
			t.applyLocked(m, i, u)
		}
		if m.open == 0 {
			delete(t.messages, k)
		}
	}
	full := t.queued >= t.opts.MaxBatch
	t.mu.Unlock()
	if full {
		select {
		case t.full <- struct{}{}:
		default:
		}
	}
}

// Called with mu held.
func (t *Tracker) applyLocked(m *tracked, i uint32, u Update) {
	f := &m.frames[i]
	if !f.moves(u) {
		return
	}
	f.stage, f.outcome, f.final = u.Stage, u.Outcome, u.final()
	if f.final {
		m.open--
	}
	s := &streaming.CommandStatus{
		AckedMessageId: m.ref.MessageID,
		StreamId:       m.ref.StreamID,
		PassId:         m.ref.PassID,
		Index:          m.ref.Index,
		ChannelId:      m.ref.ChannelID,
		MessageType:    m.ref.MessageType,
		FrameIndex:     i,
		FrameCount:     m.ref.FrameCount,
		Stage:          u.Stage,
		Outcome:        u.Outcome,
		Final:          f.final,
		MessageFinal:   f.final && m.open == 0,
		ObservedAt:     timestamppb.New(t.opts.Clock.Now()),
		Failure:        u.Failure,
		ProviderCode:   u.ProviderCode,
		Reason:         u.Reason,
		ConfigFields:   u.ConfigFields,
		MessageId:      uuid.NewString(),
	}
	if s.MessageFinal {
		s.MessageOutcome = m.outcome()
	}
	if !u.RadiatedAt.IsZero() {
		s.RadiatedAt = timestamppb.New(u.RadiatedAt)
	}
	b := t.batch(m.ref.StatusTopic)
	b.Statuses = append(b.Statuses, s)
	t.queued++
}

func (t *Tracker) batch(topic string) *streaming.CommandStatusBatch {
	b, ok := t.pending[topic]
	if !ok {
		b = &streaming.CommandStatusBatch{}
		t.pending[topic] = b
	}
	return b
}

// Skip queues a notice that indexes were declared missing. After Close
// nothing is published any more, so the notice is dropped.
func (t *Tracker) Skip(topic string, r *streaming.SkippedRange) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	b := t.batch(topic)
	b.Skipped = append(b.Skipped, r)
	t.queued++
}

// Flush publishes everything queued.
func (t *Tracker) Flush() {
	t.flush()
}

// flush publishes everything queued and reports whether it all went out. A
// batch that fails to publish is queued again ahead of newer statuses.
func (t *Tracker) flush() bool {
	if t == nil {
		return true
	}
	t.flushMu.Lock()
	defer t.flushMu.Unlock()
	t.mu.Lock()
	pending := t.pending
	t.pending = make(map[string]*streaming.CommandStatusBatch)
	t.queued = 0
	t.mu.Unlock()

	ok := true
	for topic, b := range pending {
		payload, err := proto.Marshal(b)
		if err != nil {
			t.log.Error("Failed to marshal command status", "error", err)
			continue
		}
		if err := t.pub.Publish(topic, payload); err != nil {
			t.log.Error("Failed to publish command status; will retry", "topic", topic, "error", err)
			t.requeue(topic, b)
			ok = false
			continue
		}
		t.log.Info("Published command status", "topic", topic,
			"statuses", len(b.GetStatuses()), "skipped", len(b.GetSkipped()))
	}
	return ok
}

func (t *Tracker) requeue(topic string, failed *streaming.CommandStatusBatch) {
	t.mu.Lock()
	defer t.mu.Unlock()
	b := t.batch(topic)
	b.Statuses = append(failed.GetStatuses(), b.GetStatuses()...)
	b.Skipped = append(failed.GetSkipped(), b.GetSkipped()...)
	t.queued += len(failed.GetStatuses()) + len(failed.GetSkipped())
}

// Run flushes on an interval, and as soon as a batch is full, until ctx is
// done.
func (t *Tracker) Run(ctx context.Context) {
	if t == nil {
		return
	}
	ticker := t.opts.Clock.Ticker(t.opts.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-t.full:
		}
		t.Flush()
	}
}

// Close gives every unfinished frame its final status and flushes. A frame
// never handed to a device was not sent; one that was has an unknown outcome.
func (t *Tracker) Close() {
	if t == nil {
		return
	}
	t.mu.Lock()
	for _, m := range t.messages {
		for i, f := range m.frames {
			u := Update{
				Stage:   f.stage,
				Outcome: streaming.CommandOutcome_COMMAND_OUTCOME_UNKNOWN,
				Failure: streaming.CommandFailure_COMMAND_FAILURE_NO_DEVICE_ACK,
				Reason:  "the pass ended before the device reported an outcome",
			}
			if f.stage == streaming.CommandStage_COMMAND_STAGE_UNSPECIFIED {
				u.Outcome = streaming.CommandOutcome_COMMAND_OUTCOME_FAILED
				u.Failure = streaming.CommandFailure_COMMAND_FAILURE_PLAN_ENDED
				u.Reason = NotSentReason(u.Failure)
			}
			t.applyLocked(m, uint32(i), u)
		}
	}
	clear(t.messages)
	t.closed = true
	t.mu.Unlock()
	for attempt := 1; !t.flush() && attempt < closeFlushAttempts; attempt++ {
		t.opts.Clock.Sleep(closeFlushRetryDelay)
	}
}
