// Copyright (c) 2026 InfoStellar, Inc. All Rights Reserved.

// Package commandseq orders the command messages a station receives over MQTT
// so each is sent exactly once, in index order. MQTT can duplicate and reorder
// them: duplicates are answered by replaying their first ack, a gap is waited
// out for the lane's GapWait before the missing index is skipped (at once, or
// never, if so configured), and a skipped index that arrives later is refused.
//
// State lives in memory only. Nothing restarts a station within a pass (the
// launcher sets no container restart policy and cloud-station Lambdas do not
// retry), so a crash cannot lead to a second send.
package commandseq

import (
	"context"
	"crypto/sha256"
	"sort"
	"sync"
	"time"

	"github.com/benbjohnson/clock"

	streaming "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/satellitestreamer"
)

const (
	DefaultGapWait = 5 * time.Second
	DefaultMaxHeld = 256
)

// Ack.reason_code values for NACKs.
const (
	ReasonMalformedPayload = "malformed_payload"
	ReasonWrongPlan        = "wrong_plan"
	ReasonNoCommands       = "no_commands"
	ReasonNoConfiguration  = "no_configuration"
	ReasonNoTransmitter    = "no_transmitter"
	ReasonIndexRequired    = "index_required"
	ReasonIndexReused      = "index_reused"
	ReasonArrivedLate      = "arrived_late"
	ReasonHoldBufferFull   = "hold_buffer_full"
	ReasonSuperseded       = "superseded"
	ReasonOutOfRange       = "out_of_range"
	ReasonUnsupported      = "unsupported"
	ReasonOutsideContact   = "outside_contact"
)

var reasonTexts = map[string]string{
	ReasonMalformedPayload: "malformed payload",
	ReasonWrongPlan:        "the command is for a different pass",
	ReasonNoCommands:       "message carried no commands",
	ReasonNoConfiguration:  "message set no ground station configuration field",
	ReasonNoTransmitter:    "the ground station has no transmitter for this channel",
	ReasonIndexRequired:    "the message index is required; number messages from 1",
	ReasonIndexReused:      "the message index was already used in this stream",
	ReasonArrivedLate:      "arrived after later messages were sent; not transmitted",
	ReasonHoldBufferFull:   "too many messages are waiting for an earlier index",
	ReasonSuperseded:       "a newer stream has taken over this channel",
	ReasonOutOfRange:       "a configuration value is outside what the ground station accepts",
	ReasonUnsupported:      "the ground station cannot apply this configuration",
	ReasonOutsideContact:   "arrived outside the pass execution interval",
}

// ReasonText is the human-readable Ack.reason for a reason code.
func ReasonText(code string) string { return reasonTexts[code] }

// Message is one command message offered to a Lane. Item is opaque to the
// lane and is handed back when the message is released or discarded.
type Message[T any] struct {
	StreamID  string
	MessageID string
	Index     uint64
	// Payload is the raw message, hashed to tell a duplicate from a reused index.
	Payload []byte
	// Refusal, when set, is the reason the station refuses the message on its
	// content. It is NACKed but still uses up its index, so it leaves no gap.
	Refusal string
	// RefusalReason, when set, replaces ReasonText(Refusal) in the ack.
	RefusalReason string
	Item          T
}

type Verdict int

const (
	// Accepted is ACKed: sent now, or held until the indexes below it are done.
	Accepted Verdict = iota
	// Refused is NACKed and never sent.
	Refused
	// Duplicate repeats a message already answered; its first ack is replayed.
	Duplicate
)

func (v Verdict) String() string {
	switch v {
	case Accepted:
		return "accepted"
	case Refused:
		return "refused"
	case Duplicate:
		return "duplicate"
	}
	return "unknown"
}

// Handler receives a lane's decisions. Every method is called with the lane
// locked, so Release calls arrive in index order.
type Handler[T any] interface {
	// Ack builds the ack to publish. A lane stores what it returns and replays
	// it for duplicates.
	Ack(m Message[T], v Verdict, reasonCode string) []byte
	// Release sends an accepted message.
	Release(m Message[T])
	// Discard reports an accepted message that will never be sent.
	Discard(m Message[T], failure streaming.CommandFailure)
	// Skip reports indexes declared missing.
	Skip(streamID string, first, last uint64)
}

type Options struct {
	// GapWait is how long a held message waits for the indexes below it.
	GapWait GapWait
	// MaxHeld bounds the held messages per stream.
	MaxHeld int
	Clock   clock.Clock
}

type answer struct {
	messageID string
	digest    [sha256.Size]byte
	ack       []byte
}

type held[T any] struct {
	msg Message[T]
	// deadline is when the gap below msg is skipped; zero for never.
	deadline time.Time
}

type stream[T any] struct {
	next       uint64
	held       map[uint64]held[T]
	answers    map[uint64]answer
	superseded bool
}

// lowestHeld returns the held message with the lowest index.
func (s *stream[T]) lowestHeld() (held[T], bool) {
	var lowest held[T]
	found := false
	for _, h := range s.held {
		if !found || h.msg.Index < lowest.msg.Index {
			lowest, found = h, true
		}
	}
	return lowest, found
}

// Lane sequences one kind of command message (satellite commands or ground
// station configuration) on one channel of one pass. The first accepted
// message from a new stream supersedes the previous stream: its held messages
// are discarded and its later messages refused.
type Lane[T any] struct {
	opts    Options
	handler Handler[T]
	wake    chan struct{}

	mu      sync.Mutex
	streams map[string]*stream[T]
	active  *stream[T]
	closed  bool
}

func NewLane[T any](opts Options, handler Handler[T]) *Lane[T] {
	if opts.MaxHeld <= 0 {
		opts.MaxHeld = DefaultMaxHeld
	}
	if opts.Clock == nil {
		opts.Clock = clock.New()
	}
	return &Lane[T]{
		opts:    opts,
		handler: handler,
		wake:    make(chan struct{}, 1),
		streams: make(map[string]*stream[T]),
	}
}

// Offer decides what happens to m and returns the ack to publish for it.
func (l *Lane[T]) Offer(m Message[T]) (Verdict, []byte) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if m.Index == 0 {
		return Refused, l.handler.Ack(m, Refused, ReasonIndexRequired)
	}
	s := l.stream(m.StreamID)
	digest := sha256.Sum256(m.Payload)
	if a, ok := s.answers[m.Index]; ok {
		if a.messageID == m.MessageID && a.digest == digest {
			return Duplicate, a.ack
		}
		return Refused, l.handler.Ack(m, Refused, ReasonIndexReused)
	}

	switch {
	case l.closed:
		// The pass is over for this lane: nothing further is sent.
		return l.refuse(s, m, digest, ReasonOutsideContact)
	case s.superseded:
		return l.refuse(s, m, digest, ReasonSuperseded)
	case m.Index < s.next:
		// Answered indexes returned above, so this one was skipped.
		return l.refuse(s, m, digest, ReasonArrivedLate)
	case m.Index > s.next && len(s.held) >= l.opts.MaxHeld:
		// Not recorded: the client may resend it unchanged once there is room.
		return Refused, l.handler.Ack(m, Refused, ReasonHoldBufferFull)
	}

	if m.Refusal != "" {
		// Refused on content: answered and never sent, so it is not held and
		// starts no wait. Its index is consumed when the sequence reaches it
		// (see advance), so it delays nothing and leaves no gap.
		return l.refuse(s, m, digest, m.Refusal)
	}
	if s != l.active {
		l.activate(s)
	}
	ack := l.handler.Ack(m, Accepted, "")
	s.answers[m.Index] = answer{messageID: m.MessageID, digest: digest, ack: ack}
	switch wait, skips := l.opts.GapWait.timeout(); {
	case m.Index == s.next:
		l.advance(s, m)
		// Run may be waiting on a held message advance just released; let it
		// re-arm on whatever is still held.
		l.signal()
	case !skips:
		s.held[m.Index] = held[T]{msg: m}
	case wait == 0:
		s.held[m.Index] = held[T]{msg: m, deadline: l.opts.Clock.Now()}
		l.expire()
	default:
		s.held[m.Index] = held[T]{msg: m, deadline: l.opts.Clock.Now().Add(wait)}
		l.signal()
	}
	return Accepted, ack
}

func (l *Lane[T]) refuse(s *stream[T], m Message[T], digest [sha256.Size]byte, reason string) (Verdict, []byte) {
	ack := l.handler.Ack(m, Refused, reason)
	s.answers[m.Index] = answer{messageID: m.MessageID, digest: digest, ack: ack}
	if m.Index == s.next {
		// The sequence reached an index that was refused: consume it and
		// whatever it was holding back.
		l.consume(s)
		l.signal()
	}
	return Refused, ack
}

func (l *Lane[T]) stream(id string) *stream[T] {
	if s, ok := l.streams[id]; ok {
		return s
	}
	s := &stream[T]{next: 1, held: make(map[uint64]held[T]), answers: make(map[uint64]answer)}
	l.streams[id] = s
	return s
}

// activate gives s command authority, superseding the active stream.
func (l *Lane[T]) activate(s *stream[T]) {
	if l.active != nil {
		l.discardHeld(l.active, streaming.CommandFailure_COMMAND_FAILURE_SUPERSEDED)
		l.active.superseded = true
	}
	l.active = s
}

// advance sends m, which is at s.next, then consumes what follows.
func (l *Lane[T]) advance(s *stream[T], m Message[T]) {
	l.handler.Release(m)
	l.consume(s)
}

// consume moves past s.next, then past every contiguous index that is held
// (sent now) or was answered with a refusal (never sent).
func (l *Lane[T]) consume(s *stream[T]) {
	s.next++
	for {
		if h, ok := s.held[s.next]; ok {
			delete(s.held, s.next)
			l.handler.Release(h.msg)
		} else if _, refused := s.answers[s.next]; !refused {
			return
		}
		s.next++
	}
}

func (l *Lane[T]) discardHeld(s *stream[T], failure streaming.CommandFailure) {
	indexes := make([]uint64, 0, len(s.held))
	for i := range s.held {
		indexes = append(indexes, i)
	}
	sort.Slice(indexes, func(a, b int) bool { return indexes[a] < indexes[b] })
	for _, i := range indexes {
		l.handler.Discard(s.held[i].msg, failure)
		delete(s.held, i)
	}
}

// Expire skips every gap whose held message has waited GapWait by now.
func (l *Lane[T]) Expire() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.expire()
}

func (l *Lane[T]) expire() {
	s := l.active
	if s == nil {
		return
	}
	now := l.opts.Clock.Now()
	for {
		h, ok := s.lowestHeld()
		if !ok || h.deadline.IsZero() || h.deadline.After(now) {
			return
		}
		l.handler.Skip(h.msg.StreamID, s.next, h.msg.Index-1)
		s.next = h.msg.Index
		delete(s.held, h.msg.Index)
		l.advance(s, h.msg)
	}
}

// nextDeadline returns when Expire next has work to do.
func (l *Lane[T]) nextDeadline() (time.Time, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.active == nil {
		return time.Time{}, false
	}
	h, ok := l.active.lowestHeld()
	return h.deadline, ok && !h.deadline.IsZero()
}

func (l *Lane[T]) signal() {
	select {
	case l.wake <- struct{}{}:
	default:
	}
}

// Run calls Expire whenever a held message's wait ends, until ctx is done.
func (l *Lane[T]) Run(ctx context.Context) {
	for {
		var timer *clock.Timer
		var timeout <-chan time.Time
		if deadline, ok := l.nextDeadline(); ok {
			timer = l.opts.Clock.Timer(l.opts.Clock.Until(deadline))
			timeout = timer.C
		}
		select {
		case <-ctx.Done():
		case <-l.wake:
		case <-timeout:
			l.Expire()
		}
		if timer != nil {
			timer.Stop()
		}
		if ctx.Err() != nil {
			return
		}
	}
}

// Close discards every held message as never sent. Later messages are refused.
func (l *Lane[T]) Close(failure streaming.CommandFailure) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	if l.active != nil {
		l.discardHeld(l.active, failure)
	}
}
