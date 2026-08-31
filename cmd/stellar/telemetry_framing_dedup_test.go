package main

import (
	"sync"
	"testing"

	streaming "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/satellitestreamer"
	v1 "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/starpass/v1"
)

// stubMQTTMessage is a minimal mqtt.Message for feeding handleTelemetryMessage
// a specific topic/payload without a real broker connection.
type stubMQTTMessage struct {
	topic   string
	payload []byte
}

func (m *stubMQTTMessage) Duplicate() bool   { return false }
func (m *stubMQTTMessage) Qos() byte         { return 1 }
func (m *stubMQTTMessage) Retained() bool    { return false }
func (m *stubMQTTMessage) Topic() string     { return m.topic }
func (m *stubMQTTMessage) MessageID() uint16 { return 0 }
func (m *stubMQTTMessage) Payload() []byte   { return m.payload }
func (m *stubMQTTMessage) Ack()              {}

// buildTelemetryProto constructs a FromStarPassMessage carrying one telemetry
// chunk at the given index and framing, matching what a multi-framing downlink
// sends: the channel republishes the same index once per declared framing, as
// distinct messages.
func buildTelemetryProto(t *testing.T, index int, framing v1.Framing, data []byte) *streaming.FromStarPassMessage {
	t.Helper()
	return &streaming.FromStarPassMessage{
		Message: &streaming.FromStarPassMessage_SendTelemetryMessage{
			SendTelemetryMessage: &streaming.SendTelemetryMessage{
				Type:            streaming.SendTelemetryMessage_CONTINUE,
				FirstFrameIndex: uint32(index),
				Telemetry: []*streaming.Telemetry{
					{
						Data:    data,
						Framing: framing,
					},
				},
			},
		},
	}
}

// handleTelemetryMessageHarness bundles the shared state handleTelemetryMessage
// needs across calls (the SAME state a real reader shares between the MQTT
// handler and the S3 fallback scanner for one connection), and records every
// fetchResult routed through it.
type handleTelemetryMessageHarness struct {
	cfg                Config
	stats              *statsTracker
	mu                 sync.Mutex
	receivedIndices    map[string]map[int]bool
	sequentialCounters map[string]*int
	routed             []fetchResult
}

func newHandleTelemetryMessageHarness() *handleTelemetryMessageHarness {
	return &handleTelemetryMessageHarness{
		cfg:                Config{EnableDownlink: true},
		stats:              newStatsTracker(false),
		receivedIndices:    make(map[string]map[int]bool),
		sequentialCounters: make(map[string]*int),
	}
}

func (h *handleTelemetryMessageHarness) deliver(msgProto *streaming.FromStarPassMessage) {
	route := func(res fetchResult) { h.routed = append(h.routed, res) }
	sendAck := func(string, []byte) {}
	msg := &stubMQTTMessage{topic: testChannelTopic}
	handleTelemetryMessage(msgProto, msg, h.cfg, h.stats, route, &h.mu, h.receivedIndices, h.sequentialCounters, sendAck)
}

const testChannelTopic = "env1/pass/pass-1/channel/8b4c46c4-5b83-4429-af38-874c0ae06067/downlink"

// TestHandleTelemetryMessage_SameIndexDifferentFramingBothAccepted is the
// end-to-end regression guard for the cross-framing dedup bug: a multi-framing
// channel republishes the SAME index once per framing (e.g. index 5 as
// BITSTREAM and, separately, as IQ); two distinct, intentional messages, not
// duplicates. Before telemetryDedupKey included framing, the second of these to
// arrive was silently discarded as "already received", cutting a two-framing
// channel's received count roughly in half.
func TestHandleTelemetryMessage_SameIndexDifferentFramingBothAccepted(t *testing.T) {
	h := newHandleTelemetryMessageHarness()

	h.deliver(buildTelemetryProto(t, 5, v1.Framing_BITSTREAM, []byte("bitstream-data")))
	h.deliver(buildTelemetryProto(t, 5, v1.Framing_IQ, []byte("iq-data")))

	if len(h.routed) != 2 {
		t.Fatalf("routed %d results for index 5 across two framings, want 2 (one BITSTREAM, one IQ); got %+v", len(h.routed), h.routed)
	}
	gotFramings := map[string]bool{h.routed[0].Framing: true, h.routed[1].Framing: true}
	if !gotFramings["BITSTREAM"] || !gotFramings["IQ"] {
		t.Errorf("routed framings = %v, want both BITSTREAM and IQ present", gotFramings)
	}
}

// TestHandleTelemetryMessage_SameIndexSameFramingDeduped confirms the fix does
// not disable dedup outright: a genuine retransmission of the SAME (index,
// framing), e.g. delivered once via live MQTT and again via the S3 fallback
// scanner, must still be dropped as a duplicate.
func TestHandleTelemetryMessage_SameIndexSameFramingDeduped(t *testing.T) {
	h := newHandleTelemetryMessageHarness()

	h.deliver(buildTelemetryProto(t, 5, v1.Framing_BITSTREAM, []byte("bitstream-data")))
	h.deliver(buildTelemetryProto(t, 5, v1.Framing_BITSTREAM, []byte("bitstream-data-retransmit")))

	if len(h.routed) != 1 {
		t.Fatalf("routed %d results for a repeated (index, framing) pair, want 1 (the retransmission must be deduped); got %+v", len(h.routed), h.routed)
	}
}

// TestHandleTelemetryMessage_MultiFramingChannel_AllFramingsCounted simulates a
// full multi-framing channel: N frames, each republished once per declared
// framing, arriving interleaved. Every (index, framing) pair must be routed exactly once; the
// total must equal frames * framings, not collapse to frames.
func TestHandleTelemetryMessage_MultiFramingChannel_AllFramingsCounted(t *testing.T) {
	h := newHandleTelemetryMessageHarness()
	const totalFrames = 20
	framings := []v1.Framing{v1.Framing_BITSTREAM, v1.Framing_IQ}

	for i := 1; i <= totalFrames; i++ {
		for _, f := range framings {
			h.deliver(buildTelemetryProto(t, i, f, []byte("data")))
		}
	}

	want := totalFrames * len(framings)
	if len(h.routed) != want {
		t.Fatalf("routed %d results, want %d (%d frames x %d framings)", len(h.routed), want, totalFrames, len(framings))
	}

	perFraming := map[string]int{}
	for _, res := range h.routed {
		perFraming[res.Framing]++
	}
	if perFraming["BITSTREAM"] != totalFrames || perFraming["IQ"] != totalFrames {
		t.Errorf("per-framing counts = %v, want BITSTREAM=%d IQ=%d", perFraming, totalFrames, totalFrames)
	}
}
