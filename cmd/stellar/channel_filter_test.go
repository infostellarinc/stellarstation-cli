package main

import (
	"testing"

	v1 "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/starpass/v1"
)

// deliverOn feeds handleTelemetryMessage one telemetry message arriving on the
// given channel's downlink topic.
func (h *handleTelemetryMessageHarness) deliverOn(t *testing.T, channelID string, index int) {
	t.Helper()
	route := func(res fetchResult) { h.routed = append(h.routed, res) }
	sendAck := func(string, []byte) { h.stats.acksSent++ }
	msg := &stubMQTTMessage{topic: "env1/pass/pass-1/channel/" + channelID + "/downlink"}
	proto := buildTelemetryProto(t, index, v1.Framing_BITSTREAM, []byte("data"))
	handleTelemetryMessage(proto, msg, h.cfg, h.stats, route, &h.mu, h.receivedIndices, h.sequentialCounters, sendAck)
}

// The MQTT subscriptions use per-channel wildcards (collapseChannelWildcards),
// so the broker delivers telemetry for every channel of the pass. Only the
// configured channels may be processed: anything else must be dropped before it
// is counted, routed to disk, or acked.
func TestHandleTelemetryMessageDropsUnconfiguredChannels(t *testing.T) {
	const configured = "8b4c46c4-5b83-4429-af38-874c0ae06067"
	const unconfigured = "0696b179-780b-49e5-bee5-3bfd1cef1de3"

	h := newHandleTelemetryMessageHarness()
	h.cfg.ChannelIDs = []string{configured}

	h.deliverOn(t, configured, 1)
	h.deliverOn(t, unconfigured, 1)
	h.deliverOn(t, unconfigured, 2)

	if len(h.routed) != 1 {
		t.Fatalf("routed %d messages, want 1 (only the configured channel)", len(h.routed))
	}
	if h.routed[0].ChannelID != configured+"/downlink" {
		t.Errorf("routed channel = %s, want %s/downlink", h.routed[0].ChannelID, configured)
	}
	if h.stats.acksSent != 1 {
		t.Errorf("acks sent = %d, want 1 (dropped messages must not be acked)", h.stats.acksSent)
	}
}

// Without a configured channel list the handler accepts every channel, so
// callers that subscribe broadly on purpose keep working.
func TestHandleTelemetryMessageAcceptsAllWithoutChannelList(t *testing.T) {
	h := newHandleTelemetryMessageHarness()

	h.deliverOn(t, "8b4c46c4-5b83-4429-af38-874c0ae06067", 1)
	h.deliverOn(t, "0696b179-780b-49e5-bee5-3bfd1cef1de3", 1)

	if len(h.routed) != 2 {
		t.Fatalf("routed %d messages, want 2", len(h.routed))
	}
}
