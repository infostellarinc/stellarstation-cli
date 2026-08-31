package main

import (
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"google.golang.org/protobuf/proto"

	streaming "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/satellitestreamer"
)

// recordingMQTTClient records every published payload, unlike mockMQTTClient
// which keeps only the last one.
type recordingMQTTClient struct {
	mockMQTTClient

	mu       sync.Mutex
	topics   []string
	payloads [][]byte
}

func (r *recordingMQTTClient) Publish(
	topic string, qos byte, retained bool, payload interface{},
) mqtt.Token {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.topics = append(r.topics, topic)
	if p, ok := payload.([]byte); ok {
		cpy := make([]byte, len(p))
		copy(cpy, p)
		r.payloads = append(r.payloads, cpy)
	}
	return &mockToken{err: nil}
}

func (r *recordingMQTTClient) publishCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.payloads)
}

func newTestProxyUplinkSender(client mqtt.Client, window passWindow) *proxyUplinkSender {
	return &proxyUplinkSender{
		client:   client,
		topic:    "dev/pass/p1/channel/c1/uplink",
		streamID: "stream-1",
		planID:   "plan-1",
		qos:      1,
		stats:    newStatsTracker(false),
		window:   window,
		index:    1,
	}
}

func TestProxyUplinkSenderTransmitsInOrder(t *testing.T) {
	client := &recordingMQTTClient{mockMQTTClient: mockMQTTClient{connected: true}}
	s := newTestProxyUplinkSender(client, passWindow{})

	s.send([]byte{0x0A, 0x1B})
	s.send([]byte{0x2C, 0x3D, 0x4E})

	if got := client.publishCount(); got != 2 {
		t.Fatalf("published %d commands, want 2", got)
	}
	for i, wantCmd := range [][]byte{{0x0A, 0x1B}, {0x2C, 0x3D, 0x4E}} {
		msg := &streaming.ToStarPassMessage{}
		if err := proto.Unmarshal(client.payloads[i], msg); err != nil {
			t.Fatalf("unmarshal published message %d: %v", i, err)
		}
		if msg.GetIndex() != uint32(i+1) {
			t.Errorf("message %d index = %d, want %d", i, msg.GetIndex(), i+1)
		}
		cmds := msg.GetSendCommandsMessage().GetCommand()
		if len(cmds) != 1 || string(cmds[0]) != string(wantCmd) {
			t.Errorf("message %d command = %v, want %v", i, cmds, wantCmd)
		}
		if msg.GetStreamId() != "stream-1" || msg.GetPassId() != "plan-1" {
			t.Errorf("message %d stream/pass = %q/%q, want stream-1/plan-1",
				i, msg.GetStreamId(), msg.GetPassId())
		}
	}
	if client.topics[0] != "dev/pass/p1/channel/c1/uplink" {
		t.Errorf("published to topic %q", client.topics[0])
	}
}

func TestProxyUplinkSenderDropsOutsideBookingWindow(t *testing.T) {
	client := &recordingMQTTClient{mockMQTTClient: mockMQTTClient{connected: true}}
	closed := passWindow{
		start: time.Now().Add(-2 * time.Hour),
		stop:  time.Now().Add(-1 * time.Hour),
	}
	s := newTestProxyUplinkSender(client, closed)

	s.send([]byte{0x01})

	if got := client.publishCount(); got != 0 {
		t.Fatalf("published %d commands outside the booking window, want 0", got)
	}
	if s.index != 1 {
		t.Errorf("index advanced to %d on a dropped payload, want 1", s.index)
	}
}

func TestProxyUplinkSenderIgnoresEmptyPayload(t *testing.T) {
	client := &recordingMQTTClient{mockMQTTClient: mockMQTTClient{connected: true}}
	s := newTestProxyUplinkSender(client, passWindow{})

	s.send(nil)
	s.send([]byte{})

	if got := client.publishCount(); got != 0 {
		t.Fatalf("published %d commands for empty payloads, want 0", got)
	}
}

func TestBuildProxyUplinkFuncWithoutCredentialsDegradesToDrop(t *testing.T) {
	cfg := Config{EnableUplink: true}
	fn, sender, err := buildProxyUplinkFunc(t.Context(), &cfg, "pass-1", newStatsTracker(false))
	if err != nil {
		t.Fatalf("buildProxyUplinkFunc() error = %v", err)
	}
	if sender != nil {
		t.Fatal("expected no uplink sender without authorizer credentials")
	}
	if fn == nil {
		t.Fatal("expected a drop-and-warn uplink callback")
	}
	fn([]byte{0x01}) // must not panic
}
