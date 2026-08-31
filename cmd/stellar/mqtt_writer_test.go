package main

import (
	"errors"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"google.golang.org/protobuf/proto"

	streaming "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/satellitestreamer"
	v1 "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/starpass/v1"
)

// Mock MQTT client for testing
type mockMQTTClientWriter struct {
	connected  bool
	published  bool
	pubTopic   string
	pubQoS     byte
	pubPayload []byte
	pubErr     error
}

func (m *mockMQTTClientWriter) IsConnected() bool {
	return m.connected
}

func (m *mockMQTTClientWriter) Publish(
	topic string,
	qos byte,
	retained bool,
	payload interface{},
) mqtt.Token {
	m.published = true
	m.pubTopic = topic
	m.pubQoS = qos
	if p, ok := payload.([]byte); ok {
		m.pubPayload = p
	}
	return &mockTokenWriter{err: m.pubErr}
}

func (m *mockMQTTClientWriter) Subscribe(
	topic string,
	qos byte,
	callback mqtt.MessageHandler,
) mqtt.Token {
	return &mockTokenWriter{err: nil}
}

func (m *mockMQTTClientWriter) Disconnect(quiesce uint) {}

func (m *mockMQTTClientWriter) AddRoute(topic string, callback mqtt.MessageHandler) {}

func (m *mockMQTTClientWriter) Connect() mqtt.Token {
	return &mockTokenWriter{err: nil}
}

func (m *mockMQTTClientWriter) IsConnectionOpen() bool {
	return m.connected
}

func (m *mockMQTTClientWriter) OptionsReader() mqtt.ClientOptionsReader {
	return mqtt.NewClient(nil).OptionsReader()
}

func (m *mockMQTTClientWriter) SubscribeMultiple(
	filters map[string]byte,
	callback mqtt.MessageHandler,
) mqtt.Token {
	return &mockTokenWriter{err: nil}
}

func (m *mockMQTTClientWriter) Unsubscribe(topics ...string) mqtt.Token {
	return &mockTokenWriter{err: nil}
}

type mockTokenWriter struct {
	err error
}

func (m *mockTokenWriter) Wait() bool {
	return m.err == nil
}

func (m *mockTokenWriter) WaitTimeout(timeout time.Duration) bool {
	return m.err == nil
}

func (m *mockTokenWriter) Error() error {
	return m.err
}

func (m *mockTokenWriter) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func TestPublishCommand(t *testing.T) {
	client := &mockMQTTClientWriter{connected: true}
	stats := newStatsTracker(false)

	msg := &streaming.ToStarPassMessage{
		StreamId: "stream-1",
		PassId:   "plan-1",
		Index:    1,
	}

	err := publishCommand(client, "test/topic", 1, msg, stats, "uplink", 1)
	if err != nil {
		t.Fatalf("publishCommand() error = %v", err)
	}
	if !client.published {
		t.Error("Command should have been published")
	}
	if client.pubTopic != "test/topic" {
		t.Errorf("Published topic = %v, want test/topic", client.pubTopic)
	}
	if client.pubQoS != 1 {
		t.Errorf("Published QoS = %v, want 1", client.pubQoS)
	}
	if len(stats.sentCommands) != 1 {
		t.Errorf("Sent commands count = %v, want 1", len(stats.sentCommands))
	}
}

func TestPublishCommandNotConnected(t *testing.T) {
	// Note: publishCommand itself doesn't check connection status.
	// The connection check is done in PublishSatCommand and PublishGsConfig.
	// So this test verifies that publishCommand works even when not connected
	// (the actual check happens at a higher level).
	client := &mockMQTTClientWriter{connected: false}
	stats := newStatsTracker(false)

	msg := &streaming.ToStarPassMessage{
		StreamId: "stream-1",
		PassId:   "plan-1",
		Index:    1,
	}

	// publishCommand doesn't check connection, so this should succeed
	err := publishCommand(client, "test/topic", 1, msg, stats, "uplink", 1)
	if err != nil {
		t.Errorf("publishCommand() error = %v (note: connection check is at higher level)", err)
	}
}

func TestPublishCommandTimeout(t *testing.T) {
	client := &mockMQTTClientWriter{connected: true, pubErr: errors.New("timeout")}
	stats := newStatsTracker(false)

	msg := &streaming.ToStarPassMessage{
		StreamId: "stream-1",
		PassId:   "plan-1",
		Index:    1,
	}

	err := publishCommand(client, "test/topic", 1, msg, stats, "uplink", 1)
	if err == nil {
		t.Error("publishCommand() should return error on timeout")
	}
}

func TestPublishSatCommand(t *testing.T) {
	client := &mockMQTTClientWriter{connected: true}
	stats := newStatsTracker(false)

	commands := [][]byte{{0xde, 0xad, 0xbe, 0xef}}
	err := PublishSatCommand(
		t.Context(),
		client,
		"dev/pass-123/uplink",
		"stream-1",
		"plan-1",
		1,
		commands,
		1,
		stats,
	)
	if err != nil {
		t.Fatalf("PublishSatCommand() error = %v", err)
	}
	if !client.published {
		t.Error("Sat command should have been published")
	}

	// Verify the message was marshaled correctly
	var msg streaming.ToStarPassMessage
	if err := proto.Unmarshal(client.pubPayload, &msg); err != nil {
		t.Fatalf("Failed to unmarshal published message: %v", err)
	}
	if msg.GetStreamId() != "stream-1" {
		t.Errorf("StreamId = %v, want stream-1", msg.GetStreamId())
	}
	if msg.GetPassId() != "plan-1" {
		t.Errorf("PlanId = %v, want plan-1", msg.GetPassId())
	}
	if msg.GetIndex() != 1 {
		t.Errorf("Index = %v, want 1", msg.GetIndex())
	}
}

func TestPublishSatCommandNotConnected(t *testing.T) {
	client := &mockMQTTClientWriter{connected: false}
	stats := newStatsTracker(false)

	commands := [][]byte{{0xde, 0xad, 0xbe, 0xef}}
	err := PublishSatCommand(
		t.Context(),
		client,
		"dev/pass-123/uplink",
		"stream-1",
		"plan-1",
		1,
		commands,
		1,
		stats,
	)
	if err == nil {
		t.Error("PublishSatCommand() should return error when client not connected")
	}
}

func TestPublishGsConfig(t *testing.T) {
	client := &mockMQTTClientWriter{connected: true}
	stats := newStatsTracker(false)

	configRequest := &v1.GroundStationConfigurationRequest{}
	err := PublishGsConfig(
		t.Context(),
		client,
		"dev/pass-123/config_request",
		"stream-1",
		"plan-1",
		1,
		configRequest,
		1,
		stats,
	)
	if err != nil {
		t.Fatalf("PublishGsConfig() error = %v", err)
	}
	if !client.published {
		t.Error("GS config should have been published")
	}

	// Verify the message was marshaled correctly
	var msg streaming.ToStarPassMessage
	if err := proto.Unmarshal(client.pubPayload, &msg); err != nil {
		t.Fatalf("Failed to unmarshal published message: %v", err)
	}
	if msg.GetStreamId() != "stream-1" {
		t.Errorf("StreamId = %v, want stream-1", msg.GetStreamId())
	}
	if msg.GetPassId() != "plan-1" {
		t.Errorf("PlanId = %v, want plan-1", msg.GetPassId())
	}
	if msg.GetIndex() != 1 {
		t.Errorf("Index = %v, want 1", msg.GetIndex())
	}
}

func TestPublishGsConfigNotConnected(t *testing.T) {
	client := &mockMQTTClientWriter{connected: false}
	stats := newStatsTracker(false)

	configRequest := &v1.GroundStationConfigurationRequest{}
	err := PublishGsConfig(
		t.Context(),
		client,
		"dev/pass-123/config_request",
		"stream-1",
		"plan-1",
		1,
		configRequest,
		1,
		stats,
	)
	if err == nil {
		t.Error("PublishGsConfig() should return error when client not connected")
	}
}
