package main

import (
	"errors"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func newTestPublisher(t *testing.T, client mqtt.Client) *commandPublisher {
	t.Helper()
	return newCommandPublisher(client, 1, newStatsTracker(false), passWindow{})
}

const testUplinkTopic = "dev/pass/p1/channel/c1/uplink"

func TestPublishSatCommandAndGsConfig(t *testing.T) {
	tests := []struct {
		name        string
		publish     func(*commandPublisher) (*sentCommand, error)
		wantType    string
		wantCommand []byte
		wantConfig  bool
	}{
		{
			name: "satellite command",
			publish: func(p *commandPublisher) (*sentCommand, error) {
				return PublishSatCommand(p, testUplinkTopic, "stream-1", "plan-1", 3, [][]byte{{0x0A, 0x1B}})
			},
			wantType:    cmdTypeUplink,
			wantCommand: []byte{0x0A, 0x1B},
		},
		{
			name: "config request",
			publish: func(p *commandPublisher) (*sentCommand, error) {
				return PublishGsConfig(p, testUplinkTopic, "stream-1", "plan-1", 3,
					&v1.GroundStationConfigurationRequest{})
			},
			wantType:   cmdTypeConfigRequest,
			wantConfig: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockMQTTClientWriter{connected: true}
			pub := newTestPublisher(t, client)

			cmd, err := tt.publish(pub)
			require.NoError(t, err)
			assert.Equal(t, testUplinkTopic, client.pubTopic)
			assert.Equal(t, byte(1), client.pubQoS)

			var msg streaming.ToStarPassMessage
			require.NoError(t, proto.Unmarshal(client.pubPayload, &msg))
			assert.Equal(t, "stream-1", msg.GetStreamId())
			assert.Equal(t, "plan-1", msg.GetPassId())
			assert.Equal(t, uint32(3), msg.GetIndex())
			assert.NotEmpty(t, msg.GetMessageId())
			if tt.wantCommand != nil {
				assert.Equal(t, [][]byte{tt.wantCommand}, msg.GetCommand())
			}
			assert.Equal(t, tt.wantConfig, msg.GetGroundStationConfigurationRequest() != nil)

			assert.Equal(t, tt.wantType, cmd.cmdType)
			assert.Equal(t, msg.GetMessageId(), cmd.messageID)
			assert.Equal(t, "stream-1", cmd.streamID)
			assert.Len(t, pub.stats.sentCommands, 1)
		})
	}
}

func TestPublishWhenNotConnectedIsRefused(t *testing.T) {
	pub := newTestPublisher(t, &mockMQTTClientWriter{connected: false})
	_, err := PublishSatCommand(pub, testUplinkTopic, "s", "p", 1, [][]byte{{0x01}})
	require.Error(t, err)
	_, err = PublishGsConfig(pub, testUplinkTopic, "s", "p", 1, &v1.GroundStationConfigurationRequest{})
	require.Error(t, err)
	assert.Empty(t, pub.stats.sentCommands, "a command that was never published is not recorded")
}

// A failed publish is reported, and the command keeps its index: the broker
// may still deliver it, so a copy under the same index would be refused.
func TestPublishFailureIsReportedAndKeepsTheIndex(t *testing.T) {
	pub := newTestPublisher(t, &mockMQTTClientWriter{connected: true, pubErr: errors.New("timeout")})
	cmd, err := PublishSatCommand(pub, testUplinkTopic, "s", "p", 1, [][]byte{{0x01}})
	require.ErrorContains(t, err, "command #1 was not confirmed by the broker")
	require.NotNil(t, cmd)
	assert.Len(t, pub.stats.sentCommands, 1)
}

// A command is published once; the client never retries an unanswered one.
func TestCommandIsPublishedOnce(t *testing.T) {
	client := &recordingMQTTClient{mockMQTTClient: mockMQTTClient{connected: true}}
	pub := newTestPublisher(t, client)
	publishTestCommand(t, pub)
	assert.Never(t, func() bool { return client.publishCount() != 1 }, 50*time.Millisecond, time.Millisecond)
	assert.Len(t, pub.stats.sentCommands, 1)
}

// publishTestCommand publishes command #1 on pub and returns it.
func publishTestCommand(t *testing.T, pub *commandPublisher) *sentCommand {
	t.Helper()
	cmd, err := PublishSatCommand(pub, testUplinkTopic, "stream-1", "plan-1", 1, [][]byte{{0x0A, 0x1B}})
	require.NoError(t, err)
	return cmd
}

// testAck builds an Ack payload answering cmd.
func testAck(t *testing.T, cmd *sentCommand, status streaming.Ack_Status, reasonCode string) []byte {
	t.Helper()
	payload, err := proto.Marshal(&streaming.Ack{
		AckedMessageId: cmd.messageID,
		MessageId:      "ack-" + reasonCode,
		Status:         status,
		Index:          uint64(cmd.index),
		ReasonCode:     reasonCode,
	})
	require.NoError(t, err)
	return payload
}
