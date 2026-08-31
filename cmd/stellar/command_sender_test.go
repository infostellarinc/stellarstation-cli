package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/infostellarinc/stellarstation-cli/internal/apiclient"
	"github.com/infostellarinc/stellarstation-cli/internal/auth"
)

func TestValidateCommandFeatures(t *testing.T) {
	tests := []struct {
		name            string
		cfg             Config
		sendSatCommand  string
		sendSatCommands string
		sendGsConfig    string
		interactive     bool
		wantErr         bool
	}{
		{
			name:           "sat command enabled",
			cfg:            Config{EnableUplink: true},
			sendSatCommand: "aaaabbbb",
			wantErr:        false,
		},
		{
			name:           "sat command disabled",
			cfg:            Config{EnableUplink: false},
			sendSatCommand: "aaaabbbb",
			wantErr:        true,
		},
		{
			name:         "gs config enabled",
			cfg:          Config{EnableConfigRequests: true},
			sendGsConfig: "{}",
			wantErr:      false,
		},
		{
			name:         "gs config disabled",
			cfg:          Config{EnableConfigRequests: false},
			sendGsConfig: "{}",
			wantErr:      true,
		},
		{
			name:        "interactive with sat command enabled",
			cfg:         Config{EnableUplink: true},
			interactive: true,
			wantErr:     false,
		},
		{
			name:        "interactive with gs config enabled",
			cfg:         Config{EnableConfigRequests: true},
			interactive: true,
			wantErr:     false,
		},
		{
			name:        "interactive with both enabled",
			cfg:         Config{EnableUplink: true, EnableConfigRequests: true},
			interactive: true,
			wantErr:     false,
		},
		{
			name:        "interactive with none enabled",
			cfg:         Config{EnableUplink: false, EnableConfigRequests: false},
			interactive: true,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateCommandFeatures(
				tt.cfg,
				tt.sendSatCommand,
				tt.sendSatCommands,
				tt.sendGsConfig,
				tt.interactive,
			)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateCommandFeatures() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNormalizeIDs(t *testing.T) {
	tests := []struct {
		name         string
		cfg          Config
		streamID     string
		planID       string
		passID       string
		wantStreamID string
	}{
		{
			name:         "all provided",
			cfg:          Config{},
			streamID:     "stream-123",
			planID:       "plan-456",
			passID:       "pass-789",
			wantStreamID: "stream-123",
		},
		{
			name: "empty streamID with authorizer",
			cfg: Config{
				AuthorizerCreds: &AuthorizerCredentials{StreamID: "streamer-abc123"},
			},
			streamID:     "",
			planID:       "plan-456",
			passID:       "pass-789",
			wantStreamID: "streamer-abc123",
		},
		{
			name:         "empty streamID without authorizer",
			cfg:          Config{},
			streamID:     "",
			planID:       "plan-456",
			passID:       "pass-789",
			wantStreamID: "", // Will be generated as stream-<pid>, but we can't predict it
		},
		{
			name:         "empty planID",
			cfg:          Config{},
			streamID:     "stream-123",
			planID:       "",
			passID:       "pass-789",
			wantStreamID: "stream-123",
		},
		{
			name:         "both empty",
			cfg:          Config{},
			streamID:     "",
			planID:       "",
			passID:       "pass-789",
			wantStreamID: "", // Will be generated as stream-<pid>, but we can't predict it
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotStreamID, gotPlanID := normalizeIDs(tt.cfg, tt.streamID, tt.planID, tt.passID)
			switch {
			case tt.wantStreamID != "":
				if gotStreamID != tt.wantStreamID {
					t.Errorf("normalizeIDs() streamID = %v, want %v", gotStreamID, tt.wantStreamID)
				}
			case tt.streamID != "":
				// If streamID was provided, it should be used
				if gotStreamID != tt.streamID {
					t.Errorf("normalizeIDs() streamID = %v, want %v", gotStreamID, tt.streamID)
				}
			default:
				// streamID should be generated (either from authorizer or as stream-<pid>)
				if gotStreamID == "" {
					t.Error("normalizeIDs() streamID should be generated when empty")
				}
			}
			if tt.planID == "" {
				// planID should default to passID
				if gotPlanID != tt.passID {
					t.Errorf("normalizeIDs() planID = %v, want %v", gotPlanID, tt.passID)
				}
			} else {
				if gotPlanID != tt.planID {
					t.Errorf("normalizeIDs() planID = %v, want %v", gotPlanID, tt.planID)
				}
			}
		})
	}
}

func TestParseHexCommand(t *testing.T) {
	tests := []struct {
		name    string
		hexStr  string
		want    []byte
		wantErr bool
	}{
		{
			name:    "simple hex",
			hexStr:  "aaaabbbb",
			want:    []byte{0xaa, 0xaa, 0xbb, 0xbb},
			wantErr: false,
		},
		{
			name:    "with 0x prefix",
			hexStr:  "0xaaaabbbb",
			want:    []byte{0xaa, 0xaa, 0xbb, 0xbb},
			wantErr: false,
		},
		{
			name:    "with 0X prefix",
			hexStr:  "0Xaaaabbbb",
			want:    []byte{0xaa, 0xaa, 0xbb, 0xbb},
			wantErr: false,
		},
		{
			name:    "with spaces",
			hexStr:  "aa aa bb bb",
			want:    []byte{0xaa, 0xaa, 0xbb, 0xbb},
			wantErr: false,
		},
		{
			name:    "with leading/trailing spaces",
			hexStr:  "  aaaabbbb  ",
			want:    []byte{0xaa, 0xaa, 0xbb, 0xbb},
			wantErr: false,
		},
		{
			name:    "invalid hex",
			hexStr:  "invalid",
			want:    nil,
			wantErr: true,
		},
		{
			name:    "empty string",
			hexStr:  "",
			want:    []byte{},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseHexCommand(tt.hexStr)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseHexCommand() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				if len(got) != len(tt.want) {
					t.Errorf("parseHexCommand() length = %v, want %v", len(got), len(tt.want))
					return
				}
				for i := range got {
					if got[i] != tt.want[i] {
						t.Errorf("parseHexCommand() [%d] = %v, want %v", i, got[i], tt.want[i])
					}
				}
			}
		})
	}
}

func TestHandleOneShotSatCommand(t *testing.T) {
	tests := []struct {
		name            string
		sendSatCommand  string
		sendCommandFunc func(string, uint32) error
		wantErr         bool
	}{
		{
			name:           "empty command",
			sendSatCommand: "",
			sendCommandFunc: func(string, uint32) error {
				return errors.New("should not be called")
			},
			wantErr: false,
		},
		{
			name:           "valid command",
			sendSatCommand: "aaaabbbb",
			sendCommandFunc: func(cmd string, idx uint32) error {
				if cmd != "aaaabbbb" {
					return errors.New("unexpected command")
				}
				if idx != 1 {
					return errors.New("unexpected index")
				}
				return nil
			},
			wantErr: false,
		},
		{
			name:           "command error",
			sendSatCommand: "aaaabbbb",
			sendCommandFunc: func(string, uint32) error {
				return errors.New("send error")
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := handleOneShotSatCommand(tt.sendSatCommand, tt.sendCommandFunc)
			if (err != nil) != tt.wantErr {
				t.Errorf("handleOneShotSatCommand() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHandleOneShotSatCommands(t *testing.T) {
	tests := []struct {
		name            string
		sendSatCommands string
		sendCommandFunc func(string, uint32) error
		wantErr         bool
	}{
		{
			name:            "empty commands",
			sendSatCommands: "",
			sendCommandFunc: func(string, uint32) error {
				return errors.New("should not be called")
			},
			wantErr: false,
		},
		{
			name:            "single command",
			sendSatCommands: "aaaabbbb",
			sendCommandFunc: func(cmd string, idx uint32) error {
				if cmd != "aaaabbbb" {
					return errors.New("unexpected command")
				}
				if idx != 1 {
					return errors.New("unexpected index")
				}
				return nil
			},
			wantErr: false,
		},
		{
			name:            "multiple commands",
			sendSatCommands: "aaaabbbb,12345678",
			sendCommandFunc: func(cmd string, idx uint32) error {
				if idx > 2 {
					return errors.New("unexpected index")
				}
				return nil
			},
			wantErr: false,
		},
		{
			name:            "command error",
			sendSatCommands: "aaaabbbb",
			sendCommandFunc: func(string, uint32) error {
				return errors.New("send error")
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := handleOneShotSatCommands(tt.sendSatCommands, tt.sendCommandFunc)
			if (err != nil) != tt.wantErr {
				t.Errorf("handleOneShotSatCommands() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHandleOneShotGsConfig(t *testing.T) {
	tests := []struct {
		name           string
		sendGsConfig   string
		sendConfigFunc func(string, uint32) error
		wantErr        bool
	}{
		{
			name:         "empty config",
			sendGsConfig: "",
			sendConfigFunc: func(string, uint32) error {
				return errors.New("should not be called")
			},
			wantErr: false,
		},
		{
			name:         "valid config",
			sendGsConfig: "{}",
			sendConfigFunc: func(config string, idx uint32) error {
				if config != "{}" {
					return errors.New("unexpected config")
				}
				if idx != 1 {
					return errors.New("unexpected index")
				}
				return nil
			},
			wantErr: false,
		},
		{
			name:         "config error",
			sendGsConfig: "{}",
			sendConfigFunc: func(string, uint32) error {
				return errors.New("send error")
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := handleOneShotGsConfig(tt.sendGsConfig, tt.sendConfigFunc)
			if (err != nil) != tt.wantErr {
				t.Errorf("handleOneShotGsConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestResolveAllCommandTargets(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		want    []channelTarget
		wantErr bool
	}{
		{
			name:    "nil authorizer creds",
			cfg:     Config{},
			wantErr: true,
		},
		{
			name: "single channel with both uplink and config_request",
			cfg: Config{
				AuthorizerCreds: &AuthorizerCredentials{
					Streams: StreamsConfig{
						Uplink: []CommandConfig{
							{PublishTopic: "env/pass/p1/channel/ch-aaa/uplink"},
						},
						ConfigRequest: []CommandConfig{
							{PublishTopic: "env/pass/p1/channel/ch-aaa/config_request"},
						},
					},
				},
			},
			want: []channelTarget{
				{
					ChannelID:          "ch-aaa",
					UplinkTopic:        "env/pass/p1/channel/ch-aaa/uplink",
					ConfigRequestTopic: "env/pass/p1/channel/ch-aaa/config_request",
				},
			},
		},
		{
			name: "multiple channels sorted by ID",
			cfg: Config{
				AuthorizerCreds: &AuthorizerCredentials{
					Streams: StreamsConfig{
						Uplink: []CommandConfig{
							{PublishTopic: "env/pass/p1/channel/ch-bbb/uplink"},
							{PublishTopic: "env/pass/p1/channel/ch-aaa/uplink"},
						},
					},
				},
			},
			want: []channelTarget{
				{ChannelID: "ch-aaa", UplinkTopic: "env/pass/p1/channel/ch-aaa/uplink"},
				{ChannelID: "ch-bbb", UplinkTopic: "env/pass/p1/channel/ch-bbb/uplink"},
			},
		},
		{
			name: "empty streams returns empty",
			cfg: Config{
				AuthorizerCreds: &AuthorizerCredentials{
					Streams: StreamsConfig{},
				},
			},
			want: []channelTarget{},
		},
		{
			// The authorizer returns an uplink topic for every channel, but only the
			// uplink-direction channel is a real command target. With direction
			// metadata present, downlink channels are filtered out so a single
			// command target remains (enabling the omit-channel interactive shortcut).
			name: "direction metadata restricts targets to uplink channel",
			cfg: Config{
				AuthorizerCreds: &AuthorizerCredentials{
					Channels: []ChannelMetadata{
						{ChannelID: "ch-up", Direction: "uplink"},
						{ChannelID: "ch-dn1", Direction: "downlink"},
						{ChannelID: "ch-dn2", Direction: "downlink"},
					},
					Streams: StreamsConfig{
						Uplink: []CommandConfig{
							{PublishTopic: "env/pass/p1/channel/ch-dn1/uplink"},
							{PublishTopic: "env/pass/p1/channel/ch-up/uplink"},
							{PublishTopic: "env/pass/p1/channel/ch-dn2/uplink"},
						},
						ConfigRequest: []CommandConfig{
							{PublishTopic: "env/pass/p1/channel/ch-dn1/config_request"},
							{PublishTopic: "env/pass/p1/channel/ch-up/config_request"},
						},
					},
				},
			},
			want: []channelTarget{
				{
					ChannelID:          "ch-up",
					UplinkTopic:        "env/pass/p1/channel/ch-up/uplink",
					ConfigRequestTopic: "env/pass/p1/channel/ch-up/config_request",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveAllCommandTargets(tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("resolveAllCommandTargets() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if len(got) != len(tt.want) {
				t.Errorf(
					"resolveAllCommandTargets() got %d targets, want %d",
					len(got),
					len(tt.want),
				)
				return
			}
			for i, g := range got {
				w := tt.want[i]
				if g.ChannelID != w.ChannelID || g.UplinkTopic != w.UplinkTopic ||
					g.ConfigRequestTopic != w.ConfigRequestTopic {
					t.Errorf("target[%d] = %+v, want %+v", i, g, w)
				}
			}
		})
	}
}

func TestParseChannelID(t *testing.T) {
	targets := []channelTarget{
		{ChannelID: "ch-aaa", UplinkTopic: "topic-a"},
		{ChannelID: "ch-bbb", UplinkTopic: "topic-b"},
		{ChannelID: "ch-ccc", UplinkTopic: "topic-c"},
	}

	tests := []struct {
		input    string
		wantIdx  int
		wantChID string
		wantOK   bool
	}{
		{"ch-aaa", 0, "ch-aaa", true},
		{"ch-bbb", 1, "ch-bbb", true},
		{"ch-ccc", 2, "ch-ccc", true},
		{"ch-ddd", 0, "", false},
		{"CH-AAA", 0, "", false},
		{"1", 0, "", false},
		{"", 0, "", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			idx, target, ok := parseChannelID(tt.input, targets)
			if ok != tt.wantOK {
				t.Errorf("parseChannelID(%q) ok = %v, want %v", tt.input, ok, tt.wantOK)
				return
			}
			if !ok {
				return
			}
			if idx != tt.wantIdx {
				t.Errorf("parseChannelID(%q) idx = %d, want %d", tt.input, idx, tt.wantIdx)
			}
			if target.ChannelID != tt.wantChID {
				t.Errorf(
					"parseChannelID(%q) target.ChannelID = %q, want %q",
					tt.input,
					target.ChannelID,
					tt.wantChID,
				)
			}
		})
	}
}

func TestSingleTargetIndex(t *testing.T) {
	hasUplink := func(t channelTarget) bool { return t.UplinkTopic != "" }

	tests := []struct {
		name    string
		targets []channelTarget
		wantIdx int
	}{
		{
			name:    "no targets",
			targets: nil,
			wantIdx: -1,
		},
		{
			name: "single match",
			targets: []channelTarget{
				{ChannelID: "ch-aaa", UplinkTopic: "topic-a"},
			},
			wantIdx: 0,
		},
		{
			name: "single match among multiple targets",
			targets: []channelTarget{
				{ChannelID: "ch-aaa"},
				{ChannelID: "ch-bbb", UplinkTopic: "topic-b"},
				{ChannelID: "ch-ccc"},
			},
			wantIdx: 1,
		},
		{
			name: "multiple matches returns -1",
			targets: []channelTarget{
				{ChannelID: "ch-aaa", UplinkTopic: "topic-a"},
				{ChannelID: "ch-bbb", UplinkTopic: "topic-b"},
			},
			wantIdx: -1,
		},
		{
			name: "no matches returns -1",
			targets: []channelTarget{
				{ChannelID: "ch-aaa"},
				{ChannelID: "ch-bbb"},
			},
			wantIdx: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := singleTargetIndex(tt.targets, hasUplink)
			if got != tt.wantIdx {
				t.Errorf("singleTargetIndex() = %d, want %d", got, tt.wantIdx)
			}
		})
	}
}

func TestResolveCommandArgs(t *testing.T) {
	targets := []channelTarget{
		{ChannelID: "ch-aaa", UplinkTopic: "topic-a"},
		{ChannelID: "ch-bbb", UplinkTopic: "topic-b"},
		{ChannelID: "ch-ccc", UplinkTopic: "topic-c"},
	}

	tests := []struct {
		name        string
		args        []string
		defaultIdx  int
		wantChIdx   int
		wantPayload string
		wantOK      bool
	}{
		{
			name:        "no args with default channel",
			args:        []string{},
			defaultIdx:  0,
			wantChIdx:   0,
			wantPayload: "",
			wantOK:      true,
		},
		{
			name:       "no args without default channel",
			args:       []string{},
			defaultIdx: -1,
			wantOK:     false,
		},
		{
			name:        "payload only with default channel",
			args:        []string{"aaaabbbb"},
			defaultIdx:  1,
			wantChIdx:   1,
			wantPayload: "aaaabbbb",
			wantOK:      true,
		},
		{
			name:        "channel ID and payload without default",
			args:        []string{"ch-bbb", "aaaabbbb"},
			defaultIdx:  -1,
			wantChIdx:   1,
			wantPayload: "aaaabbbb",
			wantOK:      true,
		},
		{
			name:        "channel ID only without default",
			args:        []string{"ch-aaa"},
			defaultIdx:  -1,
			wantChIdx:   0,
			wantPayload: "",
			wantOK:      true,
		},
		{
			name:       "unknown channel ID without default",
			args:       []string{"ch-zzz", "aaaabbbb"},
			defaultIdx: -1,
			wantOK:     false,
		},
		{
			name:        "payload with spaces and default channel",
			args:        []string{"{\"key\":", "\"value\"}"},
			defaultIdx:  0,
			wantChIdx:   0,
			wantPayload: "{\"key\": \"value\"}",
			wantOK:      true,
		},
		{
			name:        "channel ID with multi-word payload",
			args:        []string{"ch-ccc", "{\"key\":", "\"value\"}"},
			defaultIdx:  -1,
			wantChIdx:   2,
			wantPayload: "{\"key\": \"value\"}",
			wantOK:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			chIdx, payload, ok := resolveCommandArgs(tt.args, targets, tt.defaultIdx, "sat")
			if ok != tt.wantOK {
				t.Errorf("resolveCommandArgs() ok = %v, want %v", ok, tt.wantOK)
				return
			}
			if !ok {
				return
			}
			if chIdx != tt.wantChIdx {
				t.Errorf("resolveCommandArgs() chIdx = %d, want %d", chIdx, tt.wantChIdx)
			}
			if payload != tt.wantPayload {
				t.Errorf("resolveCommandArgs() payload = %q, want %q", payload, tt.wantPayload)
			}
		})
	}
}

// Mock MQTT client for testing
type mockMQTTClient struct {
	connected  bool
	published  bool
	pubTopic   string
	pubQoS     byte
	pubPayload []byte
}

func (m *mockMQTTClient) IsConnected() bool {
	return m.connected
}

func (m *mockMQTTClient) Publish(
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
	return &mockToken{err: nil}
}

func (m *mockMQTTClient) Subscribe(
	topic string,
	qos byte,
	callback mqtt.MessageHandler,
) mqtt.Token {
	return &mockToken{err: nil}
}

func (m *mockMQTTClient) Disconnect(quiesce uint) {}

func (m *mockMQTTClient) AddRoute(topic string, callback mqtt.MessageHandler) {}

func (m *mockMQTTClient) Connect() mqtt.Token {
	return &mockToken{err: nil}
}

func (m *mockMQTTClient) IsConnectionOpen() bool {
	return m.connected
}

func (m *mockMQTTClient) OptionsReader() mqtt.ClientOptionsReader {
	return mqtt.NewClient(nil).OptionsReader()
}

func (m *mockMQTTClient) SubscribeMultiple(
	filters map[string]byte,
	callback mqtt.MessageHandler,
) mqtt.Token {
	return &mockToken{err: nil}
}

func (m *mockMQTTClient) Unsubscribe(topics ...string) mqtt.Token {
	return &mockToken{err: nil}
}

type mockToken struct {
	err error
}

func (m *mockToken) Wait() bool {
	return m.err == nil
}

func (m *mockToken) WaitTimeout(timeout time.Duration) bool {
	return m.err == nil
}

func (m *mockToken) Error() error {
	return m.err
}

func (m *mockToken) Done() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func TestCreateSendSatCommandFunc(t *testing.T) {
	ctx := t.Context()
	client := &mockMQTTClient{connected: true}
	cfg := Config{MQTTQoS: 1}
	stats := newStatsTracker(false)

	sendFunc := createSendSatCommandFunc(
		ctx,
		client,
		"dev/pass-123/uplink",
		"stream-1",
		"plan-1",
		cfg,
		stats,
	)

	err := sendFunc("aaaabbbb", 1)
	if err != nil {
		t.Fatalf("createSendSatCommandFunc() error = %v", err)
	}
	if !client.published {
		t.Error("Command should have been published")
	}
}

func TestCreateSendGsConfigFunc(t *testing.T) {
	ctx := t.Context()
	client := &mockMQTTClient{connected: true}
	cfg := Config{MQTTQoS: 1}
	stats := newStatsTracker(false)

	sendFunc := createSendGsConfigFunc(
		ctx,
		client,
		"dev/pass-123/config_request",
		"stream-1",
		"plan-1",
		cfg,
		stats,
	)

	err := sendFunc("{}", 1)
	if err != nil {
		t.Fatalf("createSendGsConfigFunc() error = %v", err)
	}
	if !client.published {
		t.Error("Config should have been published")
	}
}

func TestPassWindowCheck(t *testing.T) {
	start := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	stop := start.Add(10 * time.Minute)
	window := passWindow{start: start, stop: stop}

	tests := []struct {
		name    string
		window  passWindow
		now     time.Time
		wantErr string
	}{
		{
			name:    "before the booking starts",
			window:  window,
			now:     start.Add(-90 * time.Second),
			wantErr: "the pass has not started",
		},
		{
			name:   "exactly at the booking start",
			window: window,
			now:    start,
		},
		{
			name:   "inside the booking",
			window: window,
			now:    start.Add(5 * time.Minute),
		},
		{
			name:   "exactly at the booking stop",
			window: window,
			now:    stop,
		},
		{
			name:    "after the booking stops",
			window:  window,
			now:     stop.Add(time.Second),
			wantErr: "the pass ended",
		},
		{
			name:   "unresolved window permits everything",
			window: passWindow{},
			now:    start.Add(-24 * time.Hour),
		},
		{
			name:   "half-resolved window permits everything",
			window: passWindow{start: start},
			now:    start.Add(-24 * time.Hour),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.window.check(tt.now)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("check(%s) = %v, want nil", tt.now, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("check(%s) = nil, want error containing %q", tt.now, tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("check(%s) = %q, want it to contain %q", tt.now, err, tt.wantErr)
			}
		})
	}
}

func TestPassWindowResolved(t *testing.T) {
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		window passWindow
		want   bool
	}{
		{name: "both edges set", window: passWindow{start: now, stop: now.Add(time.Minute)}, want: true},
		{name: "zero window", window: passWindow{}},
		{name: "stop missing", window: passWindow{start: now}},
		{name: "start missing", window: passWindow{stop: now}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.window.resolved(); got != tt.want {
				t.Fatalf("resolved() = %v, want %v", got, tt.want)
			}
		})
	}
}

// newBookingPassServer serves GET /v1/passes/{id} with the supplied booking
// interval, so the window resolver can be exercised without a live API.
func newBookingPassServer(t *testing.T, booking *apiclient.Interval) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/v1/passes/") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(apiclient.Pass{ID: "p-1", Booking: booking})
	}))
}

func TestResolveBookingWindow(t *testing.T) {
	start := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	stop := start.Add(10 * time.Minute)

	t.Run("returns the booking interval", func(t *testing.T) {
		srv := newBookingPassServer(t, &apiclient.Interval{Start: start, Stop: stop})
		defer srv.Close()

		cfg := Config{AuthorizerAPI: srv.URL, TokenSource: auth.StaticTokenSource{Value: "test-token"}}
		got := resolveBookingWindow(context.Background(), cfg, "p-1")

		if !got.start.Equal(start) || !got.stop.Equal(stop) {
			t.Fatalf("resolveBookingWindow() = %s..%s, want %s..%s", got.start, got.stop, start, stop)
		}
	})

	t.Run("a pass without a booking leaves the window unresolved", func(t *testing.T) {
		srv := newBookingPassServer(t, nil)
		defer srv.Close()

		cfg := Config{AuthorizerAPI: srv.URL, TokenSource: auth.StaticTokenSource{Value: "test-token"}}
		if got := resolveBookingWindow(context.Background(), cfg, "p-1"); got.resolved() {
			t.Fatalf("resolveBookingWindow() = %+v, want an unresolved window", got)
		}
	})

	t.Run("a failing pass fetch leaves the window unresolved", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer srv.Close()

		cfg := Config{AuthorizerAPI: srv.URL, TokenSource: auth.StaticTokenSource{Value: "test-token"}}
		if got := resolveBookingWindow(context.Background(), cfg, "p-1"); got.resolved() {
			t.Fatalf("resolveBookingWindow() = %+v, want an unresolved window", got)
		}
	})

	t.Run("missing inputs leave the window unresolved", func(t *testing.T) {
		srv := newBookingPassServer(t, &apiclient.Interval{Start: start, Stop: stop})
		defer srv.Close()
		tokens := auth.StaticTokenSource{Value: "test-token"}

		cases := []struct {
			name   string
			cfg    Config
			passID string
		}{
			{name: "no pass ID", cfg: Config{AuthorizerAPI: srv.URL, TokenSource: tokens}},
			{name: "no token source", cfg: Config{AuthorizerAPI: srv.URL}, passID: "p-1"},
			{name: "no API URL", cfg: Config{TokenSource: tokens}, passID: "p-1"},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				got := resolveBookingWindow(context.Background(), tc.cfg, tc.passID)
				if got.resolved() {
					t.Fatalf("resolveBookingWindow() = %+v, want an unresolved window", got)
				}
			})
		}
	})
}
