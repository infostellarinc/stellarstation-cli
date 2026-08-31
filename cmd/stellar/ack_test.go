package main

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	streaming "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/satellitestreamer"
)

func decodeProtoAck(t *testing.T, payload []byte) *streaming.Ack {
	t.Helper()
	var ack streaming.Ack
	if err := proto.Unmarshal(payload, &ack); err != nil {
		t.Fatalf("Failed to unmarshal proto ack: %v", err)
	}
	return &ack
}

func TestBuildAck(t *testing.T) {
	now := time.Now()
	ackBytes, err := buildAck(
		"pass-123",
		"stream-456",
		"channel-789",
		messageTypeLowRateTelemetry,
		"BITSTREAM",
		42,
		1024,
		now,
		"acked-msg-1",
	)
	if err != nil {
		t.Fatalf("buildAck() error = %v", err)
	}

	ack := decodeProtoAck(t, ackBytes)

	if ack.GetPassId() != "pass-123" {
		t.Errorf("PassId = %q, want %q", ack.GetPassId(), "pass-123")
	}
	if ack.GetStreamId() != "stream-456" {
		t.Errorf("StreamId = %q, want %q", ack.GetStreamId(), "stream-456")
	}
	if ack.GetChannelId() != "channel-789" {
		t.Errorf("ChannelId = %q, want %q", ack.GetChannelId(), "channel-789")
	}
	if ack.GetMessageType() != messageTypeLowRateTelemetry {
		t.Errorf("MessageType = %q, want %q", ack.GetMessageType(), messageTypeLowRateTelemetry)
	}
	if ack.GetFramingType() != "BITSTREAM" {
		t.Errorf("FramingType = %q, want %q", ack.GetFramingType(), "BITSTREAM")
	}
	if ack.GetIndex() != 42 {
		t.Errorf("Index = %d, want %d", ack.GetIndex(), 42)
	}
	if ack.GetBytes() != 1024 {
		t.Errorf("Bytes = %d, want %d", ack.GetBytes(), 1024)
	}
	if got := ack.GetReceivedAt().AsTime().UnixNano(); got != now.UnixNano() {
		t.Errorf("ReceivedAt = %d, want %d", got, now.UnixNano())
	}
	if ack.GetAckedMessageId() != "acked-msg-1" {
		t.Errorf("AckedMessageId = %q, want %q", ack.GetAckedMessageId(), "acked-msg-1")
	}
	if ack.GetStatus() != streaming.Ack_ACK {
		t.Errorf("Status = %v, want ACK", ack.GetStatus())
	}
	if ack.GetMessageId() == "" {
		t.Error("MessageId should be set to a fresh UUID")
	}
}

func TestBuildTelemetryAck(t *testing.T) {
	cfg := Config{
		PassID: "pass-123",
		AuthorizerCreds: &AuthorizerCredentials{
			StreamID: "stream-456",
		},
	}
	now := time.Now()
	ackBytes, err := buildTelemetryAck(
		cfg,
		"ch1",
		"BITSTREAM",
		10,
		2048,
		now,
		false,
		"acked-msg-2",
	)
	if err != nil {
		t.Fatalf("buildTelemetryAck() error = %v", err)
	}

	ack := decodeProtoAck(t, ackBytes)

	if ack.GetMessageType() != messageTypeLowRateTelemetry {
		t.Errorf("MessageType = %q, want %q", ack.GetMessageType(), messageTypeLowRateTelemetry)
	}
	if ack.GetChannelId() != "ch1" {
		t.Errorf("ChannelId = %q, want %q", ack.GetChannelId(), "ch1")
	}
	if ack.GetFramingType() != "BITSTREAM" {
		t.Errorf("FramingType = %q, want %q", ack.GetFramingType(), "BITSTREAM")
	}
	if ack.GetIndex() != 10 {
		t.Errorf("Index = %d, want %d", ack.GetIndex(), 10)
	}
	if ack.GetAckedMessageId() != "acked-msg-2" {
		t.Errorf("AckedMessageId = %q, want %q", ack.GetAckedMessageId(), "acked-msg-2")
	}
}

func TestBuildTelemetryAck_HighRate(t *testing.T) {
	cfg := Config{
		PassID: "pass-123",
		AuthorizerCreds: &AuthorizerCredentials{
			StreamID: "stream-456",
		},
	}
	now := time.Now()
	ackBytes, err := buildTelemetryAck(cfg, "ch1", "IQ", 5, 4096, now, true, "")
	if err != nil {
		t.Fatalf("buildTelemetryAck() error = %v", err)
	}

	ack := decodeProtoAck(t, ackBytes)

	if ack.GetMessageType() != messageTypeHighRateTelemetry {
		t.Errorf("MessageType = %q, want %q", ack.GetMessageType(), messageTypeHighRateTelemetry)
	}
	if ack.GetAckedMessageId() != "" {
		t.Errorf("AckedMessageId should be empty but got %q", ack.GetAckedMessageId())
	}
}

func TestBuildNonTelemetryAck(t *testing.T) {
	cfg := Config{
		PassID: "pass-123",
		AuthorizerCreds: &AuthorizerCredentials{
			StreamID: "stream-456",
		},
	}
	now := time.Now()
	ackBytes, err := buildNonTelemetryAck(cfg, "monitoring", 3, 1536, now, "acked-msg-3")
	if err != nil {
		t.Fatalf("buildNonTelemetryAck() error = %v", err)
	}

	ack := decodeProtoAck(t, ackBytes)

	if ack.GetMessageType() != "monitoring" {
		t.Errorf("MessageType = %q, want %q", ack.GetMessageType(), "monitoring")
	}
	if ack.GetBytes() != 1536 {
		t.Errorf("Bytes = %d, want %d", ack.GetBytes(), 1536)
	}
	if ack.GetChannelId() != "" {
		t.Errorf("ChannelId should be empty but got %q", ack.GetChannelId())
	}
	if ack.GetFramingType() != "" {
		t.Errorf("FramingType should be empty but got %q", ack.GetFramingType())
	}
	// Non-telemetry acks carry the payload message's capture index.
	if ack.GetIndex() != 3 {
		t.Errorf("Index = %d, want %d", ack.GetIndex(), 3)
	}
	if ack.GetAckedMessageId() != "acked-msg-3" {
		t.Errorf("AckedMessageId = %q, want %q", ack.GetAckedMessageId(), "acked-msg-3")
	}
}

// TestDecodeAckStatusIndex locks in the ack contract: acknowledgements are
// proto Ack messages, and a payload that does not decode as one (such as
// arbitrary JSON) must never register as a rejection.
func TestDecodeAckStatusIndex(t *testing.T) {
	t.Run("proto ack", func(t *testing.T) {
		payload, err := proto.Marshal(&streaming.Ack{
			Status: streaming.Ack_NACK,
			Index:  7,
			Reason: "command is for a different plan",
		})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		rejected, index, err := decodeAckStatusIndex(payload)
		if err != nil {
			t.Fatalf("decodeAckStatusIndex() error = %v", err)
		}
		if !rejected {
			t.Error("proto NACK not detected as rejection")
		}
		if index != 7 {
			t.Errorf("index = %d, want 7", index)
		}
	})

	t.Run("JSON payload is not a valid ack", func(t *testing.T) {
		rejected, _, err := decodeAckStatusIndex([]byte(`{"status":"NACK","index":-1}`))
		if err == nil && rejected {
			t.Error("non-proto JSON payload registered as a rejection; only proto Ack messages are valid")
		}
	})
}

func TestBuildTelemetryTopicFromS3Key(t *testing.T) {
	cfg := Config{}
	tests := []struct {
		name      string
		s3Key     string
		channelID string
		want      string
	}{
		{
			name:      "valid low-rate S3 key",
			s3Key:     "dev/pass/pass-123/channel/ch1/downlink/BITSTREAM/1234567890",
			channelID: "ch1",
			want:      "dev/pass/pass-123/channel/ch1/downlink/BITSTREAM",
		},
		{
			name:      "invalid key - too short",
			s3Key:     "dev/pass",
			channelID: "ch1",
			want:      "",
		},
		{
			name:      "invalid key - missing parts",
			s3Key:     "dev/pass/pass-123/channel",
			channelID: "ch1",
			want:      "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildTelemetryTopicFromS3Key(cfg, tt.s3Key, tt.channelID)
			if got != tt.want {
				t.Errorf("buildTelemetryTopicFromS3Key() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBuildNonTelemetryTopicFromS3Key(t *testing.T) {
	cfg := Config{}
	tests := []struct {
		name    string
		s3Key   string
		msgType string
		want    string
	}{
		{
			name:    "pass-level monitoring",
			s3Key:   "dev/pass/pass-123/monitoring/1234567890",
			msgType: "monitoring",
			want:    "dev/pass/pass-123/monitoring",
		},
		{
			name:    "per-channel config_state",
			s3Key:   "dev/pass/pass-123/channel/ch1/config_state/1234567890",
			msgType: "config_state",
			want:    "dev/pass/pass-123/channel/ch1/config_state",
		},
		{
			name:    "invalid key - too short",
			s3Key:   "dev/pass",
			msgType: "monitoring",
			want:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildNonTelemetryTopicFromS3Key(cfg, tt.s3Key, tt.msgType)
			if got != tt.want {
				t.Errorf("buildNonTelemetryTopicFromS3Key() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBuildHighRateTelemetryTopicFromS3Key(t *testing.T) {
	tests := []struct {
		name      string
		cfg       Config
		s3Key     string
		channelID string
		want      string
	}{
		{
			name: "valid high-rate key with env",
			cfg: Config{
				Environment: "dev",
			},
			s3Key:     "pass-123/channel/ch1/downlink/BITSTREAM/42",
			channelID: "ch1",
			want:      "dev/pass/pass-123/channel/ch1/downlink/BITSTREAM",
		},
		{
			name: "valid high-rate key without env",
			cfg: Config{
				Environment: "",
			},
			s3Key:     "pass-123/channel/ch1/downlink/IQ/10",
			channelID: "ch1",
			want:      "pass/pass-123/channel/ch1/downlink/IQ",
		},
		{
			name: "alternative format",
			cfg: Config{
				Environment: "dev",
			},
			s3Key:     "pass-123/ch1/BITSTREAM/5",
			channelID: "ch1",
			want:      "dev/pass/pass-123/channel/ch1/downlink/BITSTREAM",
		},
		{
			name: "invalid key - too short",
			cfg: Config{
				Environment: "dev",
			},
			s3Key:     "pass-123",
			channelID: "ch1",
			want:      "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildHighRateTelemetryTopicFromS3Key(tt.cfg, tt.s3Key, tt.channelID)
			if got != tt.want {
				t.Errorf("buildHighRateTelemetryTopicFromS3Key() = %q, want %q", got, tt.want)
			}
		})
	}
}
