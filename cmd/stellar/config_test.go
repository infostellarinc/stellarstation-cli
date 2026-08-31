package main

import (
	"testing"
)

func TestBuildAckTopic(t *testing.T) {
	tests := []struct {
		name      string
		baseTopic string
		want      string
	}{
		{
			name:      "pass-level config_state topic",
			baseTopic: "dev/pass/pass-123/config",
			want:      "dev/pass/pass-123/config_state/ack",
		},
		{
			name:      "per-channel config_state topic",
			baseTopic: "dev/pass/pass-123/channel/channel-1/config_state",
			want:      "dev/pass/pass-123/channel/channel-1/config_state/ack",
		},
		{
			name:      "telemetry topic with framing",
			baseTopic: "dev/pass/pass-123/channel/channel-1/downlink/BITSTREAM",
			want:      "dev/pass/pass-123/channel/channel-1/downlink/BITSTREAM/ack",
		},
		{
			name:      "uplink topic",
			baseTopic: "dev/pass/pass-123/channel/channel-1/uplink",
			want:      "dev/pass/pass-123/channel/channel-1/uplink/ack",
		},
		{
			name:      "config_request topic",
			baseTopic: "dev/pass/pass-123/channel/channel-1/config_request",
			want:      "dev/pass/pass-123/channel/channel-1/config_request/ack",
		},
		{
			name:      "monitoring topic",
			baseTopic: "dev/pass/pass-123/monitoring",
			want:      "dev/pass/pass-123/monitoring/ack",
		},
		{
			name:      "event topic",
			baseTopic: "dev/pass/pass-123/event",
			want:      "dev/pass/pass-123/event/ack",
		},
		{
			name:      "per-channel monitoring topic",
			baseTopic: "dev/pass/pass-123/channel/channel-1/monitoring",
			want:      "dev/pass/pass-123/channel/channel-1/monitoring/ack",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildAckTopic(tt.baseTopic)
			if got != tt.want {
				t.Errorf("buildAckTopic() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractChannelIDFromTopic(t *testing.T) {
	tests := []struct {
		name  string
		topic string
		want  string
	}{
		{
			name:  "telemetry topic",
			topic: "dev/pass/pass-123/channel/channel-1/downlink/BITSTREAM",
			want:  "channel-1",
		},
		{
			name:  "per-channel config_state topic",
			topic: "dev/pass/pass-123/channel/channel-2/config_state",
			want:  "channel-2",
		},
		{
			name:  "per-channel monitoring topic",
			topic: "dev/pass/pass-123/channel/channel-3/monitoring",
			want:  "channel-3",
		},
		{
			name:  "per-channel event topic",
			topic: "dev/pass/pass-123/channel/channel-4/event",
			want:  "channel-4",
		},
		{
			name:  "uplink topic",
			topic: "dev/pass/pass-123/channel/channel-5/uplink",
			want:  "channel-5",
		},
		{
			name:  "config_request topic",
			topic: "dev/pass/pass-123/channel/channel-6/config_request",
			want:  "channel-6",
		},
		{
			name:  "pass-level config_state topic (no channel)",
			topic: "dev/pass/pass-123/config_state",
			want:  "",
		},
		{
			name:  "pass-level monitoring topic (no channel)",
			topic: "dev/pass/pass-123/monitoring",
			want:  "",
		},
		{
			name:  "empty topic",
			topic: "",
			want:  "",
		},
		{
			name:  "topic without channel segment",
			topic: "dev/pass/pass-123/event",
			want:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractChannelIDFromTopic(tt.topic)
			if got != tt.want {
				t.Errorf("ExtractChannelIDFromTopic() = %v, want %v", got, tt.want)
			}
		})
	}
}
