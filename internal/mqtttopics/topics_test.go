package mqtttopics

import "testing"

// Expectations are literal strings, not calls to the builder: these topics are
// a wire contract shared with the connection-authorizer, streaming clients and
// ground stations. If a test fails, the fix is almost never the expectation.

const (
	env     = "dev"
	passID  = "p1"
	channel = "c1"
)

func TestTopicFormats(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{
			name: "prefix with environment",
			got:  TopicPrefix(env, passID),
			want: "dev/pass/p1",
		},
		{
			name: "prefix without environment",
			got:  TopicPrefix("", passID),
			want: "pass/p1",
		},
		{
			name: "pass event",
			got:  EventTopic(env, passID),
			want: "dev/pass/p1/event",
		},
		{
			name: "channel event",
			got:  EventTopicPerChannel(env, passID, channel),
			want: "dev/pass/p1/channel/c1/event",
		},
		{
			name: "pass monitoring",
			got:  MonitoringTopic(env, passID),
			want: "dev/pass/p1/monitoring",
		},
		{
			name: "channel monitoring",
			got:  MonitoringTopicPerChannel(env, passID, channel),
			want: "dev/pass/p1/channel/c1/monitoring",
		},
		{
			name: "pass config state",
			got:  ConfigStateTopic(env, passID),
			want: "dev/pass/p1/config_state",
		},
		{
			name: "channel config state",
			got:  ConfigStateTopicPerChannel(env, passID, channel),
			want: "dev/pass/p1/channel/c1/config_state",
		},
		{
			name: "downlink telemetry carries the framing",
			got:  TelemetryTopic(env, passID, channel, "BITSTREAM"),
			want: "dev/pass/p1/channel/c1/downlink/BITSTREAM",
		},
		{
			name: "downlink telemetry with a different framing",
			got:  TelemetryTopic(env, passID, channel, "IQ"),
			want: "dev/pass/p1/channel/c1/downlink/IQ",
		},
		{
			name: "uplink",
			got:  UplinkTopic(env, passID, channel),
			want: "dev/pass/p1/channel/c1/uplink",
		},
		{
			name: "config request",
			got:  ConfigRequestTopic(env, passID, channel),
			want: "dev/pass/p1/channel/c1/config_request",
		},
		{
			name: "ground station status is global, not per pass",
			got:  GroundStationStatusTopic("gs-7"),
			want: "global/groundStation/gs-7/status",
		},
		{
			name: "uplink ack",
			got:  AckTopic(UplinkTopic(env, passID, channel)),
			want: "dev/pass/p1/channel/c1/uplink/ack",
		},
		{
			name: "config request ack",
			got:  AckTopic(ConfigRequestTopic(env, passID, channel)),
			want: "dev/pass/p1/channel/c1/config_request/ack",
		},
		{
			name: "telemetry ack",
			got:  AckTopic(TelemetryTopic(env, passID, channel, "BITSTREAM")),
			want: "dev/pass/p1/channel/c1/downlink/BITSTREAM/ack",
		},
		{
			name: "telemetry ack filter, any framing",
			got:  TelemetryAckTopicFilter(env, passID, channel),
			want: "dev/pass/p1/channel/c1/downlink/+/ack",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("topic = %q, want %q", tt.got, tt.want)
			}
		})
	}
}

// Covers the empty-environment variant, which a station configured with no
// environment prefix publishes.
func TestTopicFormatsWithoutEnvironment(t *testing.T) {
	tests := []struct{ got, want string }{
		{TelemetryTopic("", passID, channel, "BITSTREAM"), "pass/p1/channel/c1/downlink/BITSTREAM"},
		{UplinkTopic("", passID, channel), "pass/p1/channel/c1/uplink"},
		{ConfigRequestTopic("", passID, channel), "pass/p1/channel/c1/config_request"},
		{MonitoringTopic("", passID), "pass/p1/monitoring"},
		{EventTopic("", passID), "pass/p1/event"},
		{ConfigStateTopic("", passID), "pass/p1/config_state"},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("topic = %q, want %q", tt.got, tt.want)
		}
	}
}

// TestAckTopicIsIdempotent matters because ack topics are sometimes derived
// from a topic that already ends in /ack, and double-suffixing would publish
// acknowledgements where nobody is listening.
func TestAckTopicIsIdempotent(t *testing.T) {
	base := UplinkTopic(env, passID, channel)
	once := AckTopic(base)
	twice := AckTopic(once)
	if once != twice {
		t.Fatalf("AckTopic is not idempotent: %q then %q", once, twice)
	}
	if twice != "dev/pass/p1/channel/c1/uplink/ack" {
		t.Fatalf("topic = %q, want dev/pass/p1/channel/c1/uplink/ack", twice)
	}
}

// TestChannelIDFromTopic pins the channel-ID parser the CLI delegates to.
func TestChannelIDFromTopic(t *testing.T) {
	tests := []struct{ topic, want string }{
		{"local/pass/p1/channel/dl-1/downlink/BITSTREAM", "dl-1"},
		{"pass/p1/channel/ul-1/uplink/ack", "ul-1"},
		{"local/pass/p1/channel/ch-only", "ch-only"},
		{"local/pass/p1/monitoring", ""},
		{"", ""},
		{"local/pass/p1/channel", ""},
	}
	for _, tt := range tests {
		if got := ChannelIDFromTopic(tt.topic); got != tt.want {
			t.Errorf("ChannelIDFromTopic(%q) = %q, want %q", tt.topic, got, tt.want)
		}
	}
}
