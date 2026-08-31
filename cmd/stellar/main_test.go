package main

import (
	"testing"
	"time"
)

func TestNewRootCommand(t *testing.T) {
	cmd := NewRootCommand()
	if cmd.Use != "stellar" {
		t.Errorf("root command Use = %q, want %q", cmd.Use, "stellar")
	}

	subNames := make(map[string]bool)
	for _, sub := range cmd.Commands() {
		subNames[sub.Name()] = true
	}
	for _, want := range []string{"satellite", "auth", "version"} {
		if !subNames[want] {
			t.Errorf("root command missing subcommand %q", want)
		}
	}
}

func TestSatelliteCommandHasOpenStream(t *testing.T) {
	cmd := newSatelliteCommand()
	subNames := make(map[string]bool)
	for _, sub := range cmd.Commands() {
		subNames[sub.Name()] = true
	}
	if !subNames["open-stream"] {
		t.Error("satellite command missing open-stream subcommand")
	}
}

func TestOpenStreamCommandFlags(t *testing.T) {
	cmd := newOpenStreamCommand()
	flags := cmd.Flags()

	want := []string{
		"source", "pass-id", "dest", "s3-poll-interval", "window", "write-in-order",
		"mqtt-qos", "channels",
		"send-sat-command", "send-sat-commands", "send-gs-config", "interactive",
		"disable-downlink", "disable-monitoring", "disable-config-state",
		"disable-event", "disable-config-requests", "disable-uplink",
		"disable-diagnostics", "output-file", "output-file-mode", "stdout",
		"stats", "verbose", "debug", "enable-auto-close", "accepted-framing",
		"proxy", "udp-listen-addr", "udp-send-addr", "tcp-listen-addr", "proxy-allow-remote",
	}

	for _, name := range want {
		if flags.Lookup(name) == nil {
			t.Errorf("open-stream command missing flag --%s", name)
		}
	}
}

func TestVersionCommand(t *testing.T) {
	cmd := newVersionCommand()
	if cmd.Use != "version" {
		t.Errorf("version command Use = %q, want %q", cmd.Use, "version")
	}
}

func TestDetermineSourceType(t *testing.T) {
	tests := []struct {
		name  string
		flags *flagSet
		want  SourceType
	}{
		{
			name:  "downlink enabled",
			flags: &flagSet{sourceType: "mqtt", disableDownlink: false},
			want:  SourceTypeMQTT,
		},
		{
			name:  "downlink disabled",
			flags: &flagSet{sourceType: "s3", disableDownlink: true},
			want:  SourceTypeS3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := determineSourceType(tt.flags)
			if got != tt.want {
				t.Errorf("determineSourceType() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseChannelIDs(t *testing.T) {
	uuid1 := "123e4567-e89b-12d3-a456-426614174000"
	uuid2 := "123e4567-e89b-12d3-a456-426614174001"
	uuid3 := "123e4567-e89b-12d3-a456-426614174002"

	tests := []struct {
		name    string
		flags   *flagSet
		want    []string
		wantErr bool
	}{
		{
			name:  "multiple channels",
			flags: &flagSet{channelsStr: uuid1 + "," + uuid2 + "," + uuid3},
			want:  []string{uuid1, uuid2, uuid3},
		},
		{
			name:  "single channel",
			flags: &flagSet{channelsStr: uuid1},
			want:  []string{uuid1},
		},
		{
			name:  "channels with spaces",
			flags: &flagSet{channelsStr: uuid1 + ", " + uuid2 + ", " + uuid3},
			want:  []string{uuid1, uuid2, uuid3},
		},
		{
			name:  "empty channels string",
			flags: &flagSet{channelsStr: ""},
			want:  []string{},
		},
		{
			name:    "invalid UUID returns error",
			flags:   &flagSet{channelsStr: "not-a-uuid"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseChannelIDs(tt.flags)
			if tt.wantErr {
				if err == nil {
					t.Error("parseChannelIDs() expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("parseChannelIDs() unexpected error: %v", err)
				return
			}
			if len(got) != len(tt.want) {
				t.Errorf("parseChannelIDs() length = %v, want %v", len(got), len(tt.want))
				return
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("parseChannelIDs() [%d] = %v, want %v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestGetEffectiveEnvironment(t *testing.T) {
	tests := []struct {
		name   string
		envVar string
		want   string
	}{
		{
			name: "unset",
			want: "",
		},
		{
			name:   "env var set",
			envVar: "prod",
			want:   "prod",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("ENV", tt.envVar)
			got := getEffectiveEnvironment()
			if got != tt.want {
				t.Errorf("getEffectiveEnvironment() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBuildPrefix(t *testing.T) {
	tests := []struct {
		name         string
		flags        *flagSet
		effectiveEnv string
		passID       string
		want         string
	}{
		{
			name:         "downlink with env",
			flags:        &flagSet{disableDownlink: false},
			effectiveEnv: "dev",
			passID:       "pass-123",
			want:         "dev/pass-123/",
		},
		{
			name:         "downlink without env",
			flags:        &flagSet{disableDownlink: false},
			effectiveEnv: "",
			passID:       "pass-123",
			want:         "pass-123/",
		},
		{
			name:         "downlink disabled (no env)",
			flags:        &flagSet{disableDownlink: true},
			effectiveEnv: "",
			passID:       "pass-123",
			want:         "pass-123/",
		},
		{
			name:         "downlink disabled (with env)",
			flags:        &flagSet{disableDownlink: true},
			effectiveEnv: "dev",
			passID:       "pass-123",
			want:         "pass-123/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildPrefix(tt.flags, tt.effectiveEnv, tt.passID)
			if got != tt.want {
				t.Errorf("buildPrefix() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNeedsMQTTFeatures(t *testing.T) {
	tests := []struct {
		name  string
		flags *flagSet
		want  bool
	}{
		{
			name: "downlink only",
			flags: &flagSet{
				disableDownlink:       false,
				disableMonitoring:     true,
				disableConfigState:    true,
				disableEvent:          true,
				disableConfigRequests: true,
				disableUplink:         true,
			},
			want: true,
		},
		{
			name: "uplink only",
			flags: &flagSet{
				disableDownlink:       true,
				disableMonitoring:     true,
				disableConfigState:    true,
				disableEvent:          true,
				disableConfigRequests: true,
				disableUplink:         false,
			},
			want: true,
		},
		{
			name: "all disabled",
			flags: &flagSet{
				disableDownlink:       true,
				disableMonitoring:     true,
				disableConfigState:    true,
				disableEvent:          true,
				disableConfigRequests: true,
				disableUplink:         true,
			},
			want: false,
		},
		{
			name: "all enabled",
			flags: &flagSet{
				disableDownlink:       false,
				disableMonitoring:     false,
				disableConfigState:    false,
				disableEvent:          false,
				disableConfigRequests: false,
				disableUplink:         false,
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := needsMQTTFeatures(tt.flags)
			if got != tt.want {
				t.Errorf("needsMQTTFeatures() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCreateOptimizedHTTPClient(t *testing.T) {
	client := createOptimizedHTTPClient()
	if client.Timeout != 600*time.Second {
		t.Errorf("Client timeout = %v, want 600s", client.Timeout)
	}
	if client.Transport == nil {
		t.Error("Client transport is nil")
	}
}

func TestResolveS3ValidationPrefix(t *testing.T) {
	tests := []struct {
		name        string
		localPrefix string
		authCreds   *AuthorizerCredentials
		want        string
	}{
		{
			name:        "no authorizer - uses local prefix",
			localPrefix: "dev/pass-123/",
			authCreds:   nil,
			want:        "dev/pass-123/",
		},
		{
			name:        "high-rate prefix from authorizer",
			localPrefix: "dev/pass-123/",
			authCreds: &AuthorizerCredentials{
				Streams: StreamsConfig{
					HighRate: &StreamConfig{S3Prefix: "pass-123/"},
				},
			},
			want: "pass-123/",
		},
		{
			name:        "MQTT-only: uses first low-rate prefix",
			localPrefix: "mqtt/",
			authCreds: &AuthorizerCredentials{
				Streams: StreamsConfig{
					LowRate: []StreamConfig{
						{S3Prefix: "dev/pass-123/low_rate/channel/1/"},
						{S3Prefix: "dev/pass-123/low_rate/channel/2/"},
					},
				},
			},
			want: "dev/pass-123/low_rate/channel/1/",
		},
		{
			name:        "monitoring-only: uses monitoring prefix",
			localPrefix: "mqtt/",
			authCreds: &AuthorizerCredentials{
				Streams: StreamsConfig{
					Monitoring: &StreamConfig{S3Prefix: "dev/pass-123/monitoring/"},
				},
			},
			want: "dev/pass-123/monitoring/",
		},
		{
			name:        "config_state-only: uses config_state prefix",
			localPrefix: "mqtt/",
			authCreds: &AuthorizerCredentials{
				Streams: StreamsConfig{
					Config: &StreamConfig{S3Prefix: "dev/pass-123/config_state/"},
				},
			},
			want: "dev/pass-123/config_state/",
		},
		{
			name:        "event-only: uses event prefix",
			localPrefix: "mqtt/",
			authCreds: &AuthorizerCredentials{
				Streams: StreamsConfig{
					Event: &StreamConfig{S3Prefix: "dev/pass-123/event/"},
				},
			},
			want: "dev/pass-123/event/",
		},
		{
			name:        "authorizer with no stream prefixes falls back to local",
			localPrefix: "dev/pass-123/",
			authCreds: &AuthorizerCredentials{
				Streams: StreamsConfig{
					Uplink: []CommandConfig{{S3AckPrefix: "dev/pass-123/ack/uplink/"}},
				},
			},
			want: "dev/pass-123/",
		},
		{
			name:        "high-rate takes priority over low-rate",
			localPrefix: "pass-123/",
			authCreds: &AuthorizerCredentials{
				Streams: StreamsConfig{
					HighRate: &StreamConfig{S3Prefix: "pass-123/"},
					LowRate:  []StreamConfig{{S3Prefix: "dev/pass-123/low_rate/channel/1/"}},
				},
			},
			want: "pass-123/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resolveS3ValidationPrefix(tt.localPrefix, tt.authCreds)
			if got != tt.want {
				t.Errorf("resolveS3ValidationPrefix() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGetBucket(t *testing.T) {
	tests := []struct {
		name      string
		authCreds *AuthorizerCredentials
		envBucket string
		want      string
	}{
		{
			name:      "authorizer provides bucket",
			authCreds: &AuthorizerCredentials{S3Bucket: "authorizer-bucket"},
			want:      "authorizer-bucket",
		},
		{
			name:      "env var fallback",
			authCreds: nil,
			envBucket: "env-bucket",
			want:      "env-bucket",
		},
		{
			name:      "no bucket available",
			authCreds: nil,
			envBucket: "",
			want:      "",
		},
		{
			name:      "authorizer takes priority over env var",
			authCreds: &AuthorizerCredentials{S3Bucket: "authorizer-bucket"},
			envBucket: "env-bucket",
			want:      "authorizer-bucket",
		},
		{
			name:      "authorizer with empty bucket falls back to env var",
			authCreds: &AuthorizerCredentials{S3Bucket: ""},
			envBucket: "env-bucket",
			want:      "env-bucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envBucket != "" {
				t.Setenv("S3_BUCKET", tt.envBucket)
			}
			got := getBucket(tt.authCreds)
			if got != tt.want {
				t.Errorf("getBucket() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestValidateBasicFlags(t *testing.T) {
	t.Run("zero window size corrected to 1", func(t *testing.T) {
		flags := &flagSet{windowSize: 0, mqttQoS: 1}
		validateBasicFlags(flags)
		if flags.windowSize != 1 {
			t.Errorf("windowSize = %d, want 1", flags.windowSize)
		}
	})

	t.Run("negative window size corrected to 1", func(t *testing.T) {
		flags := &flagSet{windowSize: -5, mqttQoS: 1}
		validateBasicFlags(flags)
		if flags.windowSize != 1 {
			t.Errorf("windowSize = %d, want 1", flags.windowSize)
		}
	})

	t.Run("valid window size unchanged", func(t *testing.T) {
		flags := &flagSet{windowSize: 100, mqttQoS: 1}
		validateBasicFlags(flags)
		if flags.windowSize != 100 {
			t.Errorf("windowSize = %d, want 100", flags.windowSize)
		}
	})
}
