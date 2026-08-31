package main

import (
	"github.com/infostellarinc/stellarstation-cli/internal/mqtttopics"

	"strings"
	"time"

	"github.com/infostellarinc/stellarstation-cli/internal/auth"
)

// SourceType indicates the data source type.
type SourceType string

const (
	SourceTypeS3   SourceType = "s3"
	SourceTypeMQTT SourceType = "mqtt"
)

// Config for the client program.
type Config struct {
	SourceType     SourceType
	Bucket         string
	Prefix         string
	DestDir        string
	S3PollInterval time.Duration
	WindowSize     int
	WriteInOrder   bool

	// MQTT-specific config
	MQTTQoS byte

	// Pass information
	PassID             string
	ChannelIDs         []string // List of channel IDs (UUIDs) to subscribe to
	HighRateChannelIDs []string // Channel IDs that are high-rate only (S3-direct, no MQTT); nil means use ChannelIDs

	// ChannelFramings maps a channel ID to the framing types it actually emits
	// (from the authorizer's per-channel metadata). The high-rate S3 reader uses
	// this to fetch only the framings a channel produces instead of probing all
	// known framings (which yields a flood of 403s for framings that don't exist).
	// A channel absent here falls back to all known framings.
	ChannelFramings map[string][]string

	// AuthorizerCreds holds the credentials and stream config returned by the
	// connection-authorizer (S3 STS credentials, IoT certificate, topic names, S3 paths).
	AuthorizerCreds *AuthorizerCredentials
	// CredStore is the live credential store updated by the background refresher.
	// When non-nil, the S3 client reads the latest credentials from it on every request,
	// and the MQTT client factory re-parses the latest IoT certificate on each reconnect cycle.
	CredStore *credentialStore
	// AuthorizerAPI is the URL to the connection-authorizer API (for stream close calls)
	AuthorizerAPI string
	// TokenSource mints Cognito bearer tokens for /authorize and /stream/close.
	// It may be nil when running in direct-S3 mode without the authorizer.
	TokenSource auth.TokenSource

	// Environment is the deployment environment name (e.g. "dev", "prod").
	// Populated from the -environment flag or ENV env var, then overridden by the authorizer response.
	Environment string

	// Feature flags
	EnableDownlink       bool // Enable downlink telemetry streaming (S3 direct + MQTT with S3 fallback)
	EnableMonitoring     bool // Enable receiving monitoring messages
	EnableConfigState    bool // Enable receiving config messages
	EnableEvent          bool // Enable receiving event messages
	EnableConfigRequests bool // Enable sending config_request messages
	EnableUplink         bool // Enable sending uplink messages
	EnableDiagnostics    bool // Enable diagnostics file generation

	// Output options
	EnableStdoutOutput bool     // Write telemetry data to stdout (for piping into downstream tooling)
	OutputFile         string   // Single output file path (optional, supports multiple modes)
	OutputFileMode     []string // Output file modes: "per-channel", "per-framing", "per-framing-channel", "all-combined" (comma-separated or multiple flags)

	// Statistics and logging
	EnableStats   bool // Enable advanced statistics collection and pass summaries
	EnableVerbose bool // Enable verbose output with detailed information
	EnableDebug   bool // Enable debug-level logging

	// Stream management
	EnableAutoClose bool // Automatically close stream after receiving stream end message

	// Framing filtering
	AcceptedFraming []string // List of accepted framing types to receive (empty = all)

	// Proxy settings
	ProxyCh chan<- []byte // When non-nil, telemetry bytes are also sent to this channel for proxy forwarding
}

// buildAckTopic builds an ack topic by appending "/ack" to the base topic.
// Format: {env}/pass/{passID}/.../ack
// Examples:
//   - "dev/pass/pass-123/config"                    -> "dev/pass/pass-123/config_state/ack"
//   - "dev/pass/pass-123/config_state"              -> "dev/pass/pass-123/config_state/ack"
//   - "dev/pass/pass-123/channel/1/downlink/BITSTREAM" -> "dev/pass/pass-123/channel/1/downlink/BITSTREAM/ack"
//   - "dev/pass/pass-123/channel/1/uplink" -> "dev/pass/pass-123/channel/1/uplink/ack"
//   - "dev/pass/pass-123/channel/1/config_request" -> "dev/pass/pass-123/channel/1/config_request/ack"
func buildAckTopic(baseTopic string) string {
	// Convert /config to /config_state (but not /config_request)
	if strings.HasSuffix(baseTopic, "/config") && !strings.HasSuffix(baseTopic, "/config_request") {
		baseTopic = strings.TrimSuffix(baseTopic, "/config") + "/config_state"
	}
	// If already ends with /ack, don't append again
	if strings.HasSuffix(baseTopic, "/ack") {
		return baseTopic
	}
	// Append "/ack" at the end
	return baseTopic + "/ack"
}

const topicPartChannel = "channel"

// ExtractChannelIDFromTopic extracts the channel ID from a topic name.
// Returns empty string if the topic is not a channel-specific topic or channel ID cannot be extracted.
// Supports formats:
//
//	<env>/pass/<pass_id>/channel/<channel_id>/downlink/<framing>
//	<env>/pass/<pass_id>/channel/<channel_id>/config_state
//	<env>/pass/<pass_id>/channel/<channel_id>/event
//	<env>/pass/<pass_id>/channel/<channel_id>/monitoring
//	<env>/pass/<pass_id>/channel/<channel_id>/uplink
//	<env>/pass/<pass_id>/channel/<channel_id>/config_request
func ExtractChannelIDFromTopic(topic string) string {
	// Topic format: {env}/pass/{passID}/channel/{channelID}/...
	// Delegates to the shared topic grammar so it cannot drift from the
	// producers.
	return mqtttopics.ChannelIDFromTopic(topic)
}

// fetchResult is the result of downloading a single index.
//
// Data contains the decoded telemetry payload (concatenated Telemetry.Data bytes),
// not the raw S3 object body.
type fetchResult struct {
	Index     int
	ChannelID string // Channel ID (UUID) for MQTT messages (empty for S3 or non-telemetry)
	Framing   string // Framing type (e.g., "BITSTREAM", "IQ") - empty if unknown
	Data      []byte
	Metadata  map[string]string
	Err       error
	Source    MessageSource // Source of the message (MQTT or S3)
	// MessageID is the FromStarPassMessage.message_id of the source message,
	// echoed as acked_message_id in the ack. Empty when the message carries no
	// ID, or was stored as raw bytes (high-rate S3 objects).
	MessageID string
}
