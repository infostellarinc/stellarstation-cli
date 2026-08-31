package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	streaming "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/satellitestreamer"
)

const (
	messageTypeLowRateTelemetry  = "low_rate_telemetry"
	messageTypeHighRateTelemetry = "high_rate_telemetry"
)

// buildAck creates a marshaled proto Ack (streaming.Ack), the wire format for
// every acknowledgement this client publishes. ackedMessageID is the
// message_id of the acknowledged FromStarPassMessage; empty when the sender
// does not set one (consumers then fall back to stream/index).
func buildAck(
	passID, streamID, channel, messageType, framingType string,
	index uint64, bytes int,
	receivedAt time.Time,
	ackedMessageID string,
) ([]byte, error) {
	return proto.Marshal(&streaming.Ack{
		AckedMessageId: ackedMessageID,
		Status:         streaming.Ack_ACK,
		MessageId:      uuid.NewString(),
		StreamId:       streamID,
		PassId:         passID,
		Index:          index,
		ReceivedAt:     timestamppb.New(receivedAt),
		Bytes:          uint64(bytes),
		ChannelId:      channel,
		FramingType:    framingType,
		MessageType:    messageType,
	})
}

// ackedMessageKey identifies the message an acknowledgement answers, for
// suppressing repeat publishes of the same acknowledgement. The ack's own
// bytes cannot serve: each carries a fresh message id and receipt time, so no
// two are ever equal. Falls back to the echoed coordinates for senders that
// do not set message ids.
func ackedMessageKey(payload []byte) string {
	var ack streaming.Ack
	if err := proto.Unmarshal(payload, &ack); err != nil {
		return string(payload)
	}
	if id := ack.GetAckedMessageId(); id != "" {
		return id
	}
	return fmt.Sprintf("%s#%s#%s#%d",
		ack.GetMessageType(), ack.GetChannelId(), ack.GetFramingType(), ack.GetIndex())
}

// decodeAckStatusIndex extracts whether an acknowledgement payload is a
// rejection and the command index it echoes. Only the proto Ack encoding is
// accepted.
func decodeAckStatusIndex(payload []byte) (rejected bool, index int64, err error) {
	var ack streaming.Ack
	if err := proto.Unmarshal(payload, &ack); err != nil {
		return false, 0, err
	}
	// A proto NACK for an undecodable payload leaves index unset (0), which
	// matches no command.
	return ack.GetStatus() == streaming.Ack_NACK, int64(ack.GetIndex()), nil
}

// buildTelemetryAck creates a proto ack for telemetry messages
func buildTelemetryAck(
	cfg Config,
	channelID, framingType string,
	index, bytes int,
	receivedAt time.Time,
	isHighRate bool,
	ackedMessageID string,
) ([]byte, error) {
	passID := cfg.PassID
	streamID := getStreamID(cfg)
	if streamID == "" {
		streamID = "streamer-unknown"
	}

	// Determine if it's low_rate or high_rate based on source
	messageType := messageTypeLowRateTelemetry
	if isHighRate {
		messageType = messageTypeHighRateTelemetry
	}

	var idx uint64
	if index > 0 {
		idx = uint64(index)
	}
	return buildAck(passID, streamID, channelID, messageType, framingType, idx, bytes, receivedAt, ackedMessageID)
}

// buildNonTelemetryAck creates a proto ack for non-telemetry messages
// (monitoring, config, event). index is the payload message's capture index
// (0 when the sender does not set it), so the ack identifies the exact
// message it answers.
func buildNonTelemetryAck(
	cfg Config,
	messageType string,
	index uint64,
	bytes int,
	receivedAt time.Time,
	ackedMessageID string,
) ([]byte, error) {
	passID := cfg.PassID
	streamID := getStreamID(cfg)
	if streamID == "" {
		streamID = "streamer-unknown"
	}

	return buildAck(passID, streamID, "", messageType, "", index, bytes, receivedAt, ackedMessageID)
}

// buildTelemetryTopicFromS3Key constructs an MQTT topic from an S3 key for telemetry
func buildTelemetryTopicFromS3Key(_ Config, s3Key, _ string) string {
	// S3 key format from IoT topic rules: <env>/pass/<passID>/channel/<channelID>/downlink/<framing>/<timestamp>
	// The S3 key is the full MQTT topic followed by a timestamp, so removing the
	// last part yields the topic
	parts := strings.Split(s3Key, "/")
	if len(parts) < 7 {
		return ""
	}

	// Find "pass" and "channel" and "downlink" in sequence to validate the format
	passIdx := -1
	channelIdx := -1
	downlinkIdx := -1
loop:
	for i, part := range parts {
		switch {
		case part == "pass" && passIdx == -1:
			passIdx = i
		case part == topicPartChannel && passIdx != -1 && channelIdx == -1:
			channelIdx = i
		case part == "downlink" && channelIdx != -1 && downlinkIdx == -1:
			downlinkIdx = i
			break loop
		}
	}

	if passIdx == -1 || channelIdx == -1 || downlinkIdx == -1 || downlinkIdx+1 >= len(parts) {
		return ""
	}

	// The S3 key is the full MQTT topic followed by a timestamp
	// Extract the topic by removing the last part (timestamp)
	// Format: <env>/pass/<passID>/channel/<channelID>/downlink/<framing>/<timestamp>
	// We want: <env>/pass/<passID>/channel/<channelID>/downlink/<framing>
	topicParts := parts[:len(parts)-1]
	return strings.Join(topicParts, "/")
}

// buildNonTelemetryTopicFromS3Key constructs an MQTT topic from an S3 key for non-telemetry messages
func buildNonTelemetryTopicFromS3Key(_ Config, s3Key, msgType string) string {
	// S3 key format can be:
	//   Pass-level: <env>/pass/<passID>/<msgType>/<timestamp>
	//   Per-channel: <env>/pass/<passID>/channel/<channelID>/<msgType>/<timestamp>
	parts := strings.Split(s3Key, "/")
	if len(parts) < 4 {
		return ""
	}

	// Find "pass" and the message type
	passIdx := -1
	msgTypeIdx := -1
	channelIdx := -1
loop:
	for i, part := range parts {
		switch {
		case part == "pass" && passIdx == -1:
			passIdx = i
		case part == topicPartChannel && passIdx != -1:
			channelIdx = i
		case part == msgType:
			msgTypeIdx = i
			break loop
		}
	}

	if passIdx == -1 || msgTypeIdx == -1 {
		return ""
	}

	// Reconstruct topic
	env := ""
	if passIdx > 0 {
		env = parts[0] + "/"
	}
	passID := parts[passIdx+1]

	// Check if it's per-channel or pass-level
	if channelIdx != -1 && channelIdx < msgTypeIdx && channelIdx+1 < len(parts) {
		// Per-channel: <env>/pass/<passID>/channel/<channelID>/<msgType>
		channelID := parts[channelIdx+1]
		return fmt.Sprintf("%spass/%s/channel/%s/%s", env, passID, channelID, msgType)
	}
	// Pass-level: <env>/pass/<passID>/<msgType>
	return fmt.Sprintf("%spass/%s/%s", env, passID, msgType)
}

// buildHighRateTelemetryTopicFromS3Key constructs an MQTT topic from an S3 key for high-rate telemetry acks.
// High-rate telemetry is S3-only, but acks are published to MQTT using the same topic format as low-rate telemetry.
// S3 key format for high-rate: <passID>/channel/<channelID>/downlink/<framing>/<index>
// Topic format: <env>/pass/<passID>/channel/<channelID>/downlink/<framing>
func buildHighRateTelemetryTopicFromS3Key(cfg Config, s3Key, channelID string) string {
	parts := strings.Split(s3Key, "/")

	// S3 key format: <passID>/channel/<channelID>/downlink/<framing>/<index>
	if len(parts) >= 6 && parts[1] == topicPartChannel && parts[3] == "downlink" {
		passID := parts[0]
		framing := parts[4]

		// Build topic with environment prefix if available
		var base string
		if cfg.Environment != "" {
			base = fmt.Sprintf("%s/pass/%s", cfg.Environment, passID)
		} else {
			base = fmt.Sprintf("pass/%s", passID)
		}
		return fmt.Sprintf("%s/channel/%s/downlink/%s", base, channelID, framing)
	}

	// Alternative format: <passID>/<channelID>/<framing>/<index>
	if len(parts) < 4 {
		return ""
	}

	passID := parts[0]
	framing := parts[2]

	var base string
	if cfg.Environment != "" {
		base = fmt.Sprintf("%s/pass/%s", cfg.Environment, passID)
	} else {
		base = fmt.Sprintf("pass/%s", passID)
	}
	return fmt.Sprintf("%s/channel/%s/downlink/%s", base, channelID, framing)
}
