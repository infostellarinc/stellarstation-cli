// Package mqtttopics builds the MQTT topic names that carry pass traffic
// between a ground station and the cloud. The connection-authorizer owns the
// contract; this package keeps every producer on one copy of the strings.
// topics_test.go pins each format as a literal.
package mqtttopics

import (
	"fmt"
	"strings"
)

func TopicPrefix(environment, passID string) string {
	if environment == "" {
		return fmt.Sprintf("pass/%s", passID)
	}
	return fmt.Sprintf("%s/pass/%s", environment, passID)
}

func EventTopic(environment, passID string) string {
	return TopicPrefix(environment, passID) + "/event"
}

func EventTopicPerChannel(environment, passID, channelID string) string {
	return fmt.Sprintf("%s/channel/%s/event", TopicPrefix(environment, passID), channelID)
}

func MonitoringTopic(environment, passID string) string {
	return TopicPrefix(environment, passID) + "/monitoring"
}

func MonitoringTopicPerChannel(environment, passID, channelID string) string {
	return fmt.Sprintf("%s/channel/%s/monitoring", TopicPrefix(environment, passID), channelID)
}

func ConfigStateTopic(environment, passID string) string {
	return TopicPrefix(environment, passID) + "/config_state"
}

func ConfigStateTopicPerChannel(environment, passID, channelID string) string {
	return fmt.Sprintf("%s/channel/%s/config_state", TopicPrefix(environment, passID), channelID)
}

func TelemetryTopic(environment, passID, channelID, framing string) string {
	return fmt.Sprintf("%s/channel/%s/downlink/%s", TopicPrefix(environment, passID), channelID, framing)
}

func UplinkTopic(environment, passID, channelID string) string {
	return fmt.Sprintf("%s/channel/%s/uplink", TopicPrefix(environment, passID), channelID)
}

func ConfigRequestTopic(environment, passID, channelID string) string {
	return fmt.Sprintf("%s/channel/%s/config_request", TopicPrefix(environment, passID), channelID)
}

func GroundStationStatusTopic(groundStationID string) string {
	return fmt.Sprintf("global/groundStation/%s/status", groundStationID)
}

func AckTopic(baseTopic string) string {
	if strings.HasSuffix(baseTopic, "/ack") {
		return baseTopic
	}
	return baseTopic + "/ack"
}

// TelemetryAckTopicFilter returns the subscribe filter for telemetry acks on a
// channel, across every framing (the ack topic embeds framing, which the
// subscriber doesn't know in advance -- one filter per channel rather than one
// per (channel, framing) keeps subscription counts well under AWS IoT Core's
// per-connection cap).
func TelemetryAckTopicFilter(environment, passID, channelID string) string {
	return fmt.Sprintf("%s/channel/%s/downlink/+/ack", TopicPrefix(environment, passID), channelID)
}

// ChannelIDFromTopic returns the channel id embedded in a pass topic, or ""
// for pass-level topics that carry no channel (e.g. .../pass/{id}/monitoring).
// Every channel-scoped topic has the shape ".../channel/{channelID}/...", so
// one extraction serves telemetry, monitoring, config-state, uplink and their
// "/ack" variants alike.
func ChannelIDFromTopic(topic string) string {
	const marker = "/channel/"
	i := strings.Index(topic, marker)
	if i < 0 {
		return ""
	}
	rest := topic[i+len(marker):]
	if j := strings.Index(rest, "/"); j >= 0 {
		return rest[:j]
	}
	return rest
}
