package main

import (
	"github.com/google/uuid"

	"context"
	"errors"
	"fmt"
	"log"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"google.golang.org/protobuf/proto"

	streaming "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/satellitestreamer"
	v1 "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/starpass/v1"
)

// publishCommand publishes a command message to the specified MQTT topic and waits for acknowledgment.
//
// Parameters:
//   - client: The MQTT client to use for publishing
//   - topic: The MQTT topic to publish to
//   - qos: The MQTT QoS level (0, 1, or 2)
//   - msg: The protobuf message to publish
//   - stats: Optional stats tracker for tracking sent commands
//   - cmdType: The command type string (e.g., "uplink", "config_request")
//   - index: The command index
//
// Returns:
//   - An error if publishing fails or times out
func publishCommand(
	client mqtt.Client,
	topic string,
	qos byte,
	msg proto.Message,
	stats interface {
		AddSentCommand(topic, cmdType string, index uint32)
	},
	cmdType string,
	index uint32,
) error {
	wire, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal command message: %w", err)
	}

	token := client.Publish(topic, qos, false, wire)
	if !token.WaitTimeout(publishCommandTimeout) {
		return errors.New("publish command timeout")
	}
	if token.Error() != nil {
		return fmt.Errorf("publish command: %w", token.Error())
	}

	log.Printf("Published command to topic=%s wire=%dB", topic, len(wire))
	if stats != nil {
		stats.AddSentCommand(topic, cmdType, index)
	}
	return nil
}

// PublishSatCommand publishes a satellite command message to MQTT.
//
// The topic must be provided from authorizer credentials - the CLI does not construct topics.
//
// Parameters:
//   - ctx: Context for cancellation (unused but kept for API compatibility)
//   - client: The MQTT client to use for publishing
//   - topic: The MQTT topic from authorizer (cfg.AuthorizerCreds.Streams.Uplink.PublishTopic)
//   - streamID: The stream ID for the command
//   - planID: The plan ID for the command
//   - index: The command index
//   - commands: Array of command byte arrays to send
//   - qos: The MQTT QoS level
//   - stats: Stats tracker for tracking sent commands
//
// Returns:
//   - An error if the client is not connected or publishing fails
func PublishSatCommand(
	_ context.Context, // ctx - unused but kept for API compatibility
	client mqtt.Client,
	topic string, // Topic from authorizer (cfg.AuthorizerCreds.Streams.Uplink.PublishTopic)
	streamID string,
	planID string,
	index uint32,
	commands [][]byte,
	qos byte,
	stats *statsTracker,
) error {
	if !client.IsConnected() {
		return errors.New("MQTT client not connected")
	}

	cmdMsg := &streaming.SendCommandsMessage{
		StreamId: streamID,
		PassId:   planID,
		Index:    index,
		Command:  commands,
	}

	toStarPassMsg := &streaming.ToStarPassMessage{
		MessageId: uuid.NewString(),
		StreamId:  streamID,
		PassId:    planID,
		Index:     index,
		Command:   commands,
		Message: &streaming.ToStarPassMessage_SendCommandsMessage{
			SendCommandsMessage: cmdMsg,
		},
	}

	return publishCommand(client, topic, qos, toStarPassMsg, stats, "uplink", index)
}

// PublishGsConfig publishes a ground station configuration request to MQTT.
//
// The topic must be provided from authorizer credentials - the CLI does not construct topics.
//
// Parameters:
//   - ctx: Context for cancellation (unused but kept for API compatibility)
//   - client: The MQTT client to use for publishing
//   - topic: The MQTT topic from authorizer (cfg.AuthorizerCreds.Streams.ConfigRequest.PublishTopic)
//   - streamID: The stream ID for the configuration
//   - planID: The plan ID for the configuration
//   - index: The configuration index
//   - configRequest: The ground station configuration request protobuf message
//   - qos: The MQTT QoS level
//   - stats: Stats tracker for tracking sent commands
//
// Returns:
//   - An error if the client is not connected or publishing fails
func PublishGsConfig(
	_ context.Context, // ctx - unused but kept for API compatibility
	client mqtt.Client,
	topic string, // Topic from authorizer (cfg.AuthorizerCreds.Streams.ConfigRequest.PublishTopic)
	streamID string,
	planID string,
	index uint32,
	configRequest *v1.GroundStationConfigurationRequest,
	qos byte,
	stats *statsTracker,
) error {
	if !client.IsConnected() {
		return errors.New("MQTT client not connected")
	}

	toStarPassMsg := &streaming.ToStarPassMessage{
		MessageId: uuid.NewString(),
		StreamId:  streamID,
		PassId:    planID,
		Index:     index,
		Message: &streaming.ToStarPassMessage_GroundStationConfigurationRequest{
			GroundStationConfigurationRequest: configRequest,
		},
	}

	return publishCommand(client, topic, qos, toStarPassMsg, stats, "config_request", index)
}
