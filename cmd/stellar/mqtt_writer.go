package main

import (
	"errors"
	"fmt"
	"log"

	"github.com/benbjohnson/clock"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	streaming "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/satellitestreamer"
	v1 "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/starpass/v1"
)

// commandPublisher publishes command messages on one MQTT connection. Each
// command is published once: the client never retries on its own, since
// retries during a pass only add to the congestion that lost the first copy.
// An unanswered command is reported as such (see awaitCommands).
type commandPublisher struct {
	client mqtt.Client
	qos    byte
	stats  *statsTracker
	window passWindow
	clock  clock.Clock
}

func newCommandPublisher(client mqtt.Client, qos byte, stats *statsTracker, window passWindow) *commandPublisher {
	if stats == nil {
		stats = newStatsTracker(false)
	}
	return &commandPublisher{
		client: client,
		qos:    qos,
		stats:  stats,
		window: window,
		clock:  clock.New(),
	}
}

// publishCommand publishes msg to topic once and records it.
func (p *commandPublisher) publishCommand(
	topic, cmdType string, msg *streaming.ToStarPassMessage,
) (*sentCommand, error) {
	wire, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal command message: %w", err)
	}
	// Backstop for the per-command check: the broker rejects an oversized
	// publish, so refuse it rather than report a send that never arrived.
	if len(wire) > mqttMaxPayloadBytes {
		return nil, fmt.Errorf(
			"message is %d bytes, over the %d byte limit for a single message",
			len(wire), mqttMaxPayloadBytes,
		)
	}
	// Recorded before the publish so an ack that beats the publish
	// confirmation still finds its command.
	cmd := p.stats.AddSentCommand(topic, cmdType, msg)
	if err := p.publishWire(topic, wire); err != nil {
		// The broker may still deliver it, so the index stays used: a copy
		// under the same index would be refused as reused, and a new command
		// takes the next index.
		return cmd, fmt.Errorf("command #%d was not confirmed by the broker and may still arrive: %w", cmd.index, err)
	}
	log.Printf("Published command to topic=%s wire=%dB", topic, len(wire))
	return cmd, nil
}

func (p *commandPublisher) publishWire(topic string, wire []byte) error {
	token := p.client.Publish(topic, p.qos, false, wire)
	if !token.WaitTimeout(publishCommandTimeout) {
		return errors.New("publish command timeout")
	}
	if token.Error() != nil {
		return fmt.Errorf("publish command: %w", token.Error())
	}
	return nil
}

// PublishSatCommand publishes a satellite command message. The topic must come
// from the authorizer credentials; the CLI does not construct topics.
func PublishSatCommand(
	p *commandPublisher,
	topic, streamID, planID string,
	index uint32,
	commands [][]byte,
) (*sentCommand, error) {
	if !p.client.IsConnected() {
		return nil, errors.New("MQTT client not connected")
	}
	if err := validateCommandSize(commands); err != nil {
		return nil, err
	}
	return p.publishCommand(topic, cmdTypeUplink, &streaming.ToStarPassMessage{
		MessageId: uuid.NewString(),
		StreamId:  streamID,
		PassId:    planID,
		Index:     index,
		Command:   commands,
	})
}

// PublishGsConfig publishes a ground station configuration request. The topic
// must come from the authorizer credentials; the CLI does not construct topics.
func PublishGsConfig(
	p *commandPublisher,
	topic, streamID, planID string,
	index uint32,
	configRequest *v1.GroundStationConfigurationRequest,
) (*sentCommand, error) {
	if !p.client.IsConnected() {
		return nil, errors.New("MQTT client not connected")
	}
	return p.publishCommand(topic, cmdTypeConfigRequest, &streaming.ToStarPassMessage{
		MessageId: uuid.NewString(),
		StreamId:  streamID,
		PassId:    planID,
		Index:     index,
		Message: &streaming.ToStarPassMessage_GroundStationConfigurationRequest{
			GroundStationConfigurationRequest: configRequest,
		},
	})
}
