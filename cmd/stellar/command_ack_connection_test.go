package main

import (
	"reflect"
	"testing"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// The command ack subscriptions must leave on their own connection while every
// other topic stays on the telemetry connection, each in the original order.
func TestSplitCommandAckTopics(t *testing.T) {
	topics := []string{
		"env1/pass/p1/channel/+/downlink/+",
		"env1/pass/p1/monitoring",
		"env1/pass/p1/channel/+/uplink/ack",
		"env1/pass/p1/config_state",
		"env1/pass/p1/event",
		"env1/pass/p1/channel/+/config_request/ack",
	}
	telemetry, acks := splitCommandAckTopics(topics)
	wantTelemetry := []string{
		"env1/pass/p1/channel/+/downlink/+",
		"env1/pass/p1/monitoring",
		"env1/pass/p1/config_state",
		"env1/pass/p1/event",
	}
	wantAcks := []string{
		"env1/pass/p1/channel/+/uplink/ack",
		"env1/pass/p1/channel/+/config_request/ack",
	}
	if !reflect.DeepEqual(telemetry, wantTelemetry) {
		t.Errorf("telemetry topics = %v, want %v", telemetry, wantTelemetry)
	}
	if !reflect.DeepEqual(acks, wantAcks) {
		t.Errorf("ack topics = %v, want %v", acks, wantAcks)
	}
}

// A pass without uplink channels has no ack topics; nothing may be mistaken
// for one (an "ack" segment in the middle of a topic is not a suffix).
func TestSplitCommandAckTopics_NoAcks(t *testing.T) {
	topics := []string{"env1/pass/p1/channel/+/downlink/+", "env1/pass/p1/ack/monitoring"}
	telemetry, acks := splitCommandAckTopics(topics)
	if !reflect.DeepEqual(telemetry, topics) {
		t.Errorf("telemetry topics = %v, want %v", telemetry, topics)
	}
	if len(acks) != 0 {
		t.Errorf("ack topics = %v, want none", acks)
	}
}

func TestSubscribeCommandAcks(t *testing.T) {
	handler := func(mqtt.Client, mqtt.Message) {}
	acks := []string{"env1/pass/p1/channel/+/uplink/ack", "env1/pass/p1/channel/+/config_request/ack"}

	t.Run("subscribes every ack topic at the reader QoS", func(t *testing.T) {
		client := &mockMQTTClient{connected: true}
		subscribeCommandAcks(client, acks, 1, handler)
		if !reflect.DeepEqual(client.subscribed, acks) {
			t.Errorf("subscribed = %v, want %v", client.subscribed, acks)
		}
		if client.subQoS != 1 {
			t.Errorf("subscribe QoS = %d, want 1", client.subQoS)
		}
	})

	t.Run("no ack topics means no subscription", func(t *testing.T) {
		client := &mockMQTTClient{connected: true}
		subscribeCommandAcks(client, nil, 1, handler)
		if len(client.subscribed) != 0 {
			t.Errorf("subscribed = %v, want none", client.subscribed)
		}
	})

	t.Run("no connection is tolerated", func(t *testing.T) {
		subscribeCommandAcks(nil, acks, 1, handler)
	})
}
