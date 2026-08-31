package main

import (
	"fmt"
	"strings"
	"testing"
)

// channelWildcard must replace only the channel identifier with "+" and leave
// pass-level topics (no /channel/ segment) untouched.
func TestChannelWildcard(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"env1/pass/p1/channel/abc123/config_request/ack", "env1/pass/p1/channel/+/config_request/ack"},
		{"env1/pass/p1/channel/abc123/uplink/ack", "env1/pass/p1/channel/+/uplink/ack"},
		{"env1/pass/p1/channel/abc123/downlink/+", "env1/pass/p1/channel/+/downlink/+"},
		{"env1/pass/p1/channel/abc123/monitoring", "env1/pass/p1/channel/+/monitoring"},
		{"env1/pass/p1/monitoring", "env1/pass/p1/monitoring"}, // pass-level, unchanged
		{"env1/pass/p1/config_state", "env1/pass/p1/config_state"},
	}
	for _, c := range cases {
		if got := channelWildcard(c.in); got != c.want {
			t.Errorf("channelWildcard(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// collectMQTTTopics must collapse per-channel subscriptions to channel wildcards.
// This is the regression guard for the AWS IoT Core 50-subscriptions-per-connection
// cap: a multi-channel pass with command acks previously produced ~57 concrete
// subscriptions, and because the config_request/ack topics were collected last they
// landed past the cap and were silently dropped, so ground-station config acks were
// never delivered. After collapsing, the count must stay far below the limit and
// must not grow with the channel count.
func TestCollectMQTTTopics_CollapsesPerChannelToWildcard(t *testing.T) {
	const iotSubscriptionLimit = 50

	build := func(numChannels int) StreamsConfig {
		s := StreamsConfig{
			Monitoring: &StreamConfig{MqttTopic: "env1/pass/p1/monitoring"},
			Config:     &StreamConfig{MqttTopic: "env1/pass/p1/config_state"},
			Event:      &StreamConfig{MqttTopic: "env1/pass/p1/event"},
		}
		for i := 0; i < numChannels; i++ {
			ch := fmt.Sprintf("ch-%04d", i)
			base := "env1/pass/p1/channel/" + ch
			s.LowRate = append(s.LowRate,
				StreamConfig{MqttTopic: base + "/downlink/+"},
				StreamConfig{MqttTopic: base + "/monitoring"},
				StreamConfig{MqttTopic: base + "/config_state"},
				StreamConfig{MqttTopic: base + "/event"},
			)
			s.Uplink = append(s.Uplink, CommandConfig{AckTopic: base + "/uplink/ack"})
			s.ConfigRequest = append(s.ConfigRequest, CommandConfig{AckTopic: base + "/config_request/ack"})
		}
		return s
	}

	// The historical failing case: 9 channels produced 57 concrete subscriptions.
	topics := collectMQTTTopics(build(9))
	if len(topics) > iotSubscriptionLimit {
		t.Fatalf("collectMQTTTopics returned %d subscriptions for 9 channels; must stay <= %d (AWS IoT cap)", len(topics), iotSubscriptionLimit)
	}

	// The count must not grow with the channel count; it should be constant.
	small := collectMQTTTopics(build(2))
	large := collectMQTTTopics(build(40))
	if len(small) != len(large) {
		t.Errorf("subscription count must be independent of channel count: 2 channels -> %d, 40 channels -> %d", len(small), len(large))
	}

	// The config_request/ack wildcard MUST be present (this is the topic that used
	// to be dropped past the cap).
	wantConfigAck := "env1/pass/p1/channel/+/config_request/ack"
	if !contains(topics, wantConfigAck) {
		t.Errorf("collapsed topics missing %q; got %v", wantConfigAck, topics)
	}
	// And it must appear exactly once (de-duplicated across all channels).
	if n := count(topics, wantConfigAck); n != 1 {
		t.Errorf("expected exactly one %q, got %d", wantConfigAck, n)
	}

	// No concrete channel ids should remain in the subscription set.
	for _, tp := range topics {
		if strings.Contains(tp, "/channel/ch-") {
			t.Errorf("topic still contains a concrete channel id: %q", tp)
		}
	}
}

func contains(xs []string, want string) bool {
	return count(xs, want) > 0
}

func count(xs []string, want string) int {
	n := 0
	for _, x := range xs {
		if x == want {
			n++
		}
	}
	return n
}
