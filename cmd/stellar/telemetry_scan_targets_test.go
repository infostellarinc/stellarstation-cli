package main

import "testing"

// The authorizer's low-rate stream list carries every stream of the pass:
// downlink telemetry, and one monitoring stream per channel. Only downlink
// streams may be scanned as telemetry; a telemetry scan of a monitoring prefix
// counts that stream's END sentinel object into the telemetry totals (observed
// on prod passes 22eada11 and a7374493 as a per-channel off-by-one between the
// summary total and its rows).
func TestLowRateTelemetryScanTargetsSkipNonDownlinkStreams(t *testing.T) {
	streams := []StreamConfig{
		{S3Prefix: "prod/pass/p1/channel/ch-low/downlink/"},
		{S3Prefix: "prod/pass/p1/channel/ch-low/monitoring/"},
		{S3Prefix: "prod/pass/p1/channel/ch-cmd/monitoring/"},
		{MqttTopic: "prod/pass/p1/channel/ch-mqtt/downlink"},
		{MqttTopic: "prod/pass/p1/channel/ch-cmd/uplink"},
	}

	targets := lowRateTelemetryScanTargets(streams)

	if len(targets) != 2 {
		t.Fatalf("got %d scan targets, want 2 (only the downlink streams): %+v", len(targets), targets)
	}
	if targets[0].channelID != "ch-low/downlink" {
		t.Errorf("targets[0].channelID = %q, want ch-low/downlink", targets[0].channelID)
	}
	if targets[0].prefix != "prod/pass/p1/channel/ch-low/downlink/" {
		t.Errorf("targets[0].prefix = %q, want the stream's S3 prefix", targets[0].prefix)
	}
	if targets[1].channelID != "ch-mqtt/downlink" {
		t.Errorf("targets[1].channelID = %q, want ch-mqtt/downlink", targets[1].channelID)
	}
}
