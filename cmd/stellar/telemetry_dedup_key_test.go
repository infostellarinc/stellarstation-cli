package main

import "testing"

// The low-rate MQTT (live) path keys receivedIndices on the bare channel UUID
// (ExtractChannelIDFromTopic), while the S3 fallback path carries the channel id
// with a trailing "/downlink" (extractChannelIDFromLowRateStream). If those two
// produce different dedup keys, a message that arrives on both sources is counted
// twice, the bug that made two concurrent clients report different, inflated
// low-rate telemetry totals. telemetryDedupKey must collapse both forms to one key
// (for the same framing).
func TestTelemetryDedupKey_MQTTAndS3Align(t *testing.T) {
	const uuid = "0dc3685a-11ca-4b28-9d49-ba2f3d73e0de"
	const framing = "BITSTREAM"

	mqttForm := uuid             // ExtractChannelIDFromTopic returns the bare UUID
	s3Form := uuid + "/downlink" // extractChannelIDFromLowRateStream appends /downlink

	mqttKey := telemetryDedupKey(mqttForm, framing)
	s3Key := telemetryDedupKey(s3Form, framing)
	if mqttKey != s3Key {
		t.Fatalf("MQTT and S3 dedup keys diverge for the same framing: %q vs %q; cross-source dedup would fail and double-count",
			mqttKey, s3Key)
	}
}

// A multi-framing channel republishes the SAME index once per declared framing
// (e.g. index 5 goes out as both a BITSTREAM message and an IQ message); these
// are distinct, intentional messages, not duplicates. telemetryDedupKey must
// therefore produce DIFFERENT keys for different framings on the same channel,
// or one framing's copy of an index silently discards the other framing's copy
// of the same index, cutting a multi-framing channel's received count roughly in
// half (the regression this test guards against).
func TestTelemetryDedupKey_DifferentFramingsDoNotCollide(t *testing.T) {
	const uuid = "8b4c46c4-5b83-4429-af38-874c0ae06067"

	bitstreamKey := telemetryDedupKey(uuid, "BITSTREAM")
	iqKey := telemetryDedupKey(uuid, "IQ")
	if bitstreamKey == iqKey {
		t.Fatalf("telemetryDedupKey(%q, BITSTREAM) == telemetryDedupKey(%q, IQ) (%q); different framings must not share a dedup key",
			uuid, uuid, bitstreamKey)
	}

	// The /downlink-suffixed (S3) form must show the same distinction.
	bitstreamKeyS3 := telemetryDedupKey(uuid+"/downlink", "BITSTREAM")
	iqKeyS3 := telemetryDedupKey(uuid+"/downlink", "IQ")
	if bitstreamKeyS3 == iqKeyS3 {
		t.Fatalf("telemetryDedupKey with /downlink suffix collapses framings: %q", bitstreamKeyS3)
	}
	if bitstreamKey != bitstreamKeyS3 {
		t.Errorf("BITSTREAM key differs between MQTT and S3 channel-ID forms: %q vs %q", bitstreamKey, bitstreamKeyS3)
	}
}
