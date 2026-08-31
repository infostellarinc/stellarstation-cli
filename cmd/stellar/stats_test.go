package main

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	streaming "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/satellitestreamer"
)

func TestNewStatsTracker(t *testing.T) {
	stats := newStatsTracker(false)
	if stats == nil {
		t.Fatal("newStatsTracker() returned nil")
		return
	}
	if stats.start.IsZero() {
		t.Error("Stats tracker start time is zero")
	}
	if stats.channelStats == nil {
		t.Error("Channel stats map is nil")
	}
	if stats.channelChecksums == nil {
		t.Error("Channel checksums map is nil")
	}
	if stats.diagnosticsEnabled {
		t.Error("Diagnostics should be disabled")
	}
}

func TestNewStatsTrackerWithDiagnostics(t *testing.T) {
	stats := newStatsTracker(true)
	if !stats.diagnosticsEnabled {
		t.Error("Diagnostics should be enabled")
	}
}

// TestActivityCounter verifies the idle-shutdown monitor's "quiet stream"
// signal: ActivityCount starts at zero and advances on every kind of message
// sent or received, so an unchanged count between samples means the stream is
// idle.
func TestActivityCounter(t *testing.T) {
	stats := newStatsTracker(false)
	if got := stats.ActivityCount(); got != 0 {
		t.Fatalf("fresh tracker ActivityCount = %d, want 0", got)
	}

	// Each of these represents a message sent or received; every one must move
	// the activity counter.
	steps := []struct {
		name string
		fn   func()
	}{
		{"telemetry download", func() { stats.AddChannelDownload("ch-a/downlink", "BITSTREAM", 100, SourceS3, 0) }},
		{"monitoring", func() { stats.AddMonitoringMessage(SourceMQTT) }},
		{"config", func() { stats.AddConfigMessage(SourceMQTT) }},
		{"event", func() { stats.AddEventMessage(SourceMQTT) }},
		{"ack received", func() { stats.AddAckReceived("env/pass/p/channel/c/uplink/ack", nil) }},
		{"command sent", func() { stats.AddSentCommand("env/pass/p/channel/c/uplink", "uplink", 1) }},
		{"ack sent", func() { stats.MarkReceivedMessageAcked("env/pass/p/channel/c/downlink/BITSTREAM") }},
	}
	prev := stats.ActivityCount()
	for _, s := range steps {
		s.fn()
		got := stats.ActivityCount()
		if got <= prev {
			t.Errorf("%s: ActivityCount did not advance (%d -> %d)", s.name, prev, got)
		}
		prev = got
	}

	// No activity between two reads means the stream looks idle.
	a := stats.ActivityCount()
	b := stats.ActivityCount()
	if a != b {
		t.Errorf("ActivityCount changed with no activity: %d != %d", a, b)
	}
}

func TestSetEnableStats(t *testing.T) {
	stats := newStatsTracker(false)
	stats.SetEnableStats(true)
	if !stats.enableStats {
		t.Error("Stats should be enabled")
	}
	stats.SetEnableStats(false)
	if stats.enableStats {
		t.Error("Stats should be disabled")
	}
}

func TestSetPassID(t *testing.T) {
	stats := newStatsTracker(false)
	stats.SetPassID("pass-123")
	if stats.passID != "pass-123" {
		t.Errorf("Pass ID = %v, want pass-123", stats.passID)
	}
}

func TestSetStreamID(t *testing.T) {
	stats := newStatsTracker(false)
	stats.SetStreamID("stream-456")
	if stats.streamID != "stream-456" {
		t.Errorf("Stream ID = %v, want stream-456", stats.streamID)
	}
}

func TestAddDownload(t *testing.T) {
	stats := newStatsTracker(false)
	stats.AddDownload(1000, SourceMQTT)
	if stats.mqttDownloadedBytes != 1000 {
		t.Errorf("MQTT downloaded bytes = %v, want 1000", stats.mqttDownloadedBytes)
	}
	if stats.mqttDownloadedFiles != 1 {
		t.Errorf("MQTT downloaded files = %v, want 1", stats.mqttDownloadedFiles)
	}

	stats.AddDownload(2000, SourceS3)
	if stats.s3DownloadedBytes != 2000 {
		t.Errorf("S3 downloaded bytes = %v, want 2000", stats.s3DownloadedBytes)
	}
	if stats.s3DownloadedFiles != 1 {
		t.Errorf("S3 downloaded files = %v, want 1", stats.s3DownloadedFiles)
	}
}

func TestAddChannelDownload(t *testing.T) {
	stats := newStatsTracker(false)
	channelID := "1"
	bytes := int64(5000)
	index := 10

	stats.AddChannelDownload(channelID, "BITSTREAM", bytes, SourceMQTT, index)

	ch := stats.getOrCreateChannelStats(channelID)
	if ch.downloadedBytes != bytes {
		t.Errorf("Channel downloaded bytes = %v, want %v", ch.downloadedBytes, bytes)
	}
	if ch.downloadedFiles != 1 {
		t.Errorf("Channel downloaded files = %v, want 1", ch.downloadedFiles)
	}
	if ch.lastIndex != index {
		t.Errorf("Channel last index = %v, want %v", ch.lastIndex, index)
	}
	if ch.nextIndex != index+1 {
		t.Errorf("Channel next index = %v, want %v", ch.nextIndex, index+1)
	}
}

func TestAddWrite(t *testing.T) {
	stats := newStatsTracker(false)
	stats.AddWrite(1000)
	if stats.writtenBytes != 1000 {
		t.Errorf("Written bytes = %v, want 1000", stats.writtenBytes)
	}
	if stats.writtenFiles != 1 {
		t.Errorf("Written files = %v, want 1", stats.writtenFiles)
	}
}

func TestAddChannelWrite(t *testing.T) {
	stats := newStatsTracker(false)
	channelID := "1"
	bytes := int64(5000)

	stats.AddChannelWrite(channelID, bytes)

	ch := stats.getOrCreateChannelStats(channelID)
	if ch.writtenBytes != bytes {
		t.Errorf("Channel written bytes = %v, want %v", ch.writtenBytes, bytes)
	}
	if ch.writtenFiles != 1 {
		t.Errorf("Channel written files = %v, want 1", ch.writtenFiles)
	}
}

func TestAddChannelDataForChecksum(t *testing.T) {
	stats := newStatsTracker(true) // Enable diagnostics for checksums
	channelID := "1"
	data := []byte("test data")

	stats.AddChannelDataForChecksum(channelID, data)

	stats.checksumMu.Lock()
	chk := stats.channelChecksums[channelID]
	stats.checksumMu.Unlock()

	if chk == nil {
		t.Fatal("Channel checksum not created")
		return
	}
	if chk.bytes != int64(len(data)) {
		t.Errorf("Checksum bytes = %v, want %v", chk.bytes, len(data))
	}
}

func TestAddChannelDataForChecksumDisabled(t *testing.T) {
	stats := newStatsTracker(false) // Disable diagnostics
	channelID := "1"
	data := []byte("test data")

	stats.AddChannelDataForChecksum(channelID, data)

	stats.checksumMu.Lock()
	chk := stats.channelChecksums[channelID]
	stats.checksumMu.Unlock()

	if chk != nil {
		t.Error("Channel checksum should not be created when diagnostics disabled")
	}
}

func TestSetClientID(t *testing.T) {
	stats := newStatsTracker(false)
	clientID := "test-client-123"
	stats.SetClientID(clientID)
	if stats.GetClientID() != clientID {
		t.Errorf("Client ID = %v, want %v", stats.GetClientID(), clientID)
	}
}

func TestStatsGetClientID(t *testing.T) {
	stats := newStatsTracker(false)
	if stats.GetClientID() != "" {
		t.Error("Client ID should be empty initially")
	}
	stats.SetClientID("test-client")
	if stats.GetClientID() != "test-client" {
		t.Errorf("Client ID = %v, want test-client", stats.GetClientID())
	}
}

func TestAddError(t *testing.T) {
	stats := newStatsTracker(true) // Enable diagnostics
	err := &testError{msg: "test error"}
	stats.AddError(err)

	stats.errorsMu.Lock()
	errors := stats.errors
	stats.errorsMu.Unlock()

	if len(errors) != 1 {
		t.Errorf("Errors count = %v, want 1", len(errors))
	}
}

func TestAddErrorDisabled(t *testing.T) {
	stats := newStatsTracker(false) // Disable diagnostics
	err := &testError{msg: "test error"}
	stats.AddError(err)

	stats.errorsMu.Lock()
	errors := stats.errors
	stats.errorsMu.Unlock()

	if len(errors) != 0 {
		t.Error("Errors should not be tracked when diagnostics disabled")
	}
}

func TestAddMonitoringMessage(t *testing.T) {
	stats := newStatsTracker(false)
	stats.AddMonitoringMessage(SourceMQTT)
	if stats.monitoringStats.mqttCount != 1 {
		t.Errorf("Monitoring MQTT count = %v, want 1", stats.monitoringStats.mqttCount)
	}

	stats.AddMonitoringMessage(SourceS3)
	if stats.monitoringStats.s3Count != 1 {
		t.Errorf("Monitoring S3 count = %v, want 1", stats.monitoringStats.s3Count)
	}
}

func TestAddConfigMessage(t *testing.T) {
	stats := newStatsTracker(false)
	stats.AddConfigMessage(SourceMQTT)
	if stats.configStats.mqttCount != 1 {
		t.Errorf("Config MQTT count = %v, want 1", stats.configStats.mqttCount)
	}

	stats.AddConfigMessage(SourceS3)
	if stats.configStats.s3Count != 1 {
		t.Errorf("Config S3 count = %v, want 1", stats.configStats.s3Count)
	}
}

func TestAddEventMessage(t *testing.T) {
	stats := newStatsTracker(false)
	stats.AddEventMessage(SourceMQTT)
	if stats.eventStats.mqttCount != 1 {
		t.Errorf("Event MQTT count = %v, want 1", stats.eventStats.mqttCount)
	}

	stats.AddEventMessage(SourceS3)
	if stats.eventStats.s3Count != 1 {
		t.Errorf("Event S3 count = %v, want 1", stats.eventStats.s3Count)
	}
}

// marshalStatsAck builds a marshaled proto Ack payload for AddAckReceived.
func marshalStatsAck(t *testing.T, status streaming.Ack_Status, index uint64, reason string) []byte {
	t.Helper()
	payload, err := proto.Marshal(&streaming.Ack{
		PassId:      "pass-123",
		MessageType: "uplink",
		Status:      status,
		Index:       index,
		Reason:      reason,
	})
	if err != nil {
		t.Fatalf("marshal proto ack: %v", err)
	}
	return payload
}

func TestAddAckReceived(t *testing.T) {
	stats := newStatsTracker(false)
	stats.AddAckReceived("pass-123/ack/uplink", marshalStatsAck(t, streaming.Ack_ACK, 1, ""))
	if stats.acksReceived != 1 {
		t.Errorf("Acks received = %v, want 1", stats.acksReceived)
	}
}

// A payload that does not decode as a proto Ack is neither an ACK nor a NACK:
// it must not be counted, and it must not mark an outstanding command
// delivered.
func TestAddAckReceivedMalformedPayload(t *testing.T) {
	stats := newStatsTracker(false)
	stats.AddSentCommand("pass-123/uplink", "uplink", 1)

	stats.AddAckReceived("pass-123/uplink/ack", []byte{0xff, 0xff})

	if stats.acksReceived != 0 {
		t.Errorf("Acks received = %v, want 0", stats.acksReceived)
	}
	if stats.nacksReceived != 0 {
		t.Errorf("Nacks received = %v, want 0", stats.nacksReceived)
	}
	if stats.sentCommands[0].acked || stats.sentCommands[0].rejected {
		t.Error("A malformed payload changed a command's delivery state")
	}
}

// Acknowledgements that carry an index must land on the command they answer,
// even when they arrive out of order.
func TestAddAckReceivedMatchesByIndex(t *testing.T) {
	stats := newStatsTracker(false)
	stats.AddSentCommand("pass-123/uplink", "uplink", 1)
	stats.AddSentCommand("pass-123/uplink", "uplink", 2)

	// The NACK for index 2 arrives first; it must not mark index 1 rejected.
	stats.AddAckReceived("pass-123/uplink/ack", marshalStatsAck(t, streaming.Ack_NACK, 2, "malformed payload"))
	stats.AddAckReceived("pass-123/uplink/ack", marshalStatsAck(t, streaming.Ack_ACK, 1, ""))

	if stats.sentCommands[0].rejected || !stats.sentCommands[0].acked {
		t.Errorf("command 1: acked = %v, rejected = %v, want acked and not rejected",
			stats.sentCommands[0].acked, stats.sentCommands[0].rejected)
	}
	if !stats.sentCommands[1].rejected || stats.sentCommands[1].acked {
		t.Errorf("command 2: acked = %v, rejected = %v, want rejected and not acked",
			stats.sentCommands[1].acked, stats.sentCommands[1].rejected)
	}
}

// An ACK without an index marks nothing: commands are numbered from 1 and the
// station always echoes the index, so an index-less ACK is a station defect.
// It is still counted (it was an acknowledgement on the wire) and recorded as
// a tracked error so the defect is visible.
func TestAddAckReceivedIndexlessAckIsTrackedError(t *testing.T) {
	stats := newStatsTracker(true)
	stats.AddSentCommand("pass-123/uplink", "uplink", 1)
	stats.AddSentCommand("pass-123/uplink", "uplink", 2)

	stats.AddAckReceived("pass-123/uplink/ack", marshalStatsAck(t, streaming.Ack_ACK, 0, ""))

	if stats.acksReceived != 1 {
		t.Errorf("Acks received = %v, want 1", stats.acksReceived)
	}
	if stats.sentCommands[0].acked || stats.sentCommands[1].acked {
		t.Error("an index-less acknowledgement marked a command delivered")
	}
	stats.errorsMu.Lock()
	trackedErrors := stats.errors
	stats.errorsMu.Unlock()
	if len(trackedErrors) != 1 {
		t.Errorf("tracked errors = %v, want exactly one for the missing index", trackedErrors)
	}
}

// A NACK without an index is legitimate in exactly one case: the station could
// not decode the payload, so there is no index to echo. It is counted but
// matches no particular command, and it is not an error.
func TestAddAckReceivedIndexlessNackMatchesNothing(t *testing.T) {
	stats := newStatsTracker(true)
	stats.AddSentCommand("pass-123/uplink", "uplink", 1)

	nack := marshalStatsAck(t, streaming.Ack_NACK, 0, "malformed payload")
	stats.AddAckReceived("pass-123/uplink/ack", nack)

	if stats.nacksReceived != 1 {
		t.Errorf("Nacks received = %v, want 1", stats.nacksReceived)
	}
	if stats.sentCommands[0].rejected || stats.sentCommands[0].acked {
		t.Error("an index-less NACK changed a command's delivery state")
	}
	stats.errorsMu.Lock()
	trackedErrors := stats.errors
	stats.errorsMu.Unlock()
	if len(trackedErrors) != 0 {
		t.Errorf("tracked errors = %v, want none for an index-less NACK", trackedErrors)
	}
}

// A duplicate acknowledgement for an already-settled index is a no-op:
// duplicate indexed acks can occur (for example when another client answers
// the same commands) and must not error or re-mark anything.
func TestAddAckReceivedDuplicateIndexIsNoOp(t *testing.T) {
	stats := newStatsTracker(true)
	stats.AddSentCommand("pass-123/uplink", "uplink", 1)

	ack := marshalStatsAck(t, streaming.Ack_ACK, 1, "")
	stats.AddAckReceived("pass-123/uplink/ack", ack)
	stats.AddAckReceived("pass-123/uplink/ack", ack)

	if stats.acksReceived != 2 {
		t.Errorf("Acks received = %v, want 2", stats.acksReceived)
	}
	if !stats.sentCommands[0].acked {
		t.Error("the command was not marked delivered")
	}
	stats.errorsMu.Lock()
	trackedErrors := stats.errors
	stats.errorsMu.Unlock()
	if len(trackedErrors) != 0 {
		t.Errorf("tracked errors = %v, want none for a duplicate indexed ack", trackedErrors)
	}
}

// A rejection on the ack topic must not be recorded as a delivered command.
func TestAddAckReceivedNack(t *testing.T) {
	stats := newStatsTracker(false)
	stats.AddSentCommand("pass-123/uplink", "uplink", 1)

	nack := marshalStatsAck(t, streaming.Ack_NACK, 1, "command is for a different plan")
	stats.AddAckReceived("pass-123/uplink/ack", nack)

	if stats.acksReceived != 0 {
		t.Errorf("Acks received = %v, want 0", stats.acksReceived)
	}
	if stats.nacksReceived != 1 {
		t.Errorf("Nacks received = %v, want 1", stats.nacksReceived)
	}
	if stats.sentCommands[0].acked {
		t.Error("Rejected command was marked as acked")
	}
	if !stats.sentCommands[0].rejected {
		t.Error("Rejected command was not marked as rejected")
	}
	if unacked := stats.GetUnackedSentCommands(); len(unacked) != 0 {
		t.Errorf("Unacked commands = %v, want 0 (rejected commands are not awaiting acks)", len(unacked))
	}
}

// An explicit ACK status and an unset status (the proto zero value) both
// count as accepted.
func TestAddAckReceivedAckStatusVariants(t *testing.T) {
	for _, payload := range [][]byte{
		marshalStatsAckStatusVariant(t, true),
		marshalStatsAckStatusVariant(t, false),
	} {
		stats := newStatsTracker(false)
		stats.AddSentCommand("pass-123/uplink", "uplink", 1)
		stats.AddAckReceived("pass-123/uplink/ack", payload)
		if stats.acksReceived != 1 {
			t.Errorf("payload %s: acks received = %v, want 1", payload, stats.acksReceived)
		}
		if !stats.sentCommands[0].acked {
			t.Errorf("payload %s: command was not marked as acked", payload)
		}
	}
}

func TestAddReceivedMessage(t *testing.T) {
	stats := newStatsTracker(false)
	stats.AddReceivedMessage("pass-123/config", "config", 0, "")
	if len(stats.receivedMessages) != 1 {
		t.Errorf("Received messages count = %v, want 1", len(stats.receivedMessages))
	}
}

func TestAddSentCommand(t *testing.T) {
	stats := newStatsTracker(false)
	stats.AddSentCommand("pass-123/uplink", "uplink", 1)
	if len(stats.sentCommands) != 1 {
		t.Errorf("Sent commands count = %v, want 1", len(stats.sentCommands))
	}
}

func TestGetUnackedReceivedMessages(t *testing.T) {
	stats := newStatsTracker(false)
	stats.AddReceivedMessage("pass-123/config", "config", 0, "")
	unacked := stats.GetUnackedReceivedMessages()
	if len(unacked) != 1 {
		t.Errorf("Unacked messages count = %v, want 1", len(unacked))
	}
}

func TestGetUnackedSentCommands(t *testing.T) {
	stats := newStatsTracker(false)
	stats.AddSentCommand("pass-123/uplink", "uplink", 1)
	unacked := stats.GetUnackedSentCommands()
	if len(unacked) != 1 {
		t.Errorf("Unacked commands count = %v, want 1", len(unacked))
	}
}

func TestGetAllSentCommands(t *testing.T) {
	stats := newStatsTracker(false)
	stats.AddSentCommand("pass-123/uplink", "uplink", 1)
	stats.AddSentCommand("pass-123/config_request", "config_request", 2)
	all := stats.GetAllSentCommands()
	if len(all) != 2 {
		t.Errorf("All sent commands count = %v, want 2", len(all))
	}
}

func TestMarkReceivedMessageAcked(t *testing.T) {
	stats := newStatsTracker(false)
	stats.AddReceivedMessage("pass-123/config", "config", 0, "")
	stats.MarkReceivedMessageAcked("pass-123/config")
	if stats.acksSent != 1 {
		t.Errorf("Acks sent = %v, want 1", stats.acksSent)
	}
}

func TestSetChannelNextIndex(t *testing.T) {
	stats := newStatsTracker(false)
	channelID := "1"
	nextIndex := 5
	stats.SetChannelNextIndex(channelID, nextIndex)

	ch := stats.getOrCreateChannelStats(channelID)
	if ch.nextIndex != nextIndex {
		t.Errorf("Channel next index = %v, want %v", ch.nextIndex, nextIndex)
	}
}

func TestGetElapsedTimeBetweenBytes(t *testing.T) {
	stats := newStatsTracker(false)
	stats.AddDownload(1000, SourceMQTT)
	time.Sleep(10 * time.Millisecond)
	stats.AddDownload(2000, SourceS3)

	elapsed := stats.getElapsedTimeBetweenBytes(time.Now())
	if elapsed <= 0 {
		t.Error("Elapsed time should be positive")
	}
}

func TestGetElapsedTimeBetweenBytesNoBytes(t *testing.T) {
	stats := newStatsTracker(false)
	elapsed := stats.getElapsedTimeBetweenBytes(time.Now())
	if elapsed <= 0 {
		t.Error("Elapsed time should be positive even with no bytes")
	}
}

func TestGetInstantaneousRate(t *testing.T) {
	stats := newStatsTracker(false)
	stats.SetEnableStats(true)

	// Add some messages to the buffer
	stats.messageBufferMu.Lock()
	now := time.Now()
	for i := 0; i < 5; i++ {
		stats.messageBuffer = append(stats.messageBuffer, telemetryWithTimestamp{
			receivedTime: now.Add(time.Duration(i) * time.Second),
			dataBytes:    1000,
		})
	}
	stats.messageBufferMu.Unlock()

	rate := stats.getInstantaneousRate()
	if rate < 0 {
		t.Error("Instantaneous rate should be non-negative")
	}
}

func TestGetInstantaneousDelay(t *testing.T) {
	stats := newStatsTracker(false)
	stats.SetEnableStats(true)

	// Add some messages with timestamps
	stats.messageBufferMu.Lock()
	now := time.Now()
	for i := 0; i < 3; i++ {
		ts := now.Add(-time.Duration(i) * time.Second)
		stats.messageBuffer = append(stats.messageBuffer, telemetryWithTimestamp{
			receivedTime:         now,
			dataBytes:            1000,
			timeLastByteReceived: &ts,
		})
	}
	stats.messageBufferMu.Unlock()

	delay := stats.getInstantaneousDelay()
	if delay < 0 {
		t.Error("Instantaneous delay should be non-negative")
	}
}

func TestHumanReadableBytes(t *testing.T) {
	tests := []struct {
		name  string
		bytes int64
		want  string
	}{
		{"bytes", 512, "512 B"},
		{"kilobytes", 1024, "1.0 KiB"},
		{"megabytes", 1024 * 1024, "1.0 MiB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := humanReadableBytes(tt.bytes)
			if got == "" {
				t.Error("humanReadableBytes() returned empty string")
			}
		})
	}
}

func TestHumanReadableCountSI(t *testing.T) {
	tests := []struct {
		name  string
		count int64
	}{
		{"small", 100},
		{"thousands", 5000},
		{"millions", 5000000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := humanReadableCountSI(tt.count)
			if got == "" {
				t.Error("humanReadableCountSI() returned empty string")
			}
		})
	}
}

func TestHumanReadableNanoSeconds(t *testing.T) {
	tests := []struct {
		name  string
		nanos int64
	}{
		{"nanoseconds", 100},
		{"microseconds", 1000},
		{"milliseconds", 1000000},
		{"seconds", 1000000000},
		{"minutes", 60000000000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := humanReadableNanoSeconds(tt.nanos)
			if got == "" {
				t.Error("humanReadableNanoSeconds() returned empty string")
			}
		})
	}
}

// Helper type for testing (defined in diagnostics_test.go)

// marshalStatsAckStatusVariant builds an indexed ACK payload, either with the
// status field explicitly set to ACK or left at the zero value.
func marshalStatsAckStatusVariant(t *testing.T, explicit bool) []byte {
	t.Helper()
	ack := &streaming.Ack{PassId: "pass-123", MessageType: "uplink", Index: 1}
	if explicit {
		ack.Status = streaming.Ack_ACK
	}
	payload, err := proto.Marshal(ack)
	if err != nil {
		t.Fatalf("marshal proto ack: %v", err)
	}
	return payload
}
