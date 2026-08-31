package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFormatUTCTimestamp(t *testing.T) {
	now := time.Date(2024, 1, 15, 10, 30, 45, 123456789, time.UTC)
	formatted := formatUTCTimestamp(now)
	expected := "2024-01-15T10:30:45.123Z"
	if formatted != expected {
		t.Errorf("formatUTCTimestamp() = %v, want %v", formatted, expected)
	}
}

func TestGetClientID(t *testing.T) {
	stats := newStatsTracker(false)
	cfg := Config{}

	// Test with stats client ID
	stats.SetClientID("stats-client")
	clientID := getClientID(stats, cfg)
	if clientID != "stats-client" {
		t.Errorf("getClientID() = %v, want stats-client", clientID)
	}

	// Test with authorizer client ID
	stats.SetClientID("")
	cfg.AuthorizerCreds = &AuthorizerCredentials{ClientID: "authorizer-client"}
	clientID = getClientID(stats, cfg)
	if clientID != "authorizer-client" {
		t.Errorf("getClientID() = %v, want authorizer-client", clientID)
	}

	// Test with unknown
	cfg.AuthorizerCreds = nil
	clientID = getClientID(stats, cfg)
	if clientID != unknownClientID {
		t.Errorf("getClientID() = %v, want %v", clientID, unknownClientID)
	}
}

func TestCalculateOverallStats(t *testing.T) {
	stats := newStatsTracker(false)
	stats.mqttDownloadedFiles = 10
	stats.mqttDownloadedBytes = 10000
	stats.s3DownloadedFiles = 5
	stats.s3DownloadedBytes = 5000
	stats.writtenBytes = 15000

	elapsedSeconds := 10.0
	o := calculateOverallStats(stats, elapsedSeconds)
	totalFiles, totalBytes, avgDlMbps, avgWrMbps := o.totalDownloadedFiles, o.totalDownloadedBytes, o.avgDlMbps, o.avgWrMbps

	if totalFiles != 15 {
		t.Errorf("Total files = %v, want 15", totalFiles)
	}
	if totalBytes != 15000 {
		t.Errorf("Total bytes = %v, want 15000", totalBytes)
	}
	if avgDlMbps < 0 {
		t.Error("Average download Mbps should be non-negative")
	}
	if avgWrMbps < 0 {
		t.Error("Average write Mbps should be non-negative")
	}
}

func TestGetMessageStats(t *testing.T) {
	stats := newStatsTracker(false)
	stats.monitoringStats.mqttCount = 5
	stats.monitoringStats.s3Count = 3
	stats.configStats.mqttCount = 2
	stats.configStats.s3Count = 1
	stats.eventStats.mqttCount = 4
	stats.eventStats.s3Count = 2

	monitoring, config, event := getMessageStats(stats)
	if monitoring != 8 {
		t.Errorf("Monitoring count = %v, want 8", monitoring)
	}
	if config != 3 {
		t.Errorf("Config count = %v, want 3", config)
	}
	if event != 6 {
		t.Errorf("Event count = %v, want 6", event)
	}
}

func TestBuildChannelStats(t *testing.T) {
	stats := newStatsTracker(false)
	stats.AddChannelDownload("1", "BITSTREAM", 1000, SourceMQTT, 10)
	stats.AddChannelWrite("1", 1000)
	stats.AddChannelDownload("2", "IQ", 2000, SourceS3, 20)
	stats.AddChannelWrite("2", 2000)

	elapsedSeconds := 10.0
	channelStats := buildChannelStats(stats, elapsedSeconds)

	if len(channelStats) != 2 {
		t.Errorf("Channel stats count = %v, want 2", len(channelStats))
	}

	// Check channel 1
	ch1 := channelStats[0]
	if ch1.ChannelID != "1" {
		t.Errorf("Channel 1 ID = %v, want 1", ch1.ChannelID)
	}
	if ch1.Download.Bytes != 1000 {
		t.Errorf("Channel 1 download bytes = %v, want 1000", ch1.Download.Bytes)
	}
	if ch1.Write.Bytes != 1000 {
		t.Errorf("Channel 1 write bytes = %v, want 1000", ch1.Write.Bytes)
	}
}

func TestBuildChecksums(t *testing.T) {
	stats := newStatsTracker(true) // Enable diagnostics
	stats.AddChannelDataForChecksum("1", []byte("test data 1"))
	stats.AddChannelDataForChecksum("1", []byte("test data 2"))
	stats.AddChannelDataForChecksum("2", []byte("test data 3"))

	cfg := Config{WriteInOrder: true}
	checksums := buildChecksums(stats, cfg)

	if len(checksums) != 2 {
		t.Errorf("Checksums count = %v, want 2", len(checksums))
	}
	if checksums["channel_1"] == "" {
		t.Error("Channel 1 checksum should be set")
	}
	if checksums["channel_2"] == "" {
		t.Error("Channel 2 checksum should be set")
	}
}

func TestBuildChecksumsDisabled(t *testing.T) {
	stats := newStatsTracker(false)
	cfg := Config{WriteInOrder: false}
	checksums := buildChecksums(stats, cfg)

	if len(checksums) != 0 {
		t.Errorf("Checksums count = %v, want 0", len(checksums))
	}
}

func TestGetErrors(t *testing.T) {
	stats := newStatsTracker(true) // Enable diagnostics
	stats.AddError(&testError{msg: "error 1"})
	stats.AddError(&testError{msg: "error 2"})

	errors := getErrors(stats)
	if len(errors) != 2 {
		t.Errorf("Errors count = %v, want 2", len(errors))
	}
}

func TestBuildCommandStats(t *testing.T) {
	stats := newStatsTracker(false)
	stats.AddSentCommand("pass-123/uplink", "uplink", 1)
	stats.AddSentCommand("pass-123/uplink", "uplink", 2)
	stats.AddSentCommand("pass-123/config_request", "config_request", 1)

	// Mark one as acked
	stats.AddAckReceived("pass-123/uplink/ack", []byte(`{"pass_id":"pass-123","message_type":"uplink","index":1}`))

	commandStats := buildCommandStats(stats)
	if commandStats.UplinkStats.TotalSent != 2 {
		t.Errorf("Sat commands total sent = %v, want 2", commandStats.UplinkStats.TotalSent)
	}
	if commandStats.ConfigRequestStats.TotalSent != 1 {
		t.Errorf("GS configs total sent = %v, want 1", commandStats.ConfigRequestStats.TotalSent)
	}
}

func TestBuildDiagnosticsData(t *testing.T) {
	stats := newStatsTracker(true) // Enable diagnostics
	stats.AddChannelDownload("1", "BITSTREAM", 1000, SourceMQTT, 10)
	stats.AddChannelWrite("1", 1000)
	stats.AddMonitoringMessage(SourceMQTT)
	stats.AddConfigMessage(SourceS3)
	stats.AddEventMessage(SourceMQTT)
	stats.AddSentCommand("pass-123/uplink", "uplink", 1)

	cfg := Config{
		PassID:            "pass-123",
		Environment:       "dev",
		ChannelIDs:        []string{"1"},
		SourceType:        SourceTypeMQTT,
		Bucket:            "test-bucket",
		DestDir:           "/tmp",
		WriteInOrder:      true,
		WindowSize:        100,
		EnableDownlink:    true,
		EnableMonitoring:  true,
		EnableConfigState: true,
		EnableEvent:       true,
	}

	endTime := time.Now()
	elapsed := endTime.Sub(stats.start)
	elapsedSeconds := elapsed.Seconds()

	diagnostics := buildDiagnosticsData(cfg, stats, "test-client", endTime, elapsed, elapsedSeconds)

	if diagnostics.StartTime == "" {
		t.Error("Start time should be set")
	}
	if diagnostics.EndTime == "" {
		t.Error("End time should be set")
	}
	if diagnostics.Config.PlanID != "pass-123" {
		t.Errorf("Plan ID = %v, want pass-123", diagnostics.Config.PlanID)
	}
	if diagnostics.Config.ClientID != "test-client" {
		t.Errorf("Client ID = %v, want test-client", diagnostics.Config.ClientID)
	}
	if diagnostics.Statistics.Download.TotalFiles != 1 {
		t.Errorf("Total download files = %v, want 1", diagnostics.Statistics.Download.TotalFiles)
	}
	if len(diagnostics.Channels) != 1 {
		t.Errorf("Channels count = %v, want 1", len(diagnostics.Channels))
	}
}

func TestWriteDiagnosticsLocally(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := Config{
		DestDir: tmpDir,
	}
	clientID := "test-client-123"
	jsonData := []byte(`{"test": "data"}`)

	writeDiagnosticsLocally(cfg, clientID, jsonData)

	expectedPath := filepath.Join(tmpDir, "diagnostics", clientID+".json")
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Errorf("Diagnostics file %s does not exist", expectedPath)
	}

	// Verify contents
	contents, err := os.ReadFile(expectedPath)
	if err != nil {
		t.Fatalf("Failed to read diagnostics file: %v", err)
	}
	if string(contents) != string(jsonData) {
		t.Errorf("Diagnostics file contents = %v, want %v", string(contents), string(jsonData))
	}
}

func TestWriteDiagnosticsFile(t *testing.T) {
	tmpDir := t.TempDir()
	stats := newStatsTracker(true) // Enable diagnostics
	stats.AddChannelDownload("1", "BITSTREAM", 1000, SourceMQTT, 10)
	stats.AddChannelWrite("1", 1000)

	cfg := Config{
		PassID:            "pass-123",
		Environment:       "dev",
		ChannelIDs:        []string{"1"},
		DestDir:           tmpDir,
		WriteInOrder:      true,
		EnableDiagnostics: true,
	}

	ctx := t.Context()
	writeDiagnosticsFile(ctx, cfg, stats, nil)

	// Verify diagnostics file was created
	clientID := stats.GetClientID()
	if clientID == "" {
		clientID = unknownClientID
	}
	expectedPath := filepath.Join(tmpDir, "diagnostics", clientID+".json")
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Errorf("Diagnostics file %s does not exist", expectedPath)
	}
}

// Helper type for testing
type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}
