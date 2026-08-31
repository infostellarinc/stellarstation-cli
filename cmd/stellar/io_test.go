package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// UUID-form fixtures: buildChunkFilePath and writeNonTelemetryMessage refuse
// pass and channel IDs that are not UUIDs (see validateChunkPathSegments).
const (
	testWritePassID    = "00000000-0000-0000-0000-000000000001"
	testWriteChannelID = "00000000-0000-0000-0000-000000000002"
)

func TestIsEndMessage(t *testing.T) {
	tests := []struct {
		name     string
		metadata map[string]string
		want     bool
	}{
		{
			name:     "nil metadata",
			metadata: nil,
			want:     false,
		},
		{
			name:     "empty metadata",
			metadata: map[string]string{},
			want:     false,
		},
		{
			name: "END message type lowercase",
			metadata: map[string]string{
				"messagetype": "end",
			},
			want: true,
		},
		{
			name: "END message type uppercase",
			metadata: map[string]string{
				"messagetype": "END",
			},
			want: true,
		},
		{
			name: "END message type mixed case",
			metadata: map[string]string{
				"messagetype": "End",
			},
			want: true,
		},
		{
			name: "non-END message type",
			metadata: map[string]string{
				"messagetype": "TELEMETRY",
			},
			want: false,
		},
		{
			name: "other metadata",
			metadata: map[string]string{
				"other": "value",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isEndMessage(tt.metadata)
			if got != tt.want {
				t.Errorf("isEndMessage() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWriteChunkToFile(t *testing.T) {
	tmpDir := t.TempDir()
	passID := testWritePassID
	index := 1
	channelID := testWriteChannelID
	framing := "BITSTREAM"
	rateType := "high-rate"
	data := []byte("test telemetry data")

	cfg := Config{
		DestDir:            tmpDir,
		PassID:             passID,
		EnableStdoutOutput: false,
		OutputFile:         "",
		WriteInOrder:       true,
	}

	stats := newStatsTracker(false)

	err := writeChunkToFile(
		data,
		cfg.DestDir,
		"prefix",
		passID,
		index,
		channelID,
		framing,
		rateType,
		stats,
		cfg.WriteInOrder,
		cfg,
	)
	if err != nil {
		t.Fatalf("writeChunkToFile() error = %v", err)
	}

	// The key is "prefix" + "1" = "prefix1", and filepath.Base("prefix1") = "prefix1"
	expectedPath := filepath.Join(
		tmpDir,
		"pass", passID,
		rateType,
		"channel", testWriteChannelID,
		framing,
		"prefix1",
	)

	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Errorf("Expected file %s does not exist", expectedPath)
	}

	// Verify file contents
	contents, err := os.ReadFile(expectedPath)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}
	if string(contents) != string(data) {
		t.Errorf("File contents = %v, want %v", string(contents), string(data))
	}
}

func TestWriteChunkToFileWithStdout(t *testing.T) {
	tmpDir := t.TempDir()
	passID := testWritePassID
	index := 1
	channelID := testWriteChannelID
	framing := "BITSTREAM"
	rateType := "high-rate"
	data := []byte("test telemetry data")

	cfg := Config{
		DestDir:            tmpDir,
		PassID:             passID,
		EnableStdoutOutput: true,
		OutputFile:         "",
		WriteInOrder:       true,
	}

	stats := newStatsTracker(false)

	// Note: This will write to stdout, but we can't easily capture it in tests
	// The function should still complete successfully
	err := writeChunkToFile(
		data,
		cfg.DestDir,
		"prefix/",
		passID,
		index,
		channelID,
		framing,
		rateType,
		stats,
		cfg.WriteInOrder,
		cfg,
	)
	if err != nil {
		t.Fatalf("writeChunkToFile() error = %v", err)
	}
}

func TestWriteChunkToFileWithOutputFile(t *testing.T) {
	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "output.bin")
	passID := testWritePassID
	index := 1
	channelID := testWriteChannelID
	framing := "BITSTREAM"
	rateType := "high-rate"
	data := []byte("test telemetry data")

	cfg := Config{
		DestDir:            tmpDir,
		PassID:             passID,
		EnableStdoutOutput: false,
		OutputFile:         outputFile,
		OutputFileMode:     []string{"all-combined"},
		WriteInOrder:       true,
	}

	stats := newStatsTracker(false)

	err := writeChunkToFile(
		data,
		cfg.DestDir,
		"prefix/",
		passID,
		index,
		channelID,
		framing,
		rateType,
		stats,
		cfg.WriteInOrder,
		cfg,
	)
	if err != nil {
		t.Fatalf("writeChunkToFile() error = %v", err)
	}

	// Verify output file was created
	if _, err := os.Stat(outputFile); os.IsNotExist(err) {
		t.Errorf("Expected output file %s does not exist", outputFile)
	}

	// Clean up
	flushOutputFiles()
}

func TestWriteChunkToFileWithUnknownFraming(t *testing.T) {
	tmpDir := t.TempDir()
	passID := testWritePassID
	index := 1
	channelID := testWriteChannelID
	framing := "" // Empty framing should use "unknown"
	rateType := "high-rate"
	data := []byte("test telemetry data")

	cfg := Config{
		DestDir:            tmpDir,
		PassID:             passID,
		EnableStdoutOutput: false,
		OutputFile:         "",
		WriteInOrder:       true,
	}

	stats := newStatsTracker(false)

	err := writeChunkToFile(
		data,
		cfg.DestDir,
		"prefix",
		passID,
		index,
		channelID,
		framing,
		rateType,
		stats,
		cfg.WriteInOrder,
		cfg,
	)
	if err != nil {
		t.Fatalf("writeChunkToFile() error = %v", err)
	}

	// The key is "prefix" + "1" = "prefix1", and filepath.Base("prefix1") = "prefix1"
	expectedPath := filepath.Join(
		tmpDir,
		"pass", passID,
		rateType,
		"channel", testWriteChannelID,
		unknownFraming,
		"prefix1",
	)

	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Errorf("Expected file %s does not exist", expectedPath)
	}
}

func TestWriteToOutputFile(t *testing.T) {
	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "output.bin")
	data := []byte("test data")
	channelID := testWriteChannelID
	framing := "BITSTREAM"

	cfg := Config{
		OutputFile:     outputFile,
		OutputFileMode: []string{"all-combined"},
	}

	err := writeToOutputFile(data, channelID, framing, cfg)
	if err != nil {
		t.Fatalf("writeToOutputFile() error = %v", err)
	}

	flushOutputFiles()

	// Verify file was created and contains data
	contents, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatalf("Failed to read output file: %v", err)
	}
	if string(contents) != string(data) {
		t.Errorf("Output file contents = %v, want %v", string(contents), string(data))
	}
}

func TestWriteToOutputFilePerChannel(t *testing.T) {
	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "output.bin")
	data := []byte("test data")
	channelID := testWriteChannelID
	framing := "BITSTREAM"

	cfg := Config{
		OutputFile:     outputFile,
		OutputFileMode: []string{"per-channel"},
	}

	err := writeToOutputFile(data, channelID, framing, cfg)
	if err != nil {
		t.Fatalf("writeToOutputFile() error = %v", err)
	}

	flushOutputFiles()

	expectedFile := outputFile + "-ch" + testWriteChannelID
	if _, err := os.Stat(expectedFile); os.IsNotExist(err) {
		t.Errorf("Expected per-channel file %s does not exist", expectedFile)
	}
}

func TestWriteToOutputFilePerFraming(t *testing.T) {
	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "output.bin")
	data := []byte("test data")
	channelID := testWriteChannelID
	framing := "BITSTREAM"

	cfg := Config{
		OutputFile:     outputFile,
		OutputFileMode: []string{"per-framing"},
	}

	err := writeToOutputFile(data, channelID, framing, cfg)
	if err != nil {
		t.Fatalf("writeToOutputFile() error = %v", err)
	}

	flushOutputFiles()

	expectedFile := outputFile + "-BITSTREAM"
	if _, err := os.Stat(expectedFile); os.IsNotExist(err) {
		t.Errorf("Expected per-framing file %s does not exist", expectedFile)
	}
}

func TestWriteToOutputFilePerFramingChannel(t *testing.T) {
	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "output.bin")
	data := []byte("test data")
	channelID := testWriteChannelID
	framing := "BITSTREAM"

	cfg := Config{
		OutputFile:     outputFile,
		OutputFileMode: []string{"per-framing-channel"},
	}

	err := writeToOutputFile(data, channelID, framing, cfg)
	if err != nil {
		t.Fatalf("writeToOutputFile() error = %v", err)
	}

	flushOutputFiles()

	expectedFile := outputFile + "-BITSTREAM-ch" + testWriteChannelID
	if _, err := os.Stat(expectedFile); os.IsNotExist(err) {
		t.Errorf("Expected per-framing-channel file %s does not exist", expectedFile)
	}
}

func TestWriteToOutputFileMultipleModes(t *testing.T) {
	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "output.bin")
	data := []byte("test data")
	channelID := testWriteChannelID
	framing := "BITSTREAM"

	cfg := Config{
		OutputFile:     outputFile,
		OutputFileMode: []string{"all-combined", "per-channel", "per-framing"},
	}

	err := writeToOutputFile(data, channelID, framing, cfg)
	if err != nil {
		t.Fatalf("writeToOutputFile() error = %v", err)
	}

	flushOutputFiles()

	// Check all three files exist
	files := []string{
		outputFile,
		outputFile + "-ch" + testWriteChannelID,
		outputFile + "-BITSTREAM",
	}
	for _, file := range files {
		if _, err := os.Stat(file); os.IsNotExist(err) {
			t.Errorf("Expected file %s does not exist", file)
		}
	}
}

func TestWriteToOutputFileUnknownFraming(t *testing.T) {
	tmpDir := t.TempDir()
	outputFile := filepath.Join(tmpDir, "output.bin")
	data := []byte("test data")
	channelID := testWriteChannelID
	framing := "" // Empty framing should use "unknown"

	cfg := Config{
		OutputFile:     outputFile,
		OutputFileMode: []string{"per-framing"},
	}

	err := writeToOutputFile(data, channelID, framing, cfg)
	if err != nil {
		t.Fatalf("writeToOutputFile() error = %v", err)
	}

	flushOutputFiles()

	expectedFile := outputFile + "-unknown"
	if _, err := os.Stat(expectedFile); os.IsNotExist(err) {
		t.Errorf("Expected file %s does not exist", expectedFile)
	}
}

func TestWriteNonTelemetryMessage(t *testing.T) {
	tmpDir := t.TempDir()
	passID := testWritePassID
	msgType := "monitoring"
	data := []byte(`{"planId":"plan-123","antennaId":"ant-1"}`)

	err := writeNonTelemetryMessage(data, tmpDir, passID, msgType)
	if err != nil {
		t.Fatalf("writeNonTelemetryMessage() error = %v", err)
	}

	// Verify directory was created
	expectedDir := filepath.Join(tmpDir, "pass", passID, msgType)
	if _, err := os.Stat(expectedDir); os.IsNotExist(err) {
		t.Errorf("Expected directory %s does not exist", expectedDir)
	}

	// Verify a JSON file exists in the directory
	entries, err := os.ReadDir(expectedDir)
	if err != nil {
		t.Fatalf("Failed to read directory: %v", err)
	}
	if len(entries) == 0 {
		t.Error("Expected at least one JSON file in the directory")
	}
}

func TestWriteNonTelemetryMessageConfig(t *testing.T) {
	tmpDir := t.TempDir()
	passID := testWritePassID
	msgType := "config"
	data := []byte(`{"planId":"plan-123"}`)

	err := writeNonTelemetryMessage(data, tmpDir, passID, msgType)
	if err != nil {
		t.Fatalf("writeNonTelemetryMessage() error = %v", err)
	}

	expectedDir := filepath.Join(tmpDir, "pass", passID, msgType)
	if _, err := os.Stat(expectedDir); os.IsNotExist(err) {
		t.Errorf("Expected directory %s does not exist", expectedDir)
	}
}

func TestWriteNonTelemetryMessageEvent(t *testing.T) {
	tmpDir := t.TempDir()
	passID := testWritePassID
	msgType := "event"
	data := []byte(`{"planId":"plan-123","eventType":"START"}`)

	err := writeNonTelemetryMessage(data, tmpDir, passID, msgType)
	if err != nil {
		t.Fatalf("writeNonTelemetryMessage() error = %v", err)
	}

	expectedDir := filepath.Join(tmpDir, "pass", passID, msgType)
	if _, err := os.Stat(expectedDir); os.IsNotExist(err) {
		t.Errorf("Expected directory %s does not exist", expectedDir)
	}
}

// Regression tests: server-provided path segments must be validated before
// they are joined into local write paths. A malicious or compromised API must
// not be able to steer writes outside the destination directory.

func TestBuildChunkFilePathRejectsMaliciousSegments(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name      string
		passID    string
		channelID string
		framing   string
	}{
		{"traversal pass ID", "../../../../tmp/evil", testWriteChannelID, "BITSTREAM"},
		{"dot-dot pass ID", "..", testWriteChannelID, "BITSTREAM"},
		{"non-UUID pass ID", "not-a-uuid", testWriteChannelID, "BITSTREAM"},
		{"empty pass ID", "", testWriteChannelID, "BITSTREAM"},
		{"traversal channel ID", testWritePassID, "../../../../tmp/evil", "BITSTREAM"},
		{"dot-dot channel ID", testWritePassID, "..", "BITSTREAM"},
		{"channel with bad suffix", testWritePassID, testWriteChannelID + "/../..", "BITSTREAM"},
		{"channel with uplink suffix", testWritePassID, testWriteChannelID + "/uplink", "BITSTREAM"},
		{"traversal framing", testWritePassID, testWriteChannelID, "../evil"},
		{"unknown framing name", testWritePassID, testWriteChannelID, "NOT_A_FRAMING"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := buildChunkFilePath(tmpDir, "prefix", tt.passID, "high-rate", 1, tt.channelID, tt.framing)
			if err == nil {
				t.Fatalf("buildChunkFilePath accepted passID=%q channelID=%q framing=%q",
					tt.passID, tt.channelID, tt.framing)
			}
		})
	}
}

func TestBuildChunkFilePathAcceptsValidSegments(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name      string
		channelID string
		framing   string
	}{
		{"bare channel UUID", testWriteChannelID, "BITSTREAM"},
		{"MQTT downlink channel form", testWriteChannelID + "/downlink", "IQ"},
		{"empty framing maps to unknown", testWriteChannelID, ""},
		{"every known framing", testWriteChannelID, "WATERFALL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, err := buildChunkFilePath(tmpDir, "prefix", testWritePassID, "high-rate", 1, tt.channelID, tt.framing)
			if err != nil {
				t.Fatalf("buildChunkFilePath() error = %v", err)
			}
			absDest, _ := filepath.Abs(tmpDir)
			absPath, _ := filepath.Abs(path)
			if absPath != absDest && !strings.HasPrefix(absPath, absDest+string(filepath.Separator)) {
				t.Fatalf("path %q escapes destination %q", absPath, absDest)
			}
		})
	}
}

func TestWriteNonTelemetryMessageRejectsMaliciousPassID(t *testing.T) {
	tmpDir := t.TempDir()
	for _, passID := range []string{"../../../../tmp/evil", "..", "not-a-uuid", ""} {
		if err := writeNonTelemetryMessage([]byte(`{}`), tmpDir, passID, "monitoring"); err == nil {
			t.Fatalf("writeNonTelemetryMessage accepted pass ID %q", passID)
		}
	}
}
