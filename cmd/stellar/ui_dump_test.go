package main

import (
	"log"
	"os"
	"testing"
	"time"
)

// TestPanelVisualDump drives the REAL stats dashboard + narration + reader log
// path and writes the raw terminal byte stream to $PANEL_DUMP_FILE, so an
// external terminal emulator can render the final screen and confirm the panel
// stays pinned to the bottom while log lines scroll above it. It is skipped
// unless PANEL_DUMP_FILE is set, so it never runs in the normal suite.
func TestPanelVisualDump(t *testing.T) {
	path := os.Getenv("PANEL_DUMP_FILE")
	if path == "" {
		t.Skip("set PANEL_DUMP_FILE to generate a terminal capture")
	}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	uiInitOnce.Do(func() {})
	prevColor, prevUI, prevOut, prevSizer := uiColor, ui, uiOut, terminalSize
	t.Cleanup(func() {
		uiColor, ui, uiOut, terminalSize = prevColor, prevUI, prevOut, prevSizer
		panelActive, panelRows, panelLines, termRows, termCols = false, 0, nil, 0, 0
	})
	uiColor = true
	ui = palette{
		reset: "\033[0m", bold: "\033[1m", dim: "\033[2m",
		red: "\033[31m", green: "\033[32m", yellow: "\033[33m",
		blue: "\033[34m", magenta: "\033[35m", cyan: "\033[36m",
	}
	uiOut = f
	terminalSize = func() (int, int, bool) { return 200, 30, true }
	panelActive, panelRows, panelLines, termRows, termCols = false, 0, nil, 0, 0

	// Route the standard logger through the panel-aware writer, exactly as a real
	// stream does.
	log.SetFlags(0)
	log.SetOutput(statusClearingWriter{w: f})
	defer log.SetOutput(os.Stderr)

	s := newStatsTracker(false)
	s.SetEnableStats(true)
	low := []string{"aaaaaaaa-0000-0000-0000-000000000001", "bbbbbbbb-0000-0000-0000-000000000002"}
	high := []string{"cccccccc-0000-0000-0000-000000000003"}
	s.SetDisplayChannels(low, high)

	// First frame: some data has arrived on two channels.
	for i := 1; i <= 20; i++ {
		s.AddChannelDownload(low[0]+"/downlink", "BITSTREAM", 1024*1024, SourceMQTT, i)
	}
	for i := 1; i <= 12; i++ {
		s.AddChannelDownload(high[0]+"/downlink", "IQ", 4*1024*1024, SourceS3, i)
	}
	s.lastLogTime = s.start.Add(-time.Hour) // bypass the throttle for the dump
	s.LogStats()

	// Reader log lines scroll above the panel.
	log.Printf("Worker connected; beginning high-rate fetch")
	uiDataf(kindTelemetry, "receiving on channel %s", low[0][:8])
	log.Printf("END detected at index 20 (channel %s, framing BITSTREAM)", low[0][:8])

	// A low-rate channel that received most data live over the broker (Topic) plus a
	// few objects recovered from Cloud Storage, to show the per-source split.
	for i := 1; i <= 30; i++ {
		s.AddChannelDownload(low[1]+"/downlink", "AX25", 256*1024, SourceMQTT, i)
	}
	for i := 31; i <= 33; i++ {
		s.AddChannelDownload(low[1]+"/downlink", "AX25", 256*1024, SourceS3, i)
	}

	// A couple of commands are sent (one satellite command, one gs config request).
	s.AddSentCommand("env/pass/p/channel/c/uplink", "uplink", 1)
	s.AddSentCommand("env/pass/p/channel/c/config_request", "config_request", 1)

	// Second frame: more data; panel numbers update in place.
	for i := 21; i <= 40; i++ {
		s.AddChannelDownload(low[0]+"/downlink", "BITSTREAM", 1024*1024, SourceMQTT, i)
	}
	s.lastLogTime = s.start.Add(-time.Hour)
	s.LogStats()

	log.Printf("END detected at index 12 (channel %s, framing IQ)", high[0][:8])
	uiOKf("Command sent. Waiting for acknowledgement...")

	// Shutdown clears the panel.
	clearStatusBlock()
	uiOKf("Pass finished.")
}
