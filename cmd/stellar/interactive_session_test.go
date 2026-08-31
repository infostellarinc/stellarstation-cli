package main

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
)

// captureStderr runs fn with os.Stderr redirected to a pipe and returns what
// was written. The interactive session reports every rejection to stderr.
func captureStderr(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stderr = w                       //nolint:reassign // Redirects stderr to capture the session's error reporting.
	defer func() { os.Stderr = orig }() //nolint:reassign // Restores the real stderr.

	fn()

	if err := w.Close(); err != nil {
		t.Fatalf("close pipe: %v", err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read pipe: %v", err)
	}
	return string(out)
}

// commandingSession builds an interactive session over the given targets with
// both command types enabled and no booking window restriction.
func commandingSession(targets []channelTarget) *interactiveSession {
	cfg := Config{EnableUplink: true, EnableConfigRequests: true}
	return newInteractiveSession(cfg, targets, nil, "stream-1", "plan-1", newStatsTracker(false), passWindow{})
}

// A stream whose channels accept no satellite commands must reject a "sat"
// line with an error, the same way a command outside the booking window or an
// invalid payload is rejected, rather than failing silently.
func TestInteractiveSatWithoutUplinkChannel(t *testing.T) {
	s := commandingSession([]channelTarget{
		{ChannelID: "ch-1", ConfigRequestTopic: "topic/config"},
	})

	out := captureStderr(t, func() {
		s.sendSatCommand(context.Background(), []string{"aaaabbbb"})
	})

	if !strings.Contains(out, "ERROR: this stream has no uplink channel") {
		t.Errorf("output = %q, want an error naming the missing uplink channel", out)
	}
	if s.satIndices[0] != 1 {
		t.Errorf("satellite command index advanced to %d; nothing may be sent", s.satIndices[0])
	}
}

// The same protection for "gs" lines on a stream with no config_request
// channel.
func TestInteractiveGsWithoutConfigRequestChannel(t *testing.T) {
	s := commandingSession([]channelTarget{
		{ChannelID: "ch-1", UplinkTopic: "topic/uplink"},
	})

	out := captureStderr(t, func() {
		s.sendGsConfig(context.Background(), []string{`{}`})
	})

	if !strings.Contains(out, "ERROR: this stream has no config_request channel") {
		t.Errorf("output = %q, want an error naming the missing config_request channel", out)
	}
	if s.gsIndices[0] != 1 {
		t.Errorf("config request index advanced to %d; nothing may be sent", s.gsIndices[0])
	}
}

// A stream with no commandable channels at all still gets a banner: the
// channel list says so explicitly, and the help renders with a placeholder
// channel id instead of indexing an empty target list.
func TestInteractiveBannerWithoutCommandChannels(t *testing.T) {
	s := commandingSession(nil)

	out := captureStderr(t, func() {
		s.printBanner("pass-1")
	})

	if !strings.Contains(out, "none: this stream has no channel that accepts commands") {
		t.Errorf("banner = %q, want the empty channel list named", out)
	}
	if !strings.Contains(out, "sat <channel-id> <hex>") {
		t.Errorf("banner = %q, want command help with a placeholder channel id", out)
	}
}

// Both rejections apply with an empty target list, the shape a stream with no
// commandable channels produces.
func TestInteractiveSendWithoutAnyChannels(t *testing.T) {
	s := commandingSession(nil)

	out := captureStderr(t, func() {
		s.sendSatCommand(context.Background(), []string{"aaaabbbb"})
		s.sendGsConfig(context.Background(), []string{`{}`})
	})

	if !strings.Contains(out, "no uplink channel") || !strings.Contains(out, "no config_request channel") {
		t.Errorf("output = %q, want both command types rejected", out)
	}
}
