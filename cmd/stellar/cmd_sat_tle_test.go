package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	tleLine1 = "1 27424U 02022A   25349.36942604  .00001147  00000+0  23955-3 0  9991"
	tleLine2 = "2 27424  98.4040 310.2912 0001189 114.3209   1.9707 14.61815411256417"
)

func TestParseOrbitalDataInput(t *testing.T) {
	t.Run("raw two-line TLE", func(t *testing.T) {
		got, err := parseOrbitalDataInput(tleLine1+"\n"+tleLine2+"\n", "TLE")
		if err != nil {
			t.Fatalf("parseOrbitalDataInput: %v", err)
		}
		if got["line1"] != tleLine1 || got["line2"] != tleLine2 {
			t.Errorf("got %+v, want line1/line2 preserved verbatim", got)
		}
	})

	t.Run("TLE with leading name line and CRLF", func(t *testing.T) {
		got, err := parseOrbitalDataInput("ISS (ZARYA)\r\n"+tleLine1+"\r\n"+tleLine2+"\r\n", "TLE")
		if err != nil {
			t.Fatalf("parseOrbitalDataInput: %v", err)
		}
		if got["line1"] != tleLine1 || got["line2"] != tleLine2 {
			t.Errorf("got %+v, want name line ignored and CR stripped", got)
		}
	})

	t.Run("TLE JSON", func(t *testing.T) {
		got, err := parseOrbitalDataInput(`{"line1":"1 25544U","line2":"2 25544"}`, "TLE")
		if err != nil {
			t.Fatalf("parseOrbitalDataInput: %v", err)
		}
		if got["line1"] != "1 25544U" || got["line2"] != "2 25544" {
			t.Errorf("got %+v, want parsed JSON", got)
		}
	})

	t.Run("OMM JSON", func(t *testing.T) {
		got, err := parseOrbitalDataInput(`{"MEAN_MOTION":15.5,"ECCENTRICITY":0.0007,"EPOCH":"2026-05-08T12:00:00Z"}`, "OMM")
		if err != nil {
			t.Fatalf("parseOrbitalDataInput(OMM): %v", err)
		}
		if got["MEAN_MOTION"] != 15.5 || got["EPOCH"] != "2026-05-08T12:00:00Z" {
			t.Errorf("got %+v, want OMM fields preserved", got)
		}
	})

	t.Run("OMM as two-line text is an error", func(t *testing.T) {
		if _, err := parseOrbitalDataInput(tleLine1+"\n"+tleLine2, "OMM"); err == nil {
			t.Error("expected error: OMM must be JSON, not two-line text")
		}
	})

	t.Run("empty is an error", func(t *testing.T) {
		if _, err := parseOrbitalDataInput("   \n", "TLE"); err == nil {
			t.Error("expected error for empty input")
		}
	})

	t.Run("non-TLE, non-JSON is an error", func(t *testing.T) {
		if _, err := parseOrbitalDataInput("just some text", "TLE"); err == nil {
			t.Error("expected error for unparseable input")
		}
	})
}

// TestAddTLECommand_DataFileTLE reproduces the ticket: a raw two-line TLE file
// passed via --data-file must be accepted (previously it failed JSON parsing).
func TestAddTLECommand_DataFileTLE(t *testing.T) {
	srv := newTestPassServer(t)
	defer srv.Close()
	tokSrv := setupAuthForTest(t)
	defer tokSrv.Close()

	path := filepath.Join(t.TempDir(), "tle.txt")
	if err := os.WriteFile(path, []byte(tleLine1+"\n"+tleLine2+"\n"), 0o600); err != nil {
		t.Fatalf("write temp TLE file: %v", err)
	}

	root := NewRootCommand()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetArgs([]string{
		"satellite", "add-tle",
		"--api-url", srv.URL,
		"--satellite-id", "sat-1",
		"--data-file", path,
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("add-tle --data-file (raw TLE): %v", err)
	}
	if !strings.Contains(buf.String(), "od-1") {
		t.Errorf("output missing orbit data: %s", buf.String())
	}
}
