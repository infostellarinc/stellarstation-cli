package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/infostellarinc/stellarstation-cli/internal/apiclient"
)

// trackingTestServer serves one satellite's tracking settings and records the
// last default it was asked to store.
func trackingTestServer(t *testing.T) (*httptest.Server, *apiclient.UpdateTrackingSettingRequest) {
	t.Helper()
	var stored apiclient.UpdateTrackingSettingRequest
	setting := apiclient.TrackingSetting{
		GroundStationID:   "gs-1",
		GroundStationName: "Haneda",
		AllowedModes:      []string{"NONE", "TRACK_STATIC_OFFSETS"},
		DefaultMode:       "TRACK_STATIC_OFFSETS",
		Effective:         apiclient.TrackingEffective{Mode: "TRACK_STATIC_OFFSETS"},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/satellites/sat-1/tracking-settings"):
			locked := setting
			locked.GroundStationID, locked.GroundStationName = "gs-2", "London"
			locked.AllowedModes, locked.DefaultMode = []string{"NONE"}, "NONE"
			locked.Effective = apiclient.TrackingEffective{Mode: "NONE", Locked: true, Reason: "FORCED_BY_OPERATOR"}
			_ = json.NewEncoder(w).Encode(apiclient.ListTrackingSettingsResponse{
				Settings: []apiclient.TrackingSetting{setting, locked},
			})
		case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/ground-stations/gs-1/tracking-settings"):
			if err := json.NewDecoder(r.Body).Decode(&stored); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			updated := setting
			updated.DefaultMode = stored.DefaultMode
			updated.Effective.Mode = stored.DefaultMode
			_ = json.NewEncoder(w).Encode(updated)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &stored
}

func TestGetTrackingSettingsCommand(t *testing.T) {
	srv, _ := trackingTestServer(t)
	tokSrv := setupAuthForTest(t)
	defer tokSrv.Close()

	root := NewRootCommand()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetArgs([]string{"satellite", "get-tracking-settings", "--api-url", srv.URL, "--satellite-id", "sat-1"})
	if err := root.Execute(); err != nil {
		t.Fatalf("get-tracking-settings: %v", err)
	}
	for _, want := range []string{"Haneda", "London", "NONE,TRACK_STATIC_OFFSETS", "true (FORCED_BY_OPERATOR)"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("output missing %q:\n%s", want, buf.String())
		}
	}
}

func TestSetTrackingDefaultCommand(t *testing.T) {
	srv, stored := trackingTestServer(t)
	tokSrv := setupAuthForTest(t)
	defer tokSrv.Close()

	root := NewRootCommand()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetArgs([]string{
		"satellite", "set-tracking-default", "--api-url", srv.URL,
		"--satellite-id", "sat-1", "--ground-station-id", "gs-1", "--mode", "none", "--output", "json",
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("set-tracking-default: %v", err)
	}
	if stored.DefaultMode != "NONE" {
		t.Fatalf("server should have been asked to store NONE, got %q", stored.DefaultMode)
	}
	var rows []apiclient.TrackingSetting
	if err := json.Unmarshal(buf.Bytes(), &rows); err != nil || len(rows) != 1 {
		t.Fatalf("parse JSON output: %v\n%s", err, buf.String())
	}
	if rows[0].Effective.Mode != "NONE" {
		t.Fatalf("expected the returned mode to be NONE: %+v", rows[0])
	}
}
