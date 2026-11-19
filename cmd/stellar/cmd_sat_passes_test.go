package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/infostellarinc/stellarstation-cli/internal/apiclient"
)

func newTestPassServer(t *testing.T) *httptest.Server {
	t.Helper()
	now := time.Now().Truncate(time.Second)
	lat, lng := 34.134117, -118.321550
	maxElTime := now.Add(5 * time.Minute)
	groundStation := apiclient.GroundStationRef{
		ID:           "gs-1",
		Name:         "MyGS",
		Organization: "MyOrg",
		Latitude:     &lat,
		Longitude:    &lng,
	}
	passes := []apiclient.Pass{
		{
			ID:                  "p-1",
			Satellite:           apiclient.SatelliteRef{ID: "sat-1", Name: "MySat"},
			GroundStation:       groundStation,
			ExecutionConfigName: "MyConfig",
			Visibility:          &apiclient.Interval{Start: now, Stop: now.Add(10 * time.Minute)},
			Execution:           &apiclient.Execution{Status: "SCHEDULED"},
			MaxElevationDegrees: 45.5,
			MaxElevationTime:    &maxElTime,
			ConflictStatus:      "NO_CONFLICT",
		},
	}
	visibilities := []apiclient.Visibility{
		{
			Satellite:           apiclient.SatelliteRef{ID: "sat-1", Name: "MySat"},
			GroundStation:       groundStation,
			Visibility:          &apiclient.Interval{Start: now, Stop: now.Add(10 * time.Minute)},
			MaxElevationDegrees: 45.5,
			MaxElevationTime:    &maxElTime,
			ConflictStatus:      "NO_CONFLICT",
		},
	}
	orbit := apiclient.OrbitData{
		ID:              "od-1",
		SatelliteID:     "sat-1",
		OrbitalDataType: "TLE",
		Source:          "manual",
		OrbitalData: map[string]interface{}{
			"line1": "1 27424U 02022A   25349.36942604  .00001147  00000+0  23955-3 0  9991",
			"line2": "2 27424  98.4040 310.2912 0001189 114.3209   1.9707 14.61815411256417",
		},
	}
	epoch := time.Date(2026, 11, 10, 3, 4, 5, 0, time.UTC)
	activatedAt := time.Date(2026, 11, 10, 6, 0, 0, 0, time.UTC)
	passDetail := passes[0]
	passDetail.OrbitData = &apiclient.PassOrbitData{
		OrbitDataID:  "od-1",
		Epoch:        &epoch,
		ActivationID: "act-1",
		ActivatedAt:  &activatedAt,
	}
	history := apiclient.ListOrbitDataResponse{SatelliteID: "sat-1", Items: []apiclient.OrbitHistoryItem{{
		ID:           "od-1",
		ActivationID: "act-1",
		Epoch:        &epoch,
		ActivatedAt:  &activatedAt,
	}}}
	orbitParams := apiclient.OrbitParameters{
		ID:                "op-1",
		SatelliteID:       "sat-1",
		OrbitalDataSource: "space-track",
		NoradID:           "25544",
	}

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/passes" && r.Method == http.MethodGet:
			json.NewEncoder(w).Encode(apiclient.ListPassesResponse{Passes: passes})
		case r.URL.Path == "/v1/passes" && r.Method == http.MethodPost:
			json.NewEncoder(w).Encode(passes[0])
		case strings.HasPrefix(r.URL.Path, "/v1/passes/") && r.Method == http.MethodDelete:
			json.NewEncoder(w).Encode(passes[0])
		case r.URL.Path == "/v1/passes/p-1" && r.Method == http.MethodGet:
			json.NewEncoder(w).Encode(passDetail)
		case strings.HasSuffix(r.URL.Path, "/orbit-data/history"):
			json.NewEncoder(w).Encode(history)
		case r.URL.Path == "/v1/visibilities":
			json.NewEncoder(w).Encode(visibilities)
		case r.URL.Path == "/v1/satellites":
			json.NewEncoder(w).Encode(apiclient.ListSatellitesResponse{
				Satellites: []apiclient.Satellite{
					{ID: "sat-1", DisplayName: "MySat", OrganizationID: "org-1", Schedulable: true},
				},
			})
		case strings.HasSuffix(r.URL.Path, "/configurations"):
			json.NewEncoder(w).Encode(apiclient.ListConfigurationsResponse{
				Configurations: []apiclient.ExecutionConfig{
					{ID: "ec-1", DisplayName: "MyConfig", SatelliteID: "sat-1", GroundStationID: "gs-1"},
				},
			})
		case strings.HasSuffix(r.URL.Path, "/orbit-data/current"):
			json.NewEncoder(w).Encode(orbit)
		case strings.HasSuffix(r.URL.Path, "/orbit-data") && r.Method == http.MethodPost:
			json.NewEncoder(w).Encode(orbit)
		case strings.HasSuffix(r.URL.Path, "/orbit-parameters") && r.Method == http.MethodGet:
			json.NewEncoder(w).Encode(orbitParams)
		case strings.HasSuffix(r.URL.Path, "/orbit-parameters") && r.Method == http.MethodPatch:
			json.NewEncoder(w).Encode(orbitParams)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func TestListPassesCommand(t *testing.T) {
	srv := newTestPassServer(t)
	defer srv.Close()
	tokSrv := setupAuthForTest(t)
	defer tokSrv.Close()

	for _, format := range []string{"wide", "csv", "json"} {
		t.Run(format, func(t *testing.T) {
			root := NewRootCommand()
			var buf bytes.Buffer
			root.SetOut(&buf)
			root.SetArgs([]string{
				"satellite", "list-passes",
				"--api-url", srv.URL,
				"--output", format,
			})
			if err := root.Execute(); err != nil {
				t.Fatalf("list-passes -o %s: %v", format, err)
			}
			if buf.Len() == 0 {
				t.Error("expected output, got empty")
			}
			if format == "wide" {
				// Exercise the pass/satellite/ground-station IDs plus the
				// detail columns shared/added for list-passes.
				for _, want := range []string{
					"p-1", "sat-1", "gs-1", "MyOrg", "MyConfig", "NO_CONFLICT",
				} {
					if !strings.Contains(buf.String(), want) {
						t.Errorf("wide output missing %q: %s", want, buf.String())
					}
				}
				// Max elevation must be a bare number with no degree symbol.
				if strings.Contains(buf.String(), "°") {
					t.Errorf("wide output should not contain a degree symbol: %s", buf.String())
				}
			}
		})
	}
}

func TestListPassesUsesStellarAPIURLWhenFlagDefault(t *testing.T) {
	srv := newTestPassServer(t)
	defer srv.Close()
	tokSrv := setupAuthForTest(t)
	defer tokSrv.Close()

	t.Setenv("STELLAR_API_URL", srv.URL)

	root := NewRootCommand()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetArgs([]string{
		"satellite", "list-passes",
		"--output", "wide",
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("list-passes: %v", err)
	}
	if !strings.Contains(buf.String(), "p-1") {
		t.Fatalf("expected pass id in output, got %q", buf.String())
	}
}

func TestReservePassCommand(t *testing.T) {
	srv := newTestPassServer(t)
	defer srv.Close()
	tokSrv := setupAuthForTest(t)
	defer tokSrv.Close()

	root := NewRootCommand()
	var buf bytes.Buffer
	root.SetOut(&buf)
	now := time.Now().Truncate(time.Second)
	root.SetArgs([]string{
		"satellite", "reserve-pass",
		"--api-url", srv.URL,
		"--satellite-id", "sat-1",
		"--ground-station-id", "gs-1",
		"--execution-config-id", "ec-1",
		"--booking-start", now.Format(time.RFC3339),
		"--booking-stop", now.Add(time.Hour).Format(time.RFC3339),
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("reserve-pass: %v", err)
	}
	if !strings.Contains(buf.String(), "p-1") {
		t.Errorf("output missing pass: %s", buf.String())
	}
}

// An empty booking window must be a clear error, not a nil-pointer panic:
// cobra's required-flag check only catches flags that are absent entirely,
// while --booking-stop "" passes it and parseTimeFlag then returns nil.
func TestReservePassCommandRequiresBookingWindow(t *testing.T) {
	srv := newTestPassServer(t)
	defer srv.Close()
	tokSrv := setupAuthForTest(t)
	defer tokSrv.Close()

	root := NewRootCommand()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetErr(&buf)
	root.SetArgs([]string{
		"satellite", "reserve-pass",
		"--api-url", srv.URL,
		"--satellite-id", "sat-1",
		"--ground-station-id", "gs-1",
		"--execution-config-id", "ec-1",
		"--booking-start", time.Now().UTC().Format(time.RFC3339),
		"--booking-stop", "",
	})
	err := root.Execute()
	if err == nil {
		t.Fatal("reserve-pass with an empty booking window succeeded, want an error")
	}
	if !strings.Contains(err.Error(), "--booking-start and --booking-stop are required") {
		t.Errorf("unexpected error: %v", err)
	}
}

// runPassCommand runs the CLI against the test pass server and returns stdout.
func runPassCommand(t *testing.T, args ...string) string {
	t.Helper()
	srv := newTestPassServer(t)
	defer srv.Close()
	tokSrv := setupAuthForTest(t)
	defer tokSrv.Close()

	root := NewRootCommand()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetArgs(append(args, "--api-url", srv.URL))
	if err := root.Execute(); err != nil {
		t.Fatalf("%v: %v", args, err)
	}
	return buf.String()
}

// get-pass names the orbit data the pass was computed from (issue #969).
func TestGetPassCommandShowsOrbitData(t *testing.T) {
	out := runPassCommand(t, "satellite", "get-pass", "p-1")

	for _, want := range []string{
		"ORBIT_DATA_ID", "od-1",
		"ORBIT_DATA_EPOCH", "2026-11-10T03:04:05Z",
		"ORBIT_DATA_ACTIVATION_ID", "act-1",
		"ORBIT_DATA_ACTIVATED", "2026-11-10T06:00:00Z",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("get-pass output missing %q:\n%s", want, out)
		}
	}
}

func TestGetPassCommandJSONCarriesOrbitData(t *testing.T) {
	out := runPassCommand(t, "satellite", "get-pass", "p-1", "--output", "json")

	var rows []apiclient.Pass
	if err := json.Unmarshal([]byte(out), &rows); err != nil || len(rows) != 1 {
		t.Fatalf("parse get-pass JSON: %v\n%s", err, out)
	}
	od := rows[0].OrbitData
	if od == nil || od.OrbitDataID != "od-1" || od.ActivationID != "act-1" {
		t.Fatalf("orbit_data = %+v, want od-1 / act-1", od)
	}
	if od.Epoch == nil || !od.Epoch.Equal(time.Date(2026, 11, 10, 3, 4, 5, 0, time.UTC)) {
		t.Errorf("epoch = %v", od.Epoch)
	}
}

// A pass without orbit data leaves the columns blank rather than failing.
func TestPassOrbitDataWithoutOrbitData(t *testing.T) {
	if got := passOrbitData(&apiclient.Pass{ID: "p-1"}); got != (apiclient.PassOrbitData{}) {
		t.Errorf("passOrbitData = %+v, want empty", got)
	}
}

// The orbit data history shows each entry's activation ID, which get-pass
// reports as ORBIT_DATA_ACTIVATION_ID.
func TestGetOrbitDataHistoryCommandShowsActivationID(t *testing.T) {
	out := runPassCommand(t, "satellite", "get-orbit-data-history", "--satellite-id", "sat-1")

	for _, want := range []string{"ACTIVATION_ID", "act-1", "od-1"} {
		if !strings.Contains(out, want) {
			t.Errorf("get-orbit-data-history output missing %q:\n%s", want, out)
		}
	}
}

func TestCancelPassCommand(t *testing.T) {
	srv := newTestPassServer(t)
	defer srv.Close()
	tokSrv := setupAuthForTest(t)
	defer tokSrv.Close()

	root := NewRootCommand()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetArgs([]string{
		"satellite", "cancel-pass", "p-1",
		"--api-url", srv.URL,
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("cancel-pass: %v", err)
	}
	if !strings.Contains(buf.String(), "p-1") {
		t.Errorf("output missing pass: %s", buf.String())
	}
}

func TestListVisibilitiesCommand(t *testing.T) {
	srv := newTestPassServer(t)
	defer srv.Close()
	tokSrv := setupAuthForTest(t)
	defer tokSrv.Close()

	root := NewRootCommand()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetArgs([]string{
		"satellite", "list-visibilities",
		"--api-url", srv.URL,
		"--satellite-id", "sat-1",
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("list-visibilities: %v", err)
	}
	// Covers satellite/ground-station name+ID columns plus the shared
	// ground-station detail columns.
	for _, want := range []string{"MySat", "sat-1", "gs-1", "MyOrg", "NO_CONFLICT"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("output missing %q: %s", want, buf.String())
		}
	}
}

func TestListSatellitesCommand(t *testing.T) {
	srv := newTestPassServer(t)
	defer srv.Close()
	tokSrv := setupAuthForTest(t)
	defer tokSrv.Close()

	root := NewRootCommand()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetArgs([]string{
		"satellite", "list-satellites",
		"--api-url", srv.URL,
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("list-satellites: %v", err)
	}
	for _, want := range []string{"sat-1", "MySat", "org-1", "true"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("output missing %q: %s", want, buf.String())
		}
	}
}

func TestListConfigurationsCommand(t *testing.T) {
	srv := newTestPassServer(t)
	defer srv.Close()
	tokSrv := setupAuthForTest(t)
	defer tokSrv.Close()

	root := NewRootCommand()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetArgs([]string{
		"satellite", "list-configurations",
		"--api-url", srv.URL,
		"--satellite-id", "sat-1",
		"--ground-station-id", "gs-1",
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("list-configurations: %v", err)
	}
	for _, want := range []string{"ec-1", "MyConfig"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("output missing %q: %s", want, buf.String())
		}
	}
}

func TestGetTLECommand(t *testing.T) {
	srv := newTestPassServer(t)
	defer srv.Close()
	tokSrv := setupAuthForTest(t)
	defer tokSrv.Close()

	root := NewRootCommand()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetArgs([]string{
		"satellite", "get-tle",
		"--api-url", srv.URL,
		"--satellite-id", "sat-1",
		"--output", "csv",
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("get-tle: %v", err)
	}
	// csv/wide output must include the orbit-data ID and both TLE lines.
	for _, want := range []string{"od-1", "1 27424U", "2 27424"} {
		if !strings.Contains(buf.String(), want) {
			t.Errorf("get-tle csv output missing %q: %s", want, buf.String())
		}
	}
}

func TestAddTLECommand(t *testing.T) {
	srv := newTestPassServer(t)
	defer srv.Close()
	tokSrv := setupAuthForTest(t)
	defer tokSrv.Close()

	root := NewRootCommand()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetArgs([]string{
		"satellite", "add-tle",
		"--api-url", srv.URL,
		"--satellite-id", "sat-1",
		"--data", `{"line1":"1 25544U","line2":"2 25544"}`,
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("add-tle: %v", err)
	}
	if !strings.Contains(buf.String(), "od-1") {
		t.Errorf("output missing orbit data: %s", buf.String())
	}
}

func TestSetTLESourceCommand(t *testing.T) {
	srv := newTestPassServer(t)
	defer srv.Close()
	tokSrv := setupAuthForTest(t)
	defer tokSrv.Close()

	root := NewRootCommand()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetArgs([]string{
		"satellite", "set-tle-source",
		"--api-url", srv.URL,
		"--satellite-id", "sat-1",
		"--source", "space-track",
		"--norad-id", "25544",
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("set-tle-source: %v", err)
	}
	if !strings.Contains(buf.String(), "space-track") {
		t.Errorf("output missing source: %s", buf.String())
	}
}
