package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/infostellarinc/stellarstation-cli/internal/apiclient"
)

const fittedOrbitData = `{"id":"od-oem","satellite_id":"sat-1","orbital_data_type":"TLE",
"orbital_data":{"line1":"1 25544U 98067A   26315.50000000  .00000000  00000+0  14541-3 0  9998",
"line2":"2 25544  51.6325 284.2547 0007335  19.0494 341.0767 15.49339933    08"},
"warnings":["the TLE deviates up to 3.4 km from the uploaded orbit (1.20 km RMS), so antenna pointing may be off"],
"conversion":{"from":"OEM","fit":{"rms_km":0.55,"max_km":1.28,"max_at":"2026-11-13T23:56:00Z",
"start":"2026-11-11T12:00:00Z","end":"2026-11-15T12:00:00Z","points":1441,
"ephemeris_start":"2026-11-11T12:00:00Z","ephemeris_end":"2026-11-15T12:00:00Z"}}}`

// orbitServer answers the orbit-data endpoints and records what the CLI sent.
type orbitServer struct {
	*httptest.Server
	paths  []string
	bodies []map[string]any
}

func newOrbitServer(t *testing.T, status int, body string) *orbitServer {
	t.Helper()
	s := &orbitServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var decoded map[string]any
		_ = json.Unmarshal(raw, &decoded)
		s.paths = append(s.paths, r.URL.Path)
		s.bodies = append(s.bodies, decoded)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(s.Close)
	return s
}

func runAddOrbitData(t *testing.T, srv *orbitServer, args ...string) (string, error) {
	t.Helper()
	tokSrv := setupAuthForTest(t)
	t.Cleanup(tokSrv.Close)
	root := NewRootCommand()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetArgs(append([]string{
		"satellite", "add-orbit-data", "--api-url", srv.URL, "--satellite-id", "sat-1",
	}, args...))
	err := root.Execute()
	return buf.String(), err
}

func writeTemp(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestAddOrbitData_UploadsAnOEMAndShowsTheFit(t *testing.T) {
	srv := newOrbitServer(t, http.StatusCreated, fittedOrbitData)
	oem := "CCSDS_OEM_VERS = 2.0\nMETA_START\nOBJECT_ID = 1998-067A\nMETA_STOP\n"

	out, err := runAddOrbitData(t, srv, "--data-type", "oem", "--data-file", writeTemp(t, "iss.oem", oem))

	if err != nil {
		t.Fatalf("add-orbit-data OEM: %v", err)
	}
	if srv.paths[0] != "/v1/satellites/sat-1/orbit-data" {
		t.Fatalf("posted to %s", srv.paths[0])
	}
	if srv.bodies[0]["orbital_data_type"] != "OEM" {
		t.Fatalf("sent type %v", srv.bodies[0]["orbital_data_type"])
	}
	if data := srv.bodies[0]["orbital_data"].(map[string]any); data["oem"] != oem {
		t.Fatalf("sent orbital_data %+v, want the OEM text whole", data)
	}
	if !strings.Contains(out, "OEM (fit 0.55 km RMS, 1.28 km max)") {
		t.Errorf("output does not show the conversion:\n%s", out)
	}
}

func TestAddOrbitData_DryRunShowsTheTLEWithoutStoring(t *testing.T) {
	srv := newOrbitServer(t, http.StatusOK, `{"valid":true,"warnings":[],"errors":[],
"tle":{"line1":"1 25544U 98067A   26316.88411739  .00007667  00000+0  14541-3 0  9992",
"line2":"2 25544  51.6325 284.2547 0007335  19.0494 341.0767 15.49339933594804"},
"conversion":{"from":"OMM"}}`)

	out, err := runAddOrbitData(t, srv, "--data-type", "OMM", "--dry-run",
		"--data", "CCSDS_OMM_VERS = 2.0\nEPOCH = 2026-11-12T21:13:07.742496\n")

	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if len(srv.paths) != 1 || srv.paths[0] != "/v1/satellites/sat-1/orbit-data/validate" {
		t.Fatalf("called %v, want only the validate endpoint", srv.paths)
	}
	for _, want := range []string{"1 25544U 98067A   26316.88411739", "OMM", "true"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestAddOrbitData_DryRunOfRejectedDataFails(t *testing.T) {
	srv := newOrbitServer(t, http.StatusOK, `{"valid":false,"warnings":[],
"errors":[{"location":"orbital_data.semi_major_axis_km","message":"perigee radius 550.0 km is inside the Earth"}]}`)

	_, err := runAddOrbitData(t, srv, "--data-type", "KEPLERIAN", "--dry-run", "--data", `{"semi_major_axis_km": 550}`)

	if err == nil || !strings.Contains(err.Error(), "rejected; nothing was stored") {
		t.Fatalf("err = %v, want a rejection", err)
	}
}

func TestAddOrbitData_RejectedUploadIsReportedAsFindings(t *testing.T) {
	srv := newOrbitServer(t, http.StatusUnprocessableEntity, `{"valid":false,"warnings":[],
"errors":[{"location":"orbital_data.oem","message":"this is an OMM (mean elements), not an OEM; upload it as OMM"}]}`)

	_, err := runAddOrbitData(t, srv, "--data-type", "OEM", "--data", "CCSDS_OMM_VERS = 2.0\n")

	if err == nil || err.Error() != "the orbit data was rejected; nothing was stored" {
		t.Fatalf("err = %v", err)
	}
}

func TestAddOrbitData_RejectsAnUnknownDataType(t *testing.T) {
	srv := newOrbitServer(t, http.StatusCreated, fittedOrbitData)

	_, err := runAddOrbitData(t, srv, "--data-type", "SP3", "--data", "x")

	if err == nil || !strings.Contains(err.Error(), "use TLE, OMM, KEPLERIAN, OEM") {
		t.Fatalf("err = %v", err)
	}
	if len(srv.paths) != 0 {
		t.Fatal("nothing should be sent")
	}
}

func TestOrbitRejectionOnlyReadsValidationBodies(t *testing.T) {
	findings := `{"valid":false,"errors":[{"location":"orbital_data","message":"bad"}]}`
	if r, ok := orbitRejection(&apiclient.APIError{StatusCode: 422, RawBody: findings}); !ok || r.Errors[0].Message != "bad" {
		t.Fatalf("422 with findings: %+v %v", r, ok)
	}
	for _, err := range []error{
		nil,
		io.EOF,
		&apiclient.APIError{StatusCode: 409, RawBody: findings},
		&apiclient.APIError{StatusCode: 422, RawBody: `{"error":"x"}`},
		&apiclient.APIError{StatusCode: 422, RawBody: `not json`},
	} {
		if _, ok := orbitRejection(err); ok {
			t.Errorf("orbitRejection(%v) = ok", err)
		}
	}
}

func TestConversionSummary(t *testing.T) {
	if got := conversionSummary(nil); got != "" {
		t.Errorf("TLE: %q", got)
	}
	if got := conversionSummary(&apiclient.OrbitConversion{From: "OMM"}); got != "OMM" {
		t.Errorf("OMM: %q", got)
	}
	got := conversionSummary(&apiclient.OrbitConversion{From: "KEPLERIAN", Fit: &apiclient.OrbitFitQuality{RmsKm: 0.123, MaxKm: 0.456}})
	if got != "KEPLERIAN (fit 0.12 km RMS, 0.46 km max)" {
		t.Errorf("KEPLERIAN: %q", got)
	}
}

func runStellar(t *testing.T, srv *orbitServer, args ...string) (string, error) {
	t.Helper()
	tokSrv := setupAuthForTest(t)
	t.Cleanup(tokSrv.Close)
	root := NewRootCommand()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetArgs(append(args, "--api-url", srv.URL))
	err := root.Execute()
	return buf.String(), err
}

func TestGetOrbitDataUpload_PrintsTheDocumentAsUploaded(t *testing.T) {
	oem := "CCSDS_OEM_VERS = 2.0\nMETA_START\nCENTER_NAME = EARTH\nMETA_STOP\n"
	body, err := json.Marshal(map[string]any{
		"orbit_data_id": "od-1", "orbital_data_type": "OEM", "orbital_data": map[string]any{"oem": oem},
		"created_at": "2026-11-13T00:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := newOrbitServer(t, http.StatusOK, string(body))

	out, err := runStellar(t, srv, "satellite", "get-orbit-data-upload", "--satellite-id", "sat-1", "--orbit-data-id", "od-1")
	if err != nil {
		t.Fatalf("get-orbit-data-upload: %v", err)
	}
	if out != oem {
		t.Fatalf("printed %q, want the OEM exactly as uploaded", out)
	}
	if srv.paths[0] != "/v1/satellites/sat-1/orbit-data/od-1/upload" {
		t.Fatalf("called %s", srv.paths[0])
	}

	path := filepath.Join(t.TempDir(), "mysat.oem")
	if _, err := runStellar(t, srv, "satellite", "get-orbit-data-upload", "--satellite-id", "sat-1",
		"--orbit-data-id", "od-1", "--output-file", path); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(path); string(got) != oem {
		t.Fatalf("wrote %q", got)
	}
}

func TestGetOrbitDataUpload_PrintsElementsAsJSON(t *testing.T) {
	srv := newOrbitServer(t, http.StatusOK, `{"orbit_data_id":"od-2","orbital_data_type":"KEPLERIAN",
"orbital_data":{"semi_major_axis_km":6878.137,"frame":"EME2000"}}`)

	out, err := runStellar(t, srv, "satellite", "get-orbit-data-upload", "--satellite-id", "sat-1", "--orbit-data-id", "od-2")
	if err != nil {
		t.Fatal(err)
	}
	if out != "{\n  \"frame\": \"EME2000\",\n  \"semi_major_axis_km\": 6878.137\n}\n" {
		t.Fatalf("printed %q", out)
	}
}

func TestGetOrbitDataUpload_ExplainsAVersionWithoutOne(t *testing.T) {
	srv := newOrbitServer(t, http.StatusNotFound, `{"error":"no upload is kept for this orbit data"}`)

	_, err := runStellar(t, srv, "satellite", "get-orbit-data-upload", "--satellite-id", "sat-1", "--orbit-data-id", "od-3")
	if err == nil || !strings.Contains(err.Error(), "uploaded as a TLE, synced automatically") {
		t.Fatalf("err = %v", err)
	}
}

func TestOrbitHistoryShowsConversionsAndKeptUploads(t *testing.T) {
	srv := newOrbitServer(t, http.StatusOK, `{"items":[
{"id":"od-1","orbital_data_type":"TLE","has_upload":true,"conversion":{"from":"OEM","fit":{"rms_km":0.55,"max_km":1.28}}},
{"id":"od-0","orbital_data_type":"TLE"}]}`)

	out, err := runStellar(t, srv, "satellite", "get-orbit-data-history", "--satellite-id", "sat-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"CONVERTED_FROM", "UPLOAD_KEPT", "OEM (fit 0.55 km RMS, 1.28 km max)", "true", "false"} {
		if !strings.Contains(out, want) {
			t.Errorf("history output missing %q:\n%s", want, out)
		}
	}
}
