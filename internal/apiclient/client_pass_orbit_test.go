package apiclient

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// recordingServer answers one request with the given payload and records the
// request it received.
func recordingServer(t *testing.T, payload any) (*httptest.Server, *http.Request) {
	t.Helper()
	var seen http.Request
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assertBearer(t, r)
		seen = *r
		_ = json.NewEncoder(w).Encode(payload)
	}))
	t.Cleanup(server.Close)
	return server, &seen
}

func TestGetPass(t *testing.T) {
	server, seen := recordingServer(t, Pass{ID: "p-1"})

	pass, err := New(server.URL, staticToken()).GetPass(t.Context(), "p-1")

	if err != nil {
		t.Fatalf("GetPass: %v", err)
	}
	if pass.ID != "p-1" {
		t.Errorf("pass id = %q, want p-1", pass.ID)
	}
	if seen.URL.Path != "/v1/passes/p-1" {
		t.Errorf("path = %q", seen.URL.Path)
	}
	if seen.Method != http.MethodGet {
		t.Errorf("method = %q, want GET", seen.Method)
	}
}

func TestUpdatePass(t *testing.T) {
	server, seen := recordingServer(t, Pass{ID: "p-1"})
	configID := "config-1"

	pass, err := New(server.URL, staticToken()).UpdatePass(
		t.Context(), "p-1", UpdatePassRequest{ExecutionConfigID: &configID},
	)

	if err != nil {
		t.Fatalf("UpdatePass: %v", err)
	}
	if pass.ID != "p-1" {
		t.Errorf("pass id = %q, want p-1", pass.ID)
	}
	if seen.Method != http.MethodPut {
		t.Errorf("method = %q, want PUT", seen.Method)
	}
}

func TestListOrbitHistory(t *testing.T) {
	server, seen := recordingServer(t, ListOrbitDataResponse{Items: []OrbitHistoryItem{{ID: "o-1"}}})

	resp, err := New(server.URL, staticToken()).ListOrbitHistory(t.Context(), "sat-1", ListOrbitHistoryOpts{
		Limit: 10, Cursor: "cursor-1", Source: "CELESTRAK",
	})

	if err != nil {
		t.Fatalf("ListOrbitHistory: %v", err)
	}
	if len(resp.Items) != 1 || resp.Items[0].ID != "o-1" {
		t.Errorf("items = %+v", resp.Items)
	}
	query := seen.URL.Query()
	if query.Get("limit") != "10" || query.Get("cursor") != "cursor-1" || query.Get("source") != "CELESTRAK" {
		t.Errorf("query = %v", query)
	}
	if seen.URL.Path != "/v1/satellites/sat-1/orbit-data/history" {
		t.Errorf("path = %q", seen.URL.Path)
	}
}

// A failing request surfaces the API error rather than a partial result.
func TestPassRequestsReportAPIErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"pass not found"}`))
	}))
	t.Cleanup(server.Close)
	client := New(server.URL, staticToken())

	if _, err := client.GetPass(t.Context(), "missing"); err == nil {
		t.Error("want an error from GetPass")
	}
	if _, err := client.UpdatePass(t.Context(), "missing", UpdatePassRequest{}); err == nil {
		t.Error("want an error from UpdatePass")
	}
	if _, err := client.ListOrbitHistory(t.Context(), "missing", ListOrbitHistoryOpts{}); err == nil {
		t.Error("want an error from ListOrbitHistory")
	}
	if _, err := client.ListSatellites(t.Context()); err == nil {
		t.Error("want an error from ListSatellites")
	}
}
