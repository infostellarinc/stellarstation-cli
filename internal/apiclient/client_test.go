package apiclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/infostellarinc/stellarstation-cli/internal/auth"
)

const testToken = "jwt-test-token"

func staticToken() auth.TokenSource {
	return auth.StaticTokenSource{Value: testToken}
}

func assertBearer(t *testing.T, r *http.Request) {
	t.Helper()
	got := r.Header.Get("Authorization")
	want := "Bearer " + testToken
	if got != want {
		t.Errorf("Authorization = %q, want %q", got, want)
	}
}

func TestListPasses(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	want := []Pass{{ID: "p-1", Satellite: SatelliteRef{ID: "sat-1", Name: "SAT"}}}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		if r.URL.Path != "/v1/passes" {
			t.Errorf("path = %s, want /v1/passes", r.URL.Path)
		}
		assertBearer(t, r)
		if r.URL.Query().Get("satellite_ids") != "sat-1" {
			t.Errorf("satellite_ids = %q, want sat-1", r.URL.Query().Get("satellite_ids"))
		}
		_ = json.NewEncoder(w).Encode(ListPassesResponse{Passes: want})
	}))
	defer srv.Close()

	c := New(srv.URL, staticToken())
	got, err := c.ListPasses(context.Background(), ListPassesOpts{
		SatelliteIDs: []string{"sat-1"},
		Start:        &now,
	})
	if err != nil {
		t.Fatalf("ListPasses: %v", err)
	}
	if len(got) != 1 || got[0].ID != "p-1" {
		t.Errorf("ListPasses = %+v, want 1 pass with id p-1", got)
	}
}

func TestNoAuthorizationHeaderWhenTokenSourceNil(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("Authorization header should be absent, got %q", got)
		}
		_ = json.NewEncoder(w).Encode(ListPassesResponse{Passes: []Pass{}})
	}))
	defer srv.Close()

	c := New(srv.URL, nil)
	if _, err := c.ListPasses(context.Background(), ListPassesOpts{}); err != nil {
		t.Fatalf("ListPasses: %v", err)
	}
}

func TestTokenSourceErrorIsPropagated(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		t.Fatal("server should not be hit when token source errors")
	}))
	defer srv.Close()

	c := New(srv.URL, failingTokenSource{})
	if _, err := c.ListPasses(context.Background(), ListPassesOpts{}); err == nil {
		t.Fatal("expected error from token source")
	}
}

type failingTokenSource struct{}

func (failingTokenSource) Token(_ context.Context) (string, error) {
	return "", errors.New("cannot mint token")
}

func TestCreatePass(t *testing.T) {
	want := Pass{ID: "p-new"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		assertBearer(t, r)
		var req PassCreateRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.SatelliteID != "sat-1" {
			t.Errorf("SatelliteID = %q, want sat-1", req.SatelliteID)
		}
		_ = json.NewEncoder(w).Encode(want)
	}))
	defer srv.Close()

	c := New(srv.URL, staticToken())
	now := time.Now().Truncate(time.Second)
	got, err := c.CreatePass(context.Background(), PassCreateRequest{
		SatelliteID: "sat-1",
		Booking:     Interval{Start: now, Stop: now.Add(time.Hour)},
		Scheduled:   Interval{Start: now, Stop: now.Add(time.Hour)},
	})
	if err != nil {
		t.Fatalf("CreatePass: %v", err)
	}
	if got.ID != "p-new" {
		t.Errorf("ID = %q, want p-new", got.ID)
	}
}

func TestCancelPass(t *testing.T) {
	want := Pass{ID: "p-del"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("method = %s, want DELETE", r.Method)
		}
		if r.URL.Path != "/v1/passes/p-del" {
			t.Errorf("path = %q, want /v1/passes/p-del", r.URL.Path)
		}
		assertBearer(t, r)
		_ = json.NewEncoder(w).Encode(want)
	}))
	defer srv.Close()

	c := New(srv.URL, staticToken())
	got, err := c.CancelPass(context.Background(), "p-del")
	if err != nil {
		t.Fatalf("CancelPass: %v", err)
	}
	if got.ID != "p-del" {
		t.Errorf("ID = %q, want p-del", got.ID)
	}
}

func TestListVisibilities(t *testing.T) {
	want := []Visibility{{
		Satellite:     SatelliteRef{ID: "sat-1", Name: "MySat"},
		GroundStation: GroundStationRef{ID: "gs-1", Name: "MyGS"},
	}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/visibilities" {
			t.Errorf("path = %q, want /v1/visibilities", r.URL.Path)
		}
		if got := r.URL.Query().Get("satellite_ids"); got != "sat-1" {
			t.Errorf("satellite_ids = %q, want sat-1", got)
		}
		assertBearer(t, r)
		_ = json.NewEncoder(w).Encode(want)
	}))
	defer srv.Close()

	c := New(srv.URL, staticToken())
	got, err := c.ListVisibilities(context.Background(), ListVisibilitiesOpts{SatelliteIDs: []string{"sat-1"}})
	if err != nil {
		t.Fatalf("ListVisibilities: %v", err)
	}
	if len(got) != 1 || got[0].Satellite.ID != "sat-1" {
		t.Errorf("visibilities = %+v, want 1 for sat-1", got)
	}
}

func TestListSatellites(t *testing.T) {
	want := ListSatellitesResponse{Satellites: []Satellite{
		{ID: "sat-1", DisplayName: "MySat", OrganizationID: "org-1", Schedulable: true},
	}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/satellites" {
			t.Errorf("path = %q, want /v1/satellites", r.URL.Path)
		}
		assertBearer(t, r)
		_ = json.NewEncoder(w).Encode(want)
	}))
	defer srv.Close()

	c := New(srv.URL, staticToken())
	got, err := c.ListSatellites(context.Background())
	if err != nil {
		t.Fatalf("ListSatellites: %v", err)
	}
	if len(got) != 1 || got[0].ID != "sat-1" || !got[0].Schedulable {
		t.Errorf("satellites = %+v, want 1 schedulable sat-1", got)
	}
}

func TestGetOrbitData(t *testing.T) {
	want := OrbitData{ID: "od-1", SatelliteID: "sat-1"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/satellites/sat-1/orbit-data/current" {
			t.Errorf("path = %q", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(want)
	}))
	defer srv.Close()

	c := New(srv.URL, staticToken())
	got, err := c.GetOrbitData(context.Background(), "sat-1")
	if err != nil {
		t.Fatalf("GetOrbitData: %v", err)
	}
	if got.ID != "od-1" {
		t.Errorf("ID = %q, want od-1", got.ID)
	}
}

func TestAPIErrorParsing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_ = json.NewEncoder(w).Encode(ErrorResponse{Error: "forbidden", Detail: "no access"})
	}))
	defer srv.Close()

	c := New(srv.URL, staticToken())
	_, err := c.ListPasses(context.Background(), ListPassesOpts{})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	apiErr := &APIError{}
	ok := errors.As(err, &apiErr)
	if !ok {
		t.Fatalf("expected *APIError, got %T", err)
	}
	if apiErr.StatusCode != http.StatusForbidden {
		t.Errorf("StatusCode = %d, want 403", apiErr.StatusCode)
	}
	if !apiErr.IsUnauthorized() {
		t.Error("expected IsUnauthorized() = true")
	}
	if apiErr.Message != "forbidden" {
		t.Errorf("Message = %q, want forbidden", apiErr.Message)
	}
}

func TestAPIErrorForbiddenIncludesCasbinHint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("Forbidden\n"))
	}))
	defer srv.Close()

	c := New(srv.URL, staticToken())
	_, err := c.ListPasses(context.Background(), ListPassesOpts{})
	if err == nil {
		t.Fatal("expected error")
	}
	apiErr := &APIError{}
	ok := errors.As(err, &apiErr)
	if !ok {
		t.Fatalf("expected *APIError, got %T", err)
	}
	msg := apiErr.Error()
	if !strings.Contains(msg, "ground station") {
		t.Errorf("expected ground station hint: %q", msg)
	}
	if !strings.Contains(msg, "Forbidden") {
		t.Errorf("expected raw body in message: %q", msg)
	}
}

func TestAPIErrorUnauthorizedEmptyBodyIncludesHint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()

	c := New(srv.URL, staticToken())
	_, err := c.ListPasses(context.Background(), ListPassesOpts{})
	if err == nil {
		t.Fatal("expected error")
	}
	apiErr := &APIError{}
	ok := errors.As(err, &apiErr)
	if !ok {
		t.Fatalf("expected *APIError, got %T", err)
	}
	if !strings.Contains(apiErr.Error(), "API key was not accepted") {
		t.Errorf("error should explain the key was rejected: %q", apiErr.Error())
	}
}

func TestUpdateOrbitParameters(t *testing.T) {
	want := OrbitParameters{ID: "op-1", OrbitalDataSource: "manual"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch {
			t.Errorf("method = %s, want PATCH", r.Method)
		}
		if r.URL.Path != "/v1/satellites/sat-1/orbit-parameters" {
			t.Errorf("path = %q", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(want)
	}))
	defer srv.Close()

	c := New(srv.URL, staticToken())
	got, err := c.UpdateOrbitParameters(context.Background(), "sat-1",
		OrbitParametersUpdateRequest{OrbitalDataSource: "manual"})
	if err != nil {
		t.Fatalf("UpdateOrbitParameters: %v", err)
	}
	if got.OrbitalDataSource != "manual" {
		t.Errorf("source = %q, want manual", got.OrbitalDataSource)
	}
}
