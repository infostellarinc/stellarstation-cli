package apiclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateOrbitData(t *testing.T) {
	want := OrbitData{SatelliteID: "sat-1", OrbitalDataType: "TLE"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/v1/satellites/sat-1/orbit-data", r.URL.Path)
		_ = json.NewEncoder(w).Encode(want)
	}))
	defer srv.Close()
	c := New(srv.URL, staticToken())
	got, err := c.CreateOrbitData(context.Background(), "sat-1", OrbitDataCreateRequest{OrbitalDataType: "TLE"})
	require.NoError(t, err)
	require.Equal(t, want.SatelliteID, got.SatelliteID)
}

func TestGetOrbitParameters(t *testing.T) {
	want := OrbitParameters{SatelliteID: "sat-1"}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/v1/satellites/sat-1/orbit-parameters", r.URL.Path)
		_ = json.NewEncoder(w).Encode(want)
	}))
	defer srv.Close()
	c := New(srv.URL, staticToken())
	got, err := c.GetOrbitParameters(context.Background(), "sat-1")
	require.NoError(t, err)
	require.Equal(t, want.SatelliteID, got.SatelliteID)
}

func TestListConfigurations_MissingSatelliteID(t *testing.T) {
	c := New("http://localhost", staticToken())
	_, err := c.ListConfigurations(context.Background(), "", "gs-1")
	require.Error(t, err)
	require.Contains(t, err.Error(), "satelliteID")
}

func TestListConfigurations_MissingGroundStationID(t *testing.T) {
	c := New("http://localhost", staticToken())
	_, err := c.ListConfigurations(context.Background(), "sat-1", "")
	require.Error(t, err)
	require.Contains(t, err.Error(), "groundStationID")
}

func TestListConfigurations_Success(t *testing.T) {
	want := []ExecutionConfig{{ID: "cfg-1"}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Contains(t, r.URL.Path, "/v1/satellites/sat-1/configurations")
		assert.Equal(t, "gs-1", r.URL.Query().Get("groundStationId"))
		_ = json.NewEncoder(w).Encode(ListConfigurationsResponse{Configurations: want})
	}))
	defer srv.Close()
	c := New(srv.URL, staticToken())
	got, err := c.ListConfigurations(context.Background(), "sat-1", "gs-1")
	require.NoError(t, err)
	require.Len(t, got, 1)
	require.Equal(t, "cfg-1", got[0].ID)
}

func TestAPIError_IsNotFound(t *testing.T) {
	e := &APIError{StatusCode: http.StatusNotFound}
	require.True(t, e.IsNotFound())
	e2 := &APIError{StatusCode: http.StatusOK}
	require.False(t, e2.IsNotFound())
}

func TestAPIError_Error_WithMessage(t *testing.T) {
	e := &APIError{StatusCode: 400, Method: "GET", Path: "/test", Message: "bad request"}
	require.Contains(t, e.Error(), "bad request")
}

func TestAPIError_Error_NotFound(t *testing.T) {
	e := &APIError{StatusCode: http.StatusNotFound, Method: "GET", Path: "/test"}
	require.Contains(t, e.Error(), "404")
}

func TestAPIError_Error_Unauthorized(t *testing.T) {
	e := &APIError{StatusCode: http.StatusUnauthorized, Method: "GET", Path: "/v1/passes"}
	s := e.Error()
	require.Contains(t, s, "401")
}

func TestAPIError_Error_ForbiddenPasses(t *testing.T) {
	e := &APIError{StatusCode: http.StatusForbidden, Method: "GET", Path: "/v1/passes"}
	s := e.Error()
	require.Contains(t, s, "403")
	require.Contains(t, s, "ground station")
}

func TestAPIError_Error_ForbiddenOther(t *testing.T) {
	e := &APIError{StatusCode: http.StatusForbidden, Method: "POST", Path: "/v1/other", RawBody: "body text"}
	s := e.Error()
	require.Contains(t, s, "403")
	require.Contains(t, s, "body text")
}

func TestDoJSON_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"internal error"}`))
	}))
	defer srv.Close()
	c := New(srv.URL, nil)
	_, err := c.ListPasses(context.Background(), ListPassesOpts{})
	require.Error(t, err)
	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	require.Equal(t, http.StatusInternalServerError, apiErr.StatusCode)
}

func TestDoJSON_HTTPRequestFails(t *testing.T) {
	// Use a closed server to force the HTTP request to fail
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close()
	c := New(srv.URL, nil)
	_, err := c.ListPasses(context.Background(), ListPassesOpts{})
	require.Error(t, err)
}

func TestListPasses_WithStopTime(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.NotEmpty(t, r.URL.Query().Get("stop"))
		_ = json.NewEncoder(w).Encode(ListPassesResponse{})
	}))
	defer srv.Close()
	c := New(srv.URL, staticToken())
	stop := time.Now()
	_, err := c.ListPasses(context.Background(), ListPassesOpts{Stop: &stop})
	require.NoError(t, err)
}

func TestListPasses_WithAllFilters(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		// Multiple satellite IDs are sent as a single comma-separated value.
		assert.Equal(t, "sat-1,sat-2", q.Get("satellite_ids"))
		assert.NotEmpty(t, q.Get("ground_station_ids"))
		assert.NotEmpty(t, q.Get("execution_status"))
		_ = json.NewEncoder(w).Encode(ListPassesResponse{})
	}))
	defer srv.Close()
	c := New(srv.URL, staticToken())
	start := time.Now()
	_, err := c.ListPasses(context.Background(), ListPassesOpts{
		SatelliteIDs:    []string{"sat-1", "sat-2"},
		GroundStationID: "gs-1",
		Start:           &start,
		ExecutionStatus: "PENDING",
	})
	require.NoError(t, err)
}

func errorServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
}

func TestCreatePass_Error(t *testing.T) {
	srv := errorServer(t)
	defer srv.Close()
	c := New(srv.URL, nil)
	_, err := c.CreatePass(context.Background(), PassCreateRequest{})
	require.Error(t, err)
}

func TestCancelPass_Error(t *testing.T) {
	srv := errorServer(t)
	defer srv.Close()
	c := New(srv.URL, nil)
	_, err := c.CancelPass(context.Background(), "p-1")
	require.Error(t, err)
}

func TestGetOrbitData_Error(t *testing.T) {
	srv := errorServer(t)
	defer srv.Close()
	c := New(srv.URL, nil)
	_, err := c.GetOrbitData(context.Background(), "sat-1")
	require.Error(t, err)
}

func TestCreateOrbitData_Error(t *testing.T) {
	srv := errorServer(t)
	defer srv.Close()
	c := New(srv.URL, nil)
	_, err := c.CreateOrbitData(context.Background(), "sat-1", OrbitDataCreateRequest{})
	require.Error(t, err)
}

func TestGetOrbitParameters_Error(t *testing.T) {
	srv := errorServer(t)
	defer srv.Close()
	c := New(srv.URL, nil)
	_, err := c.GetOrbitParameters(context.Background(), "sat-1")
	require.Error(t, err)
}

func TestUpdateOrbitParameters_Error(t *testing.T) {
	srv := errorServer(t)
	defer srv.Close()
	c := New(srv.URL, nil)
	_, err := c.UpdateOrbitParameters(context.Background(), "sat-1", OrbitParametersUpdateRequest{})
	require.Error(t, err)
}

func TestListVisibilities_Error(t *testing.T) {
	srv := errorServer(t)
	defer srv.Close()
	c := New(srv.URL, nil)
	_, err := c.ListVisibilities(context.Background(), ListVisibilitiesOpts{})
	require.Error(t, err)
}

func TestListVisibilities_WithFilters(t *testing.T) {
	start := time.Now().Truncate(time.Second)
	stop := start.Add(time.Hour)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		// Multiple satellite IDs are sent as a single comma-separated value.
		assert.Equal(t, "sat-1,sat-2", q.Get("satellite_ids"))
		assert.Equal(t, start.Format(time.RFC3339), q.Get("start"))
		assert.Equal(t, stop.Format(time.RFC3339), q.Get("stop"))
		_ = json.NewEncoder(w).Encode([]Visibility{})
	}))
	defer srv.Close()
	c := New(srv.URL, staticToken())
	_, err := c.ListVisibilities(context.Background(), ListVisibilitiesOpts{
		SatelliteIDs: []string{"sat-1", "sat-2"},
		Start:        &start,
		Stop:         &stop,
	})
	require.NoError(t, err)
}
