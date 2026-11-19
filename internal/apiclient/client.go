// Package apiclient provides a lightweight HTTP client for the StellarStation
// REST API. Authentication uses a Cognito machine-client JWT carried in the
// Authorization: Bearer <jwt> header; the token is supplied by a
// [auth.TokenSource] so the caller can refresh it transparently.
package apiclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/infostellarinc/stellarstation-cli/internal/auth"
)

const defaultTimeout = 30 * time.Second

// Client is an HTTP client for the StellarStation REST API. It attaches an
// Authorization: Bearer <jwt> header to every outbound request using the
// configured [auth.TokenSource].
type Client struct {
	baseURL     string
	tokenSource auth.TokenSource
	httpClient  *http.Client
}

// New creates a new API client. baseURL is the API endpoint
// (e.g. "https://api.stellarstation.com") and tokenSource supplies the bearer
// token used on every request. A nil tokenSource is allowed for read-only
// smoke tests but will cause requests to fail server-side with 401.
func New(baseURL string, tokenSource auth.TokenSource) *Client {
	return &Client{
		baseURL:     strings.TrimRight(baseURL, "/"),
		tokenSource: tokenSource,
		httpClient: &http.Client{
			Timeout: defaultTimeout,
		},
	}
}

// doJSON performs an HTTP request and decodes the JSON response into out.
func (c *Client) doJSON(
	ctx context.Context, method, path string,
	query url.Values, body, out interface{},
) error {
	fullURL := c.baseURL + path
	if len(query) > 0 {
		fullURL += "?" + query.Encode()
	}

	var bodyReader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, fullURL, bodyReader)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	if c.tokenSource != nil {
		token, err := c.tokenSource.Token(ctx)
		if err != nil {
			return fmt.Errorf("obtain access token: %w", err)
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
	}
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errResp ErrorResponse
		_ = json.Unmarshal(respBody, &errResp)
		return &APIError{
			StatusCode: resp.StatusCode,
			Method:     method,
			Path:       path,
			Message:    errResp.Error,
			Detail:     errResp.Detail,
			RawBody:    string(respBody),
		}
	}

	if out != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("unmarshal response (status %d): %w", resp.StatusCode, err)
		}
	}
	return nil
}

// ---- Passes ----------------------------------------------------------------

// ListPasses calls GET /v1/passes.
func (c *Client) ListPasses(ctx context.Context, opts ListPassesOpts) ([]Pass, error) {
	q := url.Values{}
	if len(opts.SatelliteIDs) > 0 {
		q.Set("satellite_ids", strings.Join(opts.SatelliteIDs, ","))
	}
	if opts.GroundStationID != "" {
		q.Set("ground_station_ids", opts.GroundStationID)
	}
	if opts.Start != nil {
		q.Set("start", opts.Start.Format(time.RFC3339))
	}
	if opts.Stop != nil {
		q.Set("stop", opts.Stop.Format(time.RFC3339))
	}
	if opts.ExecutionStatus != "" {
		q.Set("execution_status", opts.ExecutionStatus)
	}

	var body ListPassesResponse
	if err := c.doJSON(ctx, http.MethodGet, "/v1/passes", q, nil, &body); err != nil {
		return nil, err
	}
	return body.Passes, nil
}

// CreatePass calls POST /v1/passes.
func (c *Client) CreatePass(ctx context.Context, req CreatePassRequest) (*Pass, error) {
	var pass Pass
	if err := c.doJSON(ctx, http.MethodPost, "/v1/passes", nil, req, &pass); err != nil {
		return nil, err
	}
	return &pass, nil
}

// CancelPass calls DELETE /v1/passes/{passId}.
func (c *Client) CancelPass(ctx context.Context, passID string) (*Pass, error) {
	var pass Pass
	if err := c.doJSON(ctx, http.MethodDelete, "/v1/passes/"+passID, nil, nil, &pass); err != nil {
		return nil, err
	}
	return &pass, nil
}

// GetPass calls GET /v1/passes/{passId}.
func (c *Client) GetPass(ctx context.Context, passID string) (*Pass, error) {
	var pass Pass
	if err := c.doJSON(ctx, http.MethodGet, "/v1/passes/"+passID, nil, nil, &pass); err != nil {
		return nil, err
	}
	return &pass, nil
}

// GetPassChannels returns the pass's channels, from GET /v1/passes/{passId}/view.
func (c *Client) GetPassChannels(ctx context.Context, passID string) ([]PassChannel, error) {
	var view struct {
		Channels []PassChannel `json:"channels"`
	}
	if err := c.doJSON(ctx, http.MethodGet, "/v1/passes/"+passID+"/view", nil, nil, &view); err != nil {
		return nil, err
	}
	return view.Channels, nil
}

// UpdatePass calls PUT /v1/passes/{passId}.
func (c *Client) UpdatePass(ctx context.Context, passID string, req UpdatePassRequest) (*Pass, error) {
	var pass Pass
	if err := c.doJSON(ctx, http.MethodPut, "/v1/passes/"+passID, nil, req, &pass); err != nil {
		return nil, err
	}
	return &pass, nil
}

// ---- Visibilities ----------------------------------------------------------

// ListVisibilities calls GET /v1/visibilities. It returns the visibility
// windows (pass opportunities) matching the supplied filters.
func (c *Client) ListVisibilities(ctx context.Context, opts ListVisibilitiesOpts) ([]Visibility, error) {
	q := url.Values{}
	if len(opts.SatelliteIDs) > 0 {
		q.Set("satellite_ids", strings.Join(opts.SatelliteIDs, ","))
	}
	if len(opts.GroundStationIDs) > 0 {
		q.Set("ground_station_ids", strings.Join(opts.GroundStationIDs, ","))
	}
	if opts.Start != nil {
		q.Set("start", opts.Start.Format(time.RFC3339))
	}
	if opts.Stop != nil {
		q.Set("stop", opts.Stop.Format(time.RFC3339))
	}
	if opts.MinMaxElevationDegrees != nil {
		q.Set("min_max_elevation_degrees", strconv.FormatFloat(*opts.MinMaxElevationDegrees, 'f', -1, 64))
	}

	var visibilities []Visibility
	if err := c.doJSON(ctx, http.MethodGet, "/v1/visibilities", q, nil, &visibilities); err != nil {
		return nil, err
	}
	return visibilities, nil
}

// ---- Satellites ------------------------------------------------------------

// ListSatellites calls GET /v1/satellites and returns the satellites the
// caller is authorized to read (first page).
func (c *Client) ListSatellites(ctx context.Context) ([]Satellite, error) {
	var resp ListSatellitesResponse
	if err := c.doJSON(ctx, http.MethodGet, "/v1/satellites", nil, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Satellites, nil
}

// ---- Orbit / TLE -----------------------------------------------------------

// GetOrbitData calls GET /v1/satellites/{id}/orbit-data/current.
func (c *Client) GetOrbitData(ctx context.Context, satelliteID string) (*OrbitData, error) {
	var data OrbitData
	path := "/v1/satellites/" + satelliteID + "/orbit-data/current"
	if err := c.doJSON(ctx, http.MethodGet, path, nil, nil, &data); err != nil {
		return nil, err
	}
	return &data, nil
}

// CreateOrbitData calls POST /v1/satellites/{id}/orbit-data.
func (c *Client) CreateOrbitData(
	ctx context.Context,
	satelliteID string,
	req CreateOrbitDataRequest,
) (*OrbitData, error) {
	var data OrbitData
	path := "/v1/satellites/" + satelliteID + "/orbit-data"
	if err := c.doJSON(ctx, http.MethodPost, path, nil, req, &data); err != nil {
		return nil, err
	}
	return &data, nil
}

// ValidateOrbitData calls POST /v1/satellites/{id}/orbit-data/validate, which
// checks and converts orbit data without storing it. A rejected upload is a
// successful call whose result has Valid false.
func (c *Client) ValidateOrbitData(
	ctx context.Context,
	satelliteID string,
	req ValidateOrbitDataRequest,
) (*OrbitValidationResult, error) {
	var result OrbitValidationResult
	path := "/v1/satellites/" + satelliteID + "/orbit-data/validate"
	if err := c.doJSON(ctx, http.MethodPost, path, nil, req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetOrbitDataUpload calls GET /v1/satellites/{id}/orbit-data/{orbitDataId}/upload,
// the OMM, Keplerian elements or OEM an orbit-data version was converted from.
func (c *Client) GetOrbitDataUpload(ctx context.Context, satelliteID, orbitDataID string) (*OrbitDataUpload, error) {
	var upload OrbitDataUpload
	path := "/v1/satellites/" + satelliteID + "/orbit-data/" + orbitDataID + "/upload"
	if err := c.doJSON(ctx, http.MethodGet, path, nil, nil, &upload); err != nil {
		return nil, err
	}
	return &upload, nil
}

// ListOrbitHistory calls GET /v1/satellites/{id}/orbit-data/history and returns
// the orbit-data activation history (newest first).
func (c *Client) ListOrbitHistory(
	ctx context.Context,
	satelliteID string,
	opts ListOrbitHistoryOpts,
) (*ListOrbitDataResponse, error) {
	q := url.Values{}
	if opts.Limit > 0 {
		q.Set("limit", strconv.Itoa(opts.Limit))
	}
	if opts.Cursor != "" {
		q.Set("cursor", opts.Cursor)
	}
	if opts.Source != "" {
		q.Set("source", opts.Source)
	}

	var resp ListOrbitDataResponse
	path := "/v1/satellites/" + satelliteID + "/orbit-data/history"
	if err := c.doJSON(ctx, http.MethodGet, path, q, nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// GetOrbitParameters calls GET /v1/satellites/{id}/orbit-parameters.
func (c *Client) GetOrbitParameters(ctx context.Context, satelliteID string) (*OrbitParameters, error) {
	var params OrbitParameters
	path := "/v1/satellites/" + satelliteID + "/orbit-parameters"
	if err := c.doJSON(ctx, http.MethodGet, path, nil, nil, &params); err != nil {
		return nil, err
	}
	return &params, nil
}

// UpdateOrbitParameters calls PATCH /v1/satellites/{id}/orbit-parameters.
func (c *Client) UpdateOrbitParameters(
	ctx context.Context,
	satelliteID string,
	req UpdateOrbitParametersRequest,
) (*OrbitParameters, error) {
	var params OrbitParameters
	path := "/v1/satellites/" + satelliteID + "/orbit-parameters"
	if err := c.doJSON(ctx, http.MethodPatch, path, nil, req, &params); err != nil {
		return nil, err
	}
	return &params, nil
}

// ---- Execution configurations ----------------------------------------------

// ListConfigurations calls GET /v1/satellites/{id}/configurations filtered by
// ground station. Only schedulable configurations are returned.
func (c *Client) ListConfigurations(
	ctx context.Context,
	satelliteID, groundStationID string,
) ([]ExecutionConfig, error) {
	if strings.TrimSpace(satelliteID) == "" {
		return nil, errors.New("satelliteID is required")
	}
	if strings.TrimSpace(groundStationID) == "" {
		return nil, errors.New("groundStationID is required")
	}

	q := url.Values{}
	q.Set("groundStationId", groundStationID)
	var resp ListConfigurationsResponse
	path := "/v1/satellites/" + satelliteID + "/configurations"
	if err := c.doJSON(ctx, http.MethodGet, path, q, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Configurations, nil
}

// ListTrackingSettings calls GET /v1/satellites/{id}/tracking-settings.
func (c *Client) ListTrackingSettings(ctx context.Context, satelliteID string) ([]TrackingSetting, error) {
	var resp ListTrackingSettingsResponse
	path := "/v1/satellites/" + satelliteID + "/tracking-settings"
	if err := c.doJSON(ctx, http.MethodGet, path, nil, nil, &resp); err != nil {
		return nil, err
	}
	return resp.Settings, nil
}

// UpdateTrackingSetting calls PUT /v1/satellites/{id}/ground-stations/{gs}/tracking-settings.
func (c *Client) UpdateTrackingSetting(
	ctx context.Context,
	satelliteID, groundStationID, mode string,
) (*TrackingSetting, error) {
	var setting TrackingSetting
	path := "/v1/satellites/" + satelliteID + "/ground-stations/" + groundStationID + "/tracking-settings"
	body := UpdateTrackingSettingRequest{DefaultMode: mode}
	if err := c.doJSON(ctx, http.MethodPut, path, nil, body, &setting); err != nil {
		return nil, err
	}
	return &setting, nil
}
