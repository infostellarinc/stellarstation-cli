package apiclient

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

// ---- Common ----------------------------------------------------------------

// Interval is a start/stop time range.
type Interval struct {
	Start time.Time `json:"start"`
	Stop  time.Time `json:"stop"`
}

// ---- Passes ----------------------------------------------------------------

// SatelliteRef is a reference to a satellite.
type SatelliteRef struct {
	ID           string `json:"id,omitempty"`
	Name         string `json:"name,omitempty"`
	Organization string `json:"organization,omitempty"`
}

// GroundStationRef is a reference to a ground station.
type GroundStationRef struct {
	ID           string   `json:"id,omitempty"`
	Name         string   `json:"name,omitempty"`
	Organization string   `json:"organization,omitempty"`
	Latitude     *float64 `json:"latitude,omitempty"`
	Longitude    *float64 `json:"longitude,omitempty"`
}

// Execution contains execution status and associated files.
type Execution struct {
	Status string   `json:"status"`
	Files  []string `json:"files"`
}

// ScheduledDetails contains scheduling time range.
type ScheduledDetails struct {
	Start time.Time `json:"start"`
	Stop  time.Time `json:"stop"`
}

// PassConflict describes a scheduling conflict (a resource on the SAME ground
// station whose window overlaps this pass).
type PassConflict struct {
	Kind  string    `json:"kind"`
	ID    string    `json:"id"`
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

// OverlappingPass links a pass to another pass for the same satellite whose
// window overlaps it on a DIFFERENT ground station. These overlaps are
// permitted (not conflicts); the link is informational.
type OverlappingPass struct {
	PassID            string    `json:"pass_id"`
	GroundStationID   string    `json:"ground_station_id"`
	GroundStationName string    `json:"ground_station_name,omitempty"`
	Start             time.Time `json:"start"`
	End               time.Time `json:"end"`
}

// Pass is the JSON representation of a satellite pass.
type Pass struct {
	ID                  string            `json:"id"`
	Satellite           SatelliteRef      `json:"satellite"`
	GroundStation       GroundStationRef  `json:"ground_station"`
	ExecutionConfigID   string            `json:"execution_config_id"`
	ExecutionConfigName string            `json:"execution_config_name"`
	ChannelID           string            `json:"channel_id,omitempty"`
	Visibility          *Interval         `json:"visibility,omitempty"`
	Scheduled           *ScheduledDetails `json:"scheduled,omitempty"`
	Booking             *Interval         `json:"booking,omitempty"`
	Execution           *Execution        `json:"execution,omitempty"`
	Conflicts           []PassConflict    `json:"conflicts,omitempty"`
	ConflictStatus      string            `json:"conflict_status,omitempty"`
	OverlappingPasses   []OverlappingPass `json:"overlapping_passes,omitempty"`
	MaxElevationDegrees float64           `json:"max_elevation_degrees,omitempty"`
	MaxElevationTime    *time.Time        `json:"max_elevation_time,omitempty"`
}

// PassCreateRequest is the body for POST /v1/passes.
type PassCreateRequest struct {
	SatelliteID       string   `json:"satellite_id"`
	GroundStationID   string   `json:"ground_station_id"`
	ExecutionConfigID string   `json:"execution_config_id"`
	Booking           Interval `json:"booking"`
	Scheduled         Interval `json:"scheduled"`
}

// PassUpdateRequest is the body for PUT /v1/passes/{passId}. Fields are
// pointers so only the ones set are sent (a partial update / reschedule).
type PassUpdateRequest struct {
	ExecutionConfigID *string   `json:"execution_config_id,omitempty"`
	Scheduled         *Interval `json:"scheduled,omitempty"`
}

// ListPassesResponse is the response from GET /v1/passes.
type ListPassesResponse struct {
	Passes        []Pass `json:"passes"`
	NextPageToken string `json:"nextPageToken,omitempty"`
}

// ListPassesOpts are query parameters for ListPasses.
type ListPassesOpts struct {
	SatelliteIDs    []string
	GroundStationID string
	Start           *time.Time
	Stop            *time.Time
	ExecutionStatus string
}

// ---- Visibilities ----------------------------------------------------------

// Visibility is the JSON representation of a satellite visibility window
// (an opportunity for a pass), as returned by GET /v1/visibilities.
type Visibility struct {
	Satellite           SatelliteRef     `json:"satellite"`
	GroundStation       GroundStationRef `json:"ground_station"`
	Visibility          *Interval        `json:"visibility,omitempty"`
	Availability        []Interval       `json:"availability,omitempty"`
	MaxElevationDegrees float64          `json:"max_elevation_degrees,omitempty"`
	MaxElevationTime    *time.Time       `json:"max_elevation_time,omitempty"`
	Conflicts           []PassConflict   `json:"conflicts,omitempty"`
	ConflictStatus      string           `json:"conflict_status,omitempty"`
}

// ListVisibilitiesOpts are query parameters for ListVisibilities.
type ListVisibilitiesOpts struct {
	SatelliteIDs []string
	Start        *time.Time
	Stop         *time.Time
}

// ---- Satellites ------------------------------------------------------------

// Satellite is the JSON representation of a satellite, as returned by
// GET /v1/satellites.
type Satellite struct {
	ID             string     `json:"id"`
	DisplayName    string     `json:"displayName"`
	OrganizationID string     `json:"organizationId"`
	Schedulable    bool       `json:"schedulable"`
	CreatedAt      *time.Time `json:"createdAt,omitempty"`
	UpdatedAt      *time.Time `json:"updatedAt,omitempty"`
	UpdatedBy      string     `json:"updatedBy,omitempty"`
}

// ListSatellitesResponse is the response from GET /v1/satellites.
type ListSatellitesResponse struct {
	Satellites    []Satellite `json:"satellites"`
	NextPageToken string      `json:"nextPageToken,omitempty"`
}

// ---- Orbit / TLE -----------------------------------------------------------

// OrbitData is the JSON representation of orbit data (TLE).
type OrbitData struct {
	ID              string                 `json:"id,omitempty"`
	SatelliteID     string                 `json:"satellite_id,omitempty"`
	OrbitalDataType string                 `json:"orbital_data_type,omitempty"`
	OrbitalData     map[string]interface{} `json:"orbital_data,omitempty"`
	Source          string                 `json:"source,omitempty"`
	Epoch           *time.Time             `json:"epoch,omitempty"`
	CreatedAt       *time.Time             `json:"created_at,omitempty"`
	UpdatedAt       *time.Time             `json:"updated_at,omitempty"`
}

// OrbitDataCreateRequest is the body for POST /v1/satellites/{id}/orbit-data.
type OrbitDataCreateRequest struct {
	OrbitalDataType string                 `json:"orbital_data_type"`
	OrbitalData     map[string]interface{} `json:"orbital_data"`
	Source          string                 `json:"source"`
	Epoch           time.Time              `json:"epoch"`
}

// OrbitParameters is the JSON representation of orbit parameters.
type OrbitParameters struct {
	ID                string `json:"id,omitempty"`
	SatelliteID       string `json:"satellite_id,omitempty"`
	OrbitalDataSource string `json:"orbital_data_source,omitempty"`
	NoradID           string `json:"norad_id,omitempty"`
	OrbitType         string `json:"orbit_type,omitempty"`
	// OrbitParameters_ uses a trailing underscore to avoid collision with the
	// enclosing type name OrbitParameters.
	OrbitParameters_ map[string]interface{} `json:"orbit_parameters,omitempty"`
	UpdatedBy        string                 `json:"updated_by,omitempty"`
	CreatedAt        *time.Time             `json:"created_at,omitempty"`
	UpdatedAt        *time.Time             `json:"updated_at,omitempty"`
}

// OrbitParametersUpdateRequest is the body for PATCH orbit-parameters.
type OrbitParametersUpdateRequest struct {
	OrbitalDataSource string `json:"orbital_data_source,omitempty"`
	NoradID           string `json:"norad_id,omitempty"`
}

// OrbitHistoryItem is one orbit-data activation record from
// GET /v1/satellites/{id}/orbit-data/history.
type OrbitHistoryItem struct {
	ID                  string                 `json:"id,omitempty"`
	ActivationID        string                 `json:"activation_id,omitempty"`
	OrbitalDataType     string                 `json:"orbital_data_type,omitempty"`
	OrbitalData         map[string]interface{} `json:"orbital_data,omitempty"`
	Source              string                 `json:"source,omitempty"`
	OrbitalDataSource   string                 `json:"orbital_data_source,omitempty"`
	ActivationReason    string                 `json:"activation_reason,omitempty"`
	ActiveForScheduling bool                   `json:"active_for_scheduling,omitempty"`
	Epoch               *time.Time             `json:"epoch,omitempty"`
	CreatedAt           *time.Time             `json:"created_at,omitempty"`
	ActivatedAt         *time.Time             `json:"activated_at,omitempty"`
	DeactivatedAt       *time.Time             `json:"deactivated_at,omitempty"`
}

// OrbitHistoryResponse is the response from GET orbit-data/history.
type OrbitHistoryResponse struct {
	SatelliteID string             `json:"satellite_id,omitempty"`
	Items       []OrbitHistoryItem `json:"items,omitempty"`
	NextCursor  string             `json:"next_cursor,omitempty"`
}

// ListOrbitHistoryOpts are query parameters for the orbit-data history call.
type ListOrbitHistoryOpts struct {
	Limit  int
	Cursor string
	Source string
}

// ---- Execution configurations ----------------------------------------------

// RadioConfig is the radio-device configuration of one channel direction
// (uplink or downlink) on an execution config, as returned in the
// configurations payload.
type RadioConfig struct {
	CenterFrequencyHz *float64 `json:"centerFrequencyHz,omitempty"`
	Modulation        string   `json:"modulation,omitempty"`
	Bitrate           *float64 `json:"bitrate,omitempty"`
	Framing           string   `json:"framing,omitempty"`
	Polarization      string   `json:"polarization,omitempty"`
}

// ExecutionConfig is a schedulable configuration for a satellite+ground-station
// pair. Uplink/Downlink carry the per-direction channel (radio) configuration
// when the API supplies it.
type ExecutionConfig struct {
	ID              string       `json:"id"`
	SatelliteID     string       `json:"satelliteId"`
	GroundStationID string       `json:"groundStationId"`
	DisplayName     string       `json:"displayName"`
	Uplink          *RadioConfig `json:"uplink,omitempty"`
	Downlink        *RadioConfig `json:"downlink,omitempty"`
}

// ListConfigurationsResponse is the response from GET /v1/satellites/{id}/configurations.
type ListConfigurationsResponse struct {
	Configurations []ExecutionConfig `json:"configurations"`
	NextPageToken  string            `json:"nextPageToken,omitempty"`
}

// ---- Error types -----------------------------------------------------------

// ErrorResponse is a generic error from the REST API.
type ErrorResponse struct {
	Error  string `json:"error,omitempty"`
	Detail string `json:"detail,omitempty"`
}

// APIError represents a non-2xx response from the API.
type APIError struct {
	StatusCode int
	Method     string
	Path       string
	Message    string
	Detail     string
	RawBody    string
}

// Error implements the error interface.
func (e *APIError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("API %s %s: %d %s", e.Method, e.Path, e.StatusCode, e.Message)
	}
	s := fmt.Sprintf("API %s %s: %d", e.Method, e.Path, e.StatusCode)
	switch e.StatusCode {
	case http.StatusUnauthorized:
		s += ". Your API key was not accepted. Check it is for this environment and re-activate it with `stellar auth activate-api-key <key-file>`."
	case http.StatusForbidden:
		if e.Method == http.MethodGet && e.Path == "/v1/passes" {
			s += ". Your API key is not permitted to list passes. It needs read access to passes, ground stations, and satellites; ask your StellarStation administrator to grant it."
		} else {
			s += ". Access denied. Either the ID does not exist, or your API key does not have access to it. If the ID is correct, ask your StellarStation administrator to grant access."
		}
	case http.StatusNotFound:
		s += ". Not found. Check the ID is correct and that your API key has access to it."
	}
	if e.StatusCode == http.StatusForbidden && strings.TrimSpace(e.RawBody) != "" && e.Message == "" {
		s += " body: " + strings.TrimSpace(e.RawBody)
	}
	return s
}

// IsNotFound returns true if the error is a 404.
func (e *APIError) IsNotFound() bool { return e.StatusCode == http.StatusNotFound }

// IsUnauthorized returns true if the error is a 401 or 403.
func (e *APIError) IsUnauthorized() bool {
	return e.StatusCode == http.StatusUnauthorized || e.StatusCode == http.StatusForbidden
}
