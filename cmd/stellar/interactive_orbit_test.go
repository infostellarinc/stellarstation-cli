package main

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/infostellarinc/stellarstation-cli/internal/apiclient"
)

// The orbit-data half of fakeInteractiveAPI. Orbit parameters left nil answer
// 404, as for a satellite that has never had orbit data.
func (f *fakeInteractiveAPI) GetOrbitParameters(context.Context, string) (*apiclient.OrbitParameters, error) {
	if f.orbitParams == nil {
		return nil, &apiclient.APIError{StatusCode: http.StatusNotFound}
	}
	return f.orbitParams, nil
}

func (f *fakeInteractiveAPI) UpdateOrbitParameters(
	_ context.Context, _ string, req apiclient.UpdateOrbitParametersRequest,
) (*apiclient.OrbitParameters, error) {
	f.paramUpdates = append(f.paramUpdates, req)
	return &apiclient.OrbitParameters{OrbitalDataSource: req.OrbitalDataSource}, nil
}

func (f *fakeInteractiveAPI) ValidateOrbitData(
	_ context.Context, _ string, req apiclient.ValidateOrbitDataRequest,
) (*apiclient.OrbitValidationResult, error) {
	f.validated = append(f.validated, req)
	result := f.validations[0]
	if len(f.validations) > 1 {
		f.validations = f.validations[1:]
	}
	return &result, nil
}

func (f *fakeInteractiveAPI) CreateOrbitData(
	_ context.Context, satID string, req apiclient.CreateOrbitDataRequest,
) (*apiclient.OrbitData, error) {
	if f.createOrbitErr != nil {
		return nil, f.createOrbitErr
	}
	f.createdOrbit = append(f.createdOrbit, req)
	epoch := interactiveNow
	return &apiclient.OrbitData{ID: "od-1", SatelliteID: satID, Epoch: &epoch}, nil
}

const (
	fittedLine1 = "1 25544U          26316.04166667  .00000000  00000+0  00000+0 0  9993"
	fittedLine2 = "2 25544  97.4000 120.0000 0010000  90.0000   0.0000 15.21915372    04"
)

func fittedValidation(from string) apiclient.OrbitValidationResult {
	return apiclient.OrbitValidationResult{
		Valid: true,
		Tle:   &apiclient.TLELines{Line1: fittedLine1, Line2: fittedLine2},
		Conversion: &apiclient.OrbitConversion{From: from, Fit: &apiclient.OrbitFitQuality{
			RmsKm: 0.21, MaxKm: 0.48, Start: interactiveNow, End: interactiveNow.Add(24 * time.Hour), Points: 541,
		}},
	}
}

// Keplerian elements are asked for one by one, checked, previewed as the fitted
// TLE, and saved.
func TestInteractiveUploadsKeplerianElements(t *testing.T) {
	api := &fakeInteractiveAPI{
		satellites:  twoSatellites(),
		orbitParams: &apiclient.OrbitParameters{OrbitalDataSource: "MANUAL", NoradID: "25544"},
		validations: []apiclient.OrbitValidationResult{fittedValidation("KEPLERIAN")},
	}
	g, _, _, out := newTestInteractive(t, api,
		interactiveOrbitData,
		0,                      // Alpha
		2,                      // Keplerian elements
		"2026-11-12T01:00:00Z", // epoch
		"6878.137", "0.001", "97.4", "120", "90",
		1, // mean anomaly
		"0",
		0,  // EME2000
		"", // registered NORAD ID
		true,
		interactiveQuit,
	)

	require.NoError(t, g.run(t.Context()))

	require.Len(t, api.validated, 1)
	want := map[string]any{
		"epoch": "2026-11-12T01:00:00Z", "semi_major_axis_km": 6878.137, "eccentricity": 0.001,
		"inclination_deg": 97.4, "raan_deg": 120.0, "arg_of_perigee_deg": 90.0, "mean_anomaly_deg": 0.0,
		"frame": "EME2000",
	}
	assert.Equal(t, apiclient.ValidateOrbitDataRequest{OrbitalDataType: "KEPLERIAN", OrbitalData: want}, api.validated[0])
	require.Len(t, api.createdOrbit, 1)
	assert.Equal(t, "KEPLERIAN", api.createdOrbit[0].OrbitalDataType)
	assert.Equal(t, want, api.createdOrbit[0].OrbitalData)
	assert.Empty(t, api.paramUpdates, "already manual")

	text := out.String()
	for _, s := range []string{
		"Converted from KEPLERIAN to this TLE:", fittedLine1, fittedLine2,
		"It stays within 0.48 km of your orbit (0.21 km RMS) over 1d 0h",
		"Saved: the satellite now uses this TLE (epoch 2026-11-12T01:00:00Z)",
		`Next time: stellar satellite add-orbit-data --satellite-id sat-a --data-type KEPLERIAN --data '{"arg_of_perigee_deg":90,`,
	} {
		assert.Contains(t, text, s)
	}
}

// An OEM is read from a file; a satellite on automatic updates is switched to
// manual, after asking, before the upload.
func TestInteractiveUploadsAnOEMFileSwitchingToManual(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sat.oem")
	oem := "CCSDS_OEM_VERS = 2.0\nMETA_START\nMETA_STOP\n"
	require.NoError(t, os.WriteFile(path, []byte(oem), 0o600))
	validation := fittedValidation("OEM")
	validation.Warnings = []string{"only the first 7 days of the ephemeris were fitted"}
	api := &fakeInteractiveAPI{
		satellites:  []apiclient.Satellite{{ID: "sat-a", DisplayName: "Alpha"}},
		orbitParams: &apiclient.OrbitParameters{OrbitalDataSource: "AUTOMATIC", NoradID: "25544"},
		validations: []apiclient.OrbitValidationResult{validation},
	}
	g, p, _, out := newTestInteractive(t, api,
		interactiveOrbitData,
		true, // switch to manual
		3,    // OEM
		path,
		true, // save
		interactiveQuit,
	)

	require.NoError(t, g.run(t.Context()))

	assert.Contains(t, p.asked, "Uploading switches it to manual updates. Continue?")
	assert.Equal(t, []apiclient.UpdateOrbitParametersRequest{{OrbitalDataSource: "MANUAL"}}, api.paramUpdates)
	require.Len(t, api.createdOrbit, 1)
	assert.Equal(t, map[string]any{"oem": oem}, api.createdOrbit[0].OrbitalData)
	assert.Contains(t, out.String(), "Note: only the first 7 days of the ephemeris were fitted")
	assert.Contains(t, out.String(), "--data-type OEM --data-file "+path)
}

func TestInteractiveLeavesAnAutomaticSatelliteAloneWhenAsked(t *testing.T) {
	api := &fakeInteractiveAPI{
		satellites:  []apiclient.Satellite{{ID: "sat-a", DisplayName: "Alpha"}},
		orbitParams: &apiclient.OrbitParameters{OrbitalDataSource: "AUTOMATIC", NoradID: "25544"},
	}
	g, _, _, out := newTestInteractive(t, api, interactiveOrbitData, false, interactiveQuit)

	require.NoError(t, g.run(t.Context()))

	assert.Empty(t, api.paramUpdates)
	assert.Empty(t, api.createdOrbit)
	assert.Contains(t, out.String(), "Back to the menu.")
}

// A rejected upload shows why, and the operator can correct it in place.
func TestInteractiveShowsFindingsAndRetries(t *testing.T) {
	dir := t.TempDir()
	bad, good := filepath.Join(dir, "bad.omm"), filepath.Join(dir, "good.omm")
	require.NoError(t, os.WriteFile(bad, []byte("CCSDS_OMM_VERS = 2.0\nREF_FRAME = GCRF\n"), 0o600))
	require.NoError(t, os.WriteFile(good, []byte("CCSDS_OMM_VERS = 2.0\nREF_FRAME = TEME\n"), 0o600))
	api := &fakeInteractiveAPI{
		satellites: []apiclient.Satellite{{ID: "sat-a", DisplayName: "Alpha"}},
		validations: []apiclient.OrbitValidationResult{
			{Valid: false, Errors: []apiclient.OrbitValidationError{
				{Location: "orbital_data.REF_FRAME", Message: `REF_FRAME is "GCRF"; a TLE needs TEME`},
			}},
			{Valid: true, Tle: &apiclient.TLELines{Line1: fittedLine1, Line2: fittedLine2},
				Conversion: &apiclient.OrbitConversion{From: "OMM"}},
		},
	}
	g, _, _, out := newTestInteractive(t, api,
		interactiveOrbitData,
		1, // OMM
		bad,
		true, // try again
		good,
		true, // save
		interactiveQuit,
	)

	require.NoError(t, g.run(t.Context()))

	assert.Contains(t, out.String(), `orbital_data.REF_FRAME: REF_FRAME is "GCRF"; a TLE needs TEME`)
	require.Len(t, api.validated, 2)
	require.Len(t, api.createdOrbit, 1)
	assert.Equal(t, "OMM", api.createdOrbit[0].OrbitalDataType)
	assert.Equal(t, map[string]any{"omm": "CCSDS_OMM_VERS = 2.0\nREF_FRAME = TEME\n"}, api.createdOrbit[0].OrbitalData)
}

func TestInteractiveReportsARejectedSave(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sat.tle")
	require.NoError(t, os.WriteFile(path, []byte(fittedLine1+"\n"+fittedLine2+"\n"), 0o600))
	api := &fakeInteractiveAPI{
		satellites:  []apiclient.Satellite{{ID: "sat-a", DisplayName: "Alpha"}},
		validations: []apiclient.OrbitValidationResult{{Valid: true, Tle: &apiclient.TLELines{Line1: fittedLine1, Line2: fittedLine2}}},
		createOrbitErr: &apiclient.APIError{StatusCode: http.StatusUnprocessableEntity,
			RawBody: `{"valid":false,"errors":[{"location":"orbital_data","message":"changed meanwhile"}]}`},
	}
	g, _, _, out := newTestInteractive(t, api, interactiveOrbitData, 0, path, true, interactiveQuit)

	require.NoError(t, g.run(t.Context()))

	assert.Contains(t, out.String(), "orbital_data: changed meanwhile")
	assert.Contains(t, out.String(), "Problem: the orbit data was rejected; nothing was stored")
	assert.Contains(t, out.String(), "The TLE:")
}

func TestInteractiveOrbitInputChecks(t *testing.T) {
	err := checkSemiMajorAxis(550)
	require.Error(t, err)
	assert.Equal(t, "that is inside the Earth; for a 550 km altitude enter 6928", err.Error())
	assert.NoError(t, checkSemiMajorAxis(6928.137))

	eccentricity := fromZeroTo(1, false)
	assert.NoError(t, eccentricity(0))
	assert.EqualError(t, eccentricity(1), "must be from 0 below 1")
	inclination := fromZeroTo(180, true)
	assert.NoError(t, inclination(180))
	assert.EqualError(t, inclination(-1), "must be from 0 up to 180")

	assert.EqualError(t, readableFile(filepath.Join(t.TempDir(), "missing")), "no file there")
	assert.EqualError(t, readableFile(t.TempDir()), "that is a folder; give the file's path")
}
