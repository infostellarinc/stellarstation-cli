package main

// The guided orbit-data upload of `stellar interactive`: choose the format the
// operator has, enter it (a file, or the Keplerian elements one by one), see the
// TLE it becomes and how closely a fitted one follows the orbit, then save it.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/infostellarinc/stellarstation-cli/internal/apiclient"
)

// orbitFormat is one way orbit data can be uploaded.
type orbitFormat struct {
	dataType    string
	label       string
	placeholder string // example file, for formats read from a file
}

var orbitFormats = []orbitFormat{ //nolint:gochecknoglobals
	{"TLE", "TLE: a two-line element set", "./mysat.tle"},
	{"OMM", "OMM: CCSDS mean elements (KVN, XML or JSON, e.g. from CelesTrak or Space-Track)", "./mysat.omm"},
	{"KEPLERIAN", "Keplerian elements: osculating elements, e.g. from your launch provider", ""},
	{"OEM", "OEM: a CCSDS ephemeris of position and velocity (KVN or XML)", "./mysat.oem"},
}

// earthRadiusKm is the WGS-84 equatorial radius, the floor for a semi-major axis.
const earthRadiusKm = 6378.137

func (g *interactiveMode) updateOrbitData(ctx context.Context) error {
	satID, err := g.chooseSatellite(false)
	if err != nil {
		return err
	}
	switchToManual, err := g.confirmManualMode(ctx, satID)
	if err != nil {
		return err
	}

	labels := make([]string, len(orbitFormats))
	for i, f := range orbitFormats {
		labels[i] = f.label
	}
	i, err := g.p.Select("Which format is your orbit data in?", labels, 0)
	if err != nil {
		return err
	}
	format := orbitFormats[i]
	g.sayf("Whatever you upload is stored and sent to ground stations as a TLE; you will see it before it is saved.")

	for {
		data, nextTime, err := g.enterOrbitData(format)
		if err != nil {
			return err
		}
		checked, err := g.api.ValidateOrbitData(ctx, satID, apiclient.ValidateOrbitDataRequest{
			OrbitalDataType: format.dataType,
			OrbitalData:     data,
		})
		if err != nil {
			return fmt.Errorf("could not check the orbit data: %w", err)
		}
		if !checked.Valid {
			g.sayf("That orbit data cannot be used:")
			for _, e := range checked.Errors {
				g.sayf("  - %s: %s", e.Location, e.Message)
			}
			again, err := g.p.Confirm("Correct it and try again?", true)
			if err != nil || !again {
				return err
			}
			continue
		}
		g.showCheckedOrbit(checked)

		save, err := g.p.Confirm("Save it and make it the satellite's active TLE?", true)
		if err != nil || !save {
			return err
		}
		return g.saveOrbitData(ctx, satID, format.dataType, data, switchToManual, nextTime)
	}
}

// confirmManualMode tells the operator when an upload switches the satellite off
// automatic TLE updates, and asks first. It reports whether to switch.
func (g *interactiveMode) confirmManualMode(ctx context.Context, satID string) (bool, error) {
	params, err := g.api.GetOrbitParameters(ctx, satID)
	var apiErr *apiclient.APIError
	if errors.As(err, &apiErr) && apiErr.IsNotFound() {
		return false, nil // no orbit parameters yet: the first upload sets manual mode
	}
	if err != nil {
		return false, fmt.Errorf("could not read the satellite's orbit settings: %w", err)
	}
	if params.OrbitalDataSource != "AUTOMATIC" {
		return false, nil
	}
	g.sayf("This satellite's TLE is updated automatically from its NORAD ID (%s).", params.NoradID)
	sure, err := g.p.Confirm("Uploading switches it to manual updates. Continue?", false)
	if err != nil {
		return false, err
	}
	if !sure {
		return false, errInteractiveCanceled
	}
	return true, nil
}

// enterOrbitData asks for the orbit data in the chosen format and returns it as
// the API's orbital_data, with the flags that upload it again from the command
// line.
func (g *interactiveMode) enterOrbitData(format orbitFormat) (map[string]any, string, error) {
	if format.dataType == "KEPLERIAN" {
		elements, err := g.enterKeplerian()
		if err != nil {
			return nil, "", err
		}
		raw, err := json.Marshal(elements)
		if err != nil {
			return nil, "", err
		}
		return elements, fmt.Sprintf("--data '%s'", raw), nil
	}
	path, err := g.p.Input("Path to the file", format.placeholder, "", readableFile)
	if err != nil {
		return nil, "", err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, "", fmt.Errorf("could not read %s: %w", path, err)
	}
	data, err := parseOrbitalDataInput(string(raw), format.dataType)
	if err != nil {
		return nil, "", err
	}
	return data, "--data-file " + path, nil
}

func readableFile(path string) error {
	info, err := os.Stat(strings.TrimSpace(path))
	switch {
	case err != nil:
		return errors.New("no file there")
	case info.IsDir():
		return errors.New("that is a folder; give the file's path")
	}
	return nil
}

// enterKeplerian asks for the six osculating elements, their epoch and frame,
// checking each as it is typed.
func (g *interactiveMode) enterKeplerian() (map[string]any, error) {
	g.sayf("Enter the osculating elements at their epoch. A TLE will be fitted to a day of the orbit they describe.")
	elements := map[string]any{}

	epoch, err := g.p.Input("Epoch (UTC, e.g. 2026-11-12T00:00:00Z)", "2026-11-12T00:00:00Z",
		g.now().UTC().Truncate(time.Second).Format(time.RFC3339), func(s string) error {
			_, err := time.Parse(time.RFC3339, strings.TrimSpace(s))
			if err != nil {
				return errors.New("use the form 2026-11-12T00:00:00Z")
			}
			return nil
		})
	if err != nil {
		return nil, err
	}
	elements["epoch"] = strings.TrimSpace(epoch)

	numbers := []struct {
		key, title string
		check      func(float64) error
	}{
		{"semi_major_axis_km", "Semi-major axis (km): Earth's radius 6378 km plus the mean altitude", checkSemiMajorAxis},
		{"eccentricity", "Eccentricity (0 for a circular orbit, below 1)", fromZeroTo(1, false)},
		{"inclination_deg", "Inclination (degrees, 0 to 180)", fromZeroTo(180, true)},
		{"raan_deg", "Right ascension of the ascending node (degrees)", anyAngle},
		{"arg_of_perigee_deg", "Argument of perigee (degrees)", anyAngle},
	}
	for _, n := range numbers {
		v, err := g.inputNumber(n.title, n.check)
		if err != nil {
			return nil, err
		}
		elements[n.key] = v
	}

	kind, err := g.p.Select("Which anomaly do you have?", []string{"True anomaly", "Mean anomaly"}, 0)
	if err != nil {
		return nil, err
	}
	anomalyKey, anomalyTitle := "true_anomaly_deg", "True anomaly (degrees)"
	if kind == 1 {
		anomalyKey, anomalyTitle = "mean_anomaly_deg", "Mean anomaly (degrees)"
	}
	anomaly, err := g.inputNumber(anomalyTitle, anyAngle)
	if err != nil {
		return nil, err
	}
	elements[anomalyKey] = anomaly

	frames := []string{"EME2000", "GCRF", "TEME"}
	frame, err := g.p.Select("Which reference frame are they in?", []string{
		"EME2000 (also called J2000): what most launch providers use",
		"GCRF",
		"TEME: the frame of TLEs",
	}, 0)
	if err != nil {
		return nil, err
	}
	elements["frame"] = frames[frame]

	norad, err := g.p.Input("NORAD ID (leave empty to use the satellite's registered one)", "", "", func(s string) error {
		if s = strings.TrimSpace(s); s == "" {
			return nil
		}
		if n, err := strconv.Atoi(s); err != nil || n < 0 || n > 339999 {
			return errors.New("a whole number from 0 to 339999")
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if norad = strings.TrimSpace(norad); norad != "" {
		n, _ := strconv.Atoi(norad)
		elements["norad_id"] = n
	}
	return elements, nil
}

func (g *interactiveMode) inputNumber(title string, check func(float64) error) (float64, error) {
	raw, err := g.p.Input(title, "", "", func(s string) error {
		v, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
		if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
			return errors.New("enter a number")
		}
		return check(v)
	})
	if err != nil {
		return 0, err
	}
	v, _ := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	return v, nil
}

func checkSemiMajorAxis(a float64) error {
	if a <= earthRadiusKm {
		return fmt.Errorf("that is inside the Earth; for a %.0f km altitude enter %.0f", a, a+earthRadiusKm)
	}
	return nil
}

// fromZeroTo accepts 0 up to hi, and hi itself when hiInclusive.
func fromZeroTo(hi float64, hiInclusive bool) func(float64) error {
	return func(v float64) error {
		if v < 0 || v > hi || (!hiInclusive && v == hi) {
			bound := "below"
			if hiInclusive {
				bound = "up to"
			}
			return fmt.Errorf("must be from 0 %s %g", bound, hi)
		}
		return nil
	}
}

func anyAngle(float64) error { return nil }

func (g *interactiveMode) showCheckedOrbit(checked *apiclient.OrbitValidationResult) {
	if c := checked.Conversion; c != nil {
		g.sayf("Converted from %s to this TLE:", c.From)
	} else {
		g.sayf("The TLE:")
	}
	if checked.Tle != nil {
		g.sayf("  %s", checked.Tle.Line1)
		g.sayf("  %s", checked.Tle.Line2)
	}
	if c := checked.Conversion; c != nil && c.Fit != nil {
		g.sayf("It stays within %.2f km of your orbit (%.2f km RMS) over %s from %s.",
			c.Fit.MaxKm, c.Fit.RmsKm, humanDuration(c.Fit.End.Sub(c.Fit.Start)), c.Fit.Start.UTC().Format(time.RFC3339))
	}
	for _, w := range checked.Warnings {
		g.sayf("Note: %s", w)
	}
}

func (g *interactiveMode) saveOrbitData(
	ctx context.Context, satID, dataType string, data map[string]any, switchToManual bool, nextTime string,
) error {
	if switchToManual {
		if _, err := g.api.UpdateOrbitParameters(ctx, satID, apiclient.UpdateOrbitParametersRequest{
			OrbitalDataSource: "MANUAL",
		}); err != nil {
			return fmt.Errorf("could not switch the satellite to manual updates: %w", err)
		}
		g.sayf("Switched to manual TLE updates.")
	}
	created, err := g.api.CreateOrbitData(ctx, satID, apiclient.CreateOrbitDataRequest{
		OrbitalDataType: dataType,
		OrbitalData:     data,
		Source:          "MANUAL",
		Epoch:           g.now().UTC(),
	})
	if rejected, ok := orbitRejection(err); ok {
		for _, e := range rejected.Errors {
			g.sayf("  - %s: %s", e.Location, e.Message)
		}
		return errors.New("the orbit data was rejected; nothing was stored")
	}
	if err != nil {
		return fmt.Errorf("the orbit data was not saved: %w", err)
	}
	epoch := ""
	if created.Epoch != nil {
		epoch = " (epoch " + created.Epoch.UTC().Format(time.RFC3339) + ")"
	}
	g.sayf("Saved: the satellite now uses this TLE%s; booked passes are re-aligned to it.", epoch)
	g.sayf("  Next time: stellar satellite add-orbit-data --satellite-id %s --data-type %s %s",
		satID, dataType, nextTime)
	return nil
}
