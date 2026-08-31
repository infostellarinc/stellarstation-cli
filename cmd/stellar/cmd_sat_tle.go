package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/infostellarinc/stellarstation-cli/internal/apiclient"
	"github.com/infostellarinc/stellarstation-cli/internal/printer"

	"github.com/spf13/cobra"
)

// normalizeOrbitDataCreateSource maps CLI-friendly values to OrbitDataCreateRequest.source enums.
func normalizeOrbitDataCreateSource(s string) string {
	key := strings.ToLower(strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(s, "_", "-"), " ", "")))
	switch key {
	case "", "manual":
		return "MANUAL"
	case "space-track", "spacetrack":
		return "SPACE_TRACK"
	case "celestrak":
		return "CELESTRAK"
	case "unknown":
		return "UNKNOWN"
	default:
		return strings.TrimSpace(s)
	}
}

// normalizeOrbitalDataParameterSource maps CLI-friendly values to OrbitParametersUpdateRequest.orbital_data_source enums.
func normalizeOrbitalDataParameterSource(s string) string {
	key := strings.ToLower(strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(s, "_", "-"), " ", "")))
	switch key {
	case "manual":
		return "MANUAL"
	case "automatic", "auto":
		return "AUTOMATIC"
	default:
		return strings.TrimSpace(s)
	}
}

var orbitDataColumns = []printer.Column{ //nolint:gochecknoglobals
	{
		Header:  "ID",
		Extract: func(r interface{}) string { return r.(*apiclient.OrbitData).ID }, //nolint:errcheck
	},
	{
		Header:  "SATELLITE",
		Extract: func(r interface{}) string { return r.(*apiclient.OrbitData).SatelliteID }, //nolint:errcheck
	},
	{
		Header:  "TYPE",
		Extract: func(r interface{}) string { return r.(*apiclient.OrbitData).OrbitalDataType }, //nolint:errcheck
	},
	{
		Header:  "SOURCE",
		Extract: func(r interface{}) string { return r.(*apiclient.OrbitData).Source }, //nolint:errcheck
	},
	{Header: "EPOCH", Extract: func(r interface{}) string {
		d := r.(*apiclient.OrbitData) //nolint:errcheck
		if d.Epoch != nil {
			return d.Epoch.Format(timeFormat)
		}
		return ""
	}},
	{Header: "LINE1", Extract: func(r interface{}) string {
		return orbitalDataString(r.(*apiclient.OrbitData), "line1") //nolint:errcheck
	}},
	{Header: "LINE2", Extract: func(r interface{}) string {
		return orbitalDataString(r.(*apiclient.OrbitData), "line2") //nolint:errcheck
	}},
}

// orbitalDataString returns a string-valued field from the free-form
// orbital_data map (e.g. "line1"/"line2" for a TLE), or "" when absent.
func orbitalDataString(d *apiclient.OrbitData, key string) string {
	if d.OrbitalData == nil {
		return ""
	}
	if v, ok := d.OrbitalData[key].(string); ok {
		return v
	}
	return ""
}

var orbitParamsColumns = []printer.Column{ //nolint:gochecknoglobals
	{
		Header:  "ID",
		Extract: func(r interface{}) string { return r.(*apiclient.OrbitParameters).ID }, //nolint:errcheck
	},
	{
		Header:  "SATELLITE",
		Extract: func(r interface{}) string { return r.(*apiclient.OrbitParameters).SatelliteID }, //nolint:errcheck
	},
	{
		Header:  "SOURCE",
		Extract: func(r interface{}) string { return r.(*apiclient.OrbitParameters).OrbitalDataSource }, //nolint:errcheck
	},
	{
		Header:  "NORAD_ID",
		Extract: func(r interface{}) string { return r.(*apiclient.OrbitParameters).NoradID }, //nolint:errcheck
	},
	{
		Header:  "ORBIT_TYPE",
		Extract: func(r interface{}) string { return r.(*apiclient.OrbitParameters).OrbitType }, //nolint:errcheck
	},
}

// newGetTLECommand creates the "get-tle" subcommand.
func newGetTLECommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "get-orbit-data",
		Aliases: []string{"get-tle"},
		Short:   "Show a satellite's current orbit data (TLE)",
		Long: `Show the orbit data (TLE) currently used to schedule a satellite.

Example:
  stellar satellite get-tle --satellite-id <id>`,
		RunE: runGetTLE,
	}
	addOutputFlag(cmd)
	cmd.Flags().String("satellite-id", "", "Which satellite (required; from list-satellites)")
	_ = cmd.MarkFlagRequired("satellite-id")
	return cmd
}

func runGetTLE(cmd *cobra.Command, _ []string) error {
	client, err := newAPIClient(cmd)
	if err != nil {
		return err
	}
	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}

	satID, _ := cmd.Flags().GetString("satellite-id")
	data, err := client.GetOrbitData(cmd.Context(), satID)
	if err != nil {
		return err
	}
	return renderOne(cmd, format, orbitDataColumns, data, "")
}

// parseOrbitalDataInput parses orbital data supplied inline (--data) or from a
// file (--data-file). JSON is accepted for either type: a TLE as
// {"line1": "...", "line2": "..."} or an OMM as its standard mean-element field
// names. A raw two-line element set (non-JSON) is accepted only for TLE; OMM
// must be JSON.
func parseOrbitalDataInput(raw, dataType string) (map[string]interface{}, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("the orbit data is empty")
	}
	if strings.HasPrefix(strings.TrimSpace(raw), "{") {
		var m map[string]interface{}
		if err := json.Unmarshal([]byte(raw), &m); err != nil {
			return nil, fmt.Errorf("that JSON could not be read: %w", err)
		}
		return m, nil
	}
	if dataType == "OMM" {
		return nil, errors.New(`OMM must be provided as JSON (its mean-element fields, e.g. ` +
			`{"MEAN_MOTION": 15.5, "ECCENTRICITY": 0.0007, "INCLINATION": 51.6, ` +
			`"RA_OF_ASC_NODE": 247.5, "ARG_OF_PERICENTER": 130.5, "MEAN_ANOMALY": 325.0, ` +
			`"EPOCH": "2026-05-08T12:00:00Z"}); pass it with --data-file`)
	}
	return parseTLEText(raw)
}

// parseTLEText parses the standard two-line element set text format into the
// {line1, line2} orbital-data map. An optional satellite-name line preceding
// the two data lines is ignored.
func parseTLEText(raw string) (map[string]interface{}, error) {
	var line1, line2 string
	for _, ln := range strings.Split(raw, "\n") {
		ln = strings.TrimRight(ln, "\r")
		switch {
		case line1 == "" && strings.HasPrefix(ln, "1 "):
			line1 = ln
		case line2 == "" && strings.HasPrefix(ln, "2 "):
			line2 = ln
		}
	}
	if line1 == "" || line2 == "" {
		return nil, errors.New(
			`that does not look like a TLE. Provide two lines (one starting with "1 " and one ` +
				`starting with "2 ") or JSON {"line1": "...", "line2": "..."}`)
	}
	return map[string]interface{}{"line1": line1, "line2": line2}, nil
}

// newGetTLESourceCommand creates the "get-tle-source" subcommand, which reads
// the satellite's orbit-data source mode (MANUAL/AUTOMATIC) via
// GET orbit-parameters; the read counterpart to set-tle-source.
func newGetTLESourceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "get-orbit-data-source",
		Aliases: []string{"get-tle-source"},
		Short:   "Show where a satellite's orbit data comes from (manual or automatic)",
		Long: `Show whether a satellite's orbit data is updated automatically (fetched from
Space-Track) or provided manually with add-tle.

Example:
  stellar satellite get-tle-source --satellite-id <id>`,
		RunE: runGetTLESource,
	}
	addOutputFlag(cmd)
	cmd.Flags().String("satellite-id", "", "Which satellite (required; from list-satellites)")
	_ = cmd.MarkFlagRequired("satellite-id")
	return cmd
}

func runGetTLESource(cmd *cobra.Command, _ []string) error {
	client, err := newAPIClient(cmd)
	if err != nil {
		return err
	}
	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}

	satID, _ := cmd.Flags().GetString("satellite-id")
	params, err := client.GetOrbitParameters(cmd.Context(), satID)
	if err != nil {
		return err
	}
	return renderOne(cmd, format, orbitParamsColumns, params, "")
}

var orbitHistoryColumns = []printer.Column{ //nolint:gochecknoglobals
	{
		Header:  "ID",
		Extract: func(r interface{}) string { return r.(*apiclient.OrbitHistoryItem).ID }, //nolint:errcheck
	},
	{
		Header:  "TYPE",
		Extract: func(r interface{}) string { return r.(*apiclient.OrbitHistoryItem).OrbitalDataType }, //nolint:errcheck
	},
	{
		Header:  "SOURCE",
		Extract: func(r interface{}) string { return r.(*apiclient.OrbitHistoryItem).Source }, //nolint:errcheck
	},
	{
		Header:  "MODE",
		Extract: func(r interface{}) string { return r.(*apiclient.OrbitHistoryItem).OrbitalDataSource }, //nolint:errcheck
	},
	{
		Header:  "REASON",
		Extract: func(r interface{}) string { return r.(*apiclient.OrbitHistoryItem).ActivationReason }, //nolint:errcheck
	},
	{Header: "ACTIVE", Extract: func(r interface{}) string {
		if r.(*apiclient.OrbitHistoryItem).ActiveForScheduling { //nolint:errcheck
			return "true"
		}
		return "false"
	}},
	{Header: "EPOCH", Extract: func(r interface{}) string {
		return formatOptionalTime(r.(*apiclient.OrbitHistoryItem).Epoch) //nolint:errcheck
	}},
	{Header: "ACTIVATED", Extract: func(r interface{}) string {
		return formatOptionalTime(r.(*apiclient.OrbitHistoryItem).ActivatedAt) //nolint:errcheck
	}},
	{Header: "DEACTIVATED", Extract: func(r interface{}) string {
		return formatOptionalTime(r.(*apiclient.OrbitHistoryItem).DeactivatedAt) //nolint:errcheck
	}},
}

// newGetTLEHistoryCommand creates the "get-tle-history" subcommand, which lists
// the satellite's orbit-data activation history via GET orbit-data/history.
func newGetTLEHistoryCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "get-orbit-data-history",
		Aliases: []string{"get-tle-history"},
		Short:   "Show past orbit-data (TLE) updates for a satellite",
		Long: `Show the history of orbit-data (TLE) updates for a satellite, newest first.
The currently active entry is marked in the ACTIVE column.

Example:
  stellar satellite get-tle-history --satellite-id <id> --limit 10`,
		RunE: runGetTLEHistory,
	}
	addOutputFlag(cmd)
	cmd.Flags().String("satellite-id", "", "Which satellite (required; from list-satellites)")
	cmd.Flags().Int("limit", 0, "How many entries to show, 1-100 (default: 50)")
	cmd.Flags().
		String("cursor", "", "Continue a previous listing (advanced; use the next_cursor value from an earlier run)")
	cmd.Flags().String("source", "", "Only show updates from this source: manual, space-track, or celestrak")
	_ = cmd.MarkFlagRequired("satellite-id")
	return cmd
}

func runGetTLEHistory(cmd *cobra.Command, _ []string) error {
	client, err := newAPIClient(cmd)
	if err != nil {
		return err
	}
	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}

	satID, _ := cmd.Flags().GetString("satellite-id")
	limit, _ := cmd.Flags().GetInt("limit")
	cursor, _ := cmd.Flags().GetString("cursor")
	source, _ := cmd.Flags().GetString("source")

	opts := apiclient.ListOrbitHistoryOpts{Limit: limit, Cursor: cursor}
	if strings.TrimSpace(source) != "" {
		opts.Source = normalizeOrbitDataCreateSource(source)
	}

	resp, err := client.ListOrbitHistory(cmd.Context(), satID, opts)
	if err != nil {
		return err
	}
	rows := make([]interface{}, len(resp.Items))
	for i := range resp.Items {
		rows[i] = &resp.Items[i]
	}
	return renderList(cmd, format, orbitHistoryColumns, rows, "orbit-data update",
		"No orbit-data history for this satellite yet.")
}

// newAddTLECommand creates the "add-tle" subcommand.
func newAddTLECommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "add-orbit-data",
		Aliases: []string{"add-tle"},
		Short:   "Upload new orbit data (TLE or OMM) for a satellite",
		Long: `Upload new orbit data for a satellite. Provide it inline with --data or from a
file with --data-file, and say what kind it is with --data-type (TLE or OMM).

TLE is accepted as a plain two-line element set or as JSON
({"line1": "...", "line2": "..."}). OMM is accepted as JSON (the standard OMM
mean-element field names). StellarStation converts an uploaded OMM to a TLE
internally, so a TLE-only ground station is still served.

The satellite must be in manual mode (see set-orbit-data-source) to accept a
manual upload.

Upload a TLE from a file:
  stellar satellite add-orbit-data --satellite-id <id> --data-file ./mysat.tle

Upload a TLE inline (mind the quoting; a here-doc is often easier):
  stellar satellite add-orbit-data --satellite-id <id> --data \
'1 25544U 98067A   26015.00000000  .00005000  00000+0  10000-3 0  9990
2 25544  51.6416 190.0000 0001000  90.0000 270.0000 15.49000000000010'

Upload an OMM from a file:
  stellar satellite add-orbit-data --satellite-id <id> --data-type OMM --data-file ./mysat.omm.json

  where the OMM JSON looks like:
  {"NORAD_CAT_ID": 25544, "EPOCH": "2026-05-08T12:00:00Z", "MEAN_MOTION": 15.5,
   "ECCENTRICITY": 0.0007, "INCLINATION": 51.64, "RA_OF_ASC_NODE": 247.46,
   "ARG_OF_PERICENTER": 130.5, "MEAN_ANOMALY": 325.0, "BSTAR": 0.00016}`,
		RunE: runAddTLE,
	}
	addOutputFlag(cmd)
	cmd.Flags().String("satellite-id", "", "Which satellite (required; from list-satellites)")
	cmd.Flags().String("data-type", "TLE", "Kind of orbit data being uploaded: TLE or OMM")
	cmd.Flags().
		String("source", "MANUAL", "Where this data came from: manual, space-track, or celestrak")
	cmd.Flags().String("epoch", "", "Epoch of the orbit data, e.g. 2026-01-15T10:00:00Z (default: now)")
	cmd.Flags().String("data", "", "The orbit data itself: a two-line TLE, TLE JSON, or OMM JSON (use this or --data-file)")
	cmd.Flags().String("data-file", "", "Path to a file containing the orbit data (TLE text/JSON or OMM JSON)")
	_ = cmd.MarkFlagRequired("satellite-id")
	return cmd
}

func runAddTLE(cmd *cobra.Command, _ []string) error {
	client, err := newAPIClient(cmd)
	if err != nil {
		return err
	}
	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}

	satID, _ := cmd.Flags().GetString("satellite-id")
	dataType, _ := cmd.Flags().GetString("data-type")
	dataType = strings.ToUpper(strings.TrimSpace(dataType))
	if dataType != "TLE" && dataType != "OMM" {
		return fmt.Errorf("unknown --data-type %q: use TLE or OMM", dataType)
	}
	source, _ := cmd.Flags().GetString("source")
	epochStr, _ := cmd.Flags().GetString("epoch")
	dataStr, _ := cmd.Flags().GetString("data")
	dataFile, _ := cmd.Flags().GetString("data-file")

	var rawInput string
	switch {
	case dataFile != "":
		raw, readErr := os.ReadFile(dataFile)
		if readErr != nil {
			return fmt.Errorf("could not read the orbit-data file %q: %w", dataFile, readErr)
		}
		rawInput = string(raw)
	case dataStr != "":
		rawInput = dataStr
	default:
		return errors.New("no orbit data given; provide it with --data or point --data-file at a file")
	}

	orbitalData, err := parseOrbitalDataInput(rawInput, dataType)
	if err != nil {
		return err
	}

	epoch := time.Now().UTC()
	if epochStr != "" {
		parsed, parseErr := parseTimeFlag(epochStr)
		if parseErr != nil {
			return parseErr
		}
		epoch = *parsed
	}

	result, err := client.CreateOrbitData(cmd.Context(), satID, apiclient.OrbitDataCreateRequest{
		OrbitalDataType: dataType,
		OrbitalData:     orbitalData,
		// (OMM is converted to TLE server-side; the record is stored/served as TLE.)
		Source: normalizeOrbitDataCreateSource(source),
		Epoch:  epoch,
	})
	if err != nil {
		return err
	}
	return renderOne(cmd, format, orbitDataColumns, result, "Orbit data uploaded and now active for scheduling.")
}

// newSetTLESourceCommand creates the "set-tle-source" subcommand.
func newSetTLESourceCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "set-orbit-data-source",
		Aliases: []string{"set-tle-source"},
		Short:   "Choose whether orbit data updates automatically or manually",
		Long: `Choose how a satellite's orbit data (TLE) is kept up to date:

  automatic  StellarStation fetches fresh TLEs automatically (from Space-Track)
  manual     you provide TLEs yourself with add-tle

Examples:
  stellar satellite set-tle-source --satellite-id <id> --source automatic
  stellar satellite set-tle-source --satellite-id <id> --source manual`,
		RunE: runSetTLESource,
	}
	addOutputFlag(cmd)
	cmd.Flags().String("satellite-id", "", "Which satellite (required; from list-satellites)")
	cmd.Flags().String("source", "", "How orbit data is updated: automatic or manual")
	cmd.Flags().String("norad-id", "", "NORAD catalog ID (used with automatic mode)")
	_ = cmd.MarkFlagRequired("satellite-id")
	return cmd
}

func runSetTLESource(cmd *cobra.Command, _ []string) error {
	client, err := newAPIClient(cmd)
	if err != nil {
		return err
	}
	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}

	satID, _ := cmd.Flags().GetString("satellite-id")
	source, _ := cmd.Flags().GetString("source")
	noradID, _ := cmd.Flags().GetString("norad-id")

	result, err := client.UpdateOrbitParameters(cmd.Context(), satID,
		apiclient.OrbitParametersUpdateRequest{
			OrbitalDataSource: normalizeOrbitalDataParameterSource(source),
			NoradID:           noradID,
		})
	if err != nil {
		return err
	}
	return renderOne(cmd, format, orbitParamsColumns, result, "Orbit-data source updated.")
}
