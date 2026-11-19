package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/infostellarinc/stellarstation-cli/internal/apiclient"
	"github.com/infostellarinc/stellarstation-cli/internal/printer"

	"github.com/spf13/cobra"
)

// normalizeOrbitDataCreateSource maps CLI-friendly values to CreateOrbitDataRequest.source enums.
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

// normalizeOrbitalDataParameterSource maps CLI-friendly values to UpdateOrbitParametersRequest.orbital_data_source enums.
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
	{Header: "CONVERTED_FROM", Extract: func(r interface{}) string {
		return conversionSummary(r.(*apiclient.OrbitData).Conversion) //nolint:errcheck
	}},
}

// conversionSummary describes how a TLE was converted, e.g. "OEM (fit 0.55 km
// RMS, 1.28 km max)", or "" for an uploaded TLE.
func conversionSummary(c *apiclient.OrbitConversion) string {
	if c == nil {
		return ""
	}
	if c.Fit == nil {
		return c.From
	}
	return fmt.Sprintf("%s (fit %.2f km RMS, %.2f km max)", c.From, c.Fit.RmsKm, c.Fit.MaxKm)
}

// orbitValidationColumns render a dry run of add-orbit-data.
var orbitValidationColumns = []printer.Column{ //nolint:gochecknoglobals
	{Header: "VALID", Extract: func(r interface{}) string {
		return strconv.FormatBool(r.(*apiclient.OrbitValidationResult).Valid) //nolint:errcheck
	}},
	{Header: "LINE1", Extract: func(r interface{}) string {
		if tle := r.(*apiclient.OrbitValidationResult).Tle; tle != nil { //nolint:errcheck
			return tle.Line1
		}
		return ""
	}},
	{Header: "LINE2", Extract: func(r interface{}) string {
		if tle := r.(*apiclient.OrbitValidationResult).Tle; tle != nil { //nolint:errcheck
			return tle.Line2
		}
		return ""
	}},
	{Header: "CONVERTED_FROM", Extract: func(r interface{}) string {
		return conversionSummary(r.(*apiclient.OrbitValidationResult).Conversion) //nolint:errcheck
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
		Extract: func(r interface{}) string { return effectiveOrbitType(r.(*apiclient.OrbitParameters)) }, //nolint:errcheck
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

// orbitDataTypes are the --data-type values add-orbit-data accepts.
var orbitDataTypes = []string{"TLE", "OMM", "KEPLERIAN", "OEM"} //nolint:gochecknoglobals

// parseOrbitalDataInput turns orbit data supplied inline (--data) or from a file
// (--data-file) into the API's orbital_data object:
//
//	TLE        two-line text (a title line is allowed) or {"line1": ..., "line2": ...}
//	OMM        a JSON object of OMM fields, or a whole OMM document (KVN, XML, or the
//	           JSON array CelesTrak and Space-Track serve), sent as {"omm": <text>}
//	KEPLERIAN  a JSON object of the elements
//	OEM        a CCSDS OEM document (KVN or XML), sent as {"oem": <text>}
func parseOrbitalDataInput(raw, dataType string) (map[string]interface{}, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, errors.New("the orbit data is empty")
	}
	isObject := strings.HasPrefix(strings.TrimSpace(raw), "{")
	switch dataType {
	case "OEM":
		return map[string]interface{}{"oem": raw}, nil
	case "OMM":
		if !isObject {
			return map[string]interface{}{"omm": raw}, nil
		}
	case "KEPLERIAN":
		if !isObject {
			return nil, errors.New(`the Keplerian elements must be JSON, e.g. {"epoch": "2026-11-12T00:00:00Z", ` +
				`"semi_major_axis_km": 6878.137, "eccentricity": 0.001, "inclination_deg": 97.4, ` +
				`"raan_deg": 120, "arg_of_perigee_deg": 90, "true_anomaly_deg": 0, "frame": "EME2000"}`)
		}
	case "TLE":
		if !isObject {
			return parseTLEText(raw)
		}
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, fmt.Errorf("that JSON could not be read: %w", err)
	}
	return m, nil
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
		Header:  "ACTIVATION_ID",
		Extract: func(r interface{}) string { return r.(*apiclient.OrbitHistoryItem).ActivationID }, //nolint:errcheck
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
	{Header: "CONVERTED_FROM", Extract: func(r interface{}) string {
		return conversionSummary(r.(*apiclient.OrbitHistoryItem).Conversion) //nolint:errcheck
	}},
	{Header: "UPLOAD_KEPT", Extract: func(r interface{}) string {
		return strconv.FormatBool(r.(*apiclient.OrbitHistoryItem).HasUpload) //nolint:errcheck
	}},
}

// newGetOrbitDataUploadCommand creates "get-orbit-data-upload", which prints the
// OMM, Keplerian elements or OEM an orbit-data version was converted from.
func newGetOrbitDataUploadCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get-orbit-data-upload",
		Short: "Show the OMM, Keplerian elements or OEM an orbit-data version was converted from",
		Long: `Print orbit data exactly as it was uploaded, before StellarStation converted it
to the TLE it stores: an OMM or OEM document as it was given, or Keplerian
elements (and an OMM given as fields) as JSON. Find the orbit-data ID with
get-orbit-data-history, whose UPLOAD_KEPT column says which versions have one.

  stellar satellite get-orbit-data-upload --satellite-id <id> --orbit-data-id <orbit-data-id>
  stellar satellite get-orbit-data-upload --satellite-id <id> --orbit-data-id <orbit-data-id> --output-file mysat.oem`,
		RunE: runGetOrbitDataUpload,
	}
	cmd.Flags().String("satellite-id", "", "Which satellite (required)")
	cmd.Flags().String("orbit-data-id", "", "Which orbit-data version (required; the ID from get-orbit-data-history)")
	cmd.Flags().String("output-file", "", "Write the upload to this file instead of printing it")
	_ = cmd.MarkFlagRequired("satellite-id")
	_ = cmd.MarkFlagRequired("orbit-data-id")
	return cmd
}

func runGetOrbitDataUpload(cmd *cobra.Command, _ []string) error {
	client, err := newAPIClient(cmd)
	if err != nil {
		return err
	}
	satID, _ := cmd.Flags().GetString("satellite-id")
	orbitDataID, _ := cmd.Flags().GetString("orbit-data-id")
	outputFile, _ := cmd.Flags().GetString("output-file")

	upload, err := client.GetOrbitDataUpload(cmd.Context(), satID, orbitDataID)
	var apiErr *apiclient.APIError
	if errors.As(err, &apiErr) && apiErr.IsNotFound() {
		return fmt.Errorf("orbit data %s has no kept upload: it was uploaded as a TLE, synced automatically, "+
			"or uploaded before uploads were kept", orbitDataID)
	}
	if err != nil {
		return err
	}
	text, err := uploadText(upload)
	if err != nil {
		return err
	}
	if outputFile == "" {
		_, err := fmt.Fprint(cmd.OutOrStdout(), text)
		return err
	}
	if err := os.WriteFile(outputFile, []byte(text), 0o600); err != nil {
		return fmt.Errorf("could not write %s: %w", outputFile, err)
	}
	uiOKf("Wrote the %s as uploaded to %s", upload.OrbitalDataType, outputFile)
	return nil
}

// uploadText is an upload as the operator gave it: the OMM or OEM document
// itself, or the fields as indented JSON.
func uploadText(upload *apiclient.OrbitDataUpload) (string, error) {
	for _, key := range []string{"omm", "oem"} {
		if text, ok := upload.OrbitalData[key].(string); ok {
			return text, nil
		}
	}
	raw, err := json.MarshalIndent(upload.OrbitalData, "", "  ")
	if err != nil {
		return "", fmt.Errorf("encoding the upload: %w", err)
	}
	return string(raw) + "\n", nil
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
		Short:   "Upload new orbit data (TLE, OMM, Keplerian elements or OEM) for a satellite",
		Long: `Upload new orbit data for a satellite. Provide it inline with --data or from a
file with --data-file, and say what kind it is with --data-type:

  TLE        a two-line element set, or JSON {"line1": "...", "line2": "..."}
  OMM        an OMM as CCSDS KVN or XML, the JSON CelesTrak and Space-Track serve,
             or a JSON object of OMM fields. Its SGP4 mean elements are copied
             into a TLE.
  KEPLERIAN  osculating elements as JSON (see below). A TLE is fitted to a day of
             the orbit they describe, since a TLE holds SGP4 mean elements.
  OEM        a CCSDS OEM ephemeris (KVN or XML). A TLE is fitted to its states;
             a fit that deviates more than 10 km is refused.

Whatever is uploaded is stored and sent to ground stations as a TLE. Use
--dry-run to see that TLE, and how closely a fitted one follows your orbit,
without storing it.

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

  (the file may be KVN, XML or JSON, e.g. CelesTrak's gp.php?CATNR=<id>&FORMAT=kvn)

Check an OEM before storing it:
  stellar satellite add-orbit-data --satellite-id <id> --data-type OEM --data-file ./mysat.oem --dry-run

Upload Keplerian elements:
  stellar satellite add-orbit-data --satellite-id <id> --data-type KEPLERIAN --data-file ./elements.json

  where the JSON looks like (true_anomaly_deg or mean_anomaly_deg; frame EME2000,
  GCRF or TEME; norad_id, international_designator and bstar are optional):
  {"epoch": "2026-11-12T00:00:00Z", "semi_major_axis_km": 6878.137,
   "eccentricity": 0.001, "inclination_deg": 97.4, "raan_deg": 120,
   "arg_of_perigee_deg": 90, "true_anomaly_deg": 0, "frame": "EME2000"}`,
		RunE: runAddTLE,
	}
	addOutputFlag(cmd)
	cmd.Flags().String("satellite-id", "", "Which satellite (required; from list-satellites)")
	cmd.Flags().String("data-type", "TLE", "Kind of orbit data being uploaded: TLE, OMM, KEPLERIAN or OEM")
	cmd.Flags().Bool("dry-run", false, "Check and convert the orbit data and show the resulting TLE, without storing it")
	cmd.Flags().
		String("source", "MANUAL", "Where this data came from: manual, space-track, or celestrak")
	cmd.Flags().String("epoch", "", "Epoch of the orbit data, e.g. 2026-01-15T10:00:00Z (default: now)")
	cmd.Flags().String("data", "", "The orbit data itself, in the form --data-type names (use this or --data-file)")
	cmd.Flags().String("data-file", "", "Path to a file containing the orbit data")
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
	if !slices.Contains(orbitDataTypes, dataType) {
		return fmt.Errorf("unknown --data-type %q: use %s", dataType, strings.Join(orbitDataTypes, ", "))
	}
	dryRun, _ := cmd.Flags().GetBool("dry-run")
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

	if dryRun {
		checked, checkErr := client.ValidateOrbitData(cmd.Context(), satID, apiclient.ValidateOrbitDataRequest{
			OrbitalDataType: dataType,
			OrbitalData:     orbitalData,
		})
		if checkErr != nil {
			return checkErr
		}
		reportOrbitFindings(checked.Warnings, checked.Errors)
		if err := renderOne(cmd, format, orbitValidationColumns, checked, ""); err != nil {
			return err
		}
		if !checked.Valid {
			return errors.New("the orbit data was rejected; nothing was stored")
		}
		return nil
	}

	epoch := time.Now().UTC()
	if epochStr != "" {
		parsed, parseErr := parseTimeFlag(epochStr)
		if parseErr != nil {
			return parseErr
		}
		epoch = *parsed
	}

	result, err := client.CreateOrbitData(cmd.Context(), satID, apiclient.CreateOrbitDataRequest{
		OrbitalDataType: dataType,
		OrbitalData:     orbitalData,
		// Every format is converted to a TLE server-side and stored as one.
		Source: normalizeOrbitDataCreateSource(source),
		Epoch:  epoch,
	})
	if rejected, ok := orbitRejection(err); ok {
		reportOrbitFindings(rejected.Warnings, rejected.Errors)
		return errors.New("the orbit data was rejected; nothing was stored")
	}
	if err != nil {
		return err
	}
	reportOrbitFindings(result.Warnings, nil)
	return renderOne(cmd, format, orbitDataColumns, result, "Orbit data uploaded and now active for scheduling.")
}

// orbitRejection reads the findings of an upload the API refused as invalid
// (422), whose body is an OrbitValidationResult.
func orbitRejection(err error) (*apiclient.OrbitValidationResult, bool) {
	var apiErr *apiclient.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusUnprocessableEntity {
		return nil, false
	}
	var result apiclient.OrbitValidationResult
	if json.Unmarshal([]byte(apiErr.RawBody), &result) != nil || len(result.Errors) == 0 {
		return nil, false
	}
	return &result, true
}

// reportOrbitFindings prints why orbit data was refused, and what is worth
// knowing about data that was accepted, on stderr so output stays parseable.
func reportOrbitFindings(warnings []string, errs []apiclient.OrbitValidationError) {
	for _, e := range errs {
		uiErrf("%s: %s", e.Location, e.Message)
	}
	for _, w := range warnings {
		uiWarnf("%s", w)
	}
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
		apiclient.UpdateOrbitParametersRequest{
			OrbitalDataSource: normalizeOrbitalDataParameterSource(source),
			NoradID:           noradID,
		})
	if err != nil {
		return err
	}
	return renderOne(cmd, format, orbitParamsColumns, result, "Orbit-data source updated.")
}

// effectiveOrbitType prefers the server's resolved type and falls back to the
// manual field for older servers.
func effectiveOrbitType(p *apiclient.OrbitParameters) string {
	if p.EffectiveOrbitType != "" {
		return p.EffectiveOrbitType
	}
	return p.OrbitType
}
