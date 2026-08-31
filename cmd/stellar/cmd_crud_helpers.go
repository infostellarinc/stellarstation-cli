package main

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/infostellarinc/stellarstation-cli/internal/apiclient"
	"github.com/infostellarinc/stellarstation-cli/internal/auth"
	"github.com/infostellarinc/stellarstation-cli/internal/printer"

	"github.com/spf13/cobra"
)

const (
	timeFormat     = "2006-01-02T15:04:05Z"
	timeFormatHint = "RFC3339 (e.g. 2026-01-15T10:00:00Z)"

	// envCredentialsPath is honoured by newTokenSource when --credentials is
	// omitted.
	envCredentialsPath = "STELLAR_CREDENTIALS" //nolint:gosec
)

// addAPIFlags registers the --api-url and --credentials persistent flags on a
// parent command so every subcommand inherits them.
func addAPIFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().String(
		"api-url",
		"",
		"StellarStation API address (usually set once via the STELLAR_API_URL environment variable)",
	)
	cmd.PersistentFlags().String(
		"credentials",
		"",
		"Use a specific API key file instead of the one you activated (default: your activated key)",
	)
}

// addOutputFlag registers the --output flag for CRUD commands.
func addOutputFlag(cmd *cobra.Command) {
	cmd.Flags().StringP("output", "o", "wide", "How to show results: wide (readable table, default), csv, or json")
}

// newTokenSource resolves credentials from --credentials / $STELLAR_CREDENTIALS
// / the default ~/.stellarstation/credentials.json location and returns an
// OAuth2 token source that the caller can hand to apiclient.New and
// fetchAuthorizerCredentials. The resolved path is returned so callers can
// include it in error messages.
func newTokenSource(cmd *cobra.Command) (auth.TokenSource, string, error) {
	path, err := resolveCredentialsPath(cmd)
	if err != nil {
		return nil, "", err
	}
	creds, err := auth.Load(path)
	if err != nil {
		if os.IsNotExist(errors.Unwrap(err)) {
			return nil, path, fmt.Errorf(
				"no API key set up yet (looked in %s)\n"+
					"Run `stellar auth activate-api-key <key-file>` first; download a key from "+
					"the console under Organization > API Keys",
				path,
			)
		}
		return nil, path, fmt.Errorf(
			"could not read your saved API key at %s: %w\n"+
				"Re-activate it with `stellar auth activate-api-key <key-file>`",
			path, err,
		)
	}
	ts, err := auth.NewOAuth2TokenSource(creds, nil)
	if err != nil {
		return nil, path, err
	}
	return ts, path, nil
}

// resolveCredentialsPath picks the credentials file based on the --credentials
// flag, the STELLAR_CREDENTIALS env var, and the default location, in that
// order of preference.
func resolveCredentialsPath(cmd *cobra.Command) (string, error) {
	if flag, _ := cmd.Flags().GetString("credentials"); flag != "" {
		return flag, nil
	}
	if env := os.Getenv(envCredentialsPath); env != "" {
		return env, nil
	}
	return auth.DefaultCredentialsPath()
}

// resolveAPIBaseURL returns the base URL used for REST and authorizer HTTP
// calls. If the user did not override --api-url, STELLAR_API_URL is applied so
// dev shells can point at their environment without repeating the flag.
func resolveAPIBaseURL(cmd *cobra.Command) (string, error) {
	var apiURL string
	if f := cmd.Flags().Lookup("api-url"); f != nil && !f.Changed {
		if env := strings.TrimSpace(os.Getenv("STELLAR_API_URL")); env != "" {
			apiURL = env
		}
	}
	if apiURL == "" {
		apiURL, _ = cmd.Flags().GetString("api-url")
	}
	if strings.TrimSpace(apiURL) == "" {
		return "", errors.New(
			"no API address configured: set the STELLAR_API_URL environment variable or pass --api-url")
	}
	apiURL = strings.TrimRight(strings.TrimSpace(apiURL), "/")

	// Every request to the API carries a bearer token, so the address must use
	// HTTPS (or name a loopback address for local testing).
	if err := auth.CheckEndpointScheme("API address", apiURL); err != nil {
		return "", err
	}
	return apiURL, nil
}

// newAPIClient constructs an apiclient.Client authenticated with a Cognito
// JWT obtained from the caller's credentials.
func newAPIClient(cmd *cobra.Command) (*apiclient.Client, error) {
	ts, _, err := newTokenSource(cmd)
	if err != nil {
		return nil, err
	}
	apiURL, err := resolveAPIBaseURL(cmd)
	if err != nil {
		return nil, err
	}
	return apiclient.New(apiURL, ts), nil
}

// getOutputFormat reads and parses the --output flag.
func getOutputFormat(cmd *cobra.Command) (printer.Format, error) {
	raw, _ := cmd.Flags().GetString("output")
	return printer.ParseFormat(raw)
}

// requireOneArg returns a positional-argument validator for commands that take
// exactly one value. Instead of cobra's terse "accepts 1 arg(s), received 0",
// it explains what the value is and shows a worked example. what is a short
// noun phrase (e.g. "a pass ID"); example is a full command line to copy.
func requireOneArg(what, example string) cobra.PositionalArgs {
	return func(_ *cobra.Command, args []string) error {
		switch {
		case len(args) == 0:
			return fmt.Errorf("this command needs %s.\nFor example:\n  %s", what, example)
		case len(args) > 1:
			return fmt.Errorf(
				"this command takes just %s, but received %d values.\nFor example:\n  %s",
				what, len(args), example,
			)
		}
		return nil
	}
}

// humanTable reports whether the format is the readable table meant for people
// (as opposed to csv/json, which are meant for scripts and must be kept clean).
func humanTable(format printer.Format) bool { return format == printer.FormatWide }

// plural returns "1 satellite" / "3 satellites" for a singular noun (or noun
// phrase). It pluralises the final word, handling the common "-y" to "-ies" and
// "-s/-x/-ch/-sh" to "-es" cases so summaries read naturally.
func plural(n int, singular string) string {
	if n == 1 {
		return "1 " + singular
	}
	words := strings.Fields(singular)
	last := words[len(words)-1]
	switch {
	case strings.HasSuffix(last, "y") && len(last) > 1 && !strings.ContainsRune("aeiou", rune(last[len(last)-2])):
		last = last[:len(last)-1] + "ies"
	case strings.HasSuffix(last, "s"), strings.HasSuffix(last, "x"),
		strings.HasSuffix(last, "ch"), strings.HasSuffix(last, "sh"):
		last += "es"
	default:
		last += "s"
	}
	words[len(words)-1] = last
	return fmt.Sprintf("%d %s", n, strings.Join(words, " "))
}

// renderList prints a list result. For the human table format it adds context
// on stderr (a clear "nothing found" message when the list is empty, or a
// count line after the table) while leaving stdout pristine for csv/json so
// piping still works. emptyMsg is shown (as a hint) when there are no rows.
func renderList(
	cmd *cobra.Command,
	format printer.Format,
	columns []printer.Column,
	rows []interface{},
	singular, emptyMsg string,
) error {
	if humanTable(format) && len(rows) == 0 {
		uiWarnf("No %ss found.", singular)
		if emptyMsg != "" {
			uiDimf("  %s", emptyMsg)
		}
		return nil
	}
	if err := printer.Print(cmd.OutOrStdout(), format, columns, rows); err != nil {
		return err
	}
	if humanTable(format) {
		uiDimf("  %s", plural(len(rows), singular))
	}
	return nil
}

// renderOne prints a single record (the detail view returned by get/reserve/
// update/cancel/add-tle style commands). successMsg, when non-empty, is shown
// as a green confirmation on stderr for the human table format only.
func renderOne(
	cmd *cobra.Command,
	format printer.Format,
	columns []printer.Column,
	row interface{},
	successMsg string,
) error {
	if humanTable(format) && successMsg != "" {
		uiOKf("%s", successMsg)
	}
	return printer.Print(cmd.OutOrStdout(), format, columns, []interface{}{row})
}

// parseTimeFlag parses a time string from a flag value.
func parseTimeFlag(value string) (*time.Time, error) {
	if value == "" {
		return nil, nil //nolint:nilnil
	}
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil, fmt.Errorf(
			"the time %q is not in the expected format.\n"+
				"Use %s, for example 2026-01-15T10:00:00Z (the trailing Z means UTC)",
			value, timeFormatHint,
		)
	}
	return &t, nil
}

// defaultLookahead is the time window that the list commands query when the
// caller omits --start / --stop.
const defaultLookahead = 7 * 24 * time.Hour

// resolveListWindow reads the --start / --stop flags and applies defaults so
// list commands always query a bounded window: start defaults to now, and stop
// defaults to start + defaultLookahead (a 7-day lookahead). now is passed in so
// the behaviour is testable.
func resolveListWindow(cmd *cobra.Command, now time.Time) (start, stop *time.Time, err error) {
	startStr, _ := cmd.Flags().GetString("start")
	stopStr, _ := cmd.Flags().GetString("stop")

	start, err = parseTimeFlag(startStr)
	if err != nil {
		return nil, nil, err
	}
	stop, err = parseTimeFlag(stopStr)
	if err != nil {
		return nil, nil, err
	}

	if start == nil {
		start = &now
	}
	if stop == nil {
		s := start.Add(defaultLookahead)
		stop = &s
	}
	return start, stop, nil
}

// ---- Shared table-column formatting ----------------------------------------
//
// These helpers keep the passColumns and visibilityColumns definitions DRY:
// both surfaces render the same satellite/ground-station references, elevation
// figures, and time ranges.

// refDisplayName returns the human-friendly name, falling back to the id when
// the name is empty. Used for satellite, ground-station, and execution-config
// references.
func refDisplayName(name, id string) string {
	if name != "" {
		return name
	}
	return id
}

// formatCoordinate renders an optional latitude/longitude value, or "" if unset.
func formatCoordinate(v *float64) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%.6f", *v)
}

// formatMaxElevation renders a max-elevation angle in degrees (no unit
// symbol), or "" if unset (the API omits it as a zero value).
func formatMaxElevation(degrees float64) string {
	if degrees <= 0 {
		return ""
	}
	return fmt.Sprintf("%.1f", degrees)
}

// formatOptionalTime renders an optional timestamp using timeFormat, or "".
func formatOptionalTime(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.Format(timeFormat)
}

// intervalStart and intervalStop render the bounds of an optional interval
// (e.g. a pass/visibility AOS-LOS window), or "" when the interval is absent.
func intervalStart(i *apiclient.Interval) string {
	if i == nil {
		return ""
	}
	return i.Start.Format(timeFormat)
}

func intervalStop(i *apiclient.Interval) string {
	if i == nil {
		return ""
	}
	return i.Stop.Format(timeFormat)
}

// satelliteColumns builds the shared satellite display columns (name and ID)
// for any row whose satellite reference is returned by get.
func satelliteColumns(get func(row interface{}) apiclient.SatelliteRef) []printer.Column {
	return []printer.Column{
		{Header: "SATELLITE", Extract: func(r interface{}) string {
			s := get(r)
			return refDisplayName(s.Name, s.ID)
		}},
		{Header: "SATELLITE_ID", Extract: func(r interface{}) string { return get(r).ID }},
	}
}

// groundStationColumns builds the shared ground-station display columns (name,
// ID, organization, latitude, longitude) for any row whose ground-station
// reference is returned by get. Both list-passes and list-visibilities embed
// these.
func groundStationColumns(get func(row interface{}) apiclient.GroundStationRef) []printer.Column {
	return []printer.Column{
		{Header: "GS", Extract: func(r interface{}) string {
			gs := get(r)
			return refDisplayName(gs.Name, gs.ID)
		}},
		{Header: "GS_ID", Extract: func(r interface{}) string { return get(r).ID }},
		{Header: "GS_ORG", Extract: func(r interface{}) string { return get(r).Organization }},
		{Header: "GS_LATITUDE", Extract: func(r interface{}) string { return formatCoordinate(get(r).Latitude) }},
		{Header: "GS_LONGITUDE", Extract: func(r interface{}) string { return formatCoordinate(get(r).Longitude) }},
	}
}
