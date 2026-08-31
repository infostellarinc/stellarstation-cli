package main

import (
	"time"

	"github.com/infostellarinc/stellarstation-cli/internal/apiclient"
	"github.com/infostellarinc/stellarstation-cli/internal/printer"

	"github.com/spf13/cobra"
)

//nolint:gochecknoglobals
var visibilityColumns = buildVisibilityColumns()

// buildVisibilityColumns assembles the list-visibilities table columns. The
// satellite and ground-station columns are shared with list-passes via
// satelliteColumns and groundStationColumns.
func buildVisibilityColumns() []printer.Column {
	cols := satelliteColumns(func(r interface{}) apiclient.SatelliteRef {
		return r.(*apiclient.Visibility).Satellite //nolint:errcheck
	})
	cols = append(cols, groundStationColumns(func(r interface{}) apiclient.GroundStationRef {
		return r.(*apiclient.Visibility).GroundStation //nolint:errcheck
	})...)
	return append(cols,
		printer.Column{Header: "AOS", Extract: func(r interface{}) string {
			return intervalStart(r.(*apiclient.Visibility).Visibility) //nolint:errcheck
		}},
		printer.Column{Header: "LOS", Extract: func(r interface{}) string {
			return intervalStop(r.(*apiclient.Visibility).Visibility) //nolint:errcheck
		}},
		printer.Column{Header: "MAX_ELEVATION", Extract: func(r interface{}) string {
			return formatMaxElevation(r.(*apiclient.Visibility).MaxElevationDegrees) //nolint:errcheck
		}},
		printer.Column{Header: "MAX_ELEVATION_TIME", Extract: func(r interface{}) string {
			return formatOptionalTime(r.(*apiclient.Visibility).MaxElevationTime) //nolint:errcheck
		}},
		printer.Column{Header: "CONFLICT", Extract: func(r interface{}) string {
			return r.(*apiclient.Visibility).ConflictStatus //nolint:errcheck
		}},
	)
}

// newListVisibilitiesCommand creates the "list-visibilities" subcommand.
func newListVisibilitiesCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list-visibilities",
		Short: "List upcoming pass opportunities for a satellite",
		Long: `List the upcoming visibility windows (pass opportunities) for a satellite,
with AOS/LOS and max elevation for each.

These are opportunities, not bookings; to reserve one, use reserve-pass.
Defaults to the next 7 days when --start/--stop are omitted.

Examples:
  stellar satellite list-visibilities --satellite-id <id>
  stellar satellite list-visibilities --satellite-id <id> \
      --start 2026-01-15T00:00:00Z --stop 2026-01-16T00:00:00Z`,
		RunE: runListVisibilities,
	}
	addOutputFlag(cmd)
	cmd.Flags().StringSlice("satellite-id", nil, "Which satellite to check (required; from list-satellites)")
	cmd.Flags().String("start", "", "Earliest time to look from, e.g. 2026-01-15T10:00:00Z (default: now)")
	cmd.Flags().String("stop", "", "Latest time to look until, e.g. 2026-01-22T10:00:00Z (default: 7 days after start)")
	_ = cmd.MarkFlagRequired("satellite-id")
	return cmd
}

func runListVisibilities(cmd *cobra.Command, _ []string) error {
	client, err := newAPIClient(cmd)
	if err != nil {
		return err
	}
	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}

	satIDs, _ := cmd.Flags().GetStringSlice("satellite-id")

	start, stop, err := resolveListWindow(cmd, time.Now().UTC())
	if err != nil {
		return err
	}

	visibilities, err := client.ListVisibilities(cmd.Context(), apiclient.ListVisibilitiesOpts{
		SatelliteIDs: satIDs,
		Start:        start,
		Stop:         stop,
	})
	if err != nil {
		return err
	}

	rows := make([]interface{}, len(visibilities))
	for i := range visibilities {
		rows[i] = &visibilities[i]
	}
	return renderList(
		cmd,
		format,
		visibilityColumns,
		rows,
		"pass opportunity",
		"No pass opportunities in this time window; try widening --start/--stop, or check the satellite has current orbit data (get-tle).",
	)
}
