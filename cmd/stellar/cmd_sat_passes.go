package main

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/infostellarinc/stellarstation-cli/internal/apiclient"
	"github.com/infostellarinc/stellarstation-cli/internal/printer"

	"github.com/spf13/cobra"
)

//nolint:gochecknoglobals
var passColumns = buildPassColumns()

// getPassColumns extends passColumns with the overlapping-passes column. Only
// GET /v1/passes/{id} computes `overlapping_passes` (the list endpoint does
// not), so only get-pass shows the column.
//
//nolint:gochecknoglobals
var getPassColumns = append(append([]printer.Column{}, passColumns...),
	printer.Column{Header: "OVERLAPPING_SATELLITE_PASS_IDS", Extract: func(r interface{}) string {
		p := r.(*apiclient.Pass) //nolint:errcheck
		ids := make([]string, 0, len(p.OverlappingPasses))
		for _, o := range p.OverlappingPasses {
			ids = append(ids, o.PassID)
		}
		return strings.Join(ids, " ")
	}},
)

// buildPassColumns assembles the list-passes table columns. The satellite and
// ground-station columns are shared with list-visibilities via satelliteColumns
// and groundStationColumns.
func buildPassColumns() []printer.Column {
	cols := []printer.Column{
		{Header: "ID", Extract: func(r interface{}) string { return r.(*apiclient.Pass).ID }}, //nolint:errcheck
	}
	cols = append(cols, satelliteColumns(func(r interface{}) apiclient.SatelliteRef {
		return r.(*apiclient.Pass).Satellite //nolint:errcheck
	})...)
	cols = append(cols, groundStationColumns(func(r interface{}) apiclient.GroundStationRef {
		return r.(*apiclient.Pass).GroundStation //nolint:errcheck
	})...)
	return append(cols,
		printer.Column{Header: "EXEC_CONFIG", Extract: func(r interface{}) string {
			p := r.(*apiclient.Pass) //nolint:errcheck
			return refDisplayName(p.ExecutionConfigName, p.ExecutionConfigID)
		}},
		printer.Column{Header: "STATUS", Extract: func(r interface{}) string {
			p := r.(*apiclient.Pass) //nolint:errcheck
			if p.Execution != nil {
				return p.Execution.Status
			}
			return ""
		}},
		printer.Column{Header: "AOS", Extract: func(r interface{}) string {
			return intervalStart(r.(*apiclient.Pass).Visibility) //nolint:errcheck
		}},
		printer.Column{Header: "LOS", Extract: func(r interface{}) string {
			return intervalStop(r.(*apiclient.Pass).Visibility) //nolint:errcheck
		}},
		printer.Column{Header: "MAX_ELEVATION", Extract: func(r interface{}) string {
			return formatMaxElevation(r.(*apiclient.Pass).MaxElevationDegrees) //nolint:errcheck
		}},
		printer.Column{Header: "MAX_ELEVATION_TIME", Extract: func(r interface{}) string {
			return formatOptionalTime(r.(*apiclient.Pass).MaxElevationTime) //nolint:errcheck
		}},
		printer.Column{Header: "GS_CONFLICT", Extract: func(r interface{}) string {
			return r.(*apiclient.Pass).ConflictStatus //nolint:errcheck
		}},
	)
}

// newListPassesCommand creates the "list-passes" subcommand.
func newListPassesCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list-passes",
		Short: "List passes you have booked",
		Long: `List the passes you have booked, with their status, satellite, ground station
and scheduled window.

Covers the next 7 days by default. Narrow it down with --satellite-id, a
--start/--stop window, or --execution-status.

Examples:
  stellar satellite list-passes
  stellar satellite list-passes --satellite-id <id>`,
		RunE: runListPasses,
	}
	addOutputFlag(cmd)
	cmd.Flags().
		StringSlice("satellite-id", nil, "Only show passes for this satellite (repeat or comma-separate for several)")
	cmd.Flags().String("start", "", "Earliest scheduled time to include, e.g. 2026-01-15T10:00:00Z (default: now)")
	cmd.Flags().
		String("stop", "", "Latest scheduled time to include, e.g. 2026-01-22T10:00:00Z (default: 7 days after start)")
	cmd.Flags().
		String("execution-status", "", "Only show passes with this status (e.g. RESERVED, EXECUTING, COMPLETED)")
	return cmd
}

func runListPasses(cmd *cobra.Command, _ []string) error {
	client, err := newAPIClient(cmd)
	if err != nil {
		return err
	}
	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}

	satIDs, _ := cmd.Flags().GetStringSlice("satellite-id")
	status, _ := cmd.Flags().GetString("execution-status")

	start, stop, err := resolveListWindow(cmd, time.Now().UTC())
	if err != nil {
		return err
	}

	passes, err := client.ListPasses(cmd.Context(), apiclient.ListPassesOpts{
		SatelliteIDs:    satIDs,
		Start:           start,
		Stop:            stop,
		ExecutionStatus: status,
	})
	if err != nil {
		return err
	}

	rows := make([]interface{}, len(passes))
	for i := range passes {
		rows[i] = &passes[i]
	}
	return renderList(cmd, format, passColumns, rows, "pass",
		"No booked passes in this window. Reserve one with `stellar satellite reserve-pass`.")
}

// newReservePassCommand creates the "reserve-pass" subcommand.
func newReservePassCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "reserve-pass",
		Short: "Book a pass",
		Long: `Book (reserve) a pass so a ground station will contact your satellite in the
chosen window.

You will need three IDs and a time window:
  --satellite-id         from list-satellites
  --ground-station-id    the ground station to use
  --execution-config-id  from list-configurations (for that satellite + station)
  --booking-start/-stop  the window you want, e.g. 2026-01-15T10:00:00Z

Example:
  stellar satellite reserve-pass \
      --satellite-id <id> --ground-station-id <id> --execution-config-id <id> \
      --booking-start 2026-01-15T10:00:00Z --booking-stop 2026-01-15T10:12:00Z`,
		RunE: runReservePass,
	}
	addOutputFlag(cmd)
	cmd.Flags().String("satellite-id", "", "Which satellite to contact (required; from list-satellites)")
	cmd.Flags().String("ground-station-id", "", "Which ground station to use (required)")
	cmd.Flags().String("execution-config-id", "", "Which configuration to run (required; from list-configurations)")
	cmd.Flags().String("booking-start", "", "When the pass should start, e.g. 2026-01-15T10:00:00Z (required)")
	cmd.Flags().String("booking-stop", "", "When the pass should end, e.g. 2026-01-15T10:12:00Z (required)")
	_ = cmd.MarkFlagRequired("satellite-id")
	_ = cmd.MarkFlagRequired("ground-station-id")
	_ = cmd.MarkFlagRequired("execution-config-id")
	_ = cmd.MarkFlagRequired("booking-start")
	_ = cmd.MarkFlagRequired("booking-stop")
	return cmd
}

func runReservePass(cmd *cobra.Command, _ []string) error {
	client, err := newAPIClient(cmd)
	if err != nil {
		return err
	}
	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}

	satID, _ := cmd.Flags().GetString("satellite-id")
	gsID, _ := cmd.Flags().GetString("ground-station-id")
	execCfg, _ := cmd.Flags().GetString("execution-config-id")
	bStartStr, _ := cmd.Flags().GetString("booking-start")
	bStopStr, _ := cmd.Flags().GetString("booking-stop")

	bStart, err := parseTimeFlag(bStartStr)
	if err != nil {
		return err
	}
	bStop, err := parseTimeFlag(bStopStr)
	if err != nil {
		return err
	}

	// parseTimeFlag returns nil for an empty value; booking a pass needs both
	// ends of the window, so reject the request here rather than crash below.
	if bStart == nil || bStop == nil {
		return errors.New("--booking-start and --booking-stop are required")
	}

	pass, err := client.CreatePass(cmd.Context(), apiclient.PassCreateRequest{
		SatelliteID:       satID,
		GroundStationID:   gsID,
		ExecutionConfigID: execCfg,
		Booking:           apiclient.Interval{Start: *bStart, Stop: *bStop},
		Scheduled:         apiclient.Interval{Start: *bStart, Stop: *bStop},
	})
	if err != nil {
		return err
	}
	return renderOne(cmd, format, passColumns, pass,
		fmt.Sprintf("Pass booked (%s). Stream it later with `open-stream --pass-id %s`.", passID(pass), passID(pass)))
}

// passID returns a pass's ID for use in confirmation messages, or "the new
// pass" if the field is somehow empty.
func passID(p *apiclient.Pass) string {
	if p != nil && p.ID != "" {
		return p.ID
	}
	return "the new pass"
}

// newGetPassCommand creates the "get-pass" subcommand, which fetches a single
// pass by ID via GET /v1/passes/{passId}.
func newGetPassCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get-pass <pass-id>",
		Short: "Show the details of one booked pass",
		Long: `Show the full details of a single pass, looked up by its ID (the ID shown by
list-passes or returned when you reserve one).

Example:
  stellar satellite get-pass a4c3984c-dc01-4af1-81bc-d8bf7acc70c7`,
		Args: requireOneArg(
			"the ID of the pass to show (from list-passes)",
			"stellar satellite get-pass a4c3984c-dc01-4af1-81bc-d8bf7acc70c7",
		),
		RunE: runGetPass,
	}
	addOutputFlag(cmd)
	return cmd
}

func runGetPass(cmd *cobra.Command, args []string) error {
	client, err := newAPIClient(cmd)
	if err != nil {
		return err
	}
	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}

	pass, err := client.GetPass(cmd.Context(), args[0])
	if err != nil {
		return err
	}
	return renderOne(cmd, format, getPassColumns, pass, "")
}

// newUpdatePassCommand creates the "update-pass" subcommand, which reschedules
// (or re-points the execution config of) a booked pass via PUT /v1/passes/{passId}.
func newUpdatePassCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "update-pass <pass-id>",
		Short: "Reschedule or reconfigure a booked pass",
		Long: `Change a pass you have already booked: move it to a new time window and/or
switch which configuration it runs.

To move it, pass BOTH --scheduled-start and --scheduled-stop. To change the
configuration, pass --execution-config-id. You can do either or both.

Example:
  stellar satellite update-pass <pass-id> \
      --scheduled-start 2026-01-15T10:05:00Z --scheduled-stop 2026-01-15T10:15:00Z`,
		Args: requireOneArg(
			"the ID of the pass to update (from list-passes)",
			"stellar satellite update-pass a4c3984c-dc01-4af1-81bc-d8bf7acc70c7 --scheduled-start ... --scheduled-stop ...",
		),
		RunE: runUpdatePass,
	}
	addOutputFlag(cmd)
	cmd.Flags().
		String("scheduled-start", "", "New start time, e.g. 2026-01-15T10:05:00Z (use together with --scheduled-stop)")
	cmd.Flags().
		String("scheduled-stop", "", "New end time, e.g. 2026-01-15T10:15:00Z (use together with --scheduled-start)")
	cmd.Flags().String("execution-config-id", "", "Switch to a different configuration (from list-configurations)")
	return cmd
}

func runUpdatePass(cmd *cobra.Command, args []string) error {
	client, err := newAPIClient(cmd)
	if err != nil {
		return err
	}
	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}

	startStr, _ := cmd.Flags().GetString("scheduled-start")
	stopStr, _ := cmd.Flags().GetString("scheduled-stop")
	execCfg, _ := cmd.Flags().GetString("execution-config-id")

	var req apiclient.PassUpdateRequest
	if startStr != "" || stopStr != "" {
		if startStr == "" || stopStr == "" {
			return errors.New("to reschedule, give both --scheduled-start and --scheduled-stop (not just one)")
		}
		start, err := parseTimeFlag(startStr)
		if err != nil {
			return err
		}
		stop, err := parseTimeFlag(stopStr)
		if err != nil {
			return err
		}
		req.Scheduled = &apiclient.Interval{Start: *start, Stop: *stop}
	}
	if execCfg != "" {
		req.ExecutionConfigID = &execCfg
	}
	if req.Scheduled == nil && req.ExecutionConfigID == nil {
		return errors.New(
			"nothing to change; pass --scheduled-start and --scheduled-stop to reschedule, " +
				"and/or --execution-config-id to switch configuration",
		)
	}

	pass, err := client.UpdatePass(cmd.Context(), args[0], req)
	if err != nil {
		return err
	}
	return renderOne(cmd, format, passColumns, pass, "Pass updated.")
}

// newCancelPassCommand creates the "cancel-pass" subcommand.
func newCancelPassCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cancel-pass <pass-id>",
		Short: "Cancel a booked pass",
		Long: `Cancel a pass you have booked, freeing the ground station for that window.

Example:
  stellar satellite cancel-pass a4c3984c-dc01-4af1-81bc-d8bf7acc70c7`,
		Args: requireOneArg(
			"the ID of the pass to cancel (from list-passes)",
			"stellar satellite cancel-pass a4c3984c-dc01-4af1-81bc-d8bf7acc70c7",
		),
		RunE: runCancelPass,
	}
	addOutputFlag(cmd)
	return cmd
}

func runCancelPass(cmd *cobra.Command, args []string) error {
	client, err := newAPIClient(cmd)
	if err != nil {
		return err
	}
	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}

	pass, err := client.CancelPass(cmd.Context(), args[0])
	if err != nil {
		return err
	}
	return renderOne(cmd, format, passColumns, pass, "Pass cancelled.")
}
