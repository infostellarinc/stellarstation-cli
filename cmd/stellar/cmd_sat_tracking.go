package main

import (
	"errors"
	"strings"

	"github.com/infostellarinc/stellarstation-cli/internal/apiclient"
	"github.com/infostellarinc/stellarstation-cli/internal/printer"

	"github.com/spf13/cobra"
)

var trackingSettingColumns = []printer.Column{ //nolint:gochecknoglobals
	{
		Header:  "GROUND_STATION",
		Extract: func(r interface{}) string { return r.(*apiclient.TrackingSetting).GroundStationName }, //nolint:errcheck
	},
	{
		Header:  "GROUND_STATION_ID",
		Extract: func(r interface{}) string { return r.(*apiclient.TrackingSetting).GroundStationID }, //nolint:errcheck
	},
	{Header: "ALLOWED_MODES", Extract: func(r interface{}) string {
		return strings.Join(r.(*apiclient.TrackingSetting).AllowedModes, ",") //nolint:errcheck
	}},
	{
		Header:  "DEFAULT_MODE",
		Extract: func(r interface{}) string { return r.(*apiclient.TrackingSetting).DefaultMode }, //nolint:errcheck
	},
	{
		Header:  "MODE",
		Extract: func(r interface{}) string { return r.(*apiclient.TrackingSetting).Effective.Mode }, //nolint:errcheck
	},
	{Header: "LOCKED", Extract: func(r interface{}) string {
		s := r.(*apiclient.TrackingSetting) //nolint:errcheck
		if !s.Effective.Locked {
			return "false"
		}
		return "true (" + s.Effective.Reason + ")"
	}},
}

// newGetTrackingSettingsCommand creates the "get-tracking-settings" subcommand.
func newGetTrackingSettingsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "get-tracking-settings",
		Short: "Show the pass tracking mode new passes get, per ground station",
		Long: `Show the pass tracking modes for each ground station the satellite can be
scheduled at. TRACK_STATIC_OFFSETS keeps a pass aligned with the satellite's
element set: when a new TLE shifts the predicted pass, the booked window
follows it. NONE keeps the booked window as it is.

MODE is what a new pass gets when reserve-pass does not say otherwise.
ALLOWED_MODES are the modes your operator lets you choose from. LOCKED means a
booking cannot choose another mode, either because the operator allows one
mode or because the satellite is geosynchronous and always in view.

Example:
  stellar satellite get-tracking-settings --satellite-id <id>`,
		RunE: runGetTrackingSettings,
	}
	addOutputFlag(cmd)
	cmd.Flags().String("satellite-id", "", "Which satellite (required; from list-satellites)")
	_ = cmd.MarkFlagRequired("satellite-id")
	return cmd
}

func runGetTrackingSettings(cmd *cobra.Command, _ []string) error {
	client, err := newAPIClient(cmd)
	if err != nil {
		return err
	}
	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}
	satID, _ := cmd.Flags().GetString("satellite-id")
	settings, err := client.ListTrackingSettings(cmd.Context(), satID)
	if err != nil {
		return err
	}
	rows := make([]interface{}, 0, len(settings))
	for i := range settings {
		rows = append(rows, &settings[i])
	}
	return renderList(cmd, format, trackingSettingColumns, rows, "ground station",
		"The satellite has no schedulable configuration at any ground station.")
}

// newSetTrackingDefaultCommand creates the "set-tracking-default" subcommand.
func newSetTrackingDefaultCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "set-tracking-default",
		Short: "Choose the pass tracking mode new passes get at a ground station",
		Long: `Set the default pass tracking mode for one satellite and ground station pair.
It applies to passes reserved without an explicit tracking mode; a booking can
still choose another allowed mode.

The mode must be one of the pair's allowed modes (see get-tracking-settings).
The command is refused while the pair is locked by your operator or because
the satellite is geosynchronous.

Examples:
  stellar satellite set-tracking-default --satellite-id <id> --ground-station-id <id> --mode TRACK_STATIC_OFFSETS
  stellar satellite set-tracking-default --satellite-id <id> --ground-station-id <id> --mode NONE`,
		RunE: runSetTrackingDefault,
	}
	addOutputFlag(cmd)
	cmd.Flags().String("satellite-id", "", "Which satellite (required; from list-satellites)")
	cmd.Flags().String("ground-station-id", "", "Which ground station (required)")
	cmd.Flags().String("mode", "", "Default tracking mode: TRACK_STATIC_OFFSETS to follow the TLE, NONE to keep the booked window (required)")
	_ = cmd.MarkFlagRequired("satellite-id")
	_ = cmd.MarkFlagRequired("ground-station-id")
	_ = cmd.MarkFlagRequired("mode")
	return cmd
}

func runSetTrackingDefault(cmd *cobra.Command, _ []string) error {
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
	if gsID == "" {
		return errors.New("the --ground-station-id option is required; add it and run the command again")
	}
	mode, _ := cmd.Flags().GetString("mode")
	mode = strings.ToUpper(strings.TrimSpace(mode))
	if mode == "" {
		return errors.New("the --mode option is required; add it and run the command again")
	}
	setting, err := client.UpdateTrackingSetting(cmd.Context(), satID, gsID, mode)
	if err != nil {
		return err
	}
	return renderOne(cmd, format, trackingSettingColumns, setting, "Tracking default saved.")
}
