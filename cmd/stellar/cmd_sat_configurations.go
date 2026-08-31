package main

import (
	"github.com/infostellarinc/stellarstation-cli/internal/apiclient"
	"github.com/infostellarinc/stellarstation-cli/internal/printer"

	"github.com/spf13/cobra"
)

var configColumns = []printer.Column{ //nolint:gochecknoglobals
	{Header: "ID", Extract: func(r interface{}) string { return r.(*apiclient.ExecutionConfig).ID }}, //nolint:errcheck
	{Header: "NAME", Extract: func(r interface{}) string {
		return r.(*apiclient.ExecutionConfig).DisplayName //nolint:errcheck
	}},
	{Header: "SATELLITE_ID", Extract: func(r interface{}) string {
		return r.(*apiclient.ExecutionConfig).SatelliteID //nolint:errcheck
	}},
	{Header: "GS_ID", Extract: func(r interface{}) string {
		return r.(*apiclient.ExecutionConfig).GroundStationID //nolint:errcheck
	}},
}

// newListConfigurationsCommand creates the "list-configurations" subcommand,
// which surfaces the execution-config IDs needed by reserve-pass.
func newListConfigurationsCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list-configurations",
		Short: "List the ways a satellite can be scheduled at a ground station",
		Long: `List the "execution configurations" available for a satellite at a given
ground station.

A configuration describes how a contact will be run (which radio settings and
channels are used). When you book a contact with reserve-pass, you pick one of
these by its ID, so run this first to find the right --execution-config-id.

Example:
  stellar satellite list-configurations \
      --satellite-id <id> --ground-station-id <id>`,
		RunE: runListConfigurations,
	}
	addOutputFlag(cmd)
	cmd.Flags().String("satellite-id", "", "Which satellite (required; from list-satellites)")
	cmd.Flags().String("ground-station-id", "", "Which ground station (required)")
	_ = cmd.MarkFlagRequired("satellite-id")
	_ = cmd.MarkFlagRequired("ground-station-id")
	return cmd
}

func runListConfigurations(cmd *cobra.Command, _ []string) error {
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

	configs, err := client.ListConfigurations(cmd.Context(), satID, gsID)
	if err != nil {
		return err
	}

	rows := make([]interface{}, len(configs))
	for i := range configs {
		rows[i] = &configs[i]
	}
	return renderList(cmd, format, configColumns, rows, "configuration",
		"No configurations for this satellite + ground-station pair. Double-check both IDs are correct.")
}
