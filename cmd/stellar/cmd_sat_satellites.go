package main

import (
	"strconv"

	"github.com/infostellarinc/stellarstation-cli/internal/apiclient"
	"github.com/infostellarinc/stellarstation-cli/internal/printer"

	"github.com/spf13/cobra"
)

var satelliteColumnDefs = []printer.Column{ //nolint:gochecknoglobals
	{
		Header:  "ID",
		Extract: func(r interface{}) string { return r.(*apiclient.Satellite).ID },
	}, //nolint:errcheck
	{
		Header:  "NAME",
		Extract: func(r interface{}) string { return r.(*apiclient.Satellite).DisplayName },
	}, //nolint:errcheck
	{
		Header:  "ORG",
		Extract: func(r interface{}) string { return r.(*apiclient.Satellite).OrganizationID },
	}, //nolint:errcheck
	{Header: "SCHEDULABLE", Extract: func(r interface{}) string {
		return strconv.FormatBool(r.(*apiclient.Satellite).Schedulable) //nolint:errcheck
	}},
	{Header: "CREATED_AT", Extract: func(r interface{}) string {
		return formatOptionalTime(r.(*apiclient.Satellite).CreatedAt) //nolint:errcheck
	}},
	{Header: "UPDATED_AT", Extract: func(r interface{}) string {
		return formatOptionalTime(r.(*apiclient.Satellite).UpdatedAt) //nolint:errcheck
	}},
}

// newListSatellitesCommand creates the "list-satellites" subcommand.
func newListSatellitesCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list-satellites",
		Short: "Show the satellites you can access",
		Long: `List every satellite your API key can see, with its ID and name.

You will need a satellite ID for most other commands, so this is usually the
first thing to run.

Example:
  stellar satellite list-satellites`,
		RunE: runListSatellites,
	}
	addOutputFlag(cmd)
	return cmd
}

func runListSatellites(cmd *cobra.Command, _ []string) error {
	client, err := newAPIClient(cmd)
	if err != nil {
		return err
	}
	format, err := getOutputFormat(cmd)
	if err != nil {
		return err
	}

	satellites, err := client.ListSatellites(cmd.Context())
	if err != nil {
		return err
	}

	rows := make([]interface{}, len(satellites))
	for i := range satellites {
		rows[i] = &satellites[i]
	}
	return renderList(cmd, format, satelliteColumnDefs, rows, "satellite",
		"Your API key may not have access to any satellites yet; check with your StellarStation contact.")
}
