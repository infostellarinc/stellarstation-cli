package main

import (
	"github.com/spf13/cobra"
)

// newSatelliteCommand creates the "satellite" parent command that groups all
// satellite-related subcommands (open-stream, list-passes, reserve-pass, etc.).
func newSatelliteCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "satellite",
		Aliases: []string{"sat"},
		Short:   "Work with your satellites: contacts, telemetry, and orbit data",
		Long: `Everything you do with a satellite, grouped in one place.

  See your fleet        list-satellites
  Plan a pass           list-visibilities, list-configurations
  Book a pass           reserve-pass, update-pass, cancel-pass, list-passes, get-pass
  Run a pass live       open-stream   (receive telemetry, send commands)
  Manage orbit data     add-tle, get-tle, get-tle-history, set-tle-source, get-tle-source

Run any command with --help to see what it does and an example.`,
	}

	cmd.AddCommand(
		newOpenStreamCommand(),
		newListSatellitesCommand(),
		newListVisibilitiesCommand(),
		newListConfigurationsCommand(),
		newListPassesCommand(),
		newGetPassCommand(),
		newReservePassCommand(),
		newUpdatePassCommand(),
		newCancelPassCommand(),
		newAddTLECommand(),
		newGetTLECommand(),
		newGetTLEHistoryCommand(),
		newSetTLESourceCommand(),
		newGetTLESourceCommand(),
	)

	return cmd
}
