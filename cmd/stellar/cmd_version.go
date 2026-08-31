package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

// newVersionCommand creates the "version" subcommand that prints build version
// information.
func newVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the CLI version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Fprintln(cmd.OutOrStdout(), cliVersion())
		},
	}
}
