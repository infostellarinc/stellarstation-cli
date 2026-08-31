package main

import (
	"runtime/debug"

	"github.com/spf13/cobra"
)

// releaseVersion is the CLI version, set at build time via
// -ldflags "-X main.releaseVersion=...". Left at the placeholder it is resolved
// from the module version the binary was built from, so a `go install
// <module>/cmd/stellar@v1.2.3` build reports v1.2.3 rather than the placeholder.
//
//nolint:gochecknoglobals // Set by linker at build time.
var releaseVersion = "(devel)"

// cliVersion returns the version to print. It prefers the linker-provided value
// and falls back to the module version Go records in the binary, which is set
// for `go install module@version` but not for a plain `go build`.
func cliVersion() string {
	if releaseVersion != "(devel)" && releaseVersion != "" {
		return releaseVersion
	}
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" {
		return info.Main.Version
	}
	return releaseVersion
}

// NewRootCommand creates the top-level "stellar" command and registers all
// subcommand trees (satellite, auth, version).
// Global --api-url and --credentials flags are registered here so every
// subcommand inherits them.
func NewRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "stellar",
		Short: "Operate your satellites on StellarStation",
		Long: `StellarStation command-line tool for satellite operators.

Use it to list pass opportunities, book and manage passes, stream telemetry
live, send commands, and manage orbit data (TLE).

Getting started:
  1. Activate your API key (once):   stellar auth activate-api-key <key-file>
  2. See your satellites:            stellar satellite list-satellites
  3. Find upcoming passes:           stellar satellite list-visibilities --satellite-id <id>
  4. Stream a live pass:             stellar satellite open-stream --pass-id <id>

Run "stellar <command> --help" on any command for details and examples.`,
		// We print errors ourselves (see main.go) so they are not shown twice.
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	addAPIFlags(cmd)

	cmd.AddCommand(
		newSatelliteCommand(),
		newAuthCommand(),
		newVersionCommand(),
	)

	return cmd
}
