package main

import (
	"fmt"
	"os"

	"github.com/infostellarinc/stellarstation-cli/internal/auth"

	"github.com/spf13/cobra"
)

// newAuthCommand creates the "auth" parent command that groups authentication
// subcommands.
func newAuthCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "auth",
		Short: "Sign in with your API key",
		Long: `Manage the API key the CLI uses to talk to StellarStation.

Download an API key from the StellarStation console (Organization > API Keys),
then run "stellar auth activate-api-key <key-file>" once. After that, every
command signs in automatically, so you do not need to sign in each time.`,
	}

	cmd.AddCommand(newActivateAPIKeyCommand())
	cmd.AddCommand(newAuthTokenCommand())
	return cmd
}

// newActivateAPIKeyCommand installs a downloaded API-key JSON file into the
// well-known credentials location so every subsequent command can use it.
//
// The file may be:
//   - JSON downloaded from the console (Organization > API Keys), with top-level
//     clientId, clientSecret, tokenEndpoint, and optional scope; or
//   - The body returned by POST /v1/api-keys (CreateApiKeyResponse), optionally
//     with clientId nested under "apiKey".
func newActivateAPIKeyCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "activate-api-key <path-to-key-file>",
		Short: "Set up your API key so every command can sign in",
		Long: `Save a downloaded API key so the CLI can sign in automatically from now on.

Download the key from the StellarStation console (Organization > API Keys),
then point this command at the file. It is stored at
~/.stellarstation/credentials.json and reused by every later command, so you
only need to do this once (or again when you rotate your key).

Example:
  stellar auth activate-api-key ~/path/to/stellarstation-api-key.json`,
		Args: requireOneArg(
			"the path to your downloaded API key file",
			"stellar auth activate-api-key ~/path/to/stellarstation-api-key.json",
		),
		RunE: func(_ *cobra.Command, args []string) error {
			return runActivateAPIKey(args[0])
		},
	}
}

// newAuthTokenCommand prints a freshly-minted access token. It is useful for
// debugging authentication issues and for integrating the CLI with external
// scripts.
func newAuthTokenCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "token",
		Short: "Print a temporary access token (for scripts/advanced use)",
		Long: `Print a short-lived access token for your active API key.

Most operators never need this, because every command signs in automatically. It is useful when
you want to call the StellarStation API directly from a script, e.g. as an
"Authorization: Bearer <token>" header. The token is printed to stdout so it is
easy to capture in a variable.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAuthToken(cmd)
		},
	}
}

// runActivateAPIKey validates the caller-supplied JSON and writes the
// normalised credentials to the default location.
func runActivateAPIKey(srcPath string) error {
	data, err := os.ReadFile(srcPath) //nolint:gosec // srcPath is user-supplied CLI argument.
	if err != nil {
		return fmt.Errorf("could not read the API key file %q: %w", srcPath, err)
	}

	creds, err := auth.Parse(data)
	if err != nil {
		return fmt.Errorf(
			"%q does not look like a StellarStation API key file: %w\n"+
				"Download a key from the console (Organization > API Keys) and pass that file",
			srcPath, err,
		)
	}

	destPath, err := auth.DefaultCredentialsPath()
	if err != nil {
		return err
	}
	if err := auth.Save(destPath, creds); err != nil {
		return err
	}

	uiOKf("API key activated.")
	uiDimf("  Saved to %s", destPath)
	uiDimf("  You can now run stellar commands, e.g. `stellar satellite list-satellites`")
	if env := os.Getenv(envCredentialsPath); env != "" {
		uiWarnf(
			"%s is set to %s, so commands will use that file instead of the one just activated.",
			envCredentialsPath, env,
		)
	}
	return nil
}

// runAuthToken resolves the active credentials and prints a freshly-minted
// access token.
func runAuthToken(cmd *cobra.Command) error {
	ts, _, err := newTokenSource(cmd)
	if err != nil {
		return err
	}
	token, err := ts.Token(cmd.Context())
	if err != nil {
		return fmt.Errorf(
			"could not sign in with your API key: %w\n"+
				"Check the key is for this environment and re-activate it with "+
				"`stellar auth activate-api-key <file>`",
			err,
		)
	}
	_, _ = fmt.Fprintln(cmd.OutOrStdout(), token)
	return nil
}
