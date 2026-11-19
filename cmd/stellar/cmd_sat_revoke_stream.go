package main

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/spf13/cobra"
)

// newRevokeStreamCommand creates the "revoke-stream" subcommand, which cuts
// off another client's stream on a pass via POST /stream/revoke.
func newRevokeStreamCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "revoke-stream <stream-id>",
		Short: "Cut off a stale or rogue client's stream on a pass",
		Long: `Revoke another client's stream on a pass. The stream loses its commanding
authority, its MQTT certificate is revoked and StellarStation disconnects it
at once, and it can never be reopened. Its client must open a new stream to
continue.

Stream IDs are shown on the pass's monitoring page, and in the refusal when
another stream holds commanding authority. Taking over with
open-stream --override-commanding-lock revokes the stream it displaces the same
way.

Example:
  stellar satellite revoke-stream streamer-6f1c2d4e-0b7a-4c52-9d0e-3b8f1a2c4d5e \
    --pass-id a4c3984c-dc01-4af1-81bc-d8bf7acc70c7 --reason "client left running after handover"`,
		Args: requireOneArg(
			"the ID of the stream to revoke (from the pass's monitoring page)",
			"stellar satellite revoke-stream <stream-id> --pass-id <pass-id>",
		),
		RunE: runRevokeStream,
	}
	cmd.Flags().String("pass-id", "", "The pass the stream belongs to (required)")
	cmd.Flags().String("reason", "", "Why the stream is revoked, recorded with it")
	_ = cmd.MarkFlagRequired("pass-id")
	return cmd
}

func runRevokeStream(cmd *cobra.Command, args []string) error {
	ts, _, err := newTokenSource(cmd)
	if err != nil {
		return err
	}
	apiURL, err := resolveAPIBaseURL(cmd)
	if err != nil {
		return err
	}
	passID, _ := cmd.Flags().GetString("pass-id")
	reason, _ := cmd.Flags().GetString("reason")

	err = callStreamRevokeAPI(cmd.Context(), apiURL, ts, StreamRevokeRequest{
		PassID: passID, StreamID: args[0], Reason: reason,
	})
	if err != nil {
		return explainRevokeError(err)
	}
	_, err = fmt.Fprintf(cmd.OutOrStdout(), "Stream %s revoked.\n", args[0])
	return err
}

// explainRevokeError words a refusal from /stream/revoke for the operator.
func explainRevokeError(err error) error {
	var refusal *streamActionError
	if !errors.As(err, &refusal) {
		return fmt.Errorf("could not revoke the stream: %w", err)
	}
	switch refusal.Status {
	case http.StatusNotFound:
		return fmt.Errorf("no such stream on that pass (%s); check the pass and stream IDs", refusal.Message)
	case http.StatusForbidden:
		return fmt.Errorf("you do not have access to that pass (%s)", refusal.Message)
	case http.StatusConflict:
		return fmt.Errorf("that stream cannot be revoked: %s", refusal.Message)
	default:
		return fmt.Errorf("could not revoke the stream: %w", err)
	}
}
