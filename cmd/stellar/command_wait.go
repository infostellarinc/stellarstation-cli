package main

import (
	"cmp"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/benbjohnson/clock"

	streaming "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/satellitestreamer"
)

// waitMode is what a one-shot send waits for before it exits (--wait-for).
type waitMode string

const (
	// waitForReceived waits for the ground station's acknowledgement.
	waitForReceived waitMode = "received"
	// waitForFinal also waits for the final delivery status.
	waitForFinal waitMode = "final"
)

func parseWaitMode(s string) (waitMode, error) {
	switch m := waitMode(strings.ToLower(strings.TrimSpace(s))); m {
	case waitForReceived, waitForFinal:
		return m, nil
	}
	return "", fmt.Errorf("--wait-for must be %q or %q (you gave %q)", waitForReceived, waitForFinal, s)
}

// effectiveWaitMode is the wait mode a send can meet: final needs the status
// topics, which an older ground station connection does not offer.
func effectiveWaitMode(cfg Config) waitMode {
	if cfg.WaitFor != waitForFinal || cfg.AuthorizerCreds == nil || hasStatusTopic(cfg.AuthorizerCreds.Streams) {
		return cfg.WaitFor
	}
	uiWarnf("Delivery status is not available on this connection; waiting for receipt only.")
	return waitForReceived
}

func hasStatusTopic(streams StreamsConfig) bool {
	for _, cmds := range [][]CommandConfig{streams.Uplink, streams.ConfigRequest} {
		for _, c := range cmds {
			if c.StatusTopic != "" {
				return true
			}
		}
	}
	return false
}

// Exit codes of a one-shot send. Any other error also exits with exitFailed.
const (
	exitOK = 0
	// exitFailed: refused, failed, not applied, or not transmitted.
	exitFailed = 1
	// exitUnknown: the outcome cannot be confirmed. The command may have been
	// transmitted, so check spacecraft telemetry before sending it again.
	exitUnknown = 3
)

// exitCodeError ends a one-shot send with a specific exit code. Its result
// lines have already been shown.
type exitCodeError struct{ code int }

func (e *exitCodeError) Error() string { return fmt.Sprintf("exit code %d", e.code) }

// exitCodeFor maps a one-shot send error to the process exit code and reports
// whether its result lines were already shown.
func exitCodeFor(err error) (code int, shown bool) {
	var exitErr *exitCodeError
	if errors.As(err, &exitErr) {
		return exitErr.code, true
	}
	return exitFailed, false
}

// moreSevere orders exit codes: an unknown outcome outranks a failure,
// because only it makes a blind resend unsafe.
func moreSevere(a, b int) int {
	rank := func(c int) int {
		switch c {
		case exitOK:
			return 0
		case exitFailed:
			return 1
		default:
			return 2
		}
	}
	if rank(b) > rank(a) {
		return b
	}
	return a
}

// commandResult is the outcome shown for one command when a one-shot send
// finishes waiting.
type commandResult struct {
	code int
	text string
}

// awaitCommands waits up to timeout, shared across cmds, for each command to
// be answered and, in final mode, for its final status. It shows one result
// line per command and returns an exitCodeError unless all succeeded.
func awaitCommands(
	stats *statsTracker, cmds []*sentCommand, mode waitMode, timeout time.Duration, clk clock.Clock,
) error {
	timer := clk.Timer(timeout)
	defer timer.Stop()
	expired := false
	wait := func(sig <-chan struct{}) {
		if expired {
			return
		}
		select {
		case <-sig:
		case <-timer.C:
			expired = true
		}
	}

	worst := exitOK
	warnedNoStatus := false
	for _, cmd := range cmds {
		wait(cmd.answered)
		if mode == waitForFinal {
			if c := stats.commandSnapshot(cmd); !warnedNoStatus && c.acked && !c.expectsStatus() {
				warnedNoStatus = true
				uiWarnf("The ground station does not report delivery status for these commands; " +
					"only receipt can be confirmed.")
			}
			wait(cmd.finished)
		}
		r := resultOf(stats.commandSnapshot(cmd), mode, timeout)
		switch r.code {
		case exitOK:
			uiOKf("%s", r.text)
		case exitUnknown:
			uiWarnf("%s", r.text)
		default:
			uiErrf("%s", r.text)
		}
		worst = moreSevere(worst, r.code)
	}
	if worst != exitOK {
		return &exitCodeError{code: worst}
	}
	return nil
}

// expectsStatus reports whether the station said it reports delivery status
// for this command.
func (c *sentCommand) expectsStatus() bool {
	return c.expectedFinalStage != streaming.CommandStage_COMMAND_STAGE_UNSPECIFIED
}

// resultOf decides the result of one command after the wait.
func resultOf(c sentCommand, mode waitMode, timeout time.Duration) commandResult {
	subject := capitalize(fmt.Sprintf("%s #%d", c.noun(), c.index))
	d := c.delivery
	switch {
	case d.skipped:
		return commandResult{exitFailed, capitalize(c.deliveryLine()) + "."}
	case c.rejected:
		reason := cmp.Or(c.nackReason, c.nackReasonCode, "no reason given")
		return commandResult{exitFailed, fmt.Sprintf("%s was refused by the ground station: %s.", subject, reason)}
	case mode == waitForFinal && d.final:
		return finalResult(c)
	case !c.acked && !d.final:
		return commandResult{exitUnknown, fmt.Sprintf(
			"%s was not acknowledged within %s. It may still reach the satellite; "+
				"check spacecraft telemetry before sending it again.", subject, timeout)}
	case mode == waitForReceived || !c.expectsStatus():
		return commandResult{exitOK, subject + " was accepted by the ground station."}
	default:
		return commandResult{exitUnknown, fmt.Sprintf(
			"%s was accepted, but no final delivery status arrived within %s. "+
				"Check spacecraft telemetry before sending it again.", subject, timeout)}
	}
}

func finalResult(c sentCommand) commandResult {
	line := c.deliveryLine()
	if line == "" {
		line = fmt.Sprintf("%s #%d was delivered", c.noun(), c.index)
	}
	text := capitalize(line) + "."
	switch c.delivery.outcome {
	case streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED:
		return commandResult{exitOK, text}
	case streaming.CommandOutcome_COMMAND_OUTCOME_UNKNOWN:
		return commandResult{exitUnknown, text + " Check spacecraft telemetry before sending it again."}
	default:
		return commandResult{exitFailed, text}
	}
}

func capitalize(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}
