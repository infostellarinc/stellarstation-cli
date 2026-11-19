// Copyright (c) 2026 InfoStellar, Inc. All Rights Reserved.

package commandstatus

import (
	"cmp"
	"errors"
	"time"

	streaming "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/satellitestreamer"
)

// Reporter reports progress for frames [first, last] of one tracked message.
// The zero value discards every report.
type Reporter struct {
	tracker     *Tracker
	ref         Ref
	first, last uint32
}

// Frames returns a Reporter for frames [first, last] of ref.
func (t *Tracker) Frames(ref Ref, first, last uint32) Reporter {
	return Reporter{tracker: t, ref: ref, first: first, last: last}
}

// Ref returns the message r reports on.
func (r Reporter) Ref() Ref { return r.ref }

// FrameRange returns the frames r reports on, inclusive.
func (r Reporter) FrameRange() (first, last uint32) { return r.first, r.last }

// Frame narrows r to frame i.
func (r Reporter) Frame(i uint32) Reporter {
	r.first, r.last = i, i
	return r
}

// Report applies u to each of r's frames.
func (r Reporter) Report(u Update) {
	r.tracker.report(r.ref, r.first, r.last, u)
}

// Reached records that the frames reached stage, which succeeds when it is
// the furthest stage the station reports.
func (r Reporter) Reached(stage streaming.CommandStage) {
	outcome := streaming.CommandOutcome_COMMAND_OUTCOME_IN_PROGRESS
	if r.ref.FinalStage != streaming.CommandStage_COMMAND_STAGE_UNSPECIFIED && stage >= r.ref.FinalStage {
		outcome = streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED
	}
	r.Report(Update{Stage: stage, Outcome: outcome})
}

// Dispatching records that the frames are being handed to the device, so one
// still in flight at teardown ends as unknown rather than not sent. It never
// succeeds, even when DISPATCHED is the final stage: the device may still
// refuse them.
func (r Reporter) Dispatching() {
	r.Report(Update{
		Stage:   streaming.CommandStage_COMMAND_STAGE_DISPATCHED,
		Outcome: streaming.CommandOutcome_COMMAND_OUTCOME_IN_PROGRESS,
	})
}

// Dispatched records the result of handing the frames to the device: nil
// means it took them, an *Error carries the device's outcome (DISPATCHED and
// FAILED unless it says otherwise), and any other error is a station failure.
func (r Reporter) Dispatched(err error) {
	var deviceErr *Error
	switch {
	case err == nil:
		r.Reached(streaming.CommandStage_COMMAND_STAGE_DISPATCHED)
	case errors.As(err, &deviceErr):
		r.Report(Update{
			Stage:        cmp.Or(deviceErr.Stage, streaming.CommandStage_COMMAND_STAGE_DISPATCHED),
			Outcome:      cmp.Or(deviceErr.Outcome, streaming.CommandOutcome_COMMAND_OUTCOME_FAILED),
			Failure:      deviceErr.Failure,
			ProviderCode: deviceErr.ProviderCode,
			Reason:       deviceErr.Reason,
		})
	default:
		r.Report(Update{
			Stage:   streaming.CommandStage_COMMAND_STAGE_DISPATCHED,
			Outcome: streaming.CommandOutcome_COMMAND_OUTCOME_FAILED,
			Failure: streaming.CommandFailure_COMMAND_FAILURE_STATION_ERROR,
			Reason:  err.Error(),
		})
	}
}

// Radiated records that the frames were transmitted at the given device time.
func (r Reporter) Radiated(at time.Time) {
	r.Report(Update{
		Stage:      streaming.CommandStage_COMMAND_STAGE_RADIATED,
		Outcome:    streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED,
		RadiatedAt: at,
	})
}

// NotSentReason is the status reason for a message discarded with failure
// before it was sent: SUPERSEDED, or otherwise the end of the pass.
func NotSentReason(failure streaming.CommandFailure) string {
	if failure == streaming.CommandFailure_COMMAND_FAILURE_SUPERSEDED {
		return "a newer stream took over the channel before the command was sent"
	}
	return "the pass ended before the command was sent"
}

// NotSent records that the frames will never be sent.
func (r Reporter) NotSent(failure streaming.CommandFailure, reason string) {
	r.Report(Update{
		Outcome: streaming.CommandOutcome_COMMAND_OUTCOME_FAILED,
		Failure: failure,
		Reason:  reason,
	})
}
