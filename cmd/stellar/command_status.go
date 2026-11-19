package main

import (
	"cmp"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	streaming "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/satellitestreamer"
	"github.com/infostellarinc/stellarstation-cli/internal/commandseq"
	"github.com/infostellarinc/stellarstation-cli/internal/commandstatus"
)

const (
	cmdTypeUplink        = "uplink"
	cmdTypeConfigRequest = "config_request"
)

// commandDelivery is what the station reported on the status topic about a
// command after its receipt ack.
type commandDelivery struct {
	stage   streaming.CommandStage
	outcome streaming.CommandOutcome
	// final is set once nothing more will be reported.
	final bool
	// skipped is set when the station declared the command missing: it never
	// arrived in time and was not transmitted.
	skipped      bool
	radiatedAt   time.Time
	failure      streaming.CommandFailure
	providerCode string
	reason       string
	configFields []*streaming.ConfigFieldResult
}

// isCommandStatusTopic reports whether topic carries command statuses.
func isCommandStatusTopic(topic string) bool {
	return strings.HasSuffix(topic, "/"+cmdTypeUplink+"/status") ||
		strings.HasSuffix(topic, "/"+cmdTypeConfigRequest+"/status")
}

// closeSignal closes ch unless it is already closed. Callers hold the lock
// that guards ch.
func closeSignal(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

// AddStatusBatch records a CommandStatusBatch received on a status topic.
// Statuses for commands this client did not send are ignored.
func (s *statsTracker) AddStatusBatch(topic string, payload []byte) {
	s.markActivity()
	var batch streaming.CommandStatusBatch
	if err := proto.Unmarshal(payload, &batch); err != nil {
		s.AddError(fmt.Errorf("undecodable command status on %s: %w", topic, err))
		return
	}
	baseTopic := strings.TrimSuffix(topic, "/status")

	// A replayed batch changes nothing: a status only moves a command forward.
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range batch.GetStatuses() {
		cmd := s.findSentCommand(baseTopic, st.GetAckedMessageId(), st.GetStreamId(), st.GetIndex())
		if cmd == nil {
			continue
		}
		// The operator sees each command's progress as it happens, not only
		// in the summary at the end.
		if line := cmd.applyStatus(st); line != "" {
			uiInfof("[Status] %s", line)
		}
	}
	for _, r := range batch.GetSkipped() {
		if line := s.applySkipped(baseTopic, r); line != "" {
			uiInfof("[Status] %s", line)
		}
	}
}

// applyStatus records one frame's status and returns the line describing the
// command's new state, or "" when it changes nothing. Until the status that
// completes the message, the command shows the furthest stage any frame
// reached; that status carries the message's outcome, and nothing after it
// applies.
func (c *sentCommand) applyStatus(st *streaming.CommandStatus) string {
	d := &c.delivery
	if d.final {
		return ""
	}
	// Any status proves the station accepted the command, even if its ack
	// was lost, so a one-shot send need not wait out its timeout.
	if !c.acked && !c.rejected {
		c.acked = true
	}
	closeSignal(c.answered)
	c.recordDetails(st)
	if st.GetMessageFinal() {
		d.stage = max(d.stage, st.GetStage())
		d.outcome = cmp.Or(st.GetMessageOutcome(), st.GetOutcome())
		d.final = true
		closeSignal(c.finished)
		return c.deliveryLine()
	}
	if st.GetStage() <= d.stage {
		return ""
	}
	d.stage = st.GetStage()
	d.outcome = streaming.CommandOutcome_COMMAND_OUTCOME_IN_PROGRESS
	return c.deliveryLine()
}

// recordDetails keeps the latest details any frame reported.
func (c *sentCommand) recordDetails(st *streaming.CommandStatus) {
	d := &c.delivery
	if t := st.GetRadiatedAt(); t != nil {
		d.radiatedAt = t.AsTime()
	}
	if f := st.GetFailure(); f != streaming.CommandFailure_COMMAND_FAILURE_UNSPECIFIED {
		d.failure = f
	}
	if v := st.GetProviderCode(); v != "" {
		d.providerCode = v
	}
	if v := st.GetReason(); v != "" {
		d.reason = v
	}
	if fields := st.GetConfigFields(); len(fields) > 0 {
		d.configFields = fields
	}
}

// applySkipped marks this client's unanswered commands in r as skipped and
// returns the line describing the range, or "" when none of them is ours.
func (s *statsTracker) applySkipped(baseTopic string, r *streaming.SkippedRange) string {
	var noun string
	for _, cmd := range s.sentCommands {
		idx := uint64(cmd.index)
		if cmd.topic != baseTopic || idx < r.GetFirstIndex() || idx > r.GetLastIndex() {
			continue
		}
		if r.GetStreamId() != "" && cmd.streamID != r.GetStreamId() {
			continue
		}
		noun = cmd.noun()
		if cmd.acked || cmd.rejected || cmd.delivery.final {
			continue
		}
		cmd.delivery.skipped = true
		cmd.delivery.final = true
		closeSignal(cmd.answered)
		closeSignal(cmd.finished)
	}
	if noun == "" {
		return ""
	}
	return skippedLine(noun, r.GetFirstIndex(), r.GetLastIndex())
}

func skippedLine(noun string, first, last uint64) string {
	if first == last {
		return fmt.Sprintf("%s #%d was not received by the ground station in time and was not transmitted",
			noun, first)
	}
	return fmt.Sprintf("%ss #%d to #%d were not received by the ground station in time and were not transmitted",
		noun, first, last)
}

// noun names the command kind in operator-facing text.
func (c *sentCommand) noun() string {
	if c.cmdType == cmdTypeConfigRequest {
		return "configuration request"
	}
	return "command"
}

// deliveryLine describes the latest reported delivery state, or "" when there
// is nothing to report yet.
func (c *sentCommand) deliveryLine() string {
	d := c.delivery
	subject := fmt.Sprintf("%s #%d", c.noun(), c.index)
	if d.skipped {
		return skippedLine(c.noun(), uint64(c.index), uint64(c.index))
	}
	switch d.outcome {
	case streaming.CommandOutcome_COMMAND_OUTCOME_UNKNOWN:
		reason := d.reason
		if reason == "" {
			reason = "the ground station could not confirm whether it was transmitted"
		}
		return fmt.Sprintf("%s outcome unknown: %s", subject, withProviderCode(reason, d.providerCode))
	case streaming.CommandOutcome_COMMAND_OUTCOME_FAILED:
		if c.cmdType == cmdTypeConfigRequest {
			return fmt.Sprintf("%s not applied: %s", subject, c.configRefusals())
		}
		switch d.failure {
		case streaming.CommandFailure_COMMAND_FAILURE_PLAN_ENDED,
			streaming.CommandFailure_COMMAND_FAILURE_SUPERSEDED:
			return fmt.Sprintf("%s was not transmitted: %s", subject, failureText(d.failure))
		default:
			return fmt.Sprintf("%s failed: %s", subject, reasonText(d.reason, d.failure, d.providerCode))
		}
	case streaming.CommandOutcome_COMMAND_OUTCOME_PARTIAL:
		if c.cmdType == cmdTypeConfigRequest {
			return fmt.Sprintf("%s partially applied: %s", subject, c.configRefusals())
		}
		// Some frames were transmitted; resending only the failed ones is safe.
		return fmt.Sprintf("%s was only partly transmitted: %s", subject,
			reasonText(d.reason, d.failure, d.providerCode))
	}
	switch {
	case c.cmdType == cmdTypeConfigRequest:
		if d.outcome == streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED {
			return subject + " applied"
		}
	case d.stage == streaming.CommandStage_COMMAND_STAGE_RADIATED:
		if d.radiatedAt.IsZero() {
			return subject + " radiated"
		}
		return fmt.Sprintf("%s radiated at %s", subject, d.radiatedAt.UTC().Format(time.RFC3339))
	case d.stage == streaming.CommandStage_COMMAND_STAGE_DISPATCHED:
		return subject + " passed to the ground station equipment"
	}
	return ""
}

// configRefusals lists the configuration fields that were not applied, or the
// overall reason when the station named none.
func (c *sentCommand) configRefusals() string {
	var parts []string
	for _, f := range c.delivery.configFields {
		switch f.GetOutcome() {
		case streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED, streaming.CommandOutcome_COMMAND_OUTCOME_IN_PROGRESS:
			continue
		}
		parts = append(parts, f.GetField()+": "+reasonText(f.GetReason(), f.GetFailure(), f.GetProviderCode()))
	}
	if len(parts) == 0 {
		return reasonText(c.delivery.reason, c.delivery.failure, c.delivery.providerCode)
	}
	return strings.Join(parts, "; ")
}

// reasonText is the station's reason, or a description of the failure when
// it gave none, followed by the raw equipment code when there is one.
func reasonText(reason string, failure streaming.CommandFailure, providerCode string) string {
	if reason == "" {
		reason = failureText(failure)
	}
	return withProviderCode(reason, providerCode)
}

func withProviderCode(reason, providerCode string) string {
	if providerCode == "" {
		return reason
	}
	return fmt.Sprintf("%s (%s)", reason, providerCode)
}

// failureText describes a failure code for an operator.
func failureText(f streaming.CommandFailure) string {
	switch f {
	case streaming.CommandFailure_COMMAND_FAILURE_PLAN_ENDED:
		return "the pass ended before it could be sent"
	case streaming.CommandFailure_COMMAND_FAILURE_SUPERSEDED:
		return "another session took over commanding on this channel"
	case streaming.CommandFailure_COMMAND_FAILURE_INVALID_COMMAND:
		return "the ground station equipment rejected it as malformed"
	case streaming.CommandFailure_COMMAND_FAILURE_ENCODER_LOCKED_OUT:
		return "the ground station encoder was locked out"
	case streaming.CommandFailure_COMMAND_FAILURE_REJECTED_BY_DEVICE:
		return "the ground station equipment refused it"
	case streaming.CommandFailure_COMMAND_FAILURE_DEADLINE_EXCEEDED:
		return "the latest allowed transmission time passed"
	case streaming.CommandFailure_COMMAND_FAILURE_UNSUPPORTED:
		return "no equipment at this ground station supports this setting"
	case streaming.CommandFailure_COMMAND_FAILURE_OUT_OF_RANGE:
		return "the value is outside what the ground station equipment accepts"
	case streaming.CommandFailure_COMMAND_FAILURE_NO_DEVICE_ACK:
		return "the ground station equipment did not confirm it in time"
	case streaming.CommandFailure_COMMAND_FAILURE_STATION_ERROR:
		return "the ground station reported an internal error"
	default:
		return "no reason given"
	}
}

// deliveryCounts summarises what the station reported across sent commands.
type deliveryCounts struct {
	radiated, partlyRadiated, applied, partial, failed, unknown, late int
}

func countDeliveries(cmds []sentCommand) deliveryCounts {
	var n deliveryCounts
	for _, c := range cmds {
		d := c.delivery
		switch {
		case d.skipped || c.nackReasonCode == commandseq.ReasonArrivedLate:
			n.late++
		case d.outcome == streaming.CommandOutcome_COMMAND_OUTCOME_FAILED:
			n.failed++
		case d.outcome == streaming.CommandOutcome_COMMAND_OUTCOME_UNKNOWN:
			n.unknown++
		case d.outcome == streaming.CommandOutcome_COMMAND_OUTCOME_PARTIAL && c.cmdType == cmdTypeConfigRequest:
			n.partial++
		case d.outcome == streaming.CommandOutcome_COMMAND_OUTCOME_PARTIAL:
			n.partlyRadiated++
		case c.cmdType == cmdTypeConfigRequest && d.outcome == streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED:
			n.applied++
		case d.stage == streaming.CommandStage_COMMAND_STAGE_RADIATED:
			n.radiated++
		}
	}
	return n
}

// String lists the non-zero counts, for example "2 radiated, 1 late".
func (n deliveryCounts) String() string {
	var parts []string
	add := func(v int, label string) {
		if v > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", v, label))
		}
	}
	add(n.radiated, "radiated")
	add(n.partlyRadiated, "partly radiated")
	add(n.applied, "applied")
	add(n.partial, "partially applied")
	add(n.failed, "failed")
	add(n.unknown, "unknown")
	add(n.late, "late")
	return strings.Join(parts, ", ")
}

// stageName, outcomeName and failureName render a status enum for
// diagnostics in lower case, for example "radiated"; the unspecified value
// renders empty.
func stageName(v streaming.CommandStage) string { return strings.ToLower(commandstatus.StageName(v)) }

func outcomeName(v streaming.CommandOutcome) string {
	return strings.ToLower(commandstatus.OutcomeName(v))
}

func failureName(v streaming.CommandFailure) string {
	return strings.ToLower(commandstatus.FailureName(v))
}
