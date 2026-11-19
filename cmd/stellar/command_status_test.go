package main

import (
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	streaming "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/satellitestreamer"
	"github.com/infostellarinc/stellarstation-cli/internal/commandseq"
)

// testCommandMsg is a command message on stream-1 with the given index and
// no message ID, so replies match it by index.
func testCommandMsg(index uint32) *streaming.ToStarPassMessage {
	return &streaming.ToStarPassMessage{Index: index, StreamId: "stream-1"}
}

func statusBatch(t *testing.T, statuses ...*streaming.CommandStatus) []byte {
	t.Helper()
	payload, err := proto.Marshal(&streaming.CommandStatusBatch{Statuses: statuses})
	require.NoError(t, err)
	return payload
}

func skippedBatch(t *testing.T, streamID string, first, last uint64) []byte {
	t.Helper()
	payload, err := proto.Marshal(&streaming.CommandStatusBatch{Skipped: []*streaming.SkippedRange{{
		StreamId: streamID, MessageType: cmdTypeUplink, FirstIndex: first, LastIndex: last,
	}}})
	require.NoError(t, err)
	return payload
}

var testRadiatedAt = time.Date(2026, 10, 1, 12, 30, 5, 0, time.UTC)

func TestIsCommandStatusTopic(t *testing.T) {
	for topic, want := range map[string]bool{
		"dev/pass/p/channel/c/uplink/status":         true,
		"dev/pass/p/channel/c/config_request/status": true,
		"dev/pass/p/channel/c/uplink/ack":            false,
		"global/groundStation/gs-1/status":           false,
		"dev/pass/p/channel/c/downlink/IQ":           false,
	} {
		assert.Equal(t, want, isCommandStatusTopic(topic), topic)
	}
}

func TestDeliveryLine(t *testing.T) {
	radiated := commandDelivery{
		stage:      streaming.CommandStage_COMMAND_STAGE_RADIATED,
		outcome:    streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED,
		radiatedAt: testRadiatedAt.In(time.FixedZone("JST", 9*3600)),
	}
	tests := []struct {
		name     string
		cmdType  string
		delivery commandDelivery
		want     string
	}{
		{name: "nothing reported", cmdType: cmdTypeUplink, want: ""},
		{
			name: "dispatched", cmdType: cmdTypeUplink,
			delivery: commandDelivery{
				stage:   streaming.CommandStage_COMMAND_STAGE_DISPATCHED,
				outcome: streaming.CommandOutcome_COMMAND_OUTCOME_IN_PROGRESS,
			},
			want: "command #4 passed to the ground station equipment",
		},
		{
			name: "radiated, in UTC", cmdType: cmdTypeUplink, delivery: radiated,
			want: "command #4 radiated at 2026-10-01T12:30:05Z",
		},
		{
			name: "radiated without a time", cmdType: cmdTypeUplink,
			delivery: commandDelivery{stage: streaming.CommandStage_COMMAND_STAGE_RADIATED},
			want:     "command #4 radiated",
		},
		{
			name: "rejected with reason and equipment code", cmdType: cmdTypeUplink,
			delivery: commandDelivery{
				stage:        streaming.CommandStage_COMMAND_STAGE_RADIATED,
				outcome:      streaming.CommandOutcome_COMMAND_OUTCOME_FAILED,
				failure:      streaming.CommandFailure_COMMAND_FAILURE_DEADLINE_EXCEEDED,
				providerCode: "cortex-tcu:12",
				reason:       "latest radiation time reached",
			},
			want: "command #4 failed: latest radiation time reached (cortex-tcu:12)",
		},
		{
			name: "rejected without a reason falls back to the failure", cmdType: cmdTypeUplink,
			delivery: commandDelivery{
				outcome: streaming.CommandOutcome_COMMAND_OUTCOME_FAILED,
				failure: streaming.CommandFailure_COMMAND_FAILURE_INVALID_COMMAND,
			},
			want: "command #4 failed: the ground station equipment rejected it as malformed",
		},
		{
			name: "pass ended before sending", cmdType: cmdTypeUplink,
			delivery: commandDelivery{
				outcome: streaming.CommandOutcome_COMMAND_OUTCOME_FAILED,
				failure: streaming.CommandFailure_COMMAND_FAILURE_PLAN_ENDED,
			},
			want: "command #4 was not transmitted: the pass ended before it could be sent",
		},
		{
			name: "pass ended after earlier frames were sent", cmdType: cmdTypeUplink,
			delivery: commandDelivery{
				stage:   streaming.CommandStage_COMMAND_STAGE_RADIATED,
				outcome: streaming.CommandOutcome_COMMAND_OUTCOME_PARTIAL,
				failure: streaming.CommandFailure_COMMAND_FAILURE_PLAN_ENDED,
			},
			want: "command #4 was only partly transmitted: the pass ended before it could be sent",
		},
		{
			name: "superseded", cmdType: cmdTypeUplink,
			delivery: commandDelivery{
				outcome: streaming.CommandOutcome_COMMAND_OUTCOME_FAILED,
				failure: streaming.CommandFailure_COMMAND_FAILURE_SUPERSEDED,
			},
			want: "command #4 was not transmitted: another session took over commanding on this channel",
		},
		{
			name: "unknown with reason", cmdType: cmdTypeUplink,
			delivery: commandDelivery{
				outcome: streaming.CommandOutcome_COMMAND_OUTCOME_UNKNOWN,
				reason:  "connection to the encoder lost",
			},
			want: "command #4 outcome unknown: connection to the encoder lost",
		},
		{
			name: "unknown without reason", cmdType: cmdTypeUplink,
			delivery: commandDelivery{outcome: streaming.CommandOutcome_COMMAND_OUTCOME_UNKNOWN},
			want:     "command #4 outcome unknown: the ground station could not confirm whether it was transmitted",
		},
		{
			name: "skipped", cmdType: cmdTypeUplink, delivery: commandDelivery{skipped: true},
			want: "command #4 was not received by the ground station in time and was not transmitted",
		},
		{
			name: "config applied", cmdType: cmdTypeConfigRequest,
			delivery: commandDelivery{
				stage:   streaming.CommandStage_COMMAND_STAGE_APPLIED,
				outcome: streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED,
			},
			want: "configuration request #4 applied",
		},
		{
			name: "config in progress", cmdType: cmdTypeConfigRequest,
			delivery: commandDelivery{
				stage:   streaming.CommandStage_COMMAND_STAGE_DISPATCHED,
				outcome: streaming.CommandOutcome_COMMAND_OUTCOME_IN_PROGRESS,
			},
			want: "",
		},
		{
			name: "config partially applied lists refused fields", cmdType: cmdTypeConfigRequest,
			delivery: commandDelivery{
				outcome: streaming.CommandOutcome_COMMAND_OUTCOME_PARTIAL,
				configFields: []*streaming.ConfigFieldResult{
					{Field: "receiver.bitrate", Outcome: streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED},
					{
						Field: "transmitter.bitrate", Outcome: streaming.CommandOutcome_COMMAND_OUTCOME_FAILED,
						Failure: streaming.CommandFailure_COMMAND_FAILURE_OUT_OF_RANGE, ProviderCode: "cortex:4",
					},
					{
						Field: "receiver.modulation", Outcome: streaming.CommandOutcome_COMMAND_OUTCOME_FAILED,
						Reason: "not supported by this receiver",
					},
				},
			},
			want: "configuration request #4 partially applied: transmitter.bitrate: the value is outside what " +
				"the ground station equipment accepts (cortex:4); receiver.modulation: not supported by this receiver",
		},
		{
			name: "config not applied without fields uses the reason", cmdType: cmdTypeConfigRequest,
			delivery: commandDelivery{
				outcome: streaming.CommandOutcome_COMMAND_OUTCOME_FAILED,
				failure: streaming.CommandFailure_COMMAND_FAILURE_STATION_ERROR,
			},
			want: "configuration request #4 not applied: the ground station reported an internal error",
		},
		{
			name: "config unknown", cmdType: cmdTypeConfigRequest,
			delivery: commandDelivery{outcome: streaming.CommandOutcome_COMMAND_OUTCOME_UNKNOWN, reason: "timed out"},
			want:     "configuration request #4 outcome unknown: timed out",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := sentCommand{cmdType: tt.cmdType, index: 4, delivery: tt.delivery}
			assert.Equal(t, tt.want, c.deliveryLine())
		})
	}
}

func TestSkippedLine(t *testing.T) {
	assert.Equal(t, "commands #3 to #5 were not received by the ground station in time and were not transmitted",
		skippedLine("command", 3, 5))
	assert.Equal(t, "command #3 was not received by the ground station in time and was not transmitted",
		skippedLine("command", 3, 3))
	assert.Equal(t, "configuration requests #1 to #2 were not received by the ground station in time and "+
		"were not transmitted", skippedLine("configuration request", 1, 2))
}

func TestFailureTextCoversEveryFailure(t *testing.T) {
	for v := range streaming.CommandFailure_name {
		f := streaming.CommandFailure(v)
		text := failureText(f)
		if f == streaming.CommandFailure_COMMAND_FAILURE_UNSPECIFIED {
			assert.Equal(t, "no reason given", text)
			continue
		}
		assert.NotEqual(t, "no reason given", text, f.String())
	}
}

// newStatusTracker returns a tracker with uplink commands #1 to #n sent on
// testUplinkTopic.
func newStatusTracker(n int) (*statsTracker, []*sentCommand) {
	stats := newStatsTracker(true)
	cmds := make([]*sentCommand, n)
	for i := range cmds {
		msg := testCommandMsg(uint32(i + 1))
		msg.MessageId = "msg-" + string(rune('a'+i))
		cmds[i] = stats.AddSentCommand(testUplinkTopic, cmdTypeUplink, msg)
	}
	return stats, cmds
}

func isClosed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestAddStatusBatch(t *testing.T) {
	dispatched := &streaming.CommandStatus{
		AckedMessageId: "msg-a", Index: 1,
		Stage:   streaming.CommandStage_COMMAND_STAGE_DISPATCHED,
		Outcome: streaming.CommandOutcome_COMMAND_OUTCOME_IN_PROGRESS,
	}
	radiated := &streaming.CommandStatus{
		AckedMessageId: "msg-a", Index: 1,
		Stage:          streaming.CommandStage_COMMAND_STAGE_RADIATED,
		Outcome:        streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED,
		Final:          true,
		MessageFinal:   true,
		MessageOutcome: streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED,
		RadiatedAt:     timestamppb.New(testRadiatedAt),
		ProviderCode:   "cortex-tcu:0",
	}
	failed := &streaming.CommandStatus{
		AckedMessageId: "msg-a", Index: 1,
		Stage:          streaming.CommandStage_COMMAND_STAGE_DISPATCHED,
		Outcome:        streaming.CommandOutcome_COMMAND_OUTCOME_FAILED,
		Final:          true,
		MessageFinal:   true,
		MessageOutcome: streaming.CommandOutcome_COMMAND_OUTCOME_FAILED,
		Failure:        streaming.CommandFailure_COMMAND_FAILURE_REJECTED_BY_DEVICE,
	}
	// A two-frame message: frame 0 radiated, then the pass ended before frame 1.
	radiatedFrame := &streaming.CommandStatus{
		AckedMessageId: "msg-a", Index: 1, FrameCount: 2,
		Stage:      streaming.CommandStage_COMMAND_STAGE_RADIATED,
		Outcome:    streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED,
		Final:      true,
		RadiatedAt: timestamppb.New(testRadiatedAt),
	}
	notSentFrame := &streaming.CommandStatus{
		AckedMessageId: "msg-a", Index: 1, FrameIndex: 1, FrameCount: 2,
		Outcome:        streaming.CommandOutcome_COMMAND_OUTCOME_FAILED,
		Final:          true,
		MessageFinal:   true,
		MessageOutcome: streaming.CommandOutcome_COMMAND_OUTCOME_PARTIAL,
		Failure:        streaming.CommandFailure_COMMAND_FAILURE_PLAN_ENDED,
	}
	tests := []struct {
		name      string
		batches   [][]*streaming.CommandStatus
		want      commandDelivery
		wantFinal bool
	}{
		{
			name:    "progress is not final",
			batches: [][]*streaming.CommandStatus{{dispatched}},
			want: commandDelivery{
				stage:   streaming.CommandStage_COMMAND_STAGE_DISPATCHED,
				outcome: streaming.CommandOutcome_COMMAND_OUTCOME_IN_PROGRESS,
			},
		},
		{
			name:    "radiated final",
			batches: [][]*streaming.CommandStatus{{dispatched}, {radiated}},
			want: commandDelivery{
				stage:        streaming.CommandStage_COMMAND_STAGE_RADIATED,
				outcome:      streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED,
				final:        true,
				radiatedAt:   testRadiatedAt,
				providerCode: "cortex-tcu:0",
			},
			wantFinal: true,
		},
		{
			name:    "stages only move forward",
			batches: [][]*streaming.CommandStatus{{radiated}, {dispatched}},
			want: commandDelivery{
				stage:        streaming.CommandStage_COMMAND_STAGE_RADIATED,
				outcome:      streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED,
				final:        true,
				radiatedAt:   testRadiatedAt,
				providerCode: "cortex-tcu:0",
			},
			wantFinal: true,
		},
		{
			name:    "the first final status wins",
			batches: [][]*streaming.CommandStatus{{failed}, {radiated}},
			want: commandDelivery{
				stage:   streaming.CommandStage_COMMAND_STAGE_DISPATCHED,
				outcome: streaming.CommandOutcome_COMMAND_OUTCOME_FAILED,
				final:   true,
				failure: streaming.CommandFailure_COMMAND_FAILURE_REJECTED_BY_DEVICE,
			},
			wantFinal: true,
		},
		{
			name:    "the status completing the message carries its outcome, at whatever stage its frame is",
			batches: [][]*streaming.CommandStatus{{radiatedFrame}, {notSentFrame}},
			want: commandDelivery{
				stage:      streaming.CommandStage_COMMAND_STAGE_RADIATED,
				outcome:    streaming.CommandOutcome_COMMAND_OUTCOME_PARTIAL,
				final:      true,
				radiatedAt: testRadiatedAt,
				failure:    streaming.CommandFailure_COMMAND_FAILURE_PLAN_ENDED,
			},
			wantFinal: true,
		},
		{
			name: "a status for another client's command is ignored",
			batches: [][]*streaming.CommandStatus{{{
				AckedMessageId: "someone-else", Index: 1,
				Stage:        streaming.CommandStage_COMMAND_STAGE_RADIATED,
				MessageFinal: true,
			}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats, cmds := newStatusTracker(1)
			for _, b := range tt.batches {
				stats.AddStatusBatch(testUplinkTopic+"/status", statusBatch(t, b...))
			}
			got := stats.commandSnapshot(cmds[0])
			assert.Equal(t, tt.want, got.delivery)
			assert.Equal(t, tt.wantFinal, isClosed(cmds[0].finished))
			// Any status for the command answers it; one for another's does not.
			answered := tt.wantFinal || tt.want.stage != streaming.CommandStage_COMMAND_STAGE_UNSPECIFIED
			assert.Equal(t, answered, isClosed(cmds[0].answered))
		})
	}
}

func TestAddStatusBatchKeepsConfigFieldResults(t *testing.T) {
	fields := []*streaming.ConfigFieldResult{
		{Field: "receiver.bitrate", Outcome: streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED},
		{
			Field: "receiver.modulation", Outcome: streaming.CommandOutcome_COMMAND_OUTCOME_FAILED,
			Failure: streaming.CommandFailure_COMMAND_FAILURE_UNSUPPORTED,
		},
	}
	stats, cmds := newStatusTracker(1)
	stats.AddStatusBatch(testUplinkTopic+"/status", statusBatch(t, &streaming.CommandStatus{
		Index:          1,
		Stage:          streaming.CommandStage_COMMAND_STAGE_APPLIED,
		Outcome:        streaming.CommandOutcome_COMMAND_OUTCOME_PARTIAL,
		MessageFinal:   true,
		MessageOutcome: streaming.CommandOutcome_COMMAND_OUTCOME_PARTIAL,
		Reason:         "some fields were not applied",
		ConfigFields:   fields,
	}))
	d := stats.commandSnapshot(cmds[0]).delivery
	assert.Equal(t, streaming.CommandOutcome_COMMAND_OUTCOME_PARTIAL, d.outcome)
	assert.True(t, d.final)
	assert.Equal(t, "some fields were not applied", d.reason)
	require.Len(t, d.configFields, len(fields))
	for i, f := range fields {
		assert.True(t, proto.Equal(f, d.configFields[i]), "field %d", i)
	}
}

// Without an acked message ID a status is matched by index.
func TestAddStatusBatchMatchesByIndex(t *testing.T) {
	stats, cmds := newStatusTracker(2)
	stats.AddStatusBatch(testUplinkTopic+"/status", statusBatch(t, &streaming.CommandStatus{
		Index: 2, Stage: streaming.CommandStage_COMMAND_STAGE_RADIATED,
		Outcome: streaming.CommandOutcome_COMMAND_OUTCOME_IN_PROGRESS,
	}))
	assert.Equal(t, streaming.CommandStage_COMMAND_STAGE_UNSPECIFIED, stats.commandSnapshot(cmds[0]).delivery.stage)
	assert.Equal(t, streaming.CommandStage_COMMAND_STAGE_RADIATED, stats.commandSnapshot(cmds[1]).delivery.stage)
}

func TestAddStatusBatchMalformedIsTrackedError(t *testing.T) {
	stats, cmds := newStatusTracker(1)
	stats.AddStatusBatch(testUplinkTopic+"/status", []byte{0xff, 0xff})
	assert.Len(t, getErrors(stats), 1)
	assert.Equal(t, commandDelivery{}, stats.commandSnapshot(cmds[0]).delivery)
}

func TestAddStatusBatchSkipped(t *testing.T) {
	tests := []struct {
		name        string
		streamID    string
		first, last uint64
		wantSkipped []bool
	}{
		{name: "range of this stream", streamID: "stream-1", first: 2, last: 3, wantSkipped: []bool{false, true, true}},
		{name: "range without a stream", first: 1, last: 1, wantSkipped: []bool{true, false, false}},
		{
			name:        "another stream's range",
			streamID:    "stream-2",
			first:       1,
			last:        3,
			wantSkipped: []bool{false, false, false},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats, cmds := newStatusTracker(3)
			stats.AddStatusBatch(testUplinkTopic+"/status", skippedBatch(t, tt.streamID, tt.first, tt.last))
			for i, cmd := range cmds {
				snap := stats.commandSnapshot(cmd)
				assert.Equal(t, tt.wantSkipped[i], snap.delivery.skipped, "command #%d", i+1)
				assert.Equal(t, tt.wantSkipped[i], isClosed(cmd.answered), "command #%d", i+1)
			}
		})
	}
}

// A command the station already answered is never marked skipped.
func TestAddStatusBatchSkippedKeepsAnsweredCommands(t *testing.T) {
	stats, cmds := newStatusTracker(1)
	stats.AddAckReceived(testUplinkTopic+"/ack", testAck(t, cmds[0], streaming.Ack_ACK, ""))
	stats.AddStatusBatch(testUplinkTopic+"/status", skippedBatch(t, "stream-1", 1, 1))
	assert.False(t, stats.commandSnapshot(cmds[0]).delivery.skipped)
}

func TestApplySkippedLine(t *testing.T) {
	stats, _ := newStatusTracker(2)
	stats.mu.Lock()
	defer stats.mu.Unlock()
	r := &streaming.SkippedRange{StreamId: "stream-1", FirstIndex: 1, LastIndex: 2}
	assert.Equal(t, skippedLine("command", 1, 2), stats.applySkipped(testUplinkTopic, r))
	other := &streaming.SkippedRange{StreamId: "stream-1", FirstIndex: 7, LastIndex: 9}
	assert.Empty(t, stats.applySkipped(testUplinkTopic, other), "a range with none of this client's commands")
}

// A resent duplicate is answered with a byte-identical replay of its first
// ack, and a reader and a sender connection can both deliver it. Either way
// it is counted once.
func TestAckReplayIsNotDoubleCounted(t *testing.T) {
	stats, cmds := newStatusTracker(1)
	ack := testAck(t, cmds[0], streaming.Ack_ACK, "")
	for range 3 {
		require.True(t, routeCommandReply(stats, testUplinkTopic+"/ack", ack))
	}
	assert.Equal(t, 1, stats.acksReceived)
	assert.True(t, stats.commandSnapshot(cmds[0]).acked)

	nack := testAck(t, cmds[0], streaming.Ack_NACK, commandseq.ReasonArrivedLate)
	stats.AddAckReceived(testUplinkTopic+"/ack", nack)
	stats.AddAckReceived(testUplinkTopic+"/ack", nack)
	assert.Equal(t, 1, stats.nacksReceived, "a distinct ack counts once")
	assert.False(t, stats.commandSnapshot(cmds[0]).rejected, "the first answer stands")
}

// A JSON payload is not a valid ack and is never counted as a rejection.
func TestAddAckReceivedRejectsJSON(t *testing.T) {
	stats, cmds := newStatusTracker(1)
	stats.AddAckReceived(testUplinkTopic+"/ack", []byte(`{"status":"NACK","index":1}`))
	assert.Equal(t, 0, stats.nacksReceived)
	assert.False(t, stats.commandSnapshot(cmds[0]).rejected)
}

func TestAddAckReceivedRecordsStationFields(t *testing.T) {
	tests := []struct {
		name         string
		ack          *streaming.Ack
		wantAcked    bool
		wantRejected bool
		wantFinished bool
	}{
		{
			name:         "ACK from a station that reports status",
			ack:          &streaming.Ack{ExpectedFinalStage: streaming.CommandStage_COMMAND_STAGE_RADIATED},
			wantAcked:    true,
			wantFinished: false,
		},
		{
			name:         "ACK from a station that reports no status",
			ack:          &streaming.Ack{},
			wantAcked:    true,
			wantFinished: true,
		},
		{
			name: "late NACK",
			ack: &streaming.Ack{
				Status: streaming.Ack_NACK, ReasonCode: commandseq.ReasonArrivedLate,
				Reason:             "arrived after later commands were sent; not transmitted",
				ExpectedFinalStage: streaming.CommandStage_COMMAND_STAGE_RADIATED,
			},
			wantRejected: true,
			wantFinished: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats, cmds := newStatusTracker(1)
			tt.ack.AckedMessageId = cmds[0].messageID
			tt.ack.Index = 1
			payload, err := proto.Marshal(tt.ack)
			require.NoError(t, err)
			stats.AddAckReceived(testUplinkTopic+"/ack", payload)

			snap := stats.commandSnapshot(cmds[0])
			assert.Equal(t, tt.wantAcked, snap.acked)
			assert.Equal(t, tt.wantRejected, snap.rejected)
			assert.Equal(t, tt.ack.GetExpectedFinalStage(), snap.expectedFinalStage)
			assert.Equal(t, tt.ack.GetReasonCode(), snap.nackReasonCode)
			assert.Equal(t, tt.ack.GetReason(), snap.nackReason)
			assert.True(t, isClosed(cmds[0].answered))
			assert.Equal(t, tt.wantFinished, isClosed(cmds[0].finished))
		})
	}
}

// An ack carrying an acked message ID matches only that message, never
// another client's command that happens to share the index.
func TestAddAckReceivedMatchesByMessageID(t *testing.T) {
	stats, cmds := newStatusTracker(1)
	payload, err := proto.Marshal(&streaming.Ack{AckedMessageId: "someone-else", Index: 1})
	require.NoError(t, err)
	stats.AddAckReceived(testUplinkTopic+"/ack", payload)
	assert.Zero(t, stats.acksReceived, "another client's ack is not counted")
	assert.False(t, stats.commandSnapshot(cmds[0]).acked)
}

func TestCountDeliveries(t *testing.T) {
	cmds := []sentCommand{
		{cmdType: cmdTypeUplink, delivery: commandDelivery{
			stage:   streaming.CommandStage_COMMAND_STAGE_RADIATED,
			outcome: streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED,
		}},
		{cmdType: cmdTypeUplink, delivery: commandDelivery{
			stage:   streaming.CommandStage_COMMAND_STAGE_RADIATED,
			outcome: streaming.CommandOutcome_COMMAND_OUTCOME_IN_PROGRESS,
		}},
		{cmdType: cmdTypeUplink, delivery: commandDelivery{outcome: streaming.CommandOutcome_COMMAND_OUTCOME_FAILED}},
		{cmdType: cmdTypeUplink, delivery: commandDelivery{outcome: streaming.CommandOutcome_COMMAND_OUTCOME_UNKNOWN}},
		{cmdType: cmdTypeUplink, rejected: true, nackReasonCode: commandseq.ReasonArrivedLate},
		{cmdType: cmdTypeUplink, delivery: commandDelivery{skipped: true}},
		{cmdType: cmdTypeUplink, rejected: true, nackReasonCode: commandseq.ReasonIndexReused},
		{
			cmdType:  cmdTypeConfigRequest,
			delivery: commandDelivery{outcome: streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED},
		},
		{
			cmdType:  cmdTypeConfigRequest,
			delivery: commandDelivery{outcome: streaming.CommandOutcome_COMMAND_OUTCOME_PARTIAL},
		},
		{cmdType: cmdTypeUplink, delivery: commandDelivery{
			stage:   streaming.CommandStage_COMMAND_STAGE_RADIATED,
			outcome: streaming.CommandOutcome_COMMAND_OUTCOME_PARTIAL,
		}},
	}
	got := countDeliveries(cmds)
	assert.Equal(t, deliveryCounts{
		radiated: 2, partlyRadiated: 1, applied: 1, partial: 1, failed: 1, unknown: 1, late: 2,
	}, got)
	assert.Equal(t,
		"2 radiated, 1 partly radiated, 1 applied, 1 partially applied, 1 failed, 1 unknown, 2 late",
		got.String())
	assert.Empty(t, deliveryCounts{}.String())
}

func TestEnumNames(t *testing.T) {
	assert.Equal(t, "radiated", stageName(streaming.CommandStage_COMMAND_STAGE_RADIATED))
	assert.Empty(t, stageName(streaming.CommandStage_COMMAND_STAGE_UNSPECIFIED))
	assert.Equal(t, "unknown", outcomeName(streaming.CommandOutcome_COMMAND_OUTCOME_UNKNOWN))
	assert.Empty(t, outcomeName(streaming.CommandOutcome_COMMAND_OUTCOME_UNSPECIFIED))
	assert.Equal(t, "deadline_exceeded", failureName(streaming.CommandFailure_COMMAND_FAILURE_DEADLINE_EXCEEDED))
	assert.Empty(t, failureName(streaming.CommandFailure_COMMAND_FAILURE_UNSPECIFIED))
}

func TestCommandReplyTopics(t *testing.T) {
	streams := StreamsConfig{
		Uplink: []CommandConfig{
			{AckTopic: "e/pass/p/channel/c1/uplink/ack", StatusTopic: "e/pass/p/channel/c1/uplink/status"},
			{AckTopic: "e/pass/p/channel/c2/uplink/ack"},
		},
		ConfigRequest: []CommandConfig{
			{
				AckTopic:    "e/pass/p/channel/c1/config_request/ack",
				StatusTopic: "e/pass/p/channel/c1/config_request/status",
			},
		},
	}
	assert.Equal(t, []string{
		"e/pass/p/channel/c1/uplink/ack",
		"e/pass/p/channel/c1/uplink/status",
		"e/pass/p/channel/c2/uplink/ack",
		"e/pass/p/channel/c1/config_request/ack",
		"e/pass/p/channel/c1/config_request/status",
	}, commandReplyTopics(streams))
	assert.Contains(t, collectMQTTTopics(streams), "e/pass/p/channel/+/uplink/status")
	assert.Contains(t, collectMQTTTopics(streams), "e/pass/p/channel/+/config_request/status")
}

// subscribingMQTTClient records subscriptions and lets a test deliver a
// message to the registered handler.
type subscribingMQTTClient struct {
	mockMQTTClient

	mu       sync.Mutex
	handlers map[string]mqtt.MessageHandler
}

func (c *subscribingMQTTClient) Subscribe(topic string, _ byte, cb mqtt.MessageHandler) mqtt.Token {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handlers == nil {
		c.handlers = map[string]mqtt.MessageHandler{}
	}
	c.handlers[topic] = cb
	return &mockToken{}
}

type testMessage struct {
	mqtt.Message
	topic   string
	payload []byte
}

func (m testMessage) Topic() string   { return m.topic }
func (m testMessage) Payload() []byte { return m.payload }

func TestSubscribeCommandReplies(t *testing.T) {
	client := &subscribingMQTTClient{mockMQTTClient: mockMQTTClient{connected: true}}
	cfg := Config{MQTTQoS: 1, AuthorizerCreds: &AuthorizerCredentials{Streams: StreamsConfig{
		Uplink: []CommandConfig{{
			AckTopic: testUplinkTopic + "/ack", StatusTopic: testUplinkTopic + "/status",
		}},
	}}}
	stats, cmds := newStatusTracker(1)
	subscribeCommandReplies(client, cfg, stats)

	ackFilter := "dev/pass/p1/channel/+/uplink/ack"
	statusFilter := "dev/pass/p1/channel/+/uplink/status"
	require.Contains(t, client.handlers, ackFilter)
	require.Contains(t, client.handlers, statusFilter)

	client.handlers[ackFilter](client, testMessage{
		topic: testUplinkTopic + "/ack", payload: testAck(t, cmds[0], streaming.Ack_ACK, ""),
	})
	client.handlers[statusFilter](client, testMessage{
		topic: testUplinkTopic + "/status", payload: statusBatch(t, &streaming.CommandStatus{
			AckedMessageId: cmds[0].messageID, Stage: streaming.CommandStage_COMMAND_STAGE_RADIATED,
			Outcome: streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED, MessageFinal: true,
		}),
	})
	snap := stats.commandSnapshot(cmds[0])
	assert.True(t, snap.acked)
	assert.True(t, snap.delivery.final)
}

func TestSubscribeCommandRepliesWithoutCredentials(t *testing.T) {
	client := &subscribingMQTTClient{mockMQTTClient: mockMQTTClient{connected: true}}
	subscribeCommandReplies(client, Config{}, newStatsTracker(false))
	assert.Empty(t, client.handlers)
}

func TestPrintStreamSummaryDeliveryCounts(t *testing.T) {
	out := captureUI(t)
	stats, cmds := newStatusTracker(2)
	stats.AddStatusBatch(testUplinkTopic+"/status", statusBatch(t, &streaming.CommandStatus{
		AckedMessageId: cmds[0].messageID, Stage: streaming.CommandStage_COMMAND_STAGE_RADIATED,
		Outcome: streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED, MessageFinal: true,
	}))
	stats.AddAckReceived(testUplinkTopic+"/ack", testAck(t, cmds[1], streaming.Ack_NACK, commandseq.ReasonArrivedLate))

	printStreamSummary(Config{EnableUplink: true, DestDir: t.TempDir()}, stats)

	assert.Contains(t, out.String(), "[Uplink] 2 satellite commands sent, 0 ground-station config requests sent\n")
	assert.Contains(t, out.String(), "[Uplink] 1 radiated, 1 late\n")
	assert.Contains(t, out.String(), "[Acks] 0 sent, 0 received, 1 rejected\n")
}

// The station keeps no record of a command it could not hold, so an ACK for
// the same message sent again unchanged settles it; other rejections stand.
func TestAckAfterHoldBufferFullSettlesTheCommand(t *testing.T) {
	stats, cmds := newStatusTracker(2)
	stats.AddAckReceived(testUplinkTopic+"/ack", testAck(t, cmds[0], streaming.Ack_NACK, commandseq.ReasonHoldBufferFull))
	snap := stats.commandSnapshot(cmds[0])
	assert.True(t, snap.rejected)
	assert.Equal(t, commandseq.ReasonHoldBufferFull, snap.nackReasonCode)

	stats.AddAckReceived(testUplinkTopic+"/ack", testAck(t, cmds[0], streaming.Ack_ACK, ""))
	snap = stats.commandSnapshot(cmds[0])
	assert.True(t, snap.acked)
	assert.False(t, snap.rejected)
	assert.Empty(t, snap.nackReasonCode)
	assert.Equal(t, 1, stats.nacksReceived)
	assert.Equal(t, 1, stats.acksReceived)

	stats.AddAckReceived(testUplinkTopic+"/ack", testAck(t, cmds[1], streaming.Ack_NACK, commandseq.ReasonIndexReused))
	stats.AddAckReceived(testUplinkTopic+"/ack", testAck(t, cmds[1], streaming.Ack_ACK, ""))
	assert.True(t, stats.commandSnapshot(cmds[1]).rejected, "a reused index stays refused")
}

// A status for a command whose ack was lost still proves the station
// accepted it: the command counts as answered at once rather than after the
// wait timeout, and shows as acked.
func TestStatusWithoutAckAnswersTheCommand(t *testing.T) {
	stats, cmds := newStatusTracker(1)
	stats.AddStatusBatch(testUplinkTopic+"/status", statusBatch(t, &streaming.CommandStatus{
		StreamId: cmds[0].streamID, Index: uint64(cmds[0].index), MessageId: cmds[0].messageID,
		Stage: streaming.CommandStage_COMMAND_STAGE_DISPATCHED, Outcome: streaming.CommandOutcome_COMMAND_OUTCOME_IN_PROGRESS,
	}))
	snap := stats.commandSnapshot(cmds[0])
	assert.True(t, snap.acked)
	assert.True(t, isClosed(cmds[0].answered))
	assert.False(t, isClosed(cmds[0].finished), "not final yet")
	assert.Zero(t, stats.acksReceived, "no ack arrived")
}
