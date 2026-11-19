package main

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	streaming "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/satellitestreamer"
	"github.com/infostellarinc/stellarstation-cli/internal/commandseq"
)

func TestParseWaitMode(t *testing.T) {
	tests := []struct {
		in      string
		want    waitMode
		wantErr bool
	}{
		{in: "received", want: waitForReceived},
		{in: " FINAL ", want: waitForFinal},
		{in: "", wantErr: true},
		{in: "radiated", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := parseWaitMode(tt.in)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), `--wait-for must be "received" or "final"`)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMoreSevere(t *testing.T) {
	tests := []struct{ a, b, want int }{
		{exitOK, exitOK, exitOK},
		{exitOK, exitFailed, exitFailed},
		{exitFailed, exitOK, exitFailed},
		{exitFailed, exitUnknown, exitUnknown},
		{exitUnknown, exitFailed, exitUnknown},
		{exitUnknown, exitOK, exitUnknown},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, moreSevere(tt.a, tt.b), "moreSevere(%d, %d)", tt.a, tt.b)
	}
}

func TestResultOf(t *testing.T) {
	const timeout = 30 * time.Second
	radiatedStage := streaming.CommandStage_COMMAND_STAGE_RADIATED
	acked := sentCommand{cmdType: cmdTypeUplink, index: 2, acked: true}
	ackedWithStatus := acked
	ackedWithStatus.expectedFinalStage = radiatedStage
	final := func(outcome streaming.CommandOutcome, d commandDelivery) sentCommand {
		c := ackedWithStatus
		d.final = true
		d.outcome = outcome
		c.delivery = d
		return c
	}
	tests := []struct {
		name     string
		cmd      sentCommand
		mode     waitMode
		wantCode int
		wantText string
	}{
		{
			name: "accepted", cmd: acked, mode: waitForReceived, wantCode: exitOK,
			wantText: "Command #2 was accepted by the ground station.",
		},
		{
			name: "accepted, final requested but no status reported", cmd: acked, mode: waitForFinal,
			wantCode: exitOK, wantText: "Command #2 was accepted by the ground station.",
		},
		{
			name: "accepted, final status ignored when only receipt is awaited", mode: waitForReceived,
			cmd:      final(streaming.CommandOutcome_COMMAND_OUTCOME_FAILED, commandDelivery{}),
			wantCode: exitOK, wantText: "Command #2 was accepted by the ground station.",
		},
		{
			name: "refused late", mode: waitForReceived,
			cmd: sentCommand{
				cmdType: cmdTypeUplink, index: 2, rejected: true, nackReasonCode: commandseq.ReasonArrivedLate,
				nackReason: "arrived after later commands were sent; not transmitted",
			},
			wantCode: exitFailed,
			wantText: "Command #2 was refused by the ground station: arrived after later commands were sent; not transmitted.",
		},
		{
			name: "refused without a reason shows the code", mode: waitForFinal,
			cmd: sentCommand{
				cmdType: cmdTypeConfigRequest, index: 1, rejected: true, nackReasonCode: commandseq.ReasonIndexReused,
			},
			wantCode: exitFailed,
			wantText: "Configuration request #1 was refused by the ground station: index_reused.",
		},
		{
			name: "no acknowledgement", mode: waitForReceived,
			cmd:      sentCommand{cmdType: cmdTypeUplink, index: 2},
			wantCode: exitUnknown,
			wantText: "Command #2 was not acknowledged within 30s. It may still reach the satellite; " +
				"check spacecraft telemetry before sending it again.",
		},
		{
			name: "skipped", mode: waitForReceived,
			cmd:      sentCommand{cmdType: cmdTypeUplink, index: 2, delivery: commandDelivery{skipped: true, final: true}},
			wantCode: exitFailed,
			wantText: "Command #2 was not received by the ground station in time and was not transmitted.",
		},
		{
			name: "radiated", mode: waitForFinal,
			cmd: final(streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED, commandDelivery{
				stage: radiatedStage, radiatedAt: testRadiatedAt,
			}),
			wantCode: exitOK, wantText: "Command #2 radiated at 2026-10-01T12:30:05Z.",
		},
		{
			name: "final status without an ack", mode: waitForFinal,
			cmd: sentCommand{cmdType: cmdTypeUplink, index: 2, delivery: commandDelivery{
				final: true, stage: radiatedStage, outcome: streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED,
			}},
			wantCode: exitOK, wantText: "Command #2 radiated.",
		},
		{
			name: "succeeded without a stage", mode: waitForFinal,
			cmd:      final(streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED, commandDelivery{}),
			wantCode: exitOK, wantText: "Command #2 was delivered.",
		},
		{
			name: "failed", mode: waitForFinal,
			cmd: final(streaming.CommandOutcome_COMMAND_OUTCOME_FAILED, commandDelivery{
				failure: streaming.CommandFailure_COMMAND_FAILURE_ENCODER_LOCKED_OUT, providerCode: "cortex-tcu:1",
			}),
			wantCode: exitFailed,
			wantText: "Command #2 failed: the ground station encoder was locked out (cortex-tcu:1).",
		},
		{
			name: "unknown", mode: waitForFinal,
			cmd: final(streaming.CommandOutcome_COMMAND_OUTCOME_UNKNOWN, commandDelivery{
				failure: streaming.CommandFailure_COMMAND_FAILURE_NO_DEVICE_ACK,
			}),
			wantCode: exitUnknown,
			wantText: "Command #2 outcome unknown: the ground station could not confirm whether it was transmitted. " +
				"Check spacecraft telemetry before sending it again.",
		},
		{
			name: "config partially applied", mode: waitForFinal,
			cmd: func() sentCommand {
				c := final(streaming.CommandOutcome_COMMAND_OUTCOME_PARTIAL, commandDelivery{
					configFields: []*streaming.ConfigFieldResult{{
						Field: "transmitter.bitrate", Outcome: streaming.CommandOutcome_COMMAND_OUTCOME_FAILED,
						Failure: streaming.CommandFailure_COMMAND_FAILURE_UNSUPPORTED,
					}},
				})
				c.cmdType = cmdTypeConfigRequest
				return c
			}(),
			wantCode: exitFailed,
			wantText: "Configuration request #2 partially applied: transmitter.bitrate: no equipment at this " +
				"ground station supports this setting.",
		},
		{
			name: "accepted, no final status in time", mode: waitForFinal, cmd: ackedWithStatus,
			wantCode: exitUnknown,
			wantText: "Command #2 was accepted, but no final delivery status arrived within 30s. " +
				"Check spacecraft telemetry before sending it again.",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := resultOf(tt.cmd, tt.mode, timeout)
			assert.Equal(t, tt.wantCode, got.code)
			assert.Equal(t, tt.wantText, got.text)
		})
	}
}

// captureUI sends the UI narration to a buffer for the rest of the test.
func captureUI(t *testing.T) *bytes.Buffer {
	t.Helper()
	initUI()
	var buf bytes.Buffer
	uiMu.Lock()
	prev := uiOut
	uiOut = &buf
	uiMu.Unlock()
	t.Cleanup(func() {
		uiMu.Lock()
		uiOut = prev
		uiMu.Unlock()
	})
	return &buf
}

// runAwait runs awaitCommands on a mock clock, advancing it until the wait
// returns, and returns its exit code and what it printed.
func runAwait(t *testing.T, stats *statsTracker, cmds []*sentCommand, mode waitMode) (int, string) {
	t.Helper()
	const timeout = 10 * time.Second
	out := captureUI(t)
	clk := clock.NewMock()
	done := make(chan error, 1)
	go func() { done <- awaitCommands(stats, cmds, mode, timeout, clk) }()
	var err error
	require.Eventually(t, func() bool {
		select {
		case err = <-done:
			return true
		default:
			clk.Add(timeout)
			return false
		}
	}, 5*time.Second, time.Millisecond)
	return exitCodeOf(err), out.String()
}

func exitCodeOf(err error) int {
	if err == nil {
		return exitOK
	}
	var e *exitCodeError
	if errors.As(err, &e) {
		return e.code
	}
	return -1
}

func TestExitCodeFor(t *testing.T) {
	code, shown := exitCodeFor(fmt.Errorf("wrapped: %w", &exitCodeError{code: exitUnknown}))
	assert.Equal(t, exitUnknown, code)
	assert.True(t, shown)
	code, shown = exitCodeFor(errors.New("bad hex"))
	assert.Equal(t, exitFailed, code)
	assert.False(t, shown)
}

// Waiting for a final status needs the status topics; without them the wait
// falls back to receipt.
func TestEffectiveWaitMode(t *testing.T) {
	withStatus := &AuthorizerCredentials{Streams: StreamsConfig{
		Uplink: []CommandConfig{{PublishTopic: "p", AckTopic: "p/ack", StatusTopic: "p/status"}},
	}}
	withoutStatus := &AuthorizerCredentials{Streams: StreamsConfig{
		Uplink: []CommandConfig{{PublishTopic: "p", AckTopic: "p/ack"}},
	}}
	assert.Equal(t, waitForFinal, effectiveWaitMode(Config{WaitFor: waitForFinal, AuthorizerCreds: withStatus}))
	assert.Equal(t, waitForReceived, effectiveWaitMode(Config{WaitFor: waitForFinal, AuthorizerCreds: withoutStatus}))
	assert.Equal(t, waitForReceived, effectiveWaitMode(Config{WaitFor: waitForReceived, AuthorizerCreds: withStatus}))
}

func TestAwaitCommands(t *testing.T) {
	ackWith := func(t *testing.T, cmd *sentCommand, ack *streaming.Ack) []byte {
		ack.AckedMessageId = cmd.messageID
		ack.Index = uint64(cmd.index)
		ack.MessageId = "ack-" + cmd.messageID
		payload, err := proto.Marshal(ack)
		require.NoError(t, err)
		return payload
	}
	radiating := &streaming.Ack{ExpectedFinalStage: streaming.CommandStage_COMMAND_STAGE_RADIATED}
	finalStatus := func(cmd *sentCommand, outcome streaming.CommandOutcome) *streaming.CommandStatus {
		return &streaming.CommandStatus{
			AckedMessageId: cmd.messageID, Index: uint64(cmd.index),
			Stage: streaming.CommandStage_COMMAND_STAGE_RADIATED, Outcome: outcome,
			Final: true, MessageFinal: true, MessageOutcome: outcome,
			RadiatedAt: timestamppb.New(testRadiatedAt),
		}
	}
	tests := []struct {
		name string
		mode waitMode
		// replies runs before the wait, as replies arriving during it would.
		replies  func(t *testing.T, stats *statsTracker, cmds []*sentCommand)
		wantCode int
		wantOut  []string
	}{
		{
			name: "received: all acknowledged", mode: waitForReceived,
			replies: func(t *testing.T, stats *statsTracker, cmds []*sentCommand) {
				for _, c := range cmds {
					stats.AddAckReceived(testUplinkTopic+"/ack", ackWith(t, c, &streaming.Ack{}))
				}
			},
			wantCode: exitOK,
			wantOut:  []string{"Command #1 was accepted", "Command #2 was accepted"},
		},
		{
			name: "received: one refused", mode: waitForReceived,
			replies: func(t *testing.T, stats *statsTracker, cmds []*sentCommand) {
				stats.AddAckReceived(testUplinkTopic+"/ack", ackWith(t, cmds[0], &streaming.Ack{}))
				stats.AddAckReceived(testUplinkTopic+"/ack", ackWith(t, cmds[1], &streaming.Ack{
					Status: streaming.Ack_NACK, ReasonCode: commandseq.ReasonArrivedLate, Reason: "arrived late",
				}))
			},
			wantCode: exitFailed,
			wantOut:  []string{"Command #1 was accepted", "Error: Command #2 was refused by the ground station: arrived late."},
		},
		{
			name: "received: one unanswered", mode: waitForReceived,
			replies: func(t *testing.T, stats *statsTracker, cmds []*sentCommand) {
				stats.AddAckReceived(testUplinkTopic+"/ack", ackWith(t, cmds[1], &streaming.Ack{
					Status: streaming.Ack_NACK, Reason: "bad",
				}))
			},
			wantCode: exitUnknown,
			wantOut:  []string{"Warning: Command #1 was not acknowledged within 10s.", "Command #2 was refused"},
		},
		{
			name: "final: radiated", mode: waitForFinal,
			replies: func(t *testing.T, stats *statsTracker, cmds []*sentCommand) {
				for _, c := range cmds {
					stats.AddAckReceived(testUplinkTopic+"/ack", ackWith(t, c, radiating))
					stats.AddStatusBatch(testUplinkTopic+"/status", statusBatch(t,
						finalStatus(c, streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED)))
				}
			},
			wantCode: exitOK,
			wantOut:  []string{"Command #1 radiated at 2026-10-01T12:30:05Z.", "Command #2 radiated at"},
		},
		{
			name: "final: failed and unknown report unknown", mode: waitForFinal,
			replies: func(t *testing.T, stats *statsTracker, cmds []*sentCommand) {
				stats.AddAckReceived(testUplinkTopic+"/ack", ackWith(t, cmds[0], radiating))
				stats.AddAckReceived(testUplinkTopic+"/ack", ackWith(t, cmds[1], radiating))
				stats.AddStatusBatch(testUplinkTopic+"/status", statusBatch(t,
					finalStatus(cmds[0], streaming.CommandOutcome_COMMAND_OUTCOME_FAILED),
					finalStatus(cmds[1], streaming.CommandOutcome_COMMAND_OUTCOME_UNKNOWN)))
			},
			wantCode: exitUnknown,
			wantOut:  []string{"Error: Command #1 failed", "Warning: Command #2 outcome unknown"},
		},
		{
			name: "final: no status in time", mode: waitForFinal,
			replies: func(t *testing.T, stats *statsTracker, cmds []*sentCommand) {
				for _, c := range cmds {
					stats.AddAckReceived(testUplinkTopic+"/ack", ackWith(t, c, radiating))
				}
			},
			wantCode: exitUnknown,
			wantOut:  []string{"Command #1 was accepted, but no final delivery status arrived within 10s."},
		},
		{
			name: "final: station reports no status", mode: waitForFinal,
			replies: func(t *testing.T, stats *statsTracker, cmds []*sentCommand) {
				for _, c := range cmds {
					stats.AddAckReceived(testUplinkTopic+"/ack", ackWith(t, c, &streaming.Ack{}))
				}
			},
			wantCode: exitOK,
			wantOut: []string{
				"Warning: The ground station does not report delivery status for these commands; " +
					"only receipt can be confirmed.",
				"Command #2 was accepted",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats, cmds := newStatusTracker(2)
			tt.replies(t, stats, cmds)
			code, out := runAwait(t, stats, cmds, tt.mode)
			assert.Equal(t, tt.wantCode, code)
			for _, want := range tt.wantOut {
				assert.Contains(t, out, want)
			}
		})
	}
}

// A reply that arrives while the wait is running ends it.
func TestAwaitCommandsWakesOnReply(t *testing.T) {
	stats, cmds := newStatusTracker(1)
	clk := clock.NewMock()
	done := make(chan error, 1)
	go func() { done <- awaitCommands(stats, cmds, waitForReceived, time.Minute, clk) }()

	stats.AddAckReceived(testUplinkTopic+"/ack", testAck(t, cmds[0], streaming.Ack_ACK, ""))
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the wait did not end on the acknowledgement")
	}
}
