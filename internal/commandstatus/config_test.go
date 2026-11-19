// Copyright (c) 2026 InfoStellar, Inc. All Rights Reserved.

package commandstatus

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"

	streaming "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/satellitestreamer"
	v1 "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/starpass/v1"
	"github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/starpass/v1/radio"
)

func fullRequest() *v1.GroundStationConfigurationRequest {
	return &v1.GroundStationConfigurationRequest{
		TransmitterConfigurationRequest: &v1.TransmitterConfigurationRequest{
			EnableCarrier:      wrapperspb.Bool(true),
			EnableIfModulation: wrapperspb.Bool(true),
			EnableIdlePattern:  wrapperspb.Bool(false),
			EnableIfSweep:      wrapperspb.Bool(false),
			Bitrate:            wrapperspb.Float(9600),
			Modulation:         radio.Modulation_BPSK,
		},
		ReceiverConfigurationRequest: &v1.ReceiverConfigurationRequest{
			Bitrate:    wrapperspb.Float(9600),
			Modulation: radio.Modulation_QPSK,
		},
	}
}

func TestSetFields(t *testing.T) {
	assert.Equal(t, []string{
		FieldTransmitterEnableCarrier,
		FieldTransmitterEnableIFModulation,
		FieldTransmitterEnableIdlePattern,
		FieldTransmitterEnableIFSweep,
		FieldTransmitterBitrate,
		FieldTransmitterModulation,
		FieldReceiverBitrate,
		FieldReceiverModulation,
	}, SetFields(fullRequest()))

	assert.Equal(t, []string{FieldReceiverBitrate}, SetFields(&v1.GroundStationConfigurationRequest{
		TransmitterConfigurationRequest: &v1.TransmitterConfigurationRequest{Modulation: radio.Modulation_DISABLED},
		ReceiverConfigurationRequest:    &v1.ReceiverConfigurationRequest{Bitrate: wrapperspb.Float(1)},
	}), "a DISABLED modulation is not a change")

	assert.Empty(t, SetFields(&v1.GroundStationConfigurationRequest{}))
}

func receiverRequest() *v1.GroundStationConfigurationRequest {
	return &v1.GroundStationConfigurationRequest{
		ReceiverConfigurationRequest: &v1.ReceiverConfigurationRequest{
			Bitrate:    wrapperspb.Float(9600),
			Modulation: radio.Modulation_QPSK,
		},
	}
}

func TestConfigResults_AllApplied(t *testing.T) {
	c := NewConfigResults(receiverRequest())
	c.Applied(FieldReceiverBitrate)
	c.Applied(FieldReceiverModulation)
	u := c.Update(nil)
	assert.Equal(t, streaming.CommandStage_COMMAND_STAGE_APPLIED, u.Stage)
	assert.Equal(t, streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED, u.Outcome)
	assert.Len(t, u.ConfigFields, 2)
}

func TestConfigResults_Partial(t *testing.T) {
	c := NewConfigResults(receiverRequest())
	c.Applied(FieldReceiverBitrate)
	c.Applied(FieldReceiverModulation)
	c.Refused(FieldReceiverModulation, streaming.CommandFailure_COMMAND_FAILURE_UNSUPPORTED, "cortex-hdr", "QPSK is not supported")
	c.Refused(FieldReceiverModulation, streaming.CommandFailure_COMMAND_FAILURE_STATION_ERROR, "other", "later")
	c.Applied(FieldReceiverModulation)

	u := c.Update(nil)
	assert.Equal(t, streaming.CommandOutcome_COMMAND_OUTCOME_PARTIAL, u.Outcome)
	mod := u.ConfigFields[1]
	assert.Equal(t, FieldReceiverModulation, mod.GetField())
	assert.Equal(t, streaming.CommandOutcome_COMMAND_OUTCOME_FAILED, mod.GetOutcome(), "a refusal beats any success")
	assert.Equal(t, "cortex-hdr", mod.GetProviderCode(), "the first refusal wins")
}

func TestConfigResults_UnreportedFields(t *testing.T) {
	u := NewConfigResults(receiverRequest()).Update(nil)
	assert.Equal(t, streaming.CommandOutcome_COMMAND_OUTCOME_FAILED, u.Outcome)
	assert.Equal(t, streaming.CommandFailure_COMMAND_FAILURE_UNSUPPORTED, u.Failure)
	assert.Equal(t, "no device at this ground station handles receiver.bitrate", u.Reason)
	for _, f := range u.ConfigFields {
		assert.Equal(t, streaming.CommandFailure_COMMAND_FAILURE_UNSUPPORTED, f.GetFailure())
	}

	c := NewConfigResults(receiverRequest())
	c.Applied(FieldReceiverBitrate)
	u = c.Update(errors.New("device timeout"))
	assert.Equal(t, streaming.CommandOutcome_COMMAND_OUTCOME_PARTIAL, u.Outcome)
	assert.Equal(t, streaming.CommandFailure_COMMAND_FAILURE_STATION_ERROR, u.ConfigFields[1].GetFailure())
	assert.Equal(t, "device timeout", u.ConfigFields[1].GetReason())
}

func TestConfigResults_RefusedFor(t *testing.T) {
	refusal := &FieldError{
		Field: FieldReceiverBitrate, Failure: streaming.CommandFailure_COMMAND_FAILURE_OUT_OF_RANGE, Reason: "too slow",
	}
	assert.Equal(t, "receiver.bitrate: too slow", refusal.Error())

	c := NewConfigResults(receiverRequest())
	c.Applied(FieldReceiverModulation)
	c.RefusedFor(refusal)
	u := c.Update(nil)
	assert.Equal(t, streaming.CommandOutcome_COMMAND_OUTCOME_PARTIAL, u.Outcome)
	bitrate := u.ConfigFields[0]
	assert.Equal(t, FieldReceiverBitrate, bitrate.GetField())
	assert.Equal(t, streaming.CommandFailure_COMMAND_FAILURE_OUT_OF_RANGE, bitrate.GetFailure())
	assert.Equal(t, "too slow", bitrate.GetReason())
}

func TestConfigResults_EmptyRequestSucceeds(t *testing.T) {
	u := NewConfigResults(&v1.GroundStationConfigurationRequest{}).Update(nil)
	assert.Equal(t, streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED, u.Outcome)
	assert.Empty(t, u.ConfigFields)
}

func TestConfigResults_NilIgnoresReports(t *testing.T) {
	var c *ConfigResults
	c.Applied(FieldReceiverBitrate)
	c.Refused(FieldReceiverBitrate, streaming.CommandFailure_COMMAND_FAILURE_OUT_OF_RANGE, "", "")
}

// Message types and field paths are a public wire contract: clients, SCL rows
// and the pass API carry them verbatim.
func TestWireNamesArePinned(t *testing.T) {
	assert.Equal(t, "uplink", MessageTypeUplink)
	assert.Equal(t, "gs_config_request", MessageTypeConfig)
	for field, want := range map[string]string{
		FieldTransmitterEnableCarrier:      "transmitter.enable_carrier",
		FieldTransmitterEnableIFModulation: "transmitter.enable_if_modulation",
		FieldTransmitterEnableIdlePattern:  "transmitter.enable_idle_pattern",
		FieldTransmitterEnableIFSweep:      "transmitter.enable_if_sweep",
		FieldTransmitterBitrate:            "transmitter.bitrate",
		FieldTransmitterModulation:         "transmitter.modulation",
		FieldReceiverBitrate:               "receiver.bitrate",
		FieldReceiverModulation:            "receiver.modulation",
	} {
		assert.Equal(t, want, field)
	}
}

// A field with no result from a driver that gives none leaves the request
// unknown, since it may have been applied.
func TestConfigResults_Unknown(t *testing.T) {
	r := NewConfigResults(&v1.GroundStationConfigurationRequest{
		TransmitterConfigurationRequest: &v1.TransmitterConfigurationRequest{
			EnableCarrier: wrapperspb.Bool(true), Bitrate: wrapperspb.Float(9600),
		},
	})
	r.Applied(FieldTransmitterEnableCarrier)
	r.Unknown(FieldTransmitterBitrate, "the driver does not report whether it applied the request")
	u := r.Update(nil)
	assert.Equal(t, streaming.CommandOutcome_COMMAND_OUTCOME_UNKNOWN, u.Outcome)
	assert.Equal(t, streaming.CommandFailure_COMMAND_FAILURE_NO_DEVICE_ACK, u.Failure)
	assert.Equal(t, "the driver does not report whether it applied the request", u.Reason)
	assert.True(t, r.IsApplied(FieldTransmitterEnableCarrier))
	assert.False(t, r.IsApplied(FieldTransmitterBitrate))
	assert.False(t, (*ConfigResults)(nil).IsApplied(FieldTransmitterBitrate))
}

// The failure of the handling error names what happened to the fields nobody
// reported: a status error or a field error carries its own, anything else
// is a station error.
func TestConfigResults_UpdateCarriesTheHandlingFailure(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want streaming.CommandFailure
	}{
		{"status error", &Error{Failure: streaming.CommandFailure_COMMAND_FAILURE_PLAN_ENDED, Reason: "pass ended"},
			streaming.CommandFailure_COMMAND_FAILURE_PLAN_ENDED},
		{"field error", &FieldError{Field: FieldTransmitterBitrate, Failure: streaming.CommandFailure_COMMAND_FAILURE_OUT_OF_RANGE},
			streaming.CommandFailure_COMMAND_FAILURE_OUT_OF_RANGE},
		{"wrapped status error", fmt.Errorf("listener x: %w", &Error{Failure: streaming.CommandFailure_COMMAND_FAILURE_PLAN_ENDED}),
			streaming.CommandFailure_COMMAND_FAILURE_PLAN_ENDED},
		{"other error", errors.New("boom"), streaming.CommandFailure_COMMAND_FAILURE_STATION_ERROR},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := NewConfigResults(&v1.GroundStationConfigurationRequest{
				TransmitterConfigurationRequest: &v1.TransmitterConfigurationRequest{Bitrate: wrapperspb.Float(9600)},
			})
			u := r.Update(tt.err)
			assert.Equal(t, streaming.CommandOutcome_COMMAND_OUTCOME_FAILED, u.Outcome)
			assert.Equal(t, tt.want, u.Failure)
			require.Len(t, u.ConfigFields, 1)
			assert.Equal(t, tt.want, u.ConfigFields[0].GetFailure())
		})
	}
}
