// Copyright (c) 2026 InfoStellar, Inc. All Rights Reserved.

package commandstatus

import (
	"errors"
	"sync"

	streaming "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/satellitestreamer"
	v1 "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/starpass/v1"
	"github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/starpass/v1/radio"
)

// Configuration field paths, as reported in ConfigFieldResult.field.
const (
	FieldTransmitterEnableCarrier      = "transmitter.enable_carrier"
	FieldTransmitterEnableIFModulation = "transmitter.enable_if_modulation"
	FieldTransmitterEnableIdlePattern  = "transmitter.enable_idle_pattern"
	FieldTransmitterEnableIFSweep      = "transmitter.enable_if_sweep"
	FieldTransmitterBitrate            = "transmitter.bitrate"
	FieldTransmitterModulation         = "transmitter.modulation"
	FieldReceiverBitrate               = "receiver.bitrate"
	FieldReceiverModulation            = "receiver.modulation"
)

// SetFields lists the fields req changes, in a fixed order. A DISABLED
// modulation is ignored by every device, so it does not count as set.
func SetFields(req *v1.GroundStationConfigurationRequest) []string {
	tx := req.GetTransmitterConfigurationRequest()
	rx := req.GetReceiverConfigurationRequest()
	candidates := []struct {
		field string
		set   bool
	}{
		{FieldTransmitterEnableCarrier, tx.GetEnableCarrier() != nil},
		{FieldTransmitterEnableIFModulation, tx.GetEnableIfModulation() != nil},
		{FieldTransmitterEnableIdlePattern, tx.GetEnableIdlePattern() != nil},
		{FieldTransmitterEnableIFSweep, tx.GetEnableIfSweep() != nil},
		{FieldTransmitterBitrate, tx.GetBitrate() != nil},
		{FieldTransmitterModulation, tx.GetModulation() != radio.Modulation_DISABLED},
		{FieldReceiverBitrate, rx.GetBitrate() != nil},
		{FieldReceiverModulation, rx.GetModulation() != radio.Modulation_DISABLED},
	}
	fields := make([]string, 0, len(candidates))
	for _, c := range candidates {
		if c.set {
			fields = append(fields, c.field)
		}
	}
	return fields
}

// FieldError is a configuration value a device can never apply.
type FieldError struct {
	Field   string
	Failure streaming.CommandFailure
	Reason  string
}

func (e *FieldError) Error() string { return e.Field + ": " + e.Reason }

func (e *FieldError) result() *streaming.ConfigFieldResult {
	return &streaming.ConfigFieldResult{
		Field:   e.Field,
		Outcome: streaming.CommandOutcome_COMMAND_OUTCOME_FAILED,
		Failure: e.Failure,
		Reason:  e.Reason,
	}
}

// ConfigResults collects per-field results from the listeners handling one
// configuration request. Applied and Refused on a nil *ConfigResults do
// nothing.
type ConfigResults struct {
	fields []string

	mu      sync.Mutex
	results map[string]*streaming.ConfigFieldResult
}

// GobEncode and GobDecode let an OnConfiguration cross the v3 driver
// protocol, which gob-encodes it: gob refuses the type otherwise, for the
// mutex. The results stay on the receiver's side (v3.Write nils the field),
// so a decoded ConfigResults carries no fields and accepts reports.
func (*ConfigResults) GobEncode() ([]byte, error) { return nil, nil }
func (c *ConfigResults) GobDecode([]byte) error {
	*c = ConfigResults{results: make(map[string]*streaming.ConfigFieldResult)}
	return nil
}

func NewConfigResults(req *v1.GroundStationConfigurationRequest) *ConfigResults {
	return &ConfigResults{fields: SetFields(req), results: make(map[string]*streaming.ConfigFieldResult)}
}

func (c *ConfigResults) Applied(field string) {
	c.report(&streaming.ConfigFieldResult{Field: field, Outcome: streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED})
}

func (c *ConfigResults) Refused(field string, failure streaming.CommandFailure, providerCode, reason string) {
	c.report(&streaming.ConfigFieldResult{
		Field:        field,
		Outcome:      streaming.CommandOutcome_COMMAND_OUTCOME_FAILED,
		Failure:      failure,
		ProviderCode: providerCode,
		Reason:       reason,
	})
}

// RefusedFor records e as its field's result.
func (c *ConfigResults) RefusedFor(e *FieldError) {
	c.report(e.result())
}

// Unknown records that nothing reports whether field was applied: the
// request was handed to a device or driver that gives no result.
func (c *ConfigResults) Unknown(field, reason string) {
	c.report(&streaming.ConfigFieldResult{
		Field:   field,
		Outcome: streaming.CommandOutcome_COMMAND_OUTCOME_UNKNOWN,
		Failure: streaming.CommandFailure_COMMAND_FAILURE_NO_DEVICE_ACK,
		Reason:  reason,
	})
}

// IsApplied reports whether field was applied and no device refused it.
func (c *ConfigResults) IsApplied(field string) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.results[field]
	return ok && r.GetOutcome() == streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED
}

// report records a field's result. A field counts as applied only if no
// device refused it, so the first refusal wins over any success.
func (c *ConfigResults) report(r *streaming.ConfigFieldResult) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	prev, ok := c.results[r.GetField()]
	if !ok || (prev.GetOutcome() == streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED &&
		r.GetOutcome() != streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED) {
		c.results[r.GetField()] = r
	}
}

// Update builds the request's final status. Fields no listener reported are
// unsupported, or failed with err's failure when handling failed: an *Error
// or *FieldError names its own, any other error is a station error. A field
// with an unknown outcome makes the request unknown, since it may have been
// applied.
func (c *ConfigResults) Update(err error) Update {
	c.mu.Lock()
	defer c.mu.Unlock()

	u := Update{Stage: streaming.CommandStage_COMMAND_STAGE_APPLIED}
	applied, unknown := 0, 0
	for _, field := range c.fields {
		r, ok := c.results[field]
		switch {
		case ok:
		case err != nil:
			r = failedField(field, err)
		default:
			r = Unhandled(field).result()
		}
		switch r.GetOutcome() {
		case streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED:
			applied++
		case streaming.CommandOutcome_COMMAND_OUTCOME_UNKNOWN:
			unknown++
		}
		u.ConfigFields = append(u.ConfigFields, r)
	}

	switch {
	case unknown > 0:
		u.Outcome = streaming.CommandOutcome_COMMAND_OUTCOME_UNKNOWN
		u.Failure, u.Reason = firstOutcome(u.ConfigFields, streaming.CommandOutcome_COMMAND_OUTCOME_UNKNOWN)
	case applied == len(c.fields):
		u.Outcome = streaming.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED
	case applied == 0:
		u.Outcome = streaming.CommandOutcome_COMMAND_OUTCOME_FAILED
		u.Failure = u.ConfigFields[0].GetFailure()
		u.Reason = u.ConfigFields[0].GetReason()
	default:
		u.Outcome = streaming.CommandOutcome_COMMAND_OUTCOME_PARTIAL
	}
	return u
}

// firstOutcome returns the failure and reason of the first field with the
// given outcome.
func firstOutcome(fields []*streaming.ConfigFieldResult, outcome streaming.CommandOutcome) (streaming.CommandFailure, string) {
	for _, f := range fields {
		if f.GetOutcome() == outcome {
			return f.GetFailure(), f.GetReason()
		}
	}
	return streaming.CommandFailure_COMMAND_FAILURE_UNSPECIFIED, ""
}

// failedField is the result of a field whose handling failed with err before
// anything reported it.
func failedField(field string, err error) *streaming.ConfigFieldResult {
	var fieldErr *FieldError
	var statusErr *Error
	switch {
	case errors.As(err, &fieldErr):
		return &streaming.ConfigFieldResult{
			Field: field, Outcome: streaming.CommandOutcome_COMMAND_OUTCOME_FAILED,
			Failure: fieldErr.Failure, Reason: fieldErr.Reason,
		}
	case errors.As(err, &statusErr):
		return &streaming.ConfigFieldResult{
			Field: field, Outcome: streaming.CommandOutcome_COMMAND_OUTCOME_FAILED,
			Failure: statusErr.Failure, ProviderCode: statusErr.ProviderCode, Reason: statusErr.Reason,
		}
	}
	return &streaming.ConfigFieldResult{
		Field:   field,
		Outcome: streaming.CommandOutcome_COMMAND_OUTCOME_FAILED,
		Failure: streaming.CommandFailure_COMMAND_FAILURE_STATION_ERROR,
		Reason:  err.Error(),
	}
}

// Unhandled is the error for a field no device at the station applies.
func Unhandled(field string) *FieldError {
	return &FieldError{
		Field:   field,
		Failure: streaming.CommandFailure_COMMAND_FAILURE_UNSUPPORTED,
		Reason:  "no device at this ground station handles " + field,
	}
}
