package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/infostellarinc/stellarstation-cli/internal/apiclient"
)

// With channel metadata, only described high-rate channels start S3 readers,
// and an explicit selection without one starts none.
func TestConfigureChannelRatesFromMetadata(t *testing.T) {
	creds := &AuthorizerCredentials{Channels: []ChannelMetadata{
		{ChannelID: "up", Direction: directionUplink, RateClass: rateClassLowRate},
		{ChannelID: "lr", Direction: directionDownlink, RateClass: rateClassLowRate},
	}}
	cfg := Config{ChannelIDs: []string{"up", "lr"}}
	configureChannelRates(&cfg, creds)
	assert.NotNil(t, cfg.HighRateChannelIDs, "no high-rate channel is a known, empty set")
	assert.Empty(t, cfg.HighRateChannelIDs)

	creds.Channels = append(creds.Channels, ChannelMetadata{ChannelID: "hr", Direction: directionDownlink, RateClass: rateClassHighRate})
	cfg = Config{ChannelIDs: []string{"up", "lr", "hr"}}
	configureChannelRates(&cfg, creds)
	assert.Equal(t, []string{"hr"}, cfg.HighRateChannelIDs)
}

// Only low-rate downlink channels count toward MQTT end-of-data completion.
func TestLowRateTelemetryChannelCount(t *testing.T) {
	cfg := Config{
		ChannelIDs: []string{"up", "lr", "hr"},
		AuthorizerCreds: &AuthorizerCredentials{Channels: []ChannelMetadata{
			{ChannelID: "up", Direction: directionUplink, RateClass: rateClassLowRate},
			{ChannelID: "lr", Direction: directionDownlink, RateClass: rateClassLowRate},
			{ChannelID: "hr", Direction: directionDownlink, RateClass: rateClassHighRate},
		}},
	}
	assert.Equal(t, 1, lowRateTelemetryChannelCount(cfg))
	cfg.AuthorizerCreds.Channels = nil
	assert.Equal(t, 3, lowRateTelemetryChannelCount(cfg), "without metadata every channel counts")
}

// The banner promises telemetry only when a selected channel delivers it.
func TestHasTelemetryChannel(t *testing.T) {
	cfg := Config{AuthorizerCreds: &AuthorizerCredentials{Channels: []ChannelMetadata{
		{ChannelID: "up", Direction: directionUplink},
	}}}
	assert.False(t, hasTelemetryChannel(cfg))
	cfg.AuthorizerCreds.Channels = append(cfg.AuthorizerCreds.Channels, ChannelMetadata{ChannelID: "hr", Direction: directionDownlink, RateClass: rateClassHighRate})
	assert.True(t, hasTelemetryChannel(cfg))
	assert.True(t, hasTelemetryChannel(Config{}), "without metadata it is assumed")
}

// A pass without a booking takes commands during its scheduled contact.
func TestPassWindowOf(t *testing.T) {
	start := time.Date(2026, 11, 12, 7, 23, 0, 0, time.UTC)
	scheduled := &apiclient.ScheduledDetails{Start: start, Stop: start.Add(6 * time.Minute)}
	w := passWindowOf(&apiclient.Pass{Scheduled: scheduled})
	assert.True(t, w.resolved())
	assert.ErrorContains(t, w.check(start.Add(-time.Minute)), "the pass has not started")
	assert.NoError(t, w.check(start.Add(time.Minute)))

	booking := &apiclient.Interval{Start: start.Add(-time.Minute), Stop: start.Add(7 * time.Minute)}
	w = passWindowOf(&apiclient.Pass{Booking: booking, Scheduled: scheduled})
	assert.Equal(t, booking.Start, w.start, "the booking wins when present")

	assert.False(t, passWindowOf(&apiclient.Pass{}).resolved())
}
