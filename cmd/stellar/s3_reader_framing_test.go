package main

import (
	"reflect"
	"sort"
	"testing"
)

func TestFramingsToTryForChannel(t *testing.T) {
	cfg := Config{
		ChannelFramings: map[string][]string{
			"multi": {"BITSTREAM", "IQ"},
			"iq":    {"IQ"},
		},
	}

	// Known multi-framing channel: only its framings, not all 7.
	if got := framingsToTryForChannel(cfg, "multi"); !reflect.DeepEqual(got, []string{"BITSTREAM", "IQ"}) {
		t.Errorf("multi: got %v, want [BITSTREAM IQ]", got)
	}
	// Single-framing channel: only that framing (no AX25/WATERFALL/etc probing).
	if got := framingsToTryForChannel(cfg, "iq"); !reflect.DeepEqual(got, []string{"IQ"}) {
		t.Errorf("iq: got %v, want [IQ]", got)
	}
	// Unknown channel: falls back to all known framings.
	if got := framingsToTryForChannel(cfg, "unknown"); len(got) != len(getAllFramingTypes()) {
		t.Errorf("unknown: got %v, want all framings", got)
	}
	// --accepted-framing filters the channel's framings.
	cfg.AcceptedFraming = []string{"iq"}
	got := framingsToTryForChannel(cfg, "multi")
	sort.Strings(got)
	if !reflect.DeepEqual(got, []string{"IQ"}) {
		t.Errorf("multi + accepted=iq: got %v, want [IQ]", got)
	}
}
