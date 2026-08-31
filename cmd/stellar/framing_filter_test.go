package main

import "testing"

func TestFramingFromDownlinkKey(t *testing.T) {
	cases := map[string]string{
		"env1/pass/p1/channel/c1/downlink/IQ/1784799748599":         "IQ",
		"env1/pass/p1/channel/c1/downlink/BITSTREAM/12":             "BITSTREAM",
		"env1/pass/p1/channel/c1/downlink/AX25/ack/1784_streamer-x": "AX25",
		"p1/c1/BITSTREAM/3":                     "", // high-rate direct key, no /downlink/
		"env1/pass/p1/channel/c1/monitoring/17": "",
	}
	for key, want := range cases {
		if got := framingFromDownlinkKey(key); got != want {
			t.Errorf("framingFromDownlinkKey(%q) = %q, want %q", key, got, want)
		}
	}
}

func TestFramingAccepted(t *testing.T) {
	tests := []struct {
		name     string
		accepted []string
		framing  string
		want     bool
	}{
		{"empty filter accepts all", nil, "IQ", true},
		{"unknown framing accepted", []string{"BITSTREAM"}, "", true},
		{"accepted framing", []string{"BITSTREAM", "AX25"}, "AX25", true},
		{"rejected framing", []string{"BITSTREAM"}, "IQ", false},
		{"case-insensitive", []string{"bitstream"}, "BITSTREAM", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Config{AcceptedFraming: tt.accepted}
			if got := framingAccepted(cfg, tt.framing); got != tt.want {
				t.Errorf("framingAccepted(%v, %q) = %v, want %v", tt.accepted, tt.framing, got, tt.want)
			}
		})
	}
}
