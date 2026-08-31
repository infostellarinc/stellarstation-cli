// Package proxy provides local UDP and TCP socket bridges for satellite
// telemetry downlink and command uplink. Downlink data (from S3/MQTT) is
// forwarded to local sockets; uplink data (from local sockets) is forwarded
// to the command sender callback.
package proxy

import (
	"fmt"
	"strings"
)

// Mode selects the proxy transport.
type Mode string

const (
	ModeDisabled Mode = "disabled"
	ModeUDP      Mode = "udp"
	ModeTCP      Mode = "tcp"
)

// ParseMode normalises a user-supplied mode string. It is case-insensitive and
// trims surrounding whitespace to stay consistent with printer.ParseFormat.
func ParseMode(s string) (Mode, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "disabled":
		return ModeDisabled, nil
	case "udp":
		return ModeUDP, nil
	case "tcp":
		return ModeTCP, nil
	default:
		return "", fmt.Errorf("unsupported proxy mode: %q (use disabled, udp, or tcp)", s)
	}
}

// Proxy is the interface implemented by UDP and TCP proxies.
type Proxy interface {
	// Start begins forwarding in both directions. Blocks until Close is called
	// or a fatal error occurs.
	Start() error
	// Close shuts down the proxy and releases resources.
	Close() error
}

// UplinkFunc is a callback invoked when the proxy receives bytes from a local
// client that should be sent as a satellite command.
type UplinkFunc func(data []byte)
