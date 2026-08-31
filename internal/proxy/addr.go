package proxy

import (
	"fmt"
	"net"
	"strings"
)

// ValidateListenAddr checks a proxy listen address before a socket is bound to
// it. Loopback addresses are always permitted. A wildcard or otherwise
// routable address is refused unless allowRemote is set, because the proxy
// accepts connections without any authentication: every host that can reach
// the socket receives the pass downlink and, when uplink is enabled, can
// inject data that is transmitted to the satellite.
//
// It returns remote=true when the address is reachable from other machines
// (so the caller can print a warning) and an error when the address is remote
// but allowRemote is false.
func ValidateListenAddr(addr string, allowRemote bool) (remote bool, err error) {
	host, _, splitErr := net.SplitHostPort(addr)
	if splitErr != nil {
		return false, fmt.Errorf("invalid proxy listen address %q: %w", addr, splitErr)
	}
	if isLoopbackHost(host) {
		return false, nil
	}
	if !allowRemote {
		return true, fmt.Errorf(
			"proxy listen address %q is not a loopback address, so other machines on the "+
				"network could connect to it without authentication and receive the pass "+
				"downlink or transmit data to the satellite. Use a 127.0.0.1 address, or pass "+
				"--proxy-allow-remote if you intend to expose the proxy",
			addr,
		)
	}
	return true, nil
}

// isLoopbackHost reports whether the host part of a listen address restricts
// the socket to the local machine. An empty host means the wildcard address
// (all interfaces), which is not loopback.
func isLoopbackHost(host string) bool {
	if host == "" {
		return false
	}
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil {
		// An unresolved hostname cannot be proven local; treat it as remote so
		// exposure is always an explicit choice.
		return false
	}
	return ip.IsLoopback()
}
