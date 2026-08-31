package auth

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

// CheckEndpointScheme verifies that an endpoint about to receive credentials
// (the OAuth2 token endpoint, or the API address that carries bearer tokens)
// uses HTTPS. There is no override: plaintext transport would expose the API
// key secret and access tokens to anyone on the network path.
//
// Loopback addresses (localhost, 127.0.0.0/8, ::1) are exempt, matching the
// secure-context rule browsers apply: traffic to the local machine never
// crosses a network. kind names the endpoint in error messages, for example
// "token endpoint" or "API address".
func CheckEndpointScheme(kind, rawURL string) error {
	trimmed := strings.TrimSpace(rawURL)
	u, err := url.Parse(trimmed)
	if err != nil {
		return fmt.Errorf("the %s %q is not a valid URL: %w", kind, trimmed, err)
	}
	if strings.EqualFold(u.Scheme, "https") {
		return nil
	}
	if strings.EqualFold(u.Scheme, "http") && isLoopbackHost(u.Hostname()) {
		return nil
	}
	if u.Scheme == "" {
		return fmt.Errorf(
			"the %s %q has no URL scheme.\nUse an https:// address, for example https://%s",
			kind, trimmed, trimmed,
		)
	}
	return fmt.Errorf(
		"refusing to send credentials to the %s %q because it does not use https.\n"+
			"Your API key secret and access tokens would travel unencrypted and could be read by anyone on the network.\n"+
			"Use an https:// address",
		kind, trimmed,
	)
}

// isLoopbackHost reports whether host names the local machine.
func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
