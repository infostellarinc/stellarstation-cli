package proxy

import (
	"strings"
	"testing"
)

func TestValidateListenAddr(t *testing.T) {
	tests := []struct {
		name        string
		addr        string
		allowRemote bool
		wantRemote  bool
		wantErr     bool
	}{
		{name: "IPv4 loopback", addr: "127.0.0.1:6001", wantRemote: false},
		{name: "IPv4 loopback non-first octet", addr: "127.1.2.3:6001", wantRemote: false},
		{name: "IPv6 loopback", addr: "[::1]:6001", wantRemote: false},
		{name: "localhost", addr: "localhost:6001", wantRemote: false},
		{name: "localhost mixed case", addr: "LocalHost:6001", wantRemote: false},
		{name: "wildcard refused by default", addr: ":6001", wantRemote: true, wantErr: true},
		{name: "explicit 0.0.0.0 refused by default", addr: "0.0.0.0:6001", wantRemote: true, wantErr: true},
		{name: "IPv6 wildcard refused by default", addr: "[::]:6001", wantRemote: true, wantErr: true},
		{name: "routable IP refused by default", addr: "192.168.1.10:6001", wantRemote: true, wantErr: true},
		{name: "unresolved hostname refused by default", addr: "myhost.internal:6001", wantRemote: true, wantErr: true},
		{name: "wildcard permitted with opt-in", addr: ":6001", allowRemote: true, wantRemote: true},
		{name: "routable IP permitted with opt-in", addr: "192.168.1.10:6001", allowRemote: true, wantRemote: true},
		{name: "loopback with opt-in stays local", addr: "127.0.0.1:6001", allowRemote: true, wantRemote: false},
		{name: "missing port", addr: "127.0.0.1", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			remote, err := ValidateListenAddr(tt.addr, tt.allowRemote)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateListenAddr(%q, %v) error = %v, wantErr %v", tt.addr, tt.allowRemote, err, tt.wantErr)
			}
			if remote != tt.wantRemote {
				t.Fatalf("ValidateListenAddr(%q, %v) remote = %v, want %v", tt.addr, tt.allowRemote, remote, tt.wantRemote)
			}
		})
	}
}

func TestValidateListenAddrErrorMentionsOptIn(t *testing.T) {
	_, err := ValidateListenAddr(":6000", false)
	if err == nil {
		t.Fatal("expected an error for a wildcard listen address without the opt-in")
	}
	if !strings.Contains(err.Error(), "--proxy-allow-remote") {
		t.Fatalf("error should tell the user about the opt-in flag, got: %v", err)
	}
}
