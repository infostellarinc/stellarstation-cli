package auth

import (
	"strings"
	"testing"
)

func TestCheckEndpointScheme(t *testing.T) {
	tests := []struct {
		name    string
		rawURL  string
		wantErr bool
	}{
		{"https accepted", "https://api.example.com", false},
		{"https accepted regardless of case", "HTTPS://api.example.com", false},
		{"https with surrounding space accepted", "  https://api.example.com  ", false},
		{"http refused", "http://api.example.com", true},
		{"http on localhost accepted", "http://localhost:8080", false},
		{"http on 127.0.0.1 accepted", "http://127.0.0.1:8080", false},
		{"http on other 127/8 address accepted", "http://127.1.2.3:8080", false},
		{"http on IPv6 loopback accepted", "http://[::1]:8080", false},
		{"http on lookalike host refused", "http://127.0.0.1.evil.example", true},
		{"http on localhost subdomain refused", "http://localhost.evil.example", true},
		{"http on private LAN address refused", "http://192.168.1.10:8080", true},
		{"missing scheme refused", "api.example.com", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckEndpointScheme("API address", tt.rawURL)
			if (err != nil) != tt.wantErr {
				t.Fatalf("CheckEndpointScheme(%q) error = %v, wantErr %v", tt.rawURL, err, tt.wantErr)
			}
		})
	}
}

// TestCheckEndpointScheme_ErrorExplainsRequirement verifies the refusal
// message explains the https requirement rather than just rejecting.
func TestCheckEndpointScheme_ErrorExplainsRequirement(t *testing.T) {
	err := CheckEndpointScheme("token endpoint", "http://cognito.example/oauth2/token")
	if err == nil {
		t.Fatal("expected error for http token endpoint")
	}
	if msg := err.Error(); !strings.Contains(msg, "https") {
		t.Errorf("error should explain the https requirement, got: %s", msg)
	}
}
