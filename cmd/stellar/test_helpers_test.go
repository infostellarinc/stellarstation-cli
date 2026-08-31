package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/infostellarinc/stellarstation-cli/internal/auth"
)

// newTokenEndpoint returns an httptest.Server that acts as an OAuth2 token
// endpoint, minting the supplied access token for any client_credentials
// request. It is used by CLI integration tests to let the CLI's real OAuth2
// token source run end-to-end without needing a live Cognito user pool.
func newTokenEndpoint(t *testing.T, accessToken string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": accessToken,
			"token_type":   "Bearer",
			"expires_in":   3600,
			"scope":        auth.DefaultScope,
		})
	}))
}

// installTestCredentials writes a credentials file pointing at tokenEndpoint
// into the default ~/.stellarstation/credentials.json location for the test.
// It sets $HOME to a temp directory so the install is isolated from the
// developer's real credentials.
func installTestCredentials(t *testing.T, tokenEndpoint string) {
	t.Helper()
	home := t.TempDir()
	t.Setenv("HOME", home)
	path, err := auth.DefaultCredentialsPath()
	if err != nil {
		t.Fatalf("DefaultCredentialsPath: %v", err)
	}
	if err := auth.Save(path, &auth.Credentials{
		ClientID:      "test-client",
		ClientSecret:  "test-secret",
		TokenEndpoint: tokenEndpoint,
	}); err != nil {
		t.Fatalf("Save credentials: %v", err)
	}
}

// setupAuthForTest combines newTokenEndpoint and installTestCredentials: it
// mints a Bearer token backed by a disposable token endpoint, installs the
// corresponding credentials, and returns the token endpoint server so the
// test can close it.
func setupAuthForTest(t *testing.T) *httptest.Server {
	t.Helper()
	bearer := "jwt-" + t.Name()
	tokSrv := newTokenEndpoint(t, bearer)
	installTestCredentials(t, tokSrv.URL)
	return tokSrv
}
