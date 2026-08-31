package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/infostellarinc/stellarstation-cli/internal/auth"
)

// sampleCreateAPIKeyResponse mirrors the JSON returned by POST /v1/api-keys.
const sampleCreateAPIKeyResponse = `{
  "apiKey": {
    "id": "ak-1",
    "identityId": "id-1",
    "organizationId": "org-1",
    "clientId": "client-xyz"
  },
  "clientSecret": "super-secret",
  "tokenEndpoint": "https://cognito.example/oauth2/token",
  "scope": "stellarstation-api/access"
}`

func TestActivateAPIKey_AcceptsCreateApiKeyResponse(t *testing.T) {
	srcDir := t.TempDir()
	srcFile := filepath.Join(srcDir, "key.json")
	if err := os.WriteFile(srcFile, []byte(sampleCreateAPIKeyResponse), 0o600); err != nil {
		t.Fatal(err)
	}

	destDir := t.TempDir()
	t.Setenv("HOME", destDir)

	cmd := newActivateAPIKeyCommand()
	var buf bytes.Buffer
	cmd.SetOut(&buf)
	cmd.SetArgs([]string{srcFile})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("activate-api-key failed: %v", err)
	}

	destPath, err := auth.DefaultCredentialsPath()
	if err != nil {
		t.Fatal(err)
	}
	creds, err := auth.Load(destPath)
	if err != nil {
		t.Fatalf("load installed credentials: %v", err)
	}
	if creds.ClientID != "client-xyz" {
		t.Errorf("ClientID = %q, want client-xyz", creds.ClientID)
	}
	if creds.ClientSecret != "super-secret" {
		t.Errorf("ClientSecret = %q", creds.ClientSecret)
	}
	if creds.TokenEndpoint != "https://cognito.example/oauth2/token" {
		t.Errorf("TokenEndpoint = %q", creds.TokenEndpoint)
	}

	info, err := os.Stat(destPath)
	if err != nil {
		t.Fatal(err)
	}
	// Windows has no POSIX permission bits: os.Chmod there only toggles the
	// read-only flag, so a file written 0600 reads back 0666. The key is still
	// protected, by the ACL on the user profile directory rather than by mode.
	if runtime.GOOS != "windows" {
		if perm := info.Mode().Perm(); perm != auth.FilePerm {
			t.Errorf("file permissions = %o, want %o", perm, auth.FilePerm)
		}
	}
}

func TestActivateAPIKey_AcceptsFlatShape(t *testing.T) {
	srcDir := t.TempDir()
	srcFile := filepath.Join(srcDir, "key.json")
	flat := `{"clientId":"cid","clientSecret":"sec","tokenEndpoint":"https://t.example","scope":"x"}`
	if err := os.WriteFile(srcFile, []byte(flat), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", t.TempDir())

	cmd := newActivateAPIKeyCommand()
	cmd.SetArgs([]string{srcFile})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("activate-api-key: %v", err)
	}
}

func TestActivateAPIKey_MissingFields(t *testing.T) {
	srcDir := t.TempDir()
	srcFile := filepath.Join(srcDir, "key.json")
	if err := os.WriteFile(srcFile, []byte(`{"other":"value"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := newActivateAPIKeyCommand()
	cmd.SetArgs([]string{srcFile})
	cmd.SilenceUsage = true
	if err := cmd.Execute(); err == nil {
		t.Fatal("expected error for missing required fields, got nil")
	}
}

// TestActivateAPIKey_WarnsWhenCredentialsEnvOverrides guards against the
// silent-shadowing trap: STELLAR_CREDENTIALS takes precedence over the file
// this command just wrote (see resolveCredentialsPath), so every later
// command would keep using the old file with no indication why activation
// appeared to have no effect.
func TestActivateAPIKey_WarnsWhenCredentialsEnvOverrides(t *testing.T) {
	srcDir := t.TempDir()
	srcFile := filepath.Join(srcDir, "key.json")
	if err := os.WriteFile(srcFile, []byte(sampleCreateAPIKeyResponse), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", t.TempDir())
	t.Setenv(envCredentialsPath, filepath.Join(srcDir, "other-key.json"))

	origOut := uiOut
	var buf bytes.Buffer
	uiOut = &buf
	t.Cleanup(func() { uiOut = origOut })

	cmd := newActivateAPIKeyCommand()
	cmd.SetArgs([]string{srcFile})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("activate-api-key failed: %v", err)
	}

	if !strings.Contains(buf.String(), envCredentialsPath) {
		t.Errorf("expected a warning naming %s in output, got: %s", envCredentialsPath, buf.String())
	}
}

func TestActivateAPIKey_NoWarningWhenCredentialsEnvUnset(t *testing.T) {
	srcDir := t.TempDir()
	srcFile := filepath.Join(srcDir, "key.json")
	if err := os.WriteFile(srcFile, []byte(sampleCreateAPIKeyResponse), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HOME", t.TempDir())

	origOut := uiOut
	var buf bytes.Buffer
	uiOut = &buf
	t.Cleanup(func() { uiOut = origOut })

	cmd := newActivateAPIKeyCommand()
	cmd.SetArgs([]string{srcFile})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("activate-api-key failed: %v", err)
	}

	if strings.Contains(buf.String(), envCredentialsPath) {
		t.Errorf("did not expect a warning with no override set, got: %s", buf.String())
	}
}

func TestActivateAPIKey_InvalidJSON(t *testing.T) {
	srcDir := t.TempDir()
	srcFile := filepath.Join(srcDir, "key.json")
	if err := os.WriteFile(srcFile, []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := newActivateAPIKeyCommand()
	cmd.SetArgs([]string{srcFile})
	cmd.SilenceUsage = true
	if err := cmd.Execute(); err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

func TestActivateAPIKey_FileNotFound(t *testing.T) {
	cmd := newActivateAPIKeyCommand()
	cmd.SetArgs([]string{"/nonexistent/path/key.json"})
	cmd.SilenceUsage = true
	if err := cmd.Execute(); err == nil {
		t.Fatal("expected error for nonexistent file, got nil")
	}
}

func TestActivateAPIKey_RequiresExactlyOneArg(t *testing.T) {
	cmd := newActivateAPIKeyCommand()
	cmd.SetArgs([]string{})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	if err := cmd.Execute(); err == nil {
		t.Fatal("expected error for no arguments, got nil")
	}
}

// TestAuthTokenCommand_MintsToken exercises `stellar auth token` end-to-end:
// it installs a credentials file, points the token endpoint at an httptest
// server, and verifies the command prints the minted access token.
func TestAuthTokenCommand_MintsToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "jwt-from-token-cmd",
			"token_type":   "Bearer",
			"expires_in":   3600,
			"scope":        auth.DefaultScope,
		})
	}))
	defer srv.Close()

	home := t.TempDir()
	t.Setenv("HOME", home)
	// Isolate from any STELLAR_CREDENTIALS set in the developer's shell, which
	// would otherwise take precedence over the temp HOME and point the command
	// at real credentials.
	t.Setenv(envCredentialsPath, "")
	credPath, err := auth.DefaultCredentialsPath()
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Save(credPath, &auth.Credentials{
		ClientID:      "c",
		ClientSecret:  "s",
		TokenEndpoint: srv.URL,
	}); err != nil {
		t.Fatalf("save creds: %v", err)
	}

	root := NewRootCommand()
	var out bytes.Buffer
	root.SetOut(&out)
	root.SetArgs([]string{"auth", "token"})
	if err := root.Execute(); err != nil {
		t.Fatalf("auth token: %v", err)
	}
	if got := out.String(); got != "jwt-from-token-cmd\n" {
		t.Errorf("auth token output = %q, want %q", got, "jwt-from-token-cmd\n")
	}
}
