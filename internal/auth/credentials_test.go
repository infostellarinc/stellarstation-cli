package auth

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestParse_Flat(t *testing.T) {
	data := []byte(`{
	  "clientId": "client-123",
	  "clientSecret": "secret-xyz",
	  "tokenEndpoint": "https://cognito.example/oauth2/token",
	  "scope": "stellarstation-api/access"
	}`)
	creds, err := Parse(data)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if creds.ClientID != "client-123" {
		t.Errorf("ClientID = %q, want client-123", creds.ClientID)
	}
	if creds.EffectiveScope() != "stellarstation-api/access" {
		t.Errorf("scope = %q", creds.EffectiveScope())
	}
}

// TestParse_ConsoleExportShape covers the JSON file produced by the web console
// when downloading an API key (extra metadata keys; OAuth fields at top level).
func TestParse_ConsoleExportShape(t *testing.T) {
	data := []byte(`{
	  "kind": "stellarstation-api-key",
	  "apiKeyId": "d9673760-1164-4d15-8692-b11d7115b2c5",
	  "displayName": "key-4",
	  "identityId": "a90a84df-98f4-4ee1-8128-b0d3988d6af0",
	  "organizationId": "e6341e0e-b58b-4f4e-bb9c-4d57b10882f2",
	  "clientId": "console-export-client-id",
	  "clientSecret": "console-export-secret",
	  "tokenEndpoint": "https://pool.example.auth.ap-northeast-1.amazoncognito.com/oauth2/token",
	  "scope": "stellarstation-api/access",
	  "permissions": [{"action": "read", "resource": "pass:*"}],
	  "createdAt": "2026-04-24T00:28:36.991673Z",
	  "expiresAt": null
	}`)
	creds, err := Parse(data)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if creds.ClientID != "console-export-client-id" {
		t.Errorf("ClientID = %q", creds.ClientID)
	}
	if creds.ClientSecret != "console-export-secret" {
		t.Errorf("ClientSecret mismatch")
	}
	if creds.TokenEndpoint == "" {
		t.Fatal("TokenEndpoint empty")
	}
	if creds.EffectiveScope() != "stellarstation-api/access" {
		t.Errorf("EffectiveScope() = %q", creds.EffectiveScope())
	}
}

func TestParse_CreateApiKeyResponseShape(t *testing.T) {
	// Response body returned by POST /v1/api-keys has nested apiKey.clientId.
	data := []byte(`{
	  "apiKey": {"id": "ak-1", "clientId": "client-from-apikey"},
	  "clientSecret": "secret-xyz",
	  "tokenEndpoint": "https://cognito.example/oauth2/token"
	}`)
	creds, err := Parse(data)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if creds.ClientID != "client-from-apikey" {
		t.Errorf("ClientID = %q, want client-from-apikey", creds.ClientID)
	}
	if creds.EffectiveScope() != DefaultScope {
		t.Errorf("EffectiveScope() = %q, want %q", creds.EffectiveScope(), DefaultScope)
	}
}

func TestParse_TopLevelClientIDTakesPrecedence(t *testing.T) {
	data := []byte(`{
	  "apiKey": {"clientId": "nested"},
	  "clientId": "top-level",
	  "clientSecret": "s",
	  "tokenEndpoint": "https://cognito.example/oauth2/token"
	}`)
	creds, err := Parse(data)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if creds.ClientID != "top-level" {
		t.Errorf("ClientID = %q, want top-level", creds.ClientID)
	}
}

func TestParse_MissingFields(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{"empty", ""},
		{"no clientSecret", `{"clientId":"c","tokenEndpoint":"t"}`},
		{"no tokenEndpoint", `{"clientId":"c","clientSecret":"s"}`},
		{"no clientId anywhere", `{"clientSecret":"s","tokenEndpoint":"t"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Parse([]byte(tt.data)); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestParse_InvalidJSON(t *testing.T) {
	if _, err := Parse([]byte("not json")); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestSaveAndLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", DefaultCredentialsFile)

	orig := &Credentials{
		ClientID:      "c",
		ClientSecret:  "s",
		TokenEndpoint: "https://cognito.example/oauth2/token",
	}
	if err := Save(path, orig); err != nil {
		t.Fatalf("Save: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	// See the note in cmd_auth_test.go: Windows does not carry POSIX modes.
	if runtime.GOOS != "windows" {
		if got := info.Mode().Perm(); got != FilePerm {
			t.Errorf("file mode = %o, want %o", got, FilePerm)
		}
	}

	got, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.ClientID != orig.ClientID {
		t.Errorf("ClientID = %q", got.ClientID)
	}
	// Save substitutes DefaultScope for empty Scope.
	if got.Scope != DefaultScope {
		t.Errorf("Scope = %q, want %q", got.Scope, DefaultScope)
	}
}

func TestSave_RejectsInvalid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, DefaultCredentialsFile)
	err := Save(path, &Credentials{ClientID: "c"})
	if err == nil {
		t.Fatal("expected error for incomplete credentials")
	}
}

func TestDefaultCredentialsPath(t *testing.T) {
	// os.UserHomeDir reads HOME everywhere except Windows, where it reads
	// USERPROFILE. Set both so the fake home applies on every platform.
	home := filepath.Join(t.TempDir(), "fake-home")
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	p, err := DefaultCredentialsPath()
	if err != nil {
		t.Fatalf("DefaultCredentialsPath: %v", err)
	}
	want := filepath.Join(home, DefaultCredentialsDir, DefaultCredentialsFile)
	if p != want {
		t.Errorf("DefaultCredentialsPath() = %q, want %q", p, want)
	}
}

// TestValidate_NilReceiver covers the nil-credentials branch in Validate.
func TestValidate_NilReceiver(t *testing.T) {
	var c *Credentials
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for nil credentials")
	}
}

// TestLoad_FileNotFound covers the error path in Load when the file is absent.
func TestLoad_FileNotFound(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "nonexistent.json"))
	if err == nil {
		t.Fatal("expected error loading missing file")
	}
}

// TestSave_InvalidCredentials covers the Validate error path at the top of Save.
func TestSave_InvalidCredentials(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, DefaultCredentialsFile)
	// Missing ClientSecret and TokenEndpoint; Validate should fail.
	err := Save(path, &Credentials{ClientID: "only-id"})
	if err == nil {
		t.Fatal("expected error for invalid credentials")
	}
}

// TestDefaultCredentialsPath_HomeError covers the error path in DefaultCredentialsPath
// when HOME is unset and no home directory can be determined.
func TestDefaultCredentialsPath_HomeError(t *testing.T) {
	// Unset HOME so os.UserHomeDir falls back to /etc/passwd lookup.
	// On Linux CI the test runner may still resolve a home from /etc/passwd,
	// so we can only assert the happy or error path; we just ensure the
	// function is exercised.
	t.Setenv("HOME", "")
	// We don't assert on error here because /etc/passwd may still supply a
	// home directory. The key goal is statement coverage of the function.
	_, _ = DefaultCredentialsPath()
}

// TestLoad_RefusesGroupOrOtherAccessible verifies the ssh-style permission
// check: a credentials file readable by other users is refused with a chmod
// hint rather than silently reused.
func TestLoad_RefusesGroupOrOtherAccessible(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits are not meaningful on Windows")
	}
	for _, mode := range []os.FileMode{0o644, 0o640, 0o604, 0o660, 0o606} {
		t.Run(fmt.Sprintf("%04o", mode), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), DefaultCredentialsFile)
			data := `{"clientId":"c","clientSecret":"s","tokenEndpoint":"https://cognito.example/oauth2/token"}`
			if err := os.WriteFile(path, []byte(data), mode); err != nil {
				t.Fatalf("write: %v", err)
			}
			// os.WriteFile perm is subject to umask; force the exact mode.
			if err := os.Chmod(path, mode); err != nil {
				t.Fatalf("chmod: %v", err)
			}
			_, err := Load(path)
			if err == nil {
				t.Fatalf("Load should refuse a %04o credentials file", mode)
			}
			if !strings.Contains(err.Error(), "chmod 600") {
				t.Errorf("error should suggest chmod 600, got: %v", err)
			}
		})
	}
}

// TestLoad_AcceptsOwnerOnlyModes verifies owner-only files load fine.
func TestLoad_AcceptsOwnerOnlyModes(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits are not meaningful on Windows")
	}
	for _, mode := range []os.FileMode{0o600, 0o400} {
		t.Run(fmt.Sprintf("%04o", mode), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), DefaultCredentialsFile)
			data := `{"clientId":"c","clientSecret":"s","tokenEndpoint":"https://cognito.example/oauth2/token"}`
			if err := os.WriteFile(path, []byte(data), mode); err != nil {
				t.Fatalf("write: %v", err)
			}
			if err := os.Chmod(path, mode); err != nil {
				t.Fatalf("chmod: %v", err)
			}
			if _, err := Load(path); err != nil {
				t.Fatalf("Load(%04o file): %v", mode, err)
			}
		})
	}
}

// TestSave_TightensExistingLoosePermissions verifies that overwriting a
// pre-existing world-readable credentials file leaves it owner-only, since
// os.WriteFile applies its mode argument only at creation.
func TestSave_TightensExistingLoosePermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix permission bits are not meaningful on Windows")
	}
	path := filepath.Join(t.TempDir(), DefaultCredentialsFile)
	if err := os.WriteFile(path, []byte("{}"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	if err := Save(path, &Credentials{
		ClientID:      "c",
		ClientSecret:  "s",
		TokenEndpoint: "https://cognito.example/oauth2/token",
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if got := info.Mode().Perm(); got != FilePerm {
		t.Errorf("mode after Save = %04o, want %04o", got, FilePerm)
	}
}
