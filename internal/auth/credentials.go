// Package auth handles CLI authentication against StellarStation services.
//
// Callers authenticate with a Cognito machine client using the OAuth2
// client_credentials grant. Credentials are obtained from the API
// ("POST /v1/api-keys" returns a ClientId, ClientSecret, TokenEndpoint and
// Scope), persisted to disk by "stellar auth activate-api-key", and exchanged
// at runtime for short-lived access tokens.
//
// The OAuth2 client credentials above are the only credential format the API
// accepts.
package auth

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// DefaultScope is the OAuth2 scope used by StellarStation machine clients. It
// must match the resource server scope configured in Cognito and the default
// returned from the API ("stellarstation-api/access").
const DefaultScope = "stellarstation-api/access"

// DefaultCredentialsDir is the ~/.stellarstation directory where
// "stellar auth activate-api-key" installs machine-client credentials.
const DefaultCredentialsDir = ".stellarstation"

// DefaultCredentialsFile is the filename used inside DefaultCredentialsDir.
const DefaultCredentialsFile = "credentials.json"

// DirPerm is the directory permission used when creating the credentials
// directory. Credentials are secrets so we keep the directory private.
const DirPerm = 0o700

// FilePerm is the file permission used when writing credentials. Credentials
// are secrets so only the owner may read or write them.
const FilePerm = 0o600

// Credentials represent the machine-client credentials needed to obtain a
// Cognito access token. The JSON field names match the
// "CreateApiKeyResponse" schema used by "POST /v1/api-keys" so a downloaded
// response can be activated as-is.
type Credentials struct {
	ClientID      string `json:"clientId"`
	ClientSecret  string `json:"clientSecret"`
	TokenEndpoint string `json:"tokenEndpoint"`
	Scope         string `json:"scope,omitempty"`
}

// Validate checks that the credentials have the fields required to mint a
// token. Scope is optional because DefaultScope is substituted when empty.
func (c *Credentials) Validate() error {
	if c == nil {
		return errors.New("credentials are nil")
	}
	missing := []string{}
	if strings.TrimSpace(c.ClientID) == "" {
		missing = append(missing, "clientId")
	}
	if strings.TrimSpace(c.ClientSecret) == "" {
		missing = append(missing, "clientSecret")
	}
	if strings.TrimSpace(c.TokenEndpoint) == "" {
		missing = append(missing, "tokenEndpoint")
	}
	if len(missing) > 0 {
		return fmt.Errorf("credentials missing required fields: %s", strings.Join(missing, ", "))
	}
	return nil
}

// EffectiveScope returns Scope if set, otherwise DefaultScope.
func (c *Credentials) EffectiveScope() string {
	if s := strings.TrimSpace(c.Scope); s != "" {
		return s
	}
	return DefaultScope
}

// createApiKeyResponseShape is the payload returned by
// "POST /v1/api-keys". We parse it on a best-effort basis so users can
// activate the downloaded JSON verbatim.
type createAPIKeyResponseShape struct {
	APIKey *struct {
		ClientID string `json:"clientId"`
	} `json:"apiKey"`
	ClientID      string `json:"clientId"`
	ClientSecret  string `json:"clientSecret"`
	TokenEndpoint string `json:"tokenEndpoint"`
	Scope         string `json:"scope"`
}

// Parse parses raw JSON bytes into Credentials, accepting:
//   - Flat {"clientId","clientSecret","tokenEndpoint","scope?"} (console download
//     or hand-written file; unknown keys are ignored by encoding/json).
//   - CreateApiKeyResponse from POST /v1/api-keys, where clientId may appear only
//     under "apiKey".
//
// Top-level clientId takes precedence over apiKey.clientId when both are set.
func Parse(data []byte) (*Credentials, error) {
	if len(data) == 0 {
		return nil, errors.New("credentials JSON is empty")
	}
	var raw createAPIKeyResponseShape
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("decode credentials JSON: %w", err)
	}
	creds := &Credentials{
		ClientID:      raw.ClientID,
		ClientSecret:  raw.ClientSecret,
		TokenEndpoint: raw.TokenEndpoint,
		Scope:         raw.Scope,
	}
	if creds.ClientID == "" && raw.APIKey != nil {
		creds.ClientID = raw.APIKey.ClientID
	}
	if err := creds.Validate(); err != nil {
		return nil, err
	}
	return creds, nil
}

// Load reads and parses a credentials file from disk. The file contains a
// long-lived client secret, so, like ssh does for private keys, Load refuses
// files that other users on the machine could read or modify.
func Load(path string) (*Credentials, error) {
	if err := checkCredentialFileMode(path); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path) //nolint:gosec // path is user-provided.
	if err != nil {
		return nil, fmt.Errorf("read credentials file %s: %w", path, err)
	}
	return Parse(data)
}

// checkCredentialFileMode refuses credential files whose permission bits grant
// any access to group or other users. Windows does not use Unix permission
// bits, so the check is skipped there. Stat errors (including a missing file)
// are deliberately ignored here so os.ReadFile reports them and existing
// not-exist handling in callers keeps working.
func checkCredentialFileMode(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil //nolint:nilerr // let os.ReadFile report the error.
	}
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		return fmt.Errorf(
			"credentials file %s has permissions %04o, so other users on this machine can access your API key secret.\n"+
				"Restrict access with: chmod 600 %s\n"+
				"Or re-activate the key with `stellar auth activate-api-key <key-file>`, which stores it with safe permissions",
			path, mode, path,
		)
	}
	return nil
}

// Save writes credentials to disk using DirPerm / FilePerm. The parent
// directory is created if it does not already exist.
func Save(path string, creds *Credentials) error {
	if err := creds.Validate(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), DirPerm); err != nil {
		return fmt.Errorf("create credentials directory: %w", err)
	}
	out := Credentials{
		ClientID:      creds.ClientID,
		ClientSecret:  creds.ClientSecret,
		TokenEndpoint: creds.TokenEndpoint,
		Scope:         creds.EffectiveScope(),
	}
	data, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal credentials: %w", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(path, data, FilePerm); err != nil {
		return fmt.Errorf("write credentials file %s: %w", path, err)
	}
	// os.WriteFile applies FilePerm only when it creates the file; tighten a
	// pre-existing file that was left with wider permissions so re-activating
	// a key always results in a private credentials file.
	if err := os.Chmod(path, FilePerm); err != nil {
		return fmt.Errorf("restrict permissions on credentials file %s: %w", path, err)
	}
	return nil
}

// DefaultCredentialsPath returns the canonical
// ~/.stellarstation/credentials.json path for the current user.
func DefaultCredentialsPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("determine home directory: %w", err)
	}
	return filepath.Join(home, DefaultCredentialsDir, DefaultCredentialsFile), nil
}
