package main

import (
	"strings"
	"testing"
	"time"

	"github.com/infostellarinc/stellarstation-cli/internal/auth"

	"github.com/spf13/cobra"
)

// newWindowFlagCmd builds a bare command carrying the --start / --stop string
// flags that resolveListWindow reads, seeding them with the given values.
func newWindowFlagCmd(start, stop string) *cobra.Command {
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().String("start", start, "")
	cmd.Flags().String("stop", stop, "")
	return cmd
}

func TestResolveListWindow(t *testing.T) {
	now := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	sevenDays := now.Add(7 * 24 * time.Hour)
	custom := time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC)

	t.Run("defaults to now and now+7d when both omitted", func(t *testing.T) {
		start, stop, err := resolveListWindow(newWindowFlagCmd("", ""), now)
		if err != nil {
			t.Fatalf("resolveListWindow: %v", err)
		}
		if !start.Equal(now) {
			t.Errorf("start = %s, want now %s", start, now)
		}
		if !stop.Equal(sevenDays) {
			t.Errorf("stop = %s, want now+7d %s", stop, sevenDays)
		}
	})

	t.Run("stop defaults to start+7d when only start given", func(t *testing.T) {
		start, stop, err := resolveListWindow(newWindowFlagCmd(custom.Format(time.RFC3339), ""), now)
		if err != nil {
			t.Fatalf("resolveListWindow: %v", err)
		}
		if !start.Equal(custom) {
			t.Errorf("start = %s, want %s", start, custom)
		}
		if want := custom.Add(7 * 24 * time.Hour); !stop.Equal(want) {
			t.Errorf("stop = %s, want start+7d %s", stop, want)
		}
	})

	t.Run("start defaults to now when only stop given", func(t *testing.T) {
		start, stop, err := resolveListWindow(newWindowFlagCmd("", custom.Format(time.RFC3339)), now)
		if err != nil {
			t.Fatalf("resolveListWindow: %v", err)
		}
		if !start.Equal(now) {
			t.Errorf("start = %s, want now %s", start, now)
		}
		if !stop.Equal(custom) {
			t.Errorf("stop = %s, want %s", stop, custom)
		}
	})

	t.Run("honours both when provided", func(t *testing.T) {
		s := now.Add(-time.Hour)
		start, stop, err := resolveListWindow(
			newWindowFlagCmd(s.Format(time.RFC3339), custom.Format(time.RFC3339)), now)
		if err != nil {
			t.Fatalf("resolveListWindow: %v", err)
		}
		if !start.Equal(s) || !stop.Equal(custom) {
			t.Errorf("window = [%s, %s], want [%s, %s]", start, stop, s, custom)
		}
	})

	t.Run("returns error on invalid time", func(t *testing.T) {
		if _, _, err := resolveListWindow(newWindowFlagCmd("not-a-time", ""), now); err == nil {
			t.Error("expected error for invalid --start, got nil")
		}
	})
}

// newAPIURLCmd builds a bare command carrying the persistent flags that
// resolveAPIBaseURL reads.
func newAPIURLCmd(t *testing.T, apiURL string) *cobra.Command {
	t.Helper()
	cmd := &cobra.Command{Use: "test"}
	addAPIFlags(cmd)
	args := []string{}
	if apiURL != "" {
		args = append(args, "--api-url", apiURL)
	}
	// ParseFlags merges the persistent flags into cmd.Flags(), matching what
	// cobra does when the command executes for real.
	if err := cmd.ParseFlags(args); err != nil {
		t.Fatalf("ParseFlags: %v", err)
	}
	return cmd
}

func TestResolveAPIBaseURL_SchemeEnforcement(t *testing.T) {
	t.Run("https accepted", func(t *testing.T) {
		got, err := resolveAPIBaseURL(newAPIURLCmd(t, "https://api.example.com/"))
		if err != nil {
			t.Fatalf("resolveAPIBaseURL: %v", err)
		}
		if got != "https://api.example.com" {
			t.Errorf("resolveAPIBaseURL = %q", got)
		}
	})

	t.Run("http refused", func(t *testing.T) {
		_, err := resolveAPIBaseURL(newAPIURLCmd(t, "http://api.example.com"))
		if err == nil {
			t.Fatal("expected error for http:// API address")
		}
		if !strings.Contains(err.Error(), "https") {
			t.Errorf("error should explain the https requirement, got: %v", err)
		}
	})

	t.Run("http on loopback accepted", func(t *testing.T) {
		got, err := resolveAPIBaseURL(newAPIURLCmd(t, "http://127.0.0.1:8080"))
		if err != nil {
			t.Fatalf("resolveAPIBaseURL on loopback: %v", err)
		}
		if got != "http://127.0.0.1:8080" {
			t.Errorf("resolveAPIBaseURL = %q", got)
		}
	})

	t.Run("http from STELLAR_API_URL refused", func(t *testing.T) {
		t.Setenv("STELLAR_API_URL", "http://api.example.com")
		if _, err := resolveAPIBaseURL(newAPIURLCmd(t, "")); err == nil {
			t.Fatal("expected error for http:// STELLAR_API_URL")
		}
	})
}

// TestNewTokenSource_RefusesPlainHTTPTokenEndpoint verifies the token-endpoint
// scheme check in newTokenSource, using a credentials file whose tokenEndpoint
// is plain http.
func TestNewTokenSource_RefusesPlainHTTPTokenEndpoint(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv(envCredentialsPath, "")
	path, err := auth.DefaultCredentialsPath()
	if err != nil {
		t.Fatalf("DefaultCredentialsPath: %v", err)
	}
	if err := auth.Save(path, &auth.Credentials{
		ClientID:      "c",
		ClientSecret:  "s",
		TokenEndpoint: "http://cognito.example/oauth2/token",
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	cmd := newAPIURLCmd(t, "")
	if _, _, err := newTokenSource(cmd); err == nil {
		t.Fatal("expected error for http:// token endpoint")
	}
}
