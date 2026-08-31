package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/infostellarinc/stellarstation-cli/internal/auth"

	"github.com/aws/aws-sdk-go-v2/aws"
)

func refresherTestTokenSource() auth.TokenSource {
	return auth.StaticTokenSource{Value: "refresher-token"}
}

// makeTestCreds returns an AuthorizerCredentials with the given expiry.
func makeTestCreds(expiry time.Time) *AuthorizerCredentials {
	return &AuthorizerCredentials{
		AccessKeyID:     "AKIATEST",
		SecretAccessKey: "secret",
		SessionToken:    "token",
		Expiration:      expiry,
		S3Bucket:        "test-bucket",
		S3Region:        "ap-northeast-1",
	}
}

// ---- credentialStore ----

func TestCredentialStore_LoadStore(t *testing.T) {
	initial := makeTestCreds(time.Now().Add(1 * time.Hour))
	cs := newCredentialStore(initial)

	if cs.load() != initial {
		t.Fatal("expected load() to return the initial credentials")
	}

	updated := makeTestCreds(time.Now().Add(2 * time.Hour))
	cs.store(updated)

	if cs.load() != updated {
		t.Fatal("expected load() to return the updated credentials after store()")
	}
}

// ---- refreshingCredentialsProvider ----

func TestRefreshingCredentialsProvider_Retrieve(t *testing.T) {
	expiry := time.Now().Add(1 * time.Hour)
	creds := makeTestCreds(expiry)
	cs := newCredentialStore(creds)
	provider := &refreshingCredentialsProvider{store: cs}

	got, err := provider.Retrieve(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got.AccessKeyID != "AKIATEST" {
		t.Errorf("AccessKeyID: got %q, want %q", got.AccessKeyID, "AKIATEST")
	}
	if got.SecretAccessKey != "secret" {
		t.Errorf("SecretAccessKey: got %q, want %q", got.SecretAccessKey, "secret")
	}
	if got.SessionToken != "token" {
		t.Errorf("SessionToken: got %q, want %q", got.SessionToken, "token")
	}
	if !got.CanExpire {
		t.Error("expected CanExpire to be true")
	}
	if !got.Expires.Equal(expiry) {
		t.Errorf("Expires: got %v, want %v", got.Expires, expiry)
	}
}

func TestRefreshingCredentialsProvider_RetrieveAfterRefresh(t *testing.T) {
	initial := makeTestCreds(time.Now().Add(1 * time.Hour))
	cs := newCredentialStore(initial)
	provider := &refreshingCredentialsProvider{store: cs}

	// Swap in new credentials.
	refreshed := &AuthorizerCredentials{
		AccessKeyID:     "AKIANEW",
		SecretAccessKey: "newsecret",
		SessionToken:    "newtoken",
		Expiration:      time.Now().Add(2 * time.Hour),
	}
	cs.store(refreshed)

	got, err := provider.Retrieve(t.Context())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.AccessKeyID != "AKIANEW" {
		t.Errorf("expected refreshed AccessKeyID, got %q", got.AccessKeyID)
	}
}

// Verify that refreshingCredentialsProvider satisfies the aws.CredentialsProvider interface.
func TestRefreshingCredentialsProvider_ImplementsInterface(_ *testing.T) {
	var _ aws.CredentialsProvider = (*refreshingCredentialsProvider)(nil)
}

// ---- runCredentialRefresher ----

func TestRunCredentialRefresher_RefreshesBeforeExpiry(t *testing.T) {
	// Credentials that expire in 2 minutes, already inside the 5-minute refresh
	// margin, so refreshIn becomes 0 and the refresh fires immediately.
	initial := makeTestCreds(time.Now().Add(2 * time.Minute))
	cs := newCredentialStore(initial)

	refreshed := makeTestCreds(time.Now().Add(1 * time.Hour))
	refreshed.AccessKeyID = "AKIAREFRESHED"

	// Serve the refreshed credentials from a test HTTP server.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(refreshed); err != nil {
			http.Error(w, "encode error", http.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	authReq := AuthorizerRequest{PassID: "pass-1", EnableDownlink: true}
	go runCredentialRefresher(ctx, cs, srv.URL, refresherTestTokenSource(), authReq)

	// Wait until the store is updated with the refreshed credentials.
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if cs.load().AccessKeyID == "AKIAREFRESHED" {
			return // success
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf(
		"credentials were not refreshed within timeout; still have AccessKeyID=%q",
		cs.load().AccessKeyID,
	)
}

func TestRunCredentialRefresher_StopsOnContextCancel(t *testing.T) {
	// Credentials that expire far in the future; refresher should sleep and then
	// stop cleanly when the context is cancelled.
	initial := makeTestCreds(time.Now().Add(2 * time.Hour))
	cs := newCredentialStore(initial)

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan struct{})
	go func() {
		runCredentialRefresher(ctx, cs, "http://unused", refresherTestTokenSource(), AuthorizerRequest{})
		close(done)
	}()

	cancel()

	select {
	case <-done:
		// Good: refresher exited cleanly.
	case <-time.After(2 * time.Second):
		t.Fatal("runCredentialRefresher did not stop after context cancellation")
	}
}

func TestRunCredentialRefresher_RetriesOnError(t *testing.T) {
	// Credentials expiring in 2 minutes, already inside the 5-minute refresh
	// margin so the first refresh attempt fires immediately.
	initial := makeTestCreds(time.Now().Add(2 * time.Minute))
	cs := newCredentialStore(initial)

	callCount := 0
	refreshed := makeTestCreds(time.Now().Add(1 * time.Hour))
	refreshed.AccessKeyID = "AKIARETRIED"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callCount++
		if callCount == 1 {
			// First call fails.
			http.Error(
				w,
				`{"error":"internal","message":"temporary failure"}`,
				http.StatusInternalServerError,
			)
			return
		}
		// Second call succeeds.
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(refreshed); err != nil {
			http.Error(w, "encode error", http.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 40*time.Second)
	defer cancel()

	authReq := AuthorizerRequest{PassID: "pass-1"}
	go runCredentialRefresher(ctx, cs, srv.URL, refresherTestTokenSource(), authReq)

	deadline := time.Now().Add(38 * time.Second)
	for time.Now().Before(deadline) {
		if cs.load().AccessKeyID == "AKIARETRIED" {
			if callCount < 2 {
				t.Errorf("expected at least 2 calls (1 failure + 1 success), got %d", callCount)
			}
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("credentials were not refreshed after retry; callCount=%d", callCount)
}
