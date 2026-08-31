package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestOAuth2TokenSource_FetchesAndCaches(t *testing.T) {
	var calls int
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if got := r.Header.Get("Content-Type"); got != "application/x-www-form-urlencoded" {
			t.Errorf("content-type = %q", got)
		}
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Basic ") {
			t.Errorf("authorization = %q, want Basic ...", auth)
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("parse form: %v", err)
		}
		if got := r.Form.Get("grant_type"); got != "client_credentials" {
			t.Errorf("grant_type = %q", got)
		}
		if got := r.Form.Get("scope"); got != DefaultScope {
			t.Errorf("scope = %q, want %q", got, DefaultScope)
		}
		_ = json.NewEncoder(w).Encode(tokenResponse{
			AccessToken: "jwt-token-1",
			TokenType:   "Bearer",
			ExpiresIn:   3600,
			Scope:       DefaultScope,
		})
	}))
	defer srv.Close()

	ts, err := NewOAuth2TokenSource(&Credentials{
		ClientID:      "cid",
		ClientSecret:  "csec",
		TokenEndpoint: srv.URL,
	}, srv.Client())
	if err != nil {
		t.Fatalf("NewOAuth2TokenSource: %v", err)
	}

	tok1, err := ts.Token(context.Background())
	if err != nil {
		t.Fatalf("Token #1: %v", err)
	}
	if tok1 != "jwt-token-1" {
		t.Errorf("Token #1 = %q", tok1)
	}
	tok2, err := ts.Token(context.Background())
	if err != nil {
		t.Fatalf("Token #2: %v", err)
	}
	if tok2 != "jwt-token-1" {
		t.Errorf("Token #2 should be cached, got %q", tok2)
	}
	if calls != 1 {
		t.Errorf("token endpoint hit %d times, want 1 (second call should be cached)", calls)
	}
}

func TestOAuth2TokenSource_RefreshesNearExpiry(t *testing.T) {
	responses := []tokenResponse{
		{AccessToken: "jwt-1", ExpiresIn: 30, TokenType: "Bearer"},
		{AccessToken: "jwt-2", ExpiresIn: 3600, TokenType: "Bearer"},
	}
	var idx int
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		resp := responses[idx]
		idx++
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	ts, err := NewOAuth2TokenSource(&Credentials{
		ClientID:      "c",
		ClientSecret:  "s",
		TokenEndpoint: srv.URL,
	}, srv.Client())
	if err != nil {
		t.Fatalf("NewOAuth2TokenSource: %v", err)
	}

	// First token has a tiny TTL (<= refreshMargin) so it is treated as
	// already expired.
	first, err := ts.Token(context.Background())
	if err != nil {
		t.Fatalf("Token #1: %v", err)
	}
	if first != "jwt-1" {
		t.Errorf("Token #1 = %q, want jwt-1", first)
	}
	second, err := ts.Token(context.Background())
	if err != nil {
		t.Fatalf("Token #2: %v", err)
	}
	if second != "jwt-2" {
		t.Errorf("Token #2 = %q, want jwt-2 (refetch)", second)
	}
}

func TestOAuth2TokenSource_PropagatesHTTPError(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "bad client", http.StatusUnauthorized)
	}))
	defer srv.Close()

	ts, err := NewOAuth2TokenSource(&Credentials{
		ClientID: "c", ClientSecret: "s", TokenEndpoint: srv.URL,
	}, srv.Client())
	if err != nil {
		t.Fatalf("NewOAuth2TokenSource: %v", err)
	}
	if _, err := ts.Token(context.Background()); err == nil {
		t.Fatal("expected error from 401 response")
	}
}

func TestOAuth2TokenSource_RejectsInvalidCredentials(t *testing.T) {
	if _, err := NewOAuth2TokenSource(&Credentials{ClientID: "only-id"}, nil); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestStaticTokenSource(t *testing.T) {
	s := StaticTokenSource{Value: "static-tok"}
	got, err := s.Token(context.Background())
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if got != "static-tok" {
		t.Errorf("Token = %q", got)
	}
}

func TestOAuth2TokenSource_RespectsContextCancellation(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
	}))
	defer srv.Close()

	ts, err := NewOAuth2TokenSource(&Credentials{
		ClientID: "c", ClientSecret: "s", TokenEndpoint: srv.URL,
	}, srv.Client())
	if err != nil {
		t.Fatalf("NewOAuth2TokenSource: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := ts.Token(ctx); err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

// TestNewOAuth2TokenSource_NilHTTPClient verifies that a nil httpClient is
// replaced by a default private http.Client (no panic, valid source returned).
func TestNewOAuth2TokenSource_NilHTTPClient(t *testing.T) {
	creds := &Credentials{
		ClientID:      "cid",
		ClientSecret:  "csec",
		TokenEndpoint: "https://example.com/oauth2/token",
	}
	ts, err := NewOAuth2TokenSource(creds, nil)
	if err != nil {
		t.Fatalf("NewOAuth2TokenSource(nil httpClient): %v", err)
	}
	if ts == nil {
		t.Fatal("expected non-nil TokenSource")
	}
}

// TestFetchToken_EmptyAccessToken checks the error path when the token endpoint
// returns a 200 with an empty access_token field.
func TestFetchToken_EmptyAccessToken(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(tokenResponse{
			AccessToken: "",
			TokenType:   "Bearer",
			ExpiresIn:   3600,
		})
	}))
	defer srv.Close()

	ts, err := NewOAuth2TokenSource(&Credentials{
		ClientID: "c", ClientSecret: "s", TokenEndpoint: srv.URL,
	}, srv.Client())
	if err != nil {
		t.Fatalf("NewOAuth2TokenSource: %v", err)
	}
	if _, err := ts.Token(context.Background()); err == nil {
		t.Fatal("expected error for empty access_token")
	}
}

// TestFetchToken_ZeroExpiresIn checks the defensive ExpiresIn default (<=0
// gets set to 3600) and confirms a token is still returned.
func TestFetchToken_ZeroExpiresIn(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(tokenResponse{
			AccessToken: "tok",
			TokenType:   "Bearer",
			ExpiresIn:   0, // deliberately omitted / zero
		})
	}))
	defer srv.Close()

	ts, err := NewOAuth2TokenSource(&Credentials{
		ClientID: "c", ClientSecret: "s", TokenEndpoint: srv.URL,
	}, srv.Client())
	if err != nil {
		t.Fatalf("NewOAuth2TokenSource: %v", err)
	}
	tok, err := ts.Token(context.Background())
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if tok != "tok" {
		t.Errorf("Token = %q, want tok", tok)
	}
}

// TestFetchToken_BadJSON checks the error path when the response body cannot
// be decoded as JSON.
func TestFetchToken_BadJSON(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("not json"))
	}))
	defer srv.Close()

	ts, err := NewOAuth2TokenSource(&Credentials{
		ClientID: "c", ClientSecret: "s", TokenEndpoint: srv.URL,
	}, srv.Client())
	if err != nil {
		t.Fatalf("NewOAuth2TokenSource: %v", err)
	}
	if _, err := ts.Token(context.Background()); err == nil {
		t.Fatal("expected error for bad JSON response")
	}
}

// TestNewOAuth2TokenSource_RefusesPlainHTTPEndpoint verifies that the client
// secret is never sent to a plaintext token endpoint on a non-loopback host.
func TestNewOAuth2TokenSource_RefusesPlainHTTPEndpoint(t *testing.T) {
	_, err := NewOAuth2TokenSource(&Credentials{
		ClientID:      "c",
		ClientSecret:  "s",
		TokenEndpoint: "http://cognito.example/oauth2/token",
	}, nil)
	if err == nil {
		t.Fatal("expected error for http:// token endpoint")
	}
	if !strings.Contains(err.Error(), "https") {
		t.Errorf("error should explain the https requirement, got: %v", err)
	}
}

// TestNewOAuth2TokenSource_AllowsPlainHTTPOnLoopback verifies the loopback
// exemption: plain HTTP to the local machine never crosses a network, so a
// local test endpoint still works.
func TestNewOAuth2TokenSource_AllowsPlainHTTPOnLoopback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(tokenResponse{
			AccessToken: "tok",
			TokenType:   "Bearer",
			ExpiresIn:   3600,
		})
	}))
	defer srv.Close()

	ts, err := NewOAuth2TokenSource(&Credentials{
		ClientID: "c", ClientSecret: "s", TokenEndpoint: srv.URL,
	}, srv.Client())
	if err != nil {
		t.Fatalf("NewOAuth2TokenSource on loopback endpoint: %v", err)
	}
	tok, err := ts.Token(context.Background())
	if err != nil {
		t.Fatalf("Token: %v", err)
	}
	if tok != "tok" {
		t.Errorf("Token = %q, want tok", tok)
	}
}
