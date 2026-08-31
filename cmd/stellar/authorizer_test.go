package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/infostellarinc/stellarstation-cli/internal/auth"
)

const testBearerToken = "jwt-token-abc"

func testTokenSource() auth.TokenSource {
	return auth.StaticTokenSource{Value: testBearerToken}
}

func makeTestValidCreds() AuthorizerCredentials {
	return AuthorizerCredentials{
		AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
		SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		SessionToken:    "FQoGZXIvYXdzEBYaD",
		Expiration:      time.Now().Add(time.Hour),
		S3Bucket:        "dev-stellarstation-user-pass-telemetry",
		S3Region:        "ap-northeast-1",
		Environment:     "dev",
		ClientID:        "streamer-user-123",
		Streams: StreamsConfig{
			LowRate: []StreamConfig{
				{
					S3Prefix:  "dev/pass-123/low_rate/channel/1/",
					MqttTopic: "dev/pass-123/low_rate/channel/1",
				},
			},
		},
		DiagnosticsPrefix: "dev/pass-123/diagnostics/",
	}
}

func TestFetchAuthorizerCredentials_Success(t *testing.T) {
	validCreds := makeTestValidCreds()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if got, want := r.Header.Get("Authorization"), "Bearer "+testBearerToken; got != want {
			t.Errorf("Authorization = %q, want %q", got, want)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf(
				"expected Content-Type: application/json, got %s",
				r.Header.Get("Content-Type"),
			)
		}
		var req AuthorizerRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("failed to decode request body: %v", err)
		}
		if req.PassID != "pass-123" {
			t.Errorf("expected passId=pass-123, got %s", req.PassID)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(validCreds)
	}))
	defer server.Close()

	authReq := AuthorizerRequest{PassID: "pass-123", ChannelIDs: []string{"1"}, EnableDownlink: true}
	creds, err := fetchAuthorizerCredentials(t.Context(), server.URL, testTokenSource(), authReq)
	if err != nil {
		t.Fatalf("fetchAuthorizerCredentials() error = %v", err)
	}
	if creds.AccessKeyID != validCreds.AccessKeyID {
		t.Errorf("AccessKeyID = %q, want %q", creds.AccessKeyID, validCreds.AccessKeyID)
	}
	if creds.S3Bucket != validCreds.S3Bucket {
		t.Errorf("S3Bucket = %q, want %q", creds.S3Bucket, validCreds.S3Bucket)
	}
	if len(creds.Streams.LowRate) != 1 {
		t.Errorf("LowRate streams = %d, want 1", len(creds.Streams.LowRate))
	}
}

func TestFetchAuthorizerCredentials_NilTokenSource(t *testing.T) {
	validCreds := makeTestValidCreds()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("Authorization should be empty when tokenSource is nil, got %q", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(validCreds)
	}))
	defer server.Close()

	if _, err := fetchAuthorizerCredentials(t.Context(), server.URL, nil, AuthorizerRequest{PassID: "p"}); err != nil {
		t.Fatalf("fetchAuthorizerCredentials nil token: %v", err)
	}
}

func TestFetchAuthorizerCredentials_ErrorResponses(t *testing.T) {
	t.Run("401 unauthorized", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).
				Encode(map[string]string{"error": "unauthorized", "message": "Invalid token"})
		}))
		defer server.Close()
		_, err := fetchAuthorizerCredentials(
			t.Context(),
			server.URL,
			testTokenSource(),
			AuthorizerRequest{PassID: "pass-123"},
		)
		if err == nil {
			t.Fatal("expected error for 401 response, got nil")
		}
	})

	t.Run("403 forbidden", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).
				Encode(map[string]string{"error": "forbidden", "message": "Access denied"})
		}))
		defer server.Close()
		_, err := fetchAuthorizerCredentials(
			t.Context(),
			server.URL,
			testTokenSource(),
			AuthorizerRequest{PassID: "pass-123"},
		)
		if err == nil {
			t.Fatal("expected error for 403 response, got nil")
		}
	})

	t.Run("missing credentials", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(AuthorizerCredentials{S3Bucket: "some-bucket"})
		}))
		defer server.Close()
		_, err := fetchAuthorizerCredentials(
			t.Context(),
			server.URL,
			testTokenSource(),
			AuthorizerRequest{PassID: "pass-123"},
		)
		if err == nil {
			t.Fatal("expected error for missing credentials, got nil")
		}
	})

	t.Run("invalid JSON", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("not valid json"))
		}))
		defer server.Close()
		_, err := fetchAuthorizerCredentials(
			t.Context(),
			server.URL,
			testTokenSource(),
			AuthorizerRequest{PassID: "pass-123"},
		)
		if err == nil {
			t.Fatal("expected error for invalid JSON response, got nil")
		}
	})
}

func TestFetchAuthorizerCredentials_ContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(5 * time.Second):
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := fetchAuthorizerCredentials(
		ctx,
		server.URL,
		testTokenSource(),
		AuthorizerRequest{PassID: "pass-123"},
	)
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

func TestFetchAuthorizerCredentials_FeatureFlags(t *testing.T) {
	validCreds := makeTestValidCreds()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req AuthorizerRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("failed to decode request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		verifyFeatureFlags(t, req)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(validCreds)
	}))
	defer server.Close()

	authReq := AuthorizerRequest{
		PassID:           "pass-123",
		ChannelIDs:       []string{"1", "2"},
		Environment:      "dev",
		EnableDownlink:   true,
		EnableMonitoring: true,
		EnableUplink:     true,
	}
	_, err := fetchAuthorizerCredentials(t.Context(), server.URL, testTokenSource(), authReq)
	if err != nil {
		t.Fatalf("fetchAuthorizerCredentials() error = %v", err)
	}
}

func verifyFeatureFlags(t *testing.T, req AuthorizerRequest) {
	t.Helper()
	if !req.EnableDownlink {
		t.Error("expected EnableDownlink=true")
	}
	if !req.EnableMonitoring {
		t.Error("expected EnableMonitoring=true")
	}
	if !req.EnableUplink {
		t.Error("expected EnableUplink=true")
	}
	if req.Environment != "dev" {
		t.Errorf("expected Environment=dev, got %s", req.Environment)
	}
	if len(req.ChannelIDs) != 2 || req.ChannelIDs[0] != "1" || req.ChannelIDs[1] != "2" {
		t.Errorf("expected ChannelIDs=[1,2], got %v", req.ChannelIDs)
	}
}

// The authorizer records the client's version on the stream row, so the pass
// monitoring page shows which client version opened each stream. The version
// must reach the wire under the field name the authorizer parses.
func TestFetchAuthorizerCredentials_ClientVersion(t *testing.T) {
	validCreds := makeTestValidCreds()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var raw map[string]any
		if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
			t.Errorf("failed to decode request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if got := raw["clientVersion"]; got != "v1.2.3" {
			t.Errorf("clientVersion on the wire = %v, want v1.2.3", got)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(validCreds)
	}))
	defer server.Close()

	authReq := AuthorizerRequest{
		PassID:        "pass-123",
		ChannelIDs:    []string{"1"},
		ClientVersion: "v1.2.3",
	}
	if _, err := fetchAuthorizerCredentials(t.Context(), server.URL, testTokenSource(), authReq); err != nil {
		t.Fatalf("fetchAuthorizerCredentials() error = %v", err)
	}
}
