package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// revokeTestServer answers /stream/revoke with status and body, and records
// the request it got.
func revokeTestServer(t *testing.T, status int, body string) (*httptest.Server, *StreamRevokeRequest, *string) {
	t.Helper()
	var got StreamRevokeRequest
	var authorization string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/stream/revoke" {
			http.NotFound(w, r)
			return
		}
		authorization = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv, &got, &authorization
}

func TestRevokeStreamCommand(t *testing.T) {
	srv, got, authorization := revokeTestServer(t, http.StatusOK, `{"status":"revoked"}`)
	tokSrv := setupAuthForTest(t)
	defer tokSrv.Close()

	root := NewRootCommand()
	var buf bytes.Buffer
	root.SetOut(&buf)
	root.SetArgs([]string{
		"satellite", "revoke-stream", "streamer-1", "--api-url", srv.URL,
		"--pass-id", "pass-1", "--reason", "left running",
	})
	if err := root.Execute(); err != nil {
		t.Fatalf("revoke-stream: %v", err)
	}
	if want := (StreamRevokeRequest{PassID: "pass-1", StreamID: "streamer-1", Reason: "left running"}); *got != want {
		t.Errorf("request = %+v, want %+v", *got, want)
	}
	if !strings.HasPrefix(*authorization, "Bearer ") {
		t.Errorf("request was not authenticated: %q", *authorization)
	}
	if !strings.Contains(buf.String(), "Stream streamer-1 revoked.") {
		t.Errorf("output = %q", buf.String())
	}
}

func TestRevokeStreamCommandRequiresAPass(t *testing.T) {
	root := NewRootCommand()
	root.SetArgs([]string{"satellite", "revoke-stream", "streamer-1", "--api-url", "https://api.example"})
	err := root.Execute()
	if err == nil || !strings.Contains(err.Error(), "pass-id") {
		t.Fatalf("err = %v, want the missing --pass-id named", err)
	}
}

// Each refusal is worded for the operator, keeping the server's message.
func TestRevokeStreamCommandExplainsRefusals(t *testing.T) {
	for _, tt := range []struct {
		status int
		body   string
		want   string
	}{
		{http.StatusNotFound, `{"error":"not_found","message":"Stream not found"}`, "no such stream on that pass (Stream not found)"},
		{http.StatusForbidden, `{"error":"forbidden","message":"Access denied to this pass"}`, "you do not have access to that pass"},
		{
			http.StatusConflict, `{"error":"not_revocable","message":"the pass monitor's stream cannot be revoked"}`,
			"that stream cannot be revoked: the pass monitor's stream cannot be revoked",
		},
		{http.StatusInternalServerError, `oops`, "could not revoke the stream: /stream/revoke returned status 500: oops"},
	} {
		t.Run(http.StatusText(tt.status), func(t *testing.T) {
			srv, _, _ := revokeTestServer(t, tt.status, tt.body)
			tokSrv := setupAuthForTest(t)
			defer tokSrv.Close()

			root := NewRootCommand()
			root.SetArgs([]string{"satellite", "revoke-stream", "streamer-1", "--api-url", srv.URL, "--pass-id", "pass-1"})
			err := root.Execute()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tt.want)
			}
		})
	}
}

// Close and revoke share the request path, so a close refusal surfaces the
// same way.
func TestCallStreamCloseAPI(t *testing.T) {
	var got StreamCloseRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/stream/close" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewDecoder(r.Body).Decode(&got)
		if got.StreamID == "missing" {
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not_found","message":"Stream not found"}`))
		}
	}))
	t.Cleanup(srv.Close)

	if err := callStreamCloseAPI(t.Context(), srv.URL+"/", nil, "pass-1", "streamer-1"); err != nil {
		t.Fatalf("close: %v", err)
	}
	if want := (StreamCloseRequest{PassID: "pass-1", StreamID: "streamer-1", Source: "streamer"}); got != want {
		t.Errorf("request = %+v, want %+v", got, want)
	}

	err := callStreamCloseAPI(t.Context(), srv.URL, nil, "pass-1", "missing")
	var refusal *streamActionError
	if !errors.As(err, &refusal) || refusal.Status != http.StatusNotFound || refusal.Code != "not_found" {
		t.Fatalf("err = %#v, want a not_found refusal", err)
	}

	if err := callStreamCloseAPI(t.Context(), "", nil, "pass-1", "streamer-1"); err != nil {
		t.Errorf("no API configured: %v", err)
	}
}
