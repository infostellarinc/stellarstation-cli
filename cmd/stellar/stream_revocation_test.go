package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A watch over a fake authorizer that answers every call with the given status
// and body, counting calls and recording what the operator was told.
func revocationWatchOver(t *testing.T, status int, body string) (*streamRevocationWatch, *atomic.Int32, func() []string) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	var mu sync.Mutex
	var told []string
	cs := newCredentialStore(makeTestCreds(time.Now().Add(time.Hour)))
	watch := newStreamRevocationWatch(srv.URL, refresherTestTokenSource(), AuthorizerRequest{PassID: "pass-1", StreamID: "stream-1"}, cs,
		func(detail string) {
			mu.Lock()
			defer mu.Unlock()
			told = append(told, detail)
		})
	return watch, &calls, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), told...)
	}
}

// A lost connection is checked against the authorizer; a revoked stream is
// reported once, with the authorizer's explanation, and later losses are
// explained without another call.
func TestStreamRevocationWatch_LostConnectionOfARevokedStream(t *testing.T) {
	watch, calls, told := revocationWatchOver(t, http.StatusForbidden,
		`{"error":"stream_revoked","message":"this stream was revoked by user-1: stale client; open a new stream instead"}`)

	if watch.connectionLost() {
		t.Fatal("the first loss is not yet explained")
	}
	deadline := time.Now().Add(5 * time.Second)
	for !watch.isRevoked() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !watch.isRevoked() {
		t.Fatal("the stream was not found revoked")
	}
	if got := told(); len(got) != 1 || got[0] != "this stream was revoked by user-1: stale client; open a new stream instead" {
		t.Errorf("operator told %q", got)
	}
	if !watch.connectionLost() {
		t.Error("a later loss of a revoked stream is explained")
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("authorizer called %d times, want once", got)
	}
}

// A lost connection of a live stream is nothing to report; the check refreshes
// the credentials instead, and only one check runs at a time.
func TestStreamRevocationWatch_LostConnectionOfALiveStream(t *testing.T) {
	body, err := json.Marshal(makeTestCreds(time.Now().Add(2 * time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	watch, calls, told := revocationWatchOver(t, http.StatusOK, string(body))
	before := watch.store.load().Expiration

	for range 3 {
		if watch.connectionLost() {
			t.Fatal("a live stream's loss is not explained by revocation")
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for watch.store.load().Expiration.Equal(before) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if watch.store.load().Expiration.Equal(before) {
		t.Fatal("the check did not refresh the credentials")
	}
	if watch.isRevoked() || len(told()) != 0 {
		t.Error("a live stream was reported revoked")
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("authorizer called %d times, want once for the overlapping losses", got)
	}
}

// Any other authorizer failure leaves the stream as it is.
func TestStreamRevocationWatch_OtherFailuresAreNotRevocations(t *testing.T) {
	watch, _, told := revocationWatchOver(t, http.StatusServiceUnavailable, `{"error":"unavailable"}`)
	if _, err := watch.refresh(t.Context()); err == nil {
		t.Fatal("expected the refresh to fail")
	}
	if watch.isRevoked() || len(told()) != 0 {
		t.Error("a failed refresh was reported as a revocation")
	}
}

// A session opened without an authorizer has no watch; its connections are
// never explained by a revocation.
func TestStreamRevocationWatch_NilIsNeverRevoked(t *testing.T) {
	var watch *streamRevocationWatch
	if watch.connectionLost() || watch.isRevoked() {
		t.Error("a nil watch reported a revocation")
	}
}
