package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const retryTestKey = "/bucket/pass-1/chan-1/BITSTREAM/86"

// fakeS3 answers GetObject for one key with the scripted responses in turn,
// then with the full payload. Listing (the credential check) always succeeds.
type fakeS3 struct {
	payload []byte
	script  []func(w http.ResponseWriter)
	gets    atomic.Int32
}

func (f *fakeS3) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Get("list-type") != "" {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<ListBucketResult><Name>bucket</Name><KeyCount>0</KeyCount></ListBucketResult>`))
		return
	}
	if r.URL.Path != retryTestKey {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<Error><Code>NoSuchKey</Code></Error>`))
		return
	}
	n := int(f.gets.Add(1)) - 1
	if n < len(f.script) {
		f.script[n](w)
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(f.payload)))
	_, _ = w.Write(f.payload)
}

// cutOff promises the whole payload, sends a tenth of it and drops the
// connection: the "read: connection reset by peer" seen on adam.
func cutOff(payload []byte) func(w http.ResponseWriter) {
	return func(w http.ResponseWriter) {
		w.Header().Set("Content-Length", strconv.Itoa(len(payload)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload[:len(payload)/10])
		if hj, ok := w.(http.Hijacker); ok {
			if conn, _, err := hj.Hijack(); err == nil {
				_ = conn.Close()
			}
		}
	}
}

func status(code int, body string) func(w http.ResponseWriter) {
	return func(w http.ResponseWriter) {
		w.WriteHeader(code)
		_, _ = w.Write([]byte(body))
	}
}

func newFakeS3Client(t *testing.T, handler http.Handler) *s3.Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return s3.New(s3.Options{
		BaseEndpoint:     aws.String(server.URL),
		Credentials:      credentials.NewStaticCredentialsProvider("key", "secret", ""),
		Region:           "ap-northeast-1",
		RetryMaxAttempts: 1, // the SDK's own retries would hide what is under test
		UsePathStyle:     true,
	})
}

type retryCall struct {
	attempt int
	err     error
}

type retryRecorder struct {
	mu    sync.Mutex
	calls []retryCall
}

func (r *retryRecorder) report(_ string, _ int, _ string, attempt int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, retryCall{attempt: attempt, err: err})
}

func (r *retryRecorder) attempts() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]int, 0, len(r.calls))
	for _, call := range r.calls {
		out = append(out, call.attempt)
	}
	return out
}

func fetch(ctx context.Context, client *s3.Client, onRetry fetchRetryReporter) fetchResult {
	return fetchIndexForFraming(ctx, client, "bucket", "pass-1/", "chan-1", 86, "BITSTREAM",
		10*time.Millisecond, &sync.Map{}, onRetry)
}

func TestFetchIndexForFraming_RetriesADownloadCutOffMidway(t *testing.T) {
	payload := bytes.Repeat([]byte{0xAB}, 64*1024)
	fake := &fakeS3{payload: payload}
	fake.script = []func(http.ResponseWriter){cutOff(payload)}
	recorder := &retryRecorder{}

	res := fetch(context.Background(), newFakeS3Client(t, fake), recorder.report)

	if res.Err != nil {
		t.Fatalf("fetch gave up: %v", res.Err)
	}
	if !bytes.Equal(res.Data, payload) {
		t.Errorf("got %d bytes, want the whole %d-byte object", len(res.Data), len(payload))
	}
	if got := recorder.attempts(); len(got) != 1 || got[0] != 1 {
		t.Errorf("retry reports = %v, want one report for the first attempt", got)
	}
}

func TestFetchIndexForFraming_RetriesServerErrorsUntilTheObjectArrives(t *testing.T) {
	payload := []byte("telemetry")
	fake := &fakeS3{payload: payload, script: []func(http.ResponseWriter){
		status(http.StatusServiceUnavailable, `<Error><Code>SlowDown</Code></Error>`),
		status(http.StatusInternalServerError, `<Error><Code>InternalError</Code></Error>`),
	}}
	recorder := &retryRecorder{}

	res := fetch(context.Background(), newFakeS3Client(t, fake), recorder.report)

	if res.Err != nil || !bytes.Equal(res.Data, payload) {
		t.Fatalf("fetch = %q, %v; want the object", res.Data, res.Err)
	}
	if got := recorder.attempts(); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Errorf("retry reports = %v, want attempts 1 and 2", got)
	}
}

func TestFetchIndexForFraming_WaitsQuietlyForAnObjectNotUploadedYet(t *testing.T) {
	payload := []byte("late")
	fake := &fakeS3{payload: payload, script: []func(http.ResponseWriter){
		status(http.StatusNotFound, `<Error><Code>NoSuchKey</Code></Error>`),
	}}
	recorder := &retryRecorder{}

	res := fetch(context.Background(), newFakeS3Client(t, fake), recorder.report)

	if res.Err != nil || !bytes.Equal(res.Data, payload) {
		t.Fatalf("fetch = %q, %v; want the object", res.Data, res.Err)
	}
	if got := recorder.attempts(); len(got) != 0 {
		t.Errorf("a missing object was reported as a failure: %v", got)
	}
}

func TestFetchIndexForFraming_StopsRetryingWhenTheStreamEnds(t *testing.T) {
	fake := &fakeS3{script: make([]func(http.ResponseWriter), 1000)}
	for i := range fake.script {
		fake.script[i] = status(http.StatusInternalServerError, `<Error><Code>InternalError</Code></Error>`)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	res := fetch(ctx, newFakeS3Client(t, fake), nil)

	if !errors.Is(res.Err, context.DeadlineExceeded) {
		t.Errorf("err = %v, want the stream's context error", res.Err)
	}
}

func TestFetchBackoff_DoublesToACapAndResets(t *testing.T) {
	b := newFetchBackoff(time.Second)
	var got []time.Duration
	for range 6 {
		got = append(got, b.next())
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, maxFetchBackoff, maxFetchBackoff}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("backoff = %v, want %v", got, want)
		}
	}
	b.reset()
	if next := b.next(); next != time.Second {
		t.Errorf("after reset = %v, want the base again", next)
	}
	if next := newFetchBackoff(0).next(); next != minFetchBackoff {
		t.Errorf("zero poll interval backs off from %v, want %v", next, minFetchBackoff)
	}
}

func TestFetchRetryReporter_RecordsOnlyTheFirstFailure(t *testing.T) {
	stats := newStatsTracker(true)
	report := newFetchRetryReporter(stats)

	report("chan-1", 86, "BITSTREAM", 1, errors.New("read body: connection reset by peer"))
	report("chan-1", 86, "BITSTREAM", 2, errors.New("read body: connection reset by peer"))

	stats.errorsMu.Lock()
	defer stats.errorsMu.Unlock()
	if len(stats.errors) != 1 || !strings.Contains(stats.errors[0], "index 86") {
		t.Errorf("diagnostics errors = %v, want one entry for index 86", stats.errors)
	}
}
