// Copyright (c) 2026 InfoStellar, Inc. All Rights Reserved.

// Package passdir owns the on-disk layout of a pass output directory and the
// arrival statistics computed over it.
//
// open-stream writes each telemetry chunk as a file under
// <pass>/<rate-type>/channel/<id>/[downlink/]<framing>/<index>, and writes a
// send-time sidecar beside any chunk whose station stamped one. Both the
// writer (cmd/stellarcli) and the readers (the performance E2E tests) go
// through this package, so the naming and the walk cannot drift apart.
package passdir

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// SendTimestampSuffix names the send-time sidecar written beside a telemetry
// chunk. CollectChunks skips these; counting them would double every chunk
// count and inject a near-zero gap beside every real one.
const SendTimestampSuffix = ".sendts"

// RateType names the two transports a downlink channel can use. They are the
// directory names open-stream writes under, not display strings.
const (
	RateTypeLow  = "low-rate"
	RateTypeHigh = "high-rate"
)

// ErrNotEnoughSamples is returned by the statistics functions when the input
// cannot support the statistic being asked for, which for these callers
// usually means a pass produced too little data to measure. That is an
// environment outcome rather than a defect, so it is worth distinguishing
// from a real failure with errors.Is.
var ErrNotEnoughSamples = errors.New("not enough samples")

// Chunk is one telemetry chunk file found under a pass output directory.
type Chunk struct {
	RateType string // RateTypeLow or RateTypeHigh
	Channel  string
	Framing  string
	Path     string
}

// Stats is the mean and the standard deviation ("jitter") of a sample set,
// along with the number of samples they were computed from.
type Stats struct {
	Mean   time.Duration
	Jitter time.Duration
	N      int
}

// CollectChunks walks a pass output directory and returns every telemetry
// chunk under low-rate/ and high-rate/, with the channel and framing taken
// from the path. It tolerates the optional "downlink" path segment, which the
// MQTT path writes and the S3 path does not, skips send-time sidecars, and
// ignores any file whose path carries no channel segment (diagnostics output,
// for one, shares the directory tree).
//
// A missing or unreadable directory yields no chunks rather than an error: a
// pass that never executed is indistinguishable on disk from one that has not
// started yet, and callers decide what to do about an empty result.
func CollectChunks(passDir string) []Chunk {
	var chunks []Chunk
	for _, rateType := range []string{RateTypeLow, RateTypeHigh} {
		root := filepath.Join(passDir, rateType)
		_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil || info == nil || info.IsDir() {
				// Returning the error would abort the whole walk; an entry
				// that cannot be read is skipped so the rest of the pass
				// directory still counts, per this function's contract.
				return nil //nolint:nilerr // deliberate skip-and-continue
			}
			if strings.HasSuffix(path, SendTimestampSuffix) {
				return nil
			}
			rel, relErr := filepath.Rel(root, path)
			if relErr != nil {
				return nil //nolint:nilerr // deliberate skip-and-continue
			}
			parts := strings.Split(rel, string(os.PathSeparator))
			if len(parts) < 2 {
				return nil
			}
			channel := channelFromParts(parts)
			if channel == "" {
				return nil
			}
			chunks = append(chunks, Chunk{
				RateType: rateType,
				Channel:  channel,
				// The leaf is the chunk index, so its parent is the framing.
				// Normalised upwards because the two transports disagree on
				// case, and callers compare framings across both.
				Framing: strings.ToUpper(parts[len(parts)-2]),
				Path:    path,
			})
			return nil
		})
	}
	return chunks
}

// channelFromParts returns the segment following "channel" in a chunk's
// path-relative segments, or "" when the path carries no channel segment.
func channelFromParts(parts []string) string {
	for i := 0; i < len(parts)-1; i++ {
		if parts[i] == "channel" {
			return parts[i+1]
		}
	}
	return ""
}

// ReadSendTimestamp reads the send-time sidecar beside a chunk. ok is false
// when no sidecar exists, which is the case for any station that does not
// stamp TimeFirstByteReceived.
func ReadSendTimestamp(c Chunk) (ts time.Time, ok bool) {
	b, err := os.ReadFile(c.Path + SendTimestampSuffix)
	if err != nil {
		return time.Time{}, false
	}
	parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(string(b)))
	if err != nil {
		return time.Time{}, false
	}
	return parsed, true
}

// DurationStats returns the mean and standard deviation of a sample set.
//
// The mean is computed in integer nanoseconds, which keeps it within a
// nanosecond of the true value; summing the samples as fractional seconds
// instead rounds a plain 100ms mean to 99.999999ms. The standard deviation has
// no integer form, so it does go through floating point.
func DurationStats(samples []time.Duration) (Stats, error) {
	if len(samples) == 0 {
		return Stats{}, fmt.Errorf("%w: need at least 1 sample, got 0", ErrNotEnoughSamples)
	}
	mean := meanDuration(samples)
	meanSec := mean.Seconds()
	var variance float64
	for _, s := range samples {
		d := s.Seconds() - meanSec
		variance += d * d
	}
	variance /= float64(len(samples))
	return Stats{
		Mean:   mean,
		Jitter: durationFromSeconds(math.Sqrt(variance)),
		N:      len(samples),
	}, nil
}

// meanDuration averages durations without overflowing on the sum. It
// accumulates each sample's quotient and remainder against the sample count
// separately, so the running total stays bounded by the mean rather than by
// the sum. The remainder division truncates, so the result can be up to a
// nanosecond off. Negative and mixed-sign inputs are handled correctly, and do
// occur: a skewed clock produces negative latencies. Callers guarantee a
// non-empty input.
func meanDuration(samples []time.Duration) time.Duration {
	n := int64(len(samples))
	var quotient, remainder int64
	for _, s := range samples {
		quotient += int64(s) / n
		remainder += int64(s) % n
	}
	return time.Duration(quotient + remainder/n)
}

// ArrivalStats returns the mean and standard deviation of the gaps between
// consecutive timestamps. The timestamps need not be sorted. At least two are
// required, since one timestamp yields no gap.
func ArrivalStats(times []time.Time) (Stats, error) {
	if len(times) < 2 {
		return Stats{}, fmt.Errorf("%w: need at least 2 timestamps, got %d", ErrNotEnoughSamples, len(times))
	}
	sorted := append([]time.Time(nil), times...)
	sortTimes(sorted)
	gaps := make([]time.Duration, 0, len(sorted)-1)
	for i := 1; i < len(sorted); i++ {
		gaps = append(gaps, sorted[i].Sub(sorted[i-1]))
	}
	return DurationStats(gaps)
}

// ChunkArrivalStats is ArrivalStats over a set of chunks, using each chunk
// file's mtime as its arrival time. open-stream flushes a chunk to disk
// synchronously on receipt, so the mtime is a faithful arrival proxy.
func ChunkArrivalStats(chunks []Chunk) (Stats, error) {
	times := make([]time.Time, 0, len(chunks))
	for _, c := range chunks {
		if info, err := os.Stat(c.Path); err == nil {
			times = append(times, info.ModTime())
		}
	}
	return ArrivalStats(times)
}

// ChunkLatencyStats returns the one-way latency of every chunk carrying a
// send-time sidecar: the chunk's arrival mtime minus the station's own send
// time. Unlike ChunkArrivalStats, which measures spacing, this is a real
// latency, though it still carries whatever skew exists between the station's
// clock and this machine's. There is no clock synchronisation between them.
func ChunkLatencyStats(chunks []Chunk) (Stats, error) {
	var latencies []time.Duration
	for _, c := range chunks {
		sendTS, ok := ReadSendTimestamp(c)
		if !ok {
			continue
		}
		info, err := os.Stat(c.Path)
		if err != nil {
			continue
		}
		latencies = append(latencies, info.ModTime().Sub(sendTS))
	}
	if len(latencies) == 0 {
		return Stats{}, fmt.Errorf("%w: no chunks carry a %s sidecar", ErrNotEnoughSamples, SendTimestampSuffix)
	}
	return DurationStats(latencies)
}

// NonTelemetryLatencyStats returns the one-way latency of the monitoring,
// config or event messages in a directory. Each is a JSON file carrying a
// top-level "recordedAt" that the station set, diffed against the file's
// arrival mtime. The same clock-skew caveat as ChunkLatencyStats applies.
func NonTelemetryLatencyStats(dir string) (Stats, error) {
	files, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	var latencies []time.Duration
	for _, f := range files {
		info, err := os.Stat(f)
		if err != nil {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var msg struct {
			RecordedAt string `json:"recordedAt"`
		}
		if err := json.Unmarshal(b, &msg); err != nil || msg.RecordedAt == "" {
			continue
		}
		recordedAt, err := time.Parse(time.RFC3339Nano, msg.RecordedAt)
		if err != nil {
			continue
		}
		latencies = append(latencies, info.ModTime().Sub(recordedAt))
	}
	if len(latencies) == 0 {
		return Stats{}, fmt.Errorf("%w: no messages with a parseable recordedAt in %s", ErrNotEnoughSamples, dir)
	}
	return DurationStats(latencies)
}

// ChunkThroughputKbps returns the effective downlink throughput across a set
// of chunks, measured between the first and last arrival.
//
// The first chunk's bytes are excluded from the total. The span starts at that
// chunk's arrival, so its own transfer happened before the window opened, and
// counting its bytes against a window it did not use inflates the result by
// n/(n-1): +17% over seven chunks, +100% over two. What is measured is
// therefore the bytes that arrived during the span, over the span.
//
// The unit is kbit/s rather than Mbit/s because the reported metric value is
// a whole number, and the slowest sweep tier would round to zero in Mbit/s.
func ChunkThroughputKbps(chunks []Chunk) (float64, error) {
	type arrival struct {
		at    time.Time
		bytes int64
	}
	arrivals := make([]arrival, 0, len(chunks))
	for _, c := range chunks {
		info, err := os.Stat(c.Path)
		if err != nil {
			continue
		}
		arrivals = append(arrivals, arrival{at: info.ModTime(), bytes: info.Size()})
	}
	if len(arrivals) < 2 {
		return 0, fmt.Errorf("%w: need at least 2 chunks, got %d", ErrNotEnoughSamples, len(arrivals))
	}
	sort.Slice(arrivals, func(i, j int) bool { return arrivals[i].at.Before(arrivals[j].at) })

	span := arrivals[len(arrivals)-1].at.Sub(arrivals[0].at).Seconds()
	if span <= 0 {
		return 0, fmt.Errorf("%w: chunks span a zero or negative duration", ErrNotEnoughSamples)
	}
	var bytesInSpan int64
	for _, a := range arrivals[1:] {
		bytesInSpan += a.bytes
	}
	return float64(bytesInSpan*8) / span / 1e3, nil
}

// TotalBytes returns the number of bytes on disk across a set of chunks.
func TotalBytes(chunks []Chunk) int64 {
	var total int64
	for _, c := range chunks {
		if info, err := os.Stat(c.Path); err == nil {
			total += info.Size()
		}
	}
	return total
}

// Filter returns the chunks matching keep, preserving order.
func Filter(chunks []Chunk, keep func(Chunk) bool) []Chunk {
	out := make([]Chunk, 0, len(chunks))
	for _, c := range chunks {
		if keep(c) {
			out = append(out, c)
		}
	}
	return out
}

// durationFromSeconds converts fractional seconds to a Duration without
// overflowing on the absurd values a bad clock can produce.
func durationFromSeconds(sec float64) time.Duration {
	const maxSec = float64(math.MaxInt64) / float64(time.Second)
	// NaN compares false against everything, so it has to be caught first or
	// it falls through both clamps and converts to MinInt64.
	if math.IsNaN(sec) {
		return 0
	}
	// The comparisons include the boundary: converting exactly maxSec
	// overflows to MinInt64, the opposite extreme to the one intended.
	if sec >= maxSec {
		return time.Duration(math.MaxInt64)
	}
	if sec <= -maxSec {
		return time.Duration(math.MinInt64)
	}
	return time.Duration(sec * float64(time.Second))
}

// sortTimes orders timestamps ascending.
func sortTimes(times []time.Time) {
	sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
}
