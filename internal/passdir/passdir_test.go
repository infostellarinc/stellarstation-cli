// Copyright (c) 2026 InfoStellar, Inc. All Rights Reserved.

package passdir

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// writeChunk creates a chunk file at the given pass-directory-relative path
// with size bytes of content and the given mtime, and returns its full path.
func writeChunk(t *testing.T, passDir, rel string, size int, mtime time.Time) string {
	t.Helper()
	path := filepath.Join(passDir, rel)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, make([]byte, size), 0o600))
	require.NoError(t, os.Chtimes(path, mtime, mtime))
	return path
}

func writeSidecar(t *testing.T, chunkPath string, sendTS time.Time) {
	t.Helper()
	require.NoError(t, os.WriteFile(
		chunkPath+SendTimestampSuffix,
		[]byte(sendTS.UTC().Format(time.RFC3339Nano)),
		0o600,
	))
}

// requireDurationNear asserts two durations match to within a nanosecond. The
// standard deviation is computed through a float square root, so it can land a
// nanosecond either side of an exact expected value.
func requireDurationNear(t *testing.T, want, got time.Duration) {
	t.Helper()
	require.InDelta(t, float64(want), float64(got), float64(time.Nanosecond))
}

func TestCollectChunksReadsBothTransportLayouts(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	// The MQTT path carries a "downlink" segment; the S3 path does not.
	writeChunk(t, dir, "low-rate/channel/chan-a/downlink/BITSTREAM/1", 10, base)
	writeChunk(t, dir, "high-rate/channel/chan-b/IQ/1", 20, base)

	chunks := CollectChunks(dir)
	require.Len(t, chunks, 2)

	byChannel := map[string]Chunk{}
	for _, c := range chunks {
		byChannel[c.Channel] = c
	}
	require.Equal(t, RateTypeLow, byChannel["chan-a"].RateType)
	require.Equal(t, "BITSTREAM", byChannel["chan-a"].Framing)
	require.Equal(t, RateTypeHigh, byChannel["chan-b"].RateType)
	require.Equal(t, "IQ", byChannel["chan-b"].Framing)
}

// The sidecar sits in the same directory as its chunk. Counting it as a chunk
// would double every count and inject a near-zero gap beside every real one,
// which is exactly the defect this filter exists to prevent.
func TestCollectChunksSkipsSendTimestampSidecars(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	for i, offset := range []time.Duration{0, time.Second, 2 * time.Second} {
		path := writeChunk(t, dir,
			filepath.Join("low-rate/channel/chan-a/downlink/BITSTREAM", string(rune('1'+i))),
			100, base.Add(offset))
		writeSidecar(t, path, base.Add(offset))
	}

	chunks := CollectChunks(dir)
	require.Len(t, chunks, 3, "sidecars must not be counted as chunks")

	// Without the filter the gaps would collapse towards zero.
	stats, err := ChunkArrivalStats(chunks)
	require.NoError(t, err)
	require.Equal(t, 2, stats.N)
	require.Equal(t, time.Second, stats.Mean)
	require.Zero(t, stats.Jitter)
}

func TestCollectChunksToleratesMissingDirectory(t *testing.T) {
	require.Empty(t, CollectChunks(filepath.Join(t.TempDir(), "never-executed")))
}

func TestArrivalStatsSortsAndComputesJitter(t *testing.T) {
	base := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	// Deliberately out of order; gaps are 1s, 3s so mean 2s, stddev 1s.
	times := []time.Time{
		base.Add(4 * time.Second),
		base,
		base.Add(time.Second),
	}
	stats, err := ArrivalStats(times)
	require.NoError(t, err)
	require.Equal(t, 2, stats.N)
	require.Equal(t, 2*time.Second, stats.Mean)
	requireDurationNear(t, time.Second, stats.Jitter)
}

func TestArrivalStatsNeedsTwoTimestamps(t *testing.T) {
	_, err := ArrivalStats([]time.Time{time.Now()})
	require.ErrorIs(t, err, ErrNotEnoughSamples)
}

func TestDurationStatsRejectsEmptyInput(t *testing.T) {
	_, err := DurationStats(nil)
	require.ErrorIs(t, err, ErrNotEnoughSamples)
}

// The span starts when the first chunk arrives, so only what arrives after it
// was carried during the span. Counting the first chunk's bytes too would
// divide n chunks' bytes by n-1 intervals, overstating a two-chunk
// measurement by 100% and a seven-chunk one by 17%.
func TestChunkThroughputKbps(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	// 1000 bytes arrive during the 2 second span: 4000 bit/s, so 4 kbit/s.
	writeChunk(t, dir, "low-rate/channel/chan-a/downlink/BITSTREAM/1", 1000, base)
	writeChunk(t, dir, "low-rate/channel/chan-a/downlink/BITSTREAM/2", 1000, base.Add(2*time.Second))

	kbps, err := ChunkThroughputKbps(CollectChunks(dir))
	require.NoError(t, err)
	require.InDelta(t, 4.0, kbps, 0.001)
}

// A steady stream should report close to its real rate, which is the check
// that the off-by-one interval is actually gone: eleven chunks of 1000 bytes
// one second apart carry 10000 bytes over 10 seconds, so 8 kbit/s exactly.
func TestChunkThroughputKbpsOnASteadyStream(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 11; i++ {
		writeChunk(t, dir,
			filepath.Join("high-rate/channel/chan-a/BITSTREAM", strconv.Itoa(i)),
			1000, base.Add(time.Duration(i)*time.Second))
	}

	kbps, err := ChunkThroughputKbps(CollectChunks(dir))
	require.NoError(t, err)
	require.InDelta(t, 8.0, kbps, 0.001)
}

func TestChunkThroughputKbpsNeedsTwoChunks(t *testing.T) {
	dir := t.TempDir()
	writeChunk(t, dir, "low-rate/channel/chan-a/downlink/BITSTREAM/1", 1000, time.Now())

	_, err := ChunkThroughputKbps(CollectChunks(dir))
	require.ErrorIs(t, err, ErrNotEnoughSamples)
}

// The clamp guards against a clock that has jumped, so it has to hold at its
// own boundary and against NaN, both of which previously converted to the
// opposite extreme.
func TestDurationFromSecondsClampBoundaries(t *testing.T) {
	const maxSec = float64(math.MaxInt64) / float64(time.Second)

	require.Equal(t, time.Duration(math.MaxInt64), durationFromSeconds(maxSec))
	require.Equal(t, time.Duration(math.MaxInt64), durationFromSeconds(maxSec*2))
	require.Equal(t, time.Duration(math.MinInt64), durationFromSeconds(-maxSec))
	require.Equal(t, time.Duration(math.MinInt64), durationFromSeconds(-maxSec*2))
	require.Zero(t, durationFromSeconds(math.NaN()))
}

func TestChunkThroughputKbpsRejectsZeroSpan(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	writeChunk(t, dir, "low-rate/channel/chan-a/downlink/BITSTREAM/1", 1000, base)
	writeChunk(t, dir, "low-rate/channel/chan-a/downlink/BITSTREAM/2", 1000, base)

	_, err := ChunkThroughputKbps(CollectChunks(dir))
	require.ErrorIs(t, err, ErrNotEnoughSamples)
}

func TestChunkLatencyStatsUsesSidecars(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	// Arrival 100ms and 300ms after the station's send time: mean 200ms.
	a := writeChunk(t, dir, "low-rate/channel/chan-a/downlink/BITSTREAM/1", 10, base.Add(100*time.Millisecond))
	writeSidecar(t, a, base)
	b := writeChunk(t, dir, "low-rate/channel/chan-a/downlink/BITSTREAM/2", 10, base.Add(time.Second+300*time.Millisecond))
	writeSidecar(t, b, base.Add(time.Second))

	stats, err := ChunkLatencyStats(CollectChunks(dir))
	require.NoError(t, err)
	require.Equal(t, 2, stats.N)
	require.Equal(t, 200*time.Millisecond, stats.Mean)
	requireDurationNear(t, 100*time.Millisecond, stats.Jitter)
}

func TestChunkLatencyStatsWithoutSidecars(t *testing.T) {
	dir := t.TempDir()
	writeChunk(t, dir, "low-rate/channel/chan-a/downlink/BITSTREAM/1", 10, time.Now())

	_, err := ChunkLatencyStats(CollectChunks(dir))
	require.ErrorIs(t, err, ErrNotEnoughSamples)
}

func TestReadSendTimestampRejectsUnparseableContent(t *testing.T) {
	dir := t.TempDir()
	path := writeChunk(t, dir, "low-rate/channel/chan-a/downlink/BITSTREAM/1", 10, time.Now())
	require.NoError(t, os.WriteFile(path+SendTimestampSuffix, []byte("not a timestamp"), 0o600))

	_, ok := ReadSendTimestamp(Chunk{Path: path})
	require.False(t, ok)
}

func TestReadSendTimestampTrimsWhitespace(t *testing.T) {
	dir := t.TempDir()
	want := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	path := writeChunk(t, dir, "low-rate/channel/chan-a/downlink/BITSTREAM/1", 10, time.Now())
	require.NoError(t, os.WriteFile(
		path+SendTimestampSuffix,
		[]byte("\n"+want.Format(time.RFC3339Nano)+"\n"),
		0o600,
	))

	got, ok := ReadSendTimestamp(Chunk{Path: path})
	require.True(t, ok)
	require.True(t, want.Equal(got))
}

func TestNonTelemetryLatencyStats(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	write := func(name string, recordedAt, mtime time.Time) {
		path := filepath.Join(dir, name)
		body := `{"recordedAt":"` + recordedAt.Format(time.RFC3339Nano) + `"}`
		require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
		require.NoError(t, os.Chtimes(path, mtime, mtime))
	}
	write("a.json", base, base.Add(50*time.Millisecond))
	write("b.json", base, base.Add(150*time.Millisecond))
	// Ignored: not JSON, no recordedAt, and not a .json file at all.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "c.json"), []byte("{"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "d.json"), []byte("{}"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "e.txt"), []byte("ignored"), 0o600))

	stats, err := NonTelemetryLatencyStats(dir)
	require.NoError(t, err)
	require.Equal(t, 2, stats.N)
	require.Equal(t, 100*time.Millisecond, stats.Mean)
	requireDurationNear(t, 50*time.Millisecond, stats.Jitter)
}

func TestNonTelemetryLatencyStatsOnEmptyDirectory(t *testing.T) {
	_, err := NonTelemetryLatencyStats(t.TempDir())
	require.True(t, errors.Is(err, ErrNotEnoughSamples))
}

func TestTotalBytesAndFilter(t *testing.T) {
	dir := t.TempDir()
	now := time.Now()
	writeChunk(t, dir, "low-rate/channel/chan-a/downlink/BITSTREAM/1", 100, now)
	writeChunk(t, dir, "high-rate/channel/chan-b/BITSTREAM/1", 250, now)

	chunks := CollectChunks(dir)
	require.EqualValues(t, 350, TotalBytes(chunks))

	high := Filter(chunks, func(c Chunk) bool { return c.RateType == RateTypeHigh })
	require.Len(t, high, 1)
	require.EqualValues(t, 250, TotalBytes(high))
}

// A clock that has jumped by decades must not overflow the Duration returned.
func TestDurationStatsSurvivesAbsurdClockSkew(t *testing.T) {
	stats, err := DurationStats([]time.Duration{time.Duration(1 << 62), time.Duration(1 << 62)})
	require.NoError(t, err)
	require.Positive(t, stats.Mean)
}

// The MQTT and S3 transports disagree on framing case, and callers compare
// framings across both, so the walk normalises upwards.
func TestCollectChunksNormalisesFramingCase(t *testing.T) {
	dir := t.TempDir()
	writeChunk(t, dir, "high-rate/channel/chan-a/bitstream/1", 10, time.Now())

	chunks := CollectChunks(dir)
	require.Len(t, chunks, 1)
	require.Equal(t, "BITSTREAM", chunks[0].Framing)
}

func TestCollectChunksIgnoresPathsWithoutAChannelSegment(t *testing.T) {
	dir := t.TempDir()
	writeChunk(t, dir, "low-rate/stray-file", 10, time.Now())
	writeChunk(t, dir, "low-rate/diagnostics/summary/1", 10, time.Now())

	require.Empty(t, CollectChunks(dir))
}
