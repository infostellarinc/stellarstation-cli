package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"google.golang.org/protobuf/proto"

	streaming "github.com/infostellarinc/stellarstation-cli/gen/pb/stellarstation/satellitestreamer"
)

// channelFramingKey is a composite key for tracking per (channelID, framing) pair
type channelFramingKey struct {
	ChannelID string
	Framing   string
}

// spawnS3Fetcher launches a goroutine to fetch one (channel, index, framing)
// object from S3 and routes its result directly to that key's own worker (see
// keyed_result_router.go), so a slow or backed-up channel/framing can never
// delay delivery of another's result.
func spawnS3Fetcher(
	ctx context.Context,
	s3c *s3.Client,
	bucket, prefix string,
	channelID string,
	index int,
	framing string,
	pollInterval time.Duration,
	route func(fetchResult),
	inFlight *sharedCounter[channelFramingKey],
	credsLogged *sync.Map,
) {
	key := channelFramingKey{ChannelID: channelID, Framing: framing}
	inFlight.inc(key)
	go func(chID string, idx int, f string) {
		res := fetchIndexForFraming(
			ctx,
			s3c,
			bucket,
			prefix,
			chID,
			idx,
			f,
			pollInterval,
			credsLogged,
		)
		route(res)
	}(channelID, index, framing)
}

func checkAllChannelFramingsDone(
	channelsDone map[channelFramingKey]bool,
	activeKeys map[channelFramingKey]bool,
) bool {
	for key := range activeKeys {
		if !channelsDone[key] {
			return false
		}
	}
	return true
}

// spawnS3Fetchers is the in-order high-rate scheduler step: for each
// (channel, framing) it spawns fetches up to the in-flight window. It is
// scheduler-goroutine-local except for inFlight (shared with the per-key
// workers, which decrement it on receipt; see newS3InOrderWorker) and
// nextWriteShared (worker-owned; the scheduler only reads it to respect the
// write window).
func spawnS3Fetchers(
	ctx context.Context,
	cfg Config,
	s3c *s3.Client,
	route func(fetchResult),
	nextFetch map[channelFramingKey]int, // scheduler-local
	nextWriteShared *sharedCounter[channelFramingKey],
	inFlight *sharedCounter[channelFramingKey],
	credsLogged *sync.Map,
) {
	// Try each channel's framing combinations. Framings are per-channel (from the
	// authorizer metadata) so we don't probe framings a channel never emits.
	for _, chID := range cfg.ChannelIDs {
		for _, framing := range framingsToTryForChannel(cfg, chID) {
			key := channelFramingKey{ChannelID: chID, Framing: framing}
			// Initialize nextFetch to 1 if not set (indices start at 1)
			if nextFetch[key] <= 0 {
				nextFetch[key] = 1
			}
			nextWrite := nextWriteShared.get(key)
			if nextWrite <= 0 {
				nextWrite = 1
			}
			if inFlight.get(key) < cfg.WindowSize && nextFetch[key] < nextWrite+cfg.WindowSize {
				spawnS3Fetcher(
					ctx,
					s3c,
					cfg.Bucket,
					cfg.Prefix,
					chID,
					nextFetch[key],
					framing,
					cfg.S3PollInterval,
					route,
					inFlight,
					credsLogged,
				)
				nextFetch[key]++
			}
		}
	}
}

// newS3InOrderWorker returns a per-key (channel+framing) worker function for
// keyedResultRouter that writes one high-rate channel/framing's chunks to disk
// in index order. Each key gets its own call (and so its own goroutine and
// local state below, no map keyed by channelFramingKey needed), which is what
// makes one channel's write speed independent of every other channel's (and of
// every other framing of the same channel).
// classifyS3FetchResult handles the error/no-op paths for one fetchResult
// received by the in-order S3 worker: context cancellation, expected 404s (a
// framing type that doesn't exist for a given channel/index), and transient
// credential errors during rotation. done=true means the worker's receive
// loop must return immediately (context canceled). handled=true means the
// result needs no further (success-path) processing by the caller, either
// because it was fully handled here (an error), or because it carries no
// framing and should be skipped.
func classifyS3FetchResult(res fetchResult, stats *statsTracker) (done, handled bool) {
	if res.Err == nil {
		return false, res.Framing == ""
	}
	if errors.Is(res.Err, context.Canceled) {
		emitLine("")
		vlogf("Context cancelled; exiting.")
		return true, true
	}
	if isNotFound(res.Err) {
		vlogf(
			"Channel %s index %d framing %s not found in S3 (may not exist for this framing type)",
			res.ChannelID, res.Index, res.Framing,
		)
		return false, true
	}
	errMsg := fmt.Errorf(
		"fetching channel %s index %d framing %s: %w",
		res.ChannelID, res.Index, res.Framing, res.Err,
	)
	stats.AddError(errMsg)
	if isTransientS3AuthError(res.Err) {
		// Rotated credentials are still propagating; this object is retried on the
		// next poll, so keep it out of the operator's face and only surface it
		// under --verbose.
		vlogf("Transient credential error while fetching telemetry (retrying): %v", errMsg)
		return false, true
	}
	streamErrf(res.Err, "Problem while receiving telemetry: %v (continuing)", errMsg)
	return false, true
}

func newS3InOrderWorker(
	cfg Config,
	stats *statsTracker,
	client mqtt.Client,
	ackSender *ackSender,
	inFlight *sharedCounter[channelFramingKey],
	nextWriteShared *sharedCounter[channelFramingKey],
) func(ctx context.Context, key channelFramingKey, in <-chan fetchResult) error {
	return func(ctx context.Context, key channelFramingKey, in <-chan fetchResult) error {
		pending := make(map[int]fetchResult)
		nextWrite := 1
		nextWriteShared.set(key, nextWrite)
		telemetryChannelID := key.ChannelID + "/downlink"

		writeContiguous := func() error {
			for {
				pr, ok := pending[nextWrite]
				if !ok {
					break
				}

				if err := writeChunkToFile(
					pr.Data,
					cfg.DestDir,
					cfg.Prefix,
					cfg.PassID,
					pr.Index,
					pr.ChannelID,
					pr.Framing,
					"high-rate",
					stats,
					cfg.WriteInOrder,
					cfg,
				); err != nil {
					stats.AddError(
						fmt.Errorf(
							"write index %d (channel %s, framing %s): %w",
							pr.Index, key.ChannelID, key.Framing, err,
						),
					)
					return fmt.Errorf(
						"write index %d (channel %s, framing %s): %w",
						pr.Index, key.ChannelID, key.Framing, err,
					)
				}

				stats.AddChannelWrite(telemetryChannelID, int64(len(pr.Data)))
				// Send ack for high-rate S3 message via MQTT
				sendHighRateS3Ack(cfg, client, ackSender, pr.ChannelID, pr.Framing, pr.Index, len(pr.Data), pr.MessageID)

				delete(pending, nextWrite)
				nextWrite++
				nextWriteShared.set(key, nextWrite)

				stats.SetChannelNextIndex(telemetryChannelID, nextWrite)

				if isEndMessage(pr.Metadata) {
					// Record END for stats/UI but do NOT stop: an END must never stop the
					// scan (a past-pass re-stream can carry more objects after an early
					// END). Fetching continues; the stream ends via idle-shutdown,
					// Ctrl-C, or the auto-close END sentinel below.
					stats.MarkChannelEnd(key.ChannelID)
					emitLine("")
					// Auto-close: Check for zero-size data as stream end indicator
					if cfg.EnableAutoClose && len(pr.Data) == 0 {
						log.Printf("Stream auto-close: zero-size END message detected, exiting...")
						closeAuthorizerStream(cfg)
						exitAfterTeardown()
					}
					// Continue draining any further contiguous chunks already fetched.
					continue
				}
			}
			return nil
		}

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()

			case res, ok := <-in:
				if !ok {
					return nil
				}
				if res.Framing != "" {
					inFlight.dec(key)
				}

				done, handled := classifyS3FetchResult(res, stats)
				if done {
					return nil
				}
				if handled {
					continue
				}

				stats.AddChannelDownload(telemetryChannelID, key.Framing, int64(len(res.Data)), SourceS3, res.Index)
				pending[res.Index] = res
				stats.SetChannelNextIndex(telemetryChannelID, nextWrite)

				if err := writeContiguous(); err != nil {
					return err
				}
			}
		}
	}
}

// runS3InOrder streams high-rate S3 downloads for every channel/framing,
// writing each in index order. A dedicated scheduler goroutine spawns fetches
// respecting the per-key in-flight window; each key's results are processed by
// its own worker goroutine (via keyedResultRouter), so one channel's disk speed
// can never delay another's fetching or writing.
func runS3InOrder(
	ctx context.Context,
	cfg Config,
	s3c *s3.Client,
	stats *statsTracker,
	client mqtt.Client,
	ackSender *ackSender,
) error {
	vlogf("Starting S3 downloads from bucket: %s, prefix: %s, channels: %v, window size: %d",
		cfg.Bucket, cfg.Prefix, cfg.ChannelIDs, cfg.WindowSize)
	vlogf(
		"S3 key pattern: <pass-id>/<channel-id>/<framing>/<index> (trying all framing types: %v)",
		getAllFramingTypes(),
	)

	// s3ResultsChBuf sizes each KEY's (channel+framing) own inbound queue: every
	// key gets its own buffer, rather than one shared across all of them, so a
	// slow or backed-up key can only ever fill its own queue.
	s3ResultsChBuf := cfg.WindowSize * 4
	inFlight := newSharedCounter[channelFramingKey]()
	nextWriteShared := newSharedCounter[channelFramingKey]()
	worker := newS3InOrderWorker(cfg, stats, client, ackSender, inFlight, nextWriteShared)
	router := newKeyedResultRouter(ctx, s3ResultsChBuf, worker)
	route := func(res fetchResult) {
		router.Route(channelFramingKey{ChannelID: res.ChannelID, Framing: res.Framing}, res)
	}

	nextFetch := make(map[channelFramingKey]int)
	var credsLogged sync.Map

	schedCtx, cancelSched := context.WithCancel(ctx)
	defer cancelSched()
	go func() {
		ticker := time.NewTicker(s3SchedulerTick)
		defer ticker.Stop()
		for {
			spawnS3Fetchers(schedCtx, cfg, s3c, route, nextFetch, nextWriteShared, inFlight, &credsLogged)
			select {
			case <-schedCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	return router.Wait()
}

// shouldSpawnRelaxedFetch reports whether the relaxed high-rate S3 reader should
// spawn another fetch for key. An END marker must NEVER stop the scan, in any
// mode: more objects for the channel may already exist in S3 (re-streaming a past
// pass can encounter an END at a low index ahead of later data), and even live a
// late object can follow the END. Only the in-flight window cap gates fetching;
// the stream ends via idle-shutdown, Ctrl-C, or the auto-close END sentinel.
func shouldSpawnRelaxedFetch(
	cfg Config,
	key channelFramingKey,
	sawEnd map[channelFramingKey]bool, //nolint:revive,unparam // kept to document that END is intentionally ignored
	inFlight map[channelFramingKey]int,
) bool {
	// Deliberately independent of sawEnd: an END never gates fetching.
	return inFlight[key] < cfg.WindowSize
}

// spawnS3FetchersRelaxed is the relaxed high-rate scheduler step. It mirrors
// shouldSpawnRelaxedFetch's in-flight-window-only gating inline (that function
// is kept, with its plain-map signature, purely for its existing unit test;
// see s3_end_scan_test.go) since sawEnd deliberately never gates fetching in any
// mode. inFlight and activeKeys are shared with the per-key relaxed workers
// (see newS3RelaxedWorker): inFlight is decremented there on receipt, and
// activeKeys lets a worker check whether every channel/framing it knows about
// has also finished, without needing direct access to another key's state.
func spawnS3FetchersRelaxed(
	ctx context.Context,
	cfg Config,
	s3c *s3.Client,
	route func(fetchResult),
	nextFetch map[channelFramingKey]int, // scheduler-local
	inFlight *sharedCounter[channelFramingKey],
	activeKeys *keyedSet[channelFramingKey],
	credsLogged *sync.Map,
) {
	// Try each channel's framing combinations. Framings are per-channel (from the
	// authorizer metadata) so we don't probe framings a channel never emits.
	for _, chID := range cfg.ChannelIDs {
		for _, framing := range framingsToTryForChannel(cfg, chID) {
			key := channelFramingKey{ChannelID: chID, Framing: framing}
			if inFlight.get(key) >= cfg.WindowSize {
				continue
			}
			// Initialize nextFetch to 1 if not set (indices start at 1)
			if nextFetch[key] <= 0 {
				nextFetch[key] = 1
			}
			spawnS3Fetcher(
				ctx,
				s3c,
				cfg.Bucket,
				cfg.Prefix,
				chID,
				nextFetch[key],
				framing,
				cfg.S3PollInterval,
				route,
				inFlight,
				credsLogged,
			)
			nextFetch[key]++
			activeKeys.add(key)
		}
	}
}

func checkAllChannelFramingsDoneRelaxed(
	sawEnd map[channelFramingKey]bool,
	inFlight map[channelFramingKey]int,
	activeKeys map[channelFramingKey]bool,
) bool {
	for key := range activeKeys {
		if !sawEnd[key] || inFlight[key] > 0 {
			return false
		}
	}
	return true
}

// allKeysSawEndAndIdle reports whether every key currently in activeKeys has
// both seen its own END and has zero in-flight fetches: the same, live-checked
// condition checkAllChannelFramingsDoneRelaxed evaluates from a snapshot. It is
// re-evaluated on demand (never cached) because inFlight can legitimately go
// back above zero for a key even after that key has seen its own END (an END
// must never stop fetching, in any mode), so "done" here is not a permanent,
// one-way state transition.
func allKeysSawEndAndIdle(
	activeKeys *keyedSet[channelFramingKey],
	sawEndSet *keyedSet[channelFramingKey],
	inFlight *sharedCounter[channelFramingKey],
) bool {
	for k := range activeKeys.snapshot() {
		if !sawEndSet.has(k) || inFlight.get(k) > 0 {
			return false
		}
	}
	return true
}

// newS3RelaxedWorker returns a per-key (channel+framing) worker function for
// keyedResultRouter that writes one high-rate channel/framing's chunks to disk
// as they arrive (not necessarily in order), with auto-close/grace-period
// handling. Each key owns its own local sawEnd/endIndex/receivedIndices (never
// shared), so channels/framings never contend with each other over them; the
// few cross-key signals below (inFlight, activeKeys, sawEndSet, allDone) are
// cheap, I/O-free, and only used to answer "has everything else finished too".
//
//nolint:gocognit,gocyclo,cyclop,funlen // Complex state machine for relaxed-order S3 downloads with auto-close logic
func newS3RelaxedWorker(
	cfg Config,
	stats *statsTracker,
	client mqtt.Client,
	ackSender *ackSender,
	inFlight *sharedCounter[channelFramingKey],
	activeKeys *keyedSet[channelFramingKey],
	sawEndSet *keyedSet[channelFramingKey],
	allDone *keyedDoneSet[channelFramingKey],
	signalAllIdle func(),
) func(ctx context.Context, key channelFramingKey, in <-chan fetchResult) error {
	return func(ctx context.Context, key channelFramingKey, in <-chan fetchResult) error {
		sawEnd := false
		endIndex := 0
		receivedIndices := make(map[int]bool) // index -> bool (for completeness check)
		telemetryChannelID := key.ChannelID + "/downlink"
		// Grace period for auto-close: wait for missing messages after END is detected
		var autoCloseTimer *time.Timer
		defer func() {
			if autoCloseTimer != nil {
				autoCloseTimer.Stop()
			}
		}()

		checkAndAutoClose := func() {
			if !cfg.EnableAutoClose || !sawEnd {
				return
			}
			if inFlight.get(key) > 0 {
				// Still have in-flight requests, wait for them
				return
			}

			// Check if all messages up to END have been received
			endIdx := endIndex
			allReceived := true
			for i := 1; i <= endIdx; i++ {
				if !receivedIndices[i] {
					allReceived = false
					break
				}
			}

			if allReceived {
				// All messages received for this key; check if every other active key
				// is also fully complete.
				allDone.markDone(key)
				if allDone.allDoneAmong(activeKeys.snapshot()) {
					log.Printf(
						"Stream auto-close: all channels completed with all messages received, exiting...",
					)
					exitAfterTeardown()
				}
			} else {
				// Not all messages received yet, start/restart grace period timer
				if autoCloseTimer != nil {
					autoCloseTimer.Stop()
				}
				autoCloseTimer = time.AfterFunc(autoCloseGracePeriod, func() {
					// Grace period expired, check again
					allReceivedNow := true
					for i := 1; i <= endIdx; i++ {
						if !receivedIndices[i] {
							allReceivedNow = false
							break
						}
					}
					if allReceivedNow {
						allDone.markDone(key)
					}
					if allReceivedNow && allDone.allDoneAmong(activeKeys.snapshot()) {
						log.Printf("Stream auto-close: all channels completed (after grace period), exiting...")
						exitAfterTeardown()
					} else if !allReceivedNow {
						log.Printf(
							"Stream auto-close: grace period expired but missing messages for channel %s framing %s (indices 1-%d), exiting anyway...",
							key.ChannelID,
							key.Framing,
							endIdx,
						)
						exitAfterTeardown()
					}
				})
			}
		}

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()

			case res, ok := <-in:
				if !ok {
					return nil
				}
				if res.Framing != "" {
					inFlight.dec(key)
				}

				if res.Err != nil {
					if errors.Is(res.Err, context.Canceled) {
						emitLine("")
						vlogf("Context cancelled; exiting.")
						return nil
					}
					if isNotFound(res.Err) {
						// 404 is expected for framing types that don't exist, so we silently skip
						if sawEnd && allKeysSawEndAndIdle(activeKeys, sawEndSet, inFlight) {
							signalAllIdle()
						}
						continue
					}
					return fmt.Errorf(
						"fetch index %d (channel %s, framing %s): %w",
						res.Index, res.ChannelID, res.Framing, res.Err,
					)
				}

				if res.Framing == "" {
					// Skip results without framing (shouldn't happen, but be safe)
					continue
				}

				// Track received indices for completeness checking
				receivedIndices[res.Index] = true

				stats.AddChannelDownload(telemetryChannelID, key.Framing, int64(len(res.Data)), SourceS3, res.Index)

				if sawEnd && res.Index > endIndex {
					stats.SetChannelNextIndex(telemetryChannelID, endIndex+1)
					if allKeysSawEndAndIdle(activeKeys, sawEndSet, inFlight) {
						signalAllIdle()
					}
					continue
				}

				if err := writeChunkToFile(
					res.Data,
					cfg.DestDir,
					cfg.Prefix,
					cfg.PassID,
					res.Index,
					res.ChannelID,
					res.Framing,
					"high-rate",
					stats,
					cfg.WriteInOrder,
					cfg,
				); err != nil {
					stats.AddError(
						fmt.Errorf(
							"write index %d (channel %s, framing %s): %w",
							res.Index, key.ChannelID, key.Framing, err,
						),
					)
					return fmt.Errorf(
						"write index %d (channel %s, framing %s): %w",
						res.Index, key.ChannelID, key.Framing, err,
					)
				}

				stats.AddChannelWrite(telemetryChannelID, int64(len(res.Data)))
				// Send ack for high-rate S3 message via MQTT
				sendHighRateS3Ack(cfg, client, ackSender, res.ChannelID, res.Framing, res.Index, len(res.Data), res.MessageID)

				nextIdxForDisplay := res.Index + 1
				if sawEnd {
					nextIdxForDisplay = endIndex + 1
				}
				stats.SetChannelNextIndex(telemetryChannelID, nextIdxForDisplay)

				if isEndMessage(res.Metadata) {
					sawEnd = true
					endIndex = res.Index
					sawEndSet.add(key)
					emitLine("")
					// Auto-close: Check for zero-size data as stream end indicator
					if cfg.EnableAutoClose && len(res.Data) == 0 {
						log.Printf("Stream auto-close: zero-size END message detected, exiting...")
						closeAuthorizerStream(cfg)
						exitAfterTeardown()
					}
					checkAndAutoClose()
				} else if sawEnd {
					checkAndAutoClose()
				}

				if sawEnd && allKeysSawEndAndIdle(activeKeys, sawEndSet, inFlight) {
					signalAllIdle()
				}
			}
		}
	}
}

// runS3Relaxed streams high-rate S3 downloads for every channel/framing,
// writing each as it arrives (not necessarily in order). A dedicated scheduler
// goroutine spawns fetches respecting the per-key in-flight window; each key's
// results are processed by its own worker goroutine (via keyedResultRouter), so
// one channel's disk speed or backlog can never delay another's fetching or
// writing.
func runS3Relaxed(
	ctx context.Context,
	cfg Config,
	s3c *s3.Client,
	stats *statsTracker,
	client mqtt.Client,
	ackSender *ackSender,
) error {
	vlogf(
		"Starting S3 downloads (relaxed order) from bucket: %s, prefix: %s, channels: %v, window size: %d",
		cfg.Bucket,
		cfg.Prefix,
		cfg.ChannelIDs,
		cfg.WindowSize,
	)
	vlogf(
		"S3 key pattern: <pass-id>/<channel-id>/<framing>/<index> (trying all framing types: %v)",
		getAllFramingTypes(),
	)

	s3ResultsChBuf := cfg.WindowSize * 4
	inFlight := newSharedCounter[channelFramingKey]()
	activeKeys := newKeyedSet[channelFramingKey]()
	sawEndSet := newKeyedSet[channelFramingKey]()
	allDone := newKeyedDoneSet[channelFramingKey]()

	var idleOnce sync.Once
	idleCh := make(chan struct{})
	signalAllIdle := func() { idleOnce.Do(func() { close(idleCh) }) }

	worker := newS3RelaxedWorker(cfg, stats, client, ackSender, inFlight, activeKeys, sawEndSet, allDone, signalAllIdle)
	router := newKeyedResultRouter(ctx, s3ResultsChBuf, worker)
	route := func(res fetchResult) {
		router.Route(channelFramingKey{ChannelID: res.ChannelID, Framing: res.Framing}, res)
	}

	nextFetch := make(map[channelFramingKey]int)
	var credsLogged sync.Map

	schedCtx, cancelSched := context.WithCancel(ctx)
	defer cancelSched()
	go func() {
		ticker := time.NewTicker(s3SchedulerTick)
		defer ticker.Stop()
		for {
			spawnS3FetchersRelaxed(schedCtx, cfg, s3c, route, nextFetch, inFlight, activeKeys, &credsLogged)
			select {
			case <-schedCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		errCh <- router.Wait()
	}()

	select {
	case <-idleCh:
		emitLine("")
		vlogf("All channels completed.")
		return nil
	case err := <-errCh:
		return err
	}
}

// getAllFramingTypes returns the list of all valid framing types to try when fetching from S3.
func getAllFramingTypes() []string {
	return []string{
		"BITSTREAM",
		"IQ",
		"AX25",
		"IMAGE_PNG",
		"IMAGE_JPEG",
		"FREE_TEXT_UTF8",
		"WATERFALL",
	}
}

// framingsToTryForChannel returns the framing types the high-rate S3 reader should
// probe for a channel. It prefers the channel's actual framings (from the
// authorizer metadata), else all known framings; then, when --accepted-framing is
// set, it keeps only the accepted ones. This avoids GetObject-ing framings a
// channel never produces (each of which 403s when the object is absent and the
// role lacks ListBucket).
func framingsToTryForChannel(cfg Config, channelID string) []string {
	base := cfg.ChannelFramings[channelID]
	if len(base) == 0 {
		base = getAllFramingTypes()
	}
	if len(cfg.AcceptedFraming) == 0 {
		return base
	}
	accepted := make(map[string]struct{}, len(cfg.AcceptedFraming))
	for _, f := range cfg.AcceptedFraming {
		accepted[strings.ToUpper(strings.TrimSpace(f))] = struct{}{}
	}
	var out []string
	for _, f := range base {
		if _, ok := accepted[strings.ToUpper(strings.TrimSpace(f))]; ok {
			out = append(out, f)
		}
	}
	return out
}

// fetchIndexForFraming attempts GetObject for a specific framing type, index, and channel.
// If the object is not found (404), it polls until found or context cancelled.
func fetchIndexForFraming(
	ctx context.Context,
	s3c *s3.Client,
	bucket, prefix string,
	channelID string,
	index int,
	framing string,
	pollInterval time.Duration,
	credsLogged *sync.Map,
) fetchResult {
	// Log credentials once per worker goroutine (using worker ID as key)
	// This helps detect if workers are using different credentials than initialization
	workerID := fmt.Sprintf("worker-ch%s-framing-%s", channelID, framing)
	if _, logged := credsLogged.LoadOrStore(workerID, true); !logged {
		// This is the first time this worker is running
		// Make a lightweight ListObjectsV2 call with MaxKeys=0 to validate credentials
		// This is lighter than HeadBucket and verifies both credentials and ListBucket permission
		validationPrefix := prefix
		if !strings.HasSuffix(validationPrefix, "/") {
			validationPrefix += "/"
		}
		_, err := s3c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:  aws.String(bucket),
			Prefix:  aws.String(validationPrefix),
			MaxKeys: aws.Int32(0), // Just validate access, don't list objects
		})

		if err != nil {
			// Extract AWS error code for diagnostics
			var awsErr interface {
				Error() string
				ErrorCode() string
				ErrorMessage() string
			}
			switch {
			case isTransientS3AuthError(err):
				// Credentials are still propagating; the fetch below retries. Keep this
				// diagnostic detail to --verbose so it does not alarm the operator.
				vlogf("Worker [%s]: transient S3 credential error, retrying - %v", workerID, err)
			case errors.As(err, &awsErr):
				log.Printf("Worker [%s]: S3 credential validation failed - %s: %s",
					workerID, awsErr.ErrorCode(), awsErr.ErrorMessage())
			default:
				log.Printf("Worker [%s]: S3 credential validation error: %v", workerID, err)
			}
		} else {
			// Validation succeeded - credentials are valid and ListBucket permission works
			vlogf("Worker [%s]: S3 credentials validated (ListObjectsV2 succeeded)", workerID)
		}
	}

	// Construct S3 key based on prefix format:
	// - High-rate: <passID>/<channelID>/<framing>/<index> (prefix is just <passID>/)
	// - Low-rate: <passID>/channel/<channelID>/downlink/<framing>/<index> (prefix contains /channel/ or /downlink/)
	// Extract passID from prefix (remove trailing slash and any environment prefix)
	passID := strings.TrimSuffix(prefix, "/")
	if lastSlash := strings.LastIndex(passID, "/"); lastSlash != -1 {
		// If prefix was <env>/<pass-id>/, extract just <pass-id>
		passID = passID[lastSlash+1:]
	}

	// Detect high-rate mode: prefix doesn't contain /channel/ or /downlink/
	// High-rate prefix is just <passID>/, low-rate prefix contains /channel/ or /downlink/
	isHighRate := !strings.Contains(prefix, "/channel/") && !strings.Contains(prefix, "/downlink/")

	var key string
	if isHighRate {
		// High-rate format: <passID>/<channelID>/<framing>/<index>
		key = fmt.Sprintf("%s/%s/%s/%d", passID, channelID, framing, index)
	} else {
		// Low-rate format: <passID>/channel/<channelID>/downlink/<framing>/<index>
		key = fmt.Sprintf("%s/channel/%s/downlink/%s/%d", passID, channelID, framing, index)
	}
	for {
		out, err := s3c.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err == nil {
			return processS3ObjectSuccess(out, channelID, index, key)
		}

		if ctx.Err() != nil {
			return fetchResult{
				Index:     index,
				ChannelID: channelID, // Preserve channel ID in error case
				Err:       ctx.Err(),
			}
		}

		// 404, NoSuchKey or NotFound: treat as "not uploaded yet, retry".
		if isNotFound(err) {
			// Intentionally quiet: we poll frequently and don't want spam.
			select {
			case <-ctx.Done():
				return fetchResult{
					Index:     index,
					ChannelID: channelID, // Preserve channel ID in error case
					Err:       ctx.Err(),
				}
			case <-time.After(pollInterval):
				continue
			}
		}

		// Any other error is real.
		return fetchResult{
			Index:     index,
			ChannelID: channelID, // Preserve channel ID in error case
			Err:       fmt.Errorf("GetObject %s: %w", key, err),
		}
	}
}

// extractFramingFromS3Key extracts the framing type from an S3 key path.
//
// Supports two formats:
//   - High-rate: <pass-id>/<channel-id>/<framing>/<index>
//   - Low-rate: <pass-id>/channel/<channel-id>/downlink/<framing>/<index>
//
// Parameters:
//   - key: The S3 key path to parse
//
// Returns:
//   - The framing type if found and valid, empty string otherwise
func extractFramingFromS3Key(key string) string {
	parts := strings.Split(key, "/")
	if len(parts) >= 3 {
		// Framing is always the second-to-last part (before index)
		// Works for both formats:
		// - High-rate: [passID, channelID, framing, index] -> parts[len-2] = framing
		// - Low-rate: [passID, channel, channelID, downlink, framing, index] -> parts[len-2] = framing
		framing := strings.ToUpper(parts[len(parts)-2])
		for _, validFraming := range getAllFramingTypes() {
			if framing == validFraming {
				return framing
			}
		}
	}
	return ""
}

// processS3ObjectSuccess processes a successfully fetched S3 object
func processS3ObjectSuccess(
	out *s3.GetObjectOutput,
	channelID string,
	index int,
	s3Key string,
) fetchResult {
	defer out.Body.Close()

	var buf bytes.Buffer
	if out.ContentLength != nil && *out.ContentLength > 0 {
		buf.Grow(int(*out.ContentLength))
	}
	copyBuf := make([]byte, s3DownloadCopyBufSize)
	if _, readErr := io.CopyBuffer(&buf, out.Body, copyBuf); readErr != nil {
		return fetchResult{
			Index:     index,
			ChannelID: channelID, // Preserve channel ID in error case
			Err:       fmt.Errorf("read body: %w", readErr),
		}
	}
	raw := buf.Bytes()

	// High-rate data in S3 is stored as raw telemetry bytes, not protobuf messages.
	// Try to parse as protobuf first, but if that fails, treat it as raw bytes.
	var payload []byte
	var messageID string
	msg := &streaming.FromStarPassMessage{}
	if err := proto.Unmarshal(raw, msg); err == nil {
		// Successfully parsed as protobuf - extract telemetry data
		messageID = msg.GetMessageId()
		sendTelemetry := msg.GetSendTelemetryMessage()
		if sendTelemetry != nil {
			for _, t := range sendTelemetry.GetTelemetry() {
				if d := t.GetData(); len(d) > 0 {
					payload = append(payload, d...)
				}
			}
		}
	} else {
		// Not protobuf - treat as raw telemetry bytes (high-rate format)
		payload = raw
	}

	// Extract framing type from S3 key path
	framing := extractFramingFromS3Key(s3Key)

	// Use the channel ID from the S3 path (it's always in the path for high-rate)
	// Note: for END or non-telemetry messages, payload may be empty.
	// END detection is still done via S3 metadata (isEndMessage).
	return fetchResult{
		Index:     index,
		ChannelID: channelID, // Always use the channel ID from the S3 path
		Framing:   framing,   // Framing type extracted from S3 key
		Data:      payload,
		Metadata:  out.Metadata,
		Err:       nil,
		Source:    SourceS3,
		MessageID: messageID,
	}
}

// sendHighRateS3Ack sends an ack for a high-rate S3 message via MQTT.
// ackedMessageID is empty for objects stored as raw telemetry bytes, which
// carry no message ID.
func sendHighRateS3Ack(
	cfg Config,
	client mqtt.Client,
	ackSender *ackSender,
	channelID, framing string,
	index, dataLen int,
	ackedMessageID string,
) {
	if client != nil && ackSender != nil && client.IsConnected() {
		// High-rate S3 key format: <passID>/<channelID>/<framing>/<index>
		// But topic format is: <env>/pass/<passID>/channel/<channelID>/downlink/<framing>
		// So we build the topic directly instead of parsing from S3 key
		var base string
		if cfg.Environment != "" {
			base = fmt.Sprintf("%s/pass/%s", cfg.Environment, cfg.PassID)
		} else {
			base = fmt.Sprintf("pass/%s", cfg.PassID)
		}
		topic := fmt.Sprintf("%s/channel/%s/downlink/%s", base, channelID, framing)
		if topic != "" {
			ackPayload, err := buildTelemetryAck(
				cfg, channelID, framing, index, dataLen, time.Now(), true, ackedMessageID,
			)
			if err == nil {
				if cfg.EnableDebug {
					ackTopic := buildAckTopic(topic)
					log.Printf(
						"DEBUG: Sending high-rate ack: channel=%s framing=%s index=%d topic=%s ackTopic=%s",
						channelID, framing, index, topic, ackTopic,
					)
				}
				ackSender.sendAck(client, topic, ackPayload)
			} else {
				log.Printf(
					"ERROR: Failed to build ack JSON for high-rate (channel %s, index %d): %v",
					channelID, index, err,
				)
			}
		} else {
			log.Printf(
				"WARNING: Could not build high-rate topic (channel: %s, framing: %s, index: %d)",
				channelID, framing, index,
			)
		}
	} else {
		// Always log these warnings - they indicate acks won't be sent
		switch {
		case client == nil:
			log.Printf(
				"WARNING: MQTT client is nil, cannot send high-rate ack (channel %s, index %d)",
				channelID, index,
			)
		case ackSender == nil:
			log.Printf(
				"WARNING: ackSender is nil, cannot send high-rate ack (channel %s, index %d)",
				channelID, index,
			)
		case !client.IsConnected():
			log.Printf(
				"WARNING: MQTT client not connected, cannot send high-rate ack (channel %s, index %d)",
				channelID, index,
			)
		}
	}
}

// isNotFound returns true if the error is a "key does not exist" type.
func isNotFound(err error) bool {
	var noSuchKey *s3types.NoSuchKey
	if errors.As(err, &noSuchKey) {
		return true
	}
	var notFound *s3types.NotFound
	return errors.As(err, &notFound)
}

// isTransientS3AuthError reports whether err is a transient S3 authorization
// failure of the kind seen briefly while rotated credentials propagate
// (InvalidAccessKeyId / AccessDenied / expired token). These are expected during
// a pass (the affected fetch is retried on the next poll), so callers demote them
// to verbose-only instead of alarming the operator with a wall of 403s.
func isTransientS3AuthError(err error) bool {
	if err == nil {
		return false
	}
	var awsErr interface{ ErrorCode() string }
	if !errors.As(err, &awsErr) {
		return false
	}
	switch awsErr.ErrorCode() {
	case "InvalidAccessKeyId", "AccessDenied", "ExpiredToken", "RequestExpired", "TokenRefreshRequired":
		return true
	default:
		return false
	}
}
