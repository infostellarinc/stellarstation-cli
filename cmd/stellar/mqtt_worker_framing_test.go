// Copyright (c) 2026 InfoStellar, Inc. All Rights Reserved.

package main

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// Telemetry indexes are scoped per (pass, channel, framing): a multi-framing
// channel carries independent sequences that both start at 1. These tests pin
// the worker partitioning that keeps those sequences from sharing one cursor,
// pending map, and END state; the failure mode being one framing's messages
// stuck behind the other's cursor, overwritten in the shared pending map, or
// discarded after the other framing's END.
func TestInOrderWorkerKeepsFramingSequencesIndependent(t *testing.T) {
	cfg := Config{
		DestDir:    t.TempDir(),
		PassID:     testWritePassID,
		ChannelIDs: []string{testWriteChannelID},
	}
	cfg.WriteInOrder = true
	stats := newStatsTracker(false)
	allDone := newKeyedDoneSet[channelFramingKey]()
	activeKeys := newKeyedSet[channelFramingKey]()
	finishedOnce := &sync.Once{}
	worker := newMQTTInOrderWorker(cfg, stats, allDone, activeKeys, nil, finishedOnce)

	bitstreamKey := channelFramingKey{ChannelID: testWriteChannelID + "/downlink", Framing: "BITSTREAM"}
	iqKey := channelFramingKey{ChannelID: testWriteChannelID + "/downlink", Framing: "IQ"}
	activeKeys.add(bitstreamKey)
	activeKeys.add(iqKey)

	result := func(framing string, index int, end bool) fetchResult {
		res := fetchResult{
			Index:     index,
			ChannelID: testWriteChannelID + "/downlink",
			Framing:   framing,
			Data:      []byte("payload-" + framing),
			Source:    SourceMQTT,
		}
		if end {
			res.Data = []byte{}
			res.Metadata = map[string]string{"messagetype": "END"}
		}
		return res
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Two independent workers, exactly as the router would create them: one
	// per (channel, framing) key. Both framings use the same indexes 1..3.
	bitstreamCh := make(chan fetchResult, 8)
	iqCh := make(chan fetchResult, 8)
	var wg sync.WaitGroup
	wg.Add(2)
	var bitstreamErr, iqErr error
	go func() { defer wg.Done(); bitstreamErr = worker(ctx, bitstreamKey, bitstreamCh) }()
	go func() { defer wg.Done(); iqErr = worker(ctx, iqKey, iqCh) }()

	// Interleave: BITSTREAM ends while IQ is still mid-stream.
	bitstreamCh <- result("BITSTREAM", 1, false)
	iqCh <- result("IQ", 1, false)
	bitstreamCh <- result("BITSTREAM", 2, false)
	bitstreamCh <- result("BITSTREAM", 3, true)

	// Wait for the BITSTREAM worker to process its END, then confirm the
	// still-active IQ sequence blocks completion.
	deadline := time.Now().Add(5 * time.Second)
	for allDone.countDone() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("BITSTREAM sequence never completed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if mqttAllSequencesFinished(allDone, activeKeys, nil, len(cfg.ChannelIDs)) {
		t.Fatal("completion fired while the IQ sequence was still active")
	}

	// IQ continues past BITSTREAM's END and completes on its own END.
	iqCh <- result("IQ", 2, false)
	iqCh <- result("IQ", 3, true)
	for allDone.countDone() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("IQ sequence never completed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !mqttAllSequencesFinished(allDone, activeKeys, nil, len(cfg.ChannelIDs)) {
		t.Fatal("completion did not fire after both sequences ended")
	}

	close(bitstreamCh)
	close(iqCh)
	wg.Wait()
	if bitstreamErr != nil || iqErr != nil {
		t.Fatalf("worker errors: bitstream=%v iq=%v", bitstreamErr, iqErr)
	}

	// Both framings' data messages must be on disk: indexes 1-2 for each
	// framing (END markers are empty). With the old channel-keyed worker, IQ
	// index 1 arrived behind the shared cursor and was never written.
	written := 0
	err := filepath.Walk(cfg.DestDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && info.Size() > 0 {
			written++
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if written != 4 {
		t.Fatalf("wrote %d non-empty chunk files, want 4 (two per framing)", written)
	}
}

// mqttAllSequencesFinished requires every configured channel to be covered:
// one channel's sequences finishing must not complete a stream whose other
// configured channel has not produced anything yet.
func TestMQTTAllSequencesFinishedRequiresChannelCoverage(t *testing.T) {
	allDone := newKeyedDoneSet[channelFramingKey]()
	activeKeys := newKeyedSet[channelFramingKey]()

	key1 := channelFramingKey{ChannelID: testWriteChannelID + "/downlink", Framing: "BITSTREAM"}
	activeKeys.add(key1)
	allDone.markDone(key1)

	if mqttAllSequencesFinished(allDone, activeKeys, nil, 2) {
		t.Fatal("completion fired with only one of two configured channels seen")
	}
	key2 := channelFramingKey{ChannelID: "00000000-0000-0000-0000-000000000003/downlink", Framing: "IQ"}
	activeKeys.add(key2)
	if mqttAllSequencesFinished(allDone, activeKeys, nil, 2) {
		t.Fatal("completion fired while a discovered sequence was unfinished")
	}
	allDone.markDone(key2)
	if !mqttAllSequencesFinished(allDone, activeKeys, nil, 2) {
		t.Fatal("completion did not fire with all discovered sequences done")
	}
}

// With the authorizer declaring both framings of a channel, one framing
// reaching END must not complete the pass while the other has yet to deliver
// its first message: auto-close there would exit before the second sequence
// started and lose it entirely.
func TestMQTTAllSequencesFinishedWaitsForDeclaredFramings(t *testing.T) {
	allDone := newKeyedDoneSet[channelFramingKey]()
	activeKeys := newKeyedSet[channelFramingKey]()
	bitstream := channelFramingKey{ChannelID: testWriteChannelID + "/downlink", Framing: "BITSTREAM"}
	iq := channelFramingKey{ChannelID: testWriteChannelID + "/downlink", Framing: "IQ"}
	expected := map[channelFramingKey]bool{bitstream: true, iq: true}

	// Only BITSTREAM has been seen, and it has finished. IQ has not started.
	activeKeys.add(bitstream)
	allDone.markDone(bitstream)
	if mqttAllSequencesFinished(allDone, activeKeys, expected, 1) {
		t.Fatal("completion fired before the declared IQ sequence produced anything")
	}
	// Without the declared set there is nothing to wait for, so the
	// seen-sequences fallback does complete here.
	if !mqttAllSequencesFinished(allDone, activeKeys, nil, 1) {
		t.Fatal("fallback should complete once every seen sequence is done")
	}

	allDone.markDone(iq)
	if !mqttAllSequencesFinished(allDone, activeKeys, expected, 1) {
		t.Fatal("completion did not fire after both declared sequences finished")
	}
}

// A channel declared as uplink or high-rate produces no MQTT telemetry
// sequence, so it must not be part of the expected set.
func TestExpectedTelemetryKeysSkipsUplinkAndHighRate(t *testing.T) {
	cfg := Config{AuthorizerCreds: &AuthorizerCredentials{Channels: []ChannelMetadata{
		{ChannelID: "dl", Direction: directionDownlink, RateClass: rateClassLowRate, Framings: []string{"BITSTREAM", "IQ"}},
		{ChannelID: "ul", Direction: directionUplink, Framings: []string{"BITSTREAM"}},
		{ChannelID: "hr", Direction: directionDownlink, RateClass: rateClassHighRate, Framings: []string{"IQ"}},
	}}}
	got := expectedTelemetryKeys(cfg)
	if len(got) != 2 {
		t.Fatalf("expected 2 keys for the low-rate downlink channel, got %d: %v", len(got), got)
	}
	for _, fr := range []string{"BITSTREAM", "IQ"} {
		if !got[channelFramingKey{ChannelID: "dl/downlink", Framing: fr}] {
			t.Errorf("missing expected key for framing %s: %v", fr, got)
		}
	}
}
