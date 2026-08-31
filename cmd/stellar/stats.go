package main

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// MessageSource indicates where a message came from
type MessageSource string

const (
	SourceMQTT MessageSource = "MQTT"
	SourceS3   MessageSource = "S3"
)

// framingCount tracks telemetry counts for one framing type on a channel.
type framingCount struct {
	files int
	bytes int64
}

// displayChannel names a channel to show in the live dashboard and its rate class.
type displayChannel struct {
	channelID string
	rateClass string // "low-rate" | "high-rate"
}

// channelStats tracks per-channel telemetry statistics
type channelStats struct {
	channelID       string
	downloadedFiles int
	downloadedBytes int64
	writtenFiles    int
	writtenBytes    int64
	nextIndex       int // Next expected index for this channel
	lastIndex       int // Last received index
	// Source split: MQTT is real-time low-rate; S3 is high-rate direct or a
	// low-rate catch-up/backup fetch. Surfacing the split shows how much of a
	// low-rate channel arrived live vs. was recovered from S3.
	mqttFiles int
	mqttBytes int64
	s3Files   int
	s3Bytes   int64
	// Per-framing telemetry breakdown for this channel (a channel may carry
	// several framings, e.g. BITSTREAM + IQ).
	framings map[string]*framingCount
}

// messageTypeStats tracks statistics for non-telemetry message types
type messageTypeStats struct {
	mqttCount int
	s3Count   int
	lastIndex int64 // Last received index/timestamp
}

// statsTracker tracks download/write stats and prints progress.
type statsTracker struct {
	start time.Time

	// Track first and last byte timestamps for accurate bitrate calculation
	firstByteTime *time.Time // Time when first byte was received/written
	lastByteTime  *time.Time // Time when last byte was received/written
	byteTimeMu    sync.RWMutex

	// Per-channel telemetry stats
	channelStats map[string]*channelStats
	channelMu    sync.RWMutex

	// displayChannels is the expected downlink channel set (with rate class) so
	// the live dashboard shows one row per channel from the start, before any
	// data has arrived. Set once via SetDisplayChannels.
	displayChannels []displayChannel

	// channelEnded marks (by bare channel id) channels that have received an
	// end-of-data (END) telemetry message, so the dashboard can flag them.
	channelEnded map[string]bool

	// Non-telemetry message stats
	monitoringStats messageTypeStats
	configStats     messageTypeStats
	eventStats      messageTypeStats
	messageMu       sync.RWMutex

	// Aggregated stats
	mqttDownloadedFiles int
	mqttDownloadedBytes int64
	s3DownloadedFiles   int
	s3DownloadedBytes   int64
	writtenFiles        int
	writtenBytes        int64

	// Ack counts
	acksReceived  int
	nacksReceived int
	acksSent      int

	// Track received messages that should be acked (but we don't currently send acks)
	receivedMessages []*receivedMessage
	// Track sent commands waiting for acks
	sentCommands []*sentCommand
	mu           sync.Mutex

	lastLogTime time.Time

	// Diagnostics tracking
	diagnosticsEnabled bool
	clientID           string // Resolved MQTT client ID (set after connection)
	clientIDMu         sync.RWMutex
	errors             []string // List of errors encountered
	errorsMu           sync.Mutex
	// Per-channel checksums (for ordered telemetry data)
	channelChecksums map[string]*channelChecksum
	checksumMu       sync.Mutex

	// announced backs the one-time "first data received" notices.
	announced  map[string]bool
	announceMu sync.Mutex

	// Advanced statistics (for --stats flag)
	enableStats           bool
	passID                string
	streamID              string
	firstByteTimeProto    *time.Time // First byte time from protobuf (datatake timestamp)
	lastByteTimeProto     *time.Time // Last byte time from protobuf (datatake timestamp)
	firstByteTimeLocal    *time.Time // First byte time locally received
	lastByteTimeLocal     *time.Time // Last byte time locally received
	totalDelayNanos       int64      // Sum of delays for all messages
	totalBytesReceived    int64      // Total bytes received (for pass summary)
	totalMessagesReceived int64      // Total messages received (for pass summary)
	messageBuffer         []telemetryWithTimestamp
	messageBufferMu       sync.Mutex

	// activity monotonically increments on every message sent or received (of any
	// kind: telemetry, monitoring, config, event, and acks/commands in either
	// direction). The idle-shutdown monitor samples it to detect a quiet stream;
	// see startIdleShutdownMonitor. Atomic so it is safe to read without a lock.
	activity atomic.Int64
}

// markActivity records that a message was just sent or received. Cheap and
// lock-free; called from every send/receive path.
func (s *statsTracker) markActivity() { s.activity.Add(1) }

// ActivityCount returns the running count of messages sent or received. The
// idle-shutdown monitor compares successive samples: an unchanged count means
// the stream has been quiet since the previous sample.
func (s *statsTracker) ActivityCount() int64 { return s.activity.Load() }

// channelChecksum tracks checksum calculation for a channel's ordered telemetry data
type channelChecksum struct {
	channelID string
	hash      hash.Hash
	bytes     int64 // Total bytes hashed
}

// telemetryWithTimestamp tracks telemetry data with timestamps for instantaneous stats
type telemetryWithTimestamp struct {
	receivedTime         time.Time
	dataBytes            int
	timeLastByteReceived *time.Time // From protobuf if available
}

// receivedMessage tracks a message that was received and should be acked
type receivedMessage struct {
	topic     string
	msgType   string // "config", "monitoring", "event", "telemetry"
	index     int    // for telemetry
	channelID string // for telemetry
	timestamp int64
	acked     bool
}

// sentCommand tracks a command that was sent and is waiting for an ack
type sentCommand struct {
	topic     string
	cmdType   string // "uplink", "config_request"
	index     uint32
	timestamp int64
	acked     bool
	rejected  bool
}

func newStatsTracker(enableDiagnostics bool) *statsTracker {
	now := time.Now()
	return &statsTracker{
		start:              now,
		channelStats:       make(map[string]*channelStats),
		channelEnded:       make(map[string]bool),
		lastLogTime:        now,
		diagnosticsEnabled: enableDiagnostics,
		channelChecksums:   make(map[string]*channelChecksum),
		messageBuffer:      make([]telemetryWithTimestamp, 0),
		announced:          make(map[string]bool),
	}
}

// downlinkChannelBase reports whether the given channel key represents downlink
// telemetry and, if so, returns the bare channel UUID with any message-type
// suffix (e.g. "/downlink") stripped. Non-telemetry keys return ("", false).
func downlinkChannelBase(channelID string) (string, bool) {
	if channelID == "" {
		return "", false
	}
	if i := strings.Index(channelID, "/"); i >= 0 {
		if strings.HasSuffix(channelID, "/downlink") {
			return channelID[:i], true
		}
		return "", false
	}
	// No suffix: treat as a bare telemetry channel.
	return channelID, true
}

// announceOnce returns true the first time it is called with a given key, and
// false thereafter. It backs the one-time "first data received" notices so an
// operator sees each data type light up exactly once as it starts flowing.
func (s *statsTracker) announceOnce(key string) bool {
	s.announceMu.Lock()
	defer s.announceMu.Unlock()
	if s.announced[key] {
		return false
	}
	s.announced[key] = true
	return true
}

// SetEnableStats enables or disables advanced statistics collection
func (s *statsTracker) SetEnableStats(enabled bool) {
	s.enableStats = enabled
}

// SetPassID sets the pass ID for statistics tracking.
func (s *statsTracker) SetPassID(passID string) {
	if s.passID != passID && s.passID != "" {
		// Pass changed, log report for the previous pass.
		s.logPassSummary()
	}
	s.passID = passID
}

// SetStreamID sets the stream ID for statistics tracking
func (s *statsTracker) SetStreamID(streamID string) {
	s.streamID = streamID
}

// channelBreakdownRow is a per-channel telemetry snapshot for the live dashboard
// and the summary table.
type channelBreakdownRow struct {
	channelID   string
	rateClass   string // "low-rate" | "high-rate" | "" when unknown
	files       int
	bytes       int64
	mqttFiles   int
	mqttBytes   int64
	s3Files     int
	s3Bytes     int64
	framings    map[string]framingCount
	endReceived bool // channel has received its end-of-data (END) message
}

// baseChannelID strips a telemetry-path suffix (e.g. "uuid/downlink") to the bare
// channel id used to key display state.
func baseChannelID(channelID string) string {
	if b, ok := downlinkChannelBase(channelID); ok {
		return b
	}
	return channelID
}

// MarkChannelEnd records that a channel has received its end-of-data (END)
// telemetry message. Accepts any channel-id form (bare or "/downlink").
func (s *statsTracker) MarkChannelEnd(channelID string) {
	base := baseChannelID(channelID)
	s.channelMu.Lock()
	s.channelEnded[base] = true
	s.channelMu.Unlock()
}

// SetDisplayChannels records the expected downlink channels (with rate class) so
// the live dashboard shows a row per channel from the start. low are the low-rate
// (MQTT) channel IDs; high are the high-rate (S3) channel IDs.
func (s *statsTracker) SetDisplayChannels(low, high []string) {
	s.channelMu.Lock()
	defer s.channelMu.Unlock()
	s.displayChannels = s.displayChannels[:0]
	for _, id := range low {
		s.displayChannels = append(s.displayChannels, displayChannel{channelID: id, rateClass: "low-rate"})
	}
	for _, id := range high {
		s.displayChannels = append(s.displayChannels, displayChannel{channelID: id, rateClass: "high-rate"})
	}
	sort.Slice(s.displayChannels, func(i, j int) bool {
		if s.displayChannels[i].rateClass != s.displayChannels[j].rateClass {
			return s.displayChannels[i].rateClass < s.displayChannels[j].rateClass
		}
		return s.displayChannels[i].channelID < s.displayChannels[j].channelID
	})
}

// channelBreakdown returns a per-channel telemetry snapshot (rate class,
// message/byte totals, MQTT-vs-S3 source split, and per-framing counts). When the
// expected channels have been registered (SetDisplayChannels), it returns one row
// per expected channel, including channels with no data yet, so the live
// dashboard is stable; otherwise it returns a row per channel seen so far.
func (s *statsTracker) channelBreakdown() []channelBreakdownRow {
	s.channelMu.RLock()
	defer s.channelMu.RUnlock()

	rowFor := func(base, rateClass string, ch *channelStats) channelBreakdownRow {
		row := channelBreakdownRow{channelID: base, rateClass: rateClass, endReceived: s.channelEnded[base]}
		if ch != nil {
			row.files = ch.downloadedFiles
			row.bytes = ch.downloadedBytes
			row.mqttFiles, row.mqttBytes = ch.mqttFiles, ch.mqttBytes
			row.s3Files, row.s3Bytes = ch.s3Files, ch.s3Bytes
			row.framings = make(map[string]framingCount, len(ch.framings))
			for f, fc := range ch.framings {
				row.framings[f] = *fc
			}
		}
		return row
	}

	if len(s.displayChannels) > 0 {
		rows := make([]channelBreakdownRow, 0, len(s.displayChannels))
		for _, dc := range s.displayChannels {
			rows = append(rows, rowFor(dc.channelID, dc.rateClass, s.channelStats[dc.channelID+"/downlink"]))
		}
		return rows
	}

	rows := make([]channelBreakdownRow, 0, len(s.channelStats))
	for id, ch := range s.channelStats {
		base := id
		if b, ok := downlinkChannelBase(id); ok {
			base = b
		}
		rows = append(rows, rowFor(base, "", ch))
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].channelID < rows[j].channelID })
	return rows
}

// shortID abbreviates a UUID-like channel id for compact table rows.
func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// formatFramings renders a channel's per-framing counts as "BITSTREAM 6/4.9 MiB, IQ 6/4.9 MiB".
func formatFramings(framings map[string]framingCount) string {
	if len(framings) == 0 {
		return "-"
	}
	keys := make([]string, 0, len(framings))
	for f := range framings {
		keys = append(keys, f)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, f := range keys {
		fc := framings[f]
		parts = append(parts, fmt.Sprintf("%s %d/%s", f, fc.files, humanReadableBytes(fc.bytes)))
	}
	return strings.Join(parts, ", ")
}

// formatSourceSplit renders the MQTT-vs-S3 source split for a channel row. MQTT is
// real-time low-rate; S3 is high-rate direct or a low-rate catch-up.
func (r channelBreakdownRow) formatSourceSplit() string {
	switch {
	case r.mqttFiles > 0 && r.s3Files > 0:
		return fmt.Sprintf("MQTT %d/%s + S3 %d/%s",
			r.mqttFiles, humanReadableBytes(r.mqttBytes), r.s3Files, humanReadableBytes(r.s3Bytes))
	case r.s3Files > 0:
		return fmt.Sprintf("S3 %d/%s", r.s3Files, humanReadableBytes(r.s3Bytes))
	case r.mqttFiles > 0:
		return fmt.Sprintf("MQTT %d/%s", r.mqttFiles, humanReadableBytes(r.mqttBytes))
	default:
		return "-"
	}
}

// ---- Live panel rendering (colour + aligned columns) ----------------------
//
// These helpers build the bottom-pinned dashboard rows. They are deliberately
// separate from formatSourceSplit/formatFramings (used by the plain end-of-pass
// summary table) so the live panel can be colour-coded and column-aligned without
// changing the recap.

// padCell left- or right-pads s to width visible columns, then paints it. Padding
// is applied to the raw text before colouring so the colour escapes (which have no
// visible width) never throw the column alignment off.
func padCell(code, s string, width int, right bool) string {
	if n := utf8.RuneCountInString(s); n < width {
		pad := strings.Repeat(" ", width-n)
		if right {
			s = pad + s
		} else {
			s += pad
		}
	}
	return paint(code, s)
}

// rateClassColor maps a channel's rate class to a distinct colour so high-rate and
// low-rate channels are easy to tell apart at a glance.
func rateClassColor(rateClass string) string {
	switch rateClass {
	case "high-rate":
		return ui.yellow
	case "low-rate":
		return ui.blue
	default:
		return ui.dim
	}
}

// sourceBytesCell formats one channel's byte total from a single source (Topic or
// Cloud Storage) for the live panel, returning the raw cell text and the colour to
// paint it. A source that delivered nothing shows a dim placeholder so the eye
// skips it.
func sourceBytesCell(files int, bytes int64, color string) (text, code string) {
	if files == 0 {
		return "-", ui.dim
	}
	return humanReadableBytes(bytes), color
}

// formatFramingsPanel renders a channel's framings for the live panel as coloured
// "NAME count" pairs (e.g. "BITSTREAM 49, IQ 50"). The per-framing byte detail is
// kept for the end-of-pass summary via formatFramings so the live row stays short.
func formatFramingsPanel(framings map[string]framingCount) string {
	if len(framings) == 0 {
		return paint(ui.dim, "-")
	}
	keys := make([]string, 0, len(framings))
	for f := range framings {
		keys = append(keys, f)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, f := range keys {
		parts = append(parts, paint(ui.cyan, f)+" "+paint(ui.dim, fmt.Sprintf("%d", framings[f].files)))
	}
	return strings.Join(parts, paint(ui.dim, ", "))
}

// panelDivider is a dim horizontal rule drawn along the top of the live panel to
// separate the pinned dashboard from the scrolling output above it. It is made
// generously wide and trimmed to the terminal width when the panel is drawn.
func panelDivider() string {
	return paint(ui.dim, strings.Repeat("-", 200))
}

// panelChannelHeader is the dim column header shown above the per-channel rows.
// "Topic" and "Cloud Storage" are the two sources a channel's data can arrive
// over (real-time broker vs. object storage).
func panelChannelHeader() string {
	return paint(ui.dim, fmt.Sprintf("    %-8s  %-9s  %6s  %9s  %13s  %s",
		"channel", "rate", "msgs", "Topic", "Cloud Storage", "framings"))
}

// panelChannelRow renders one colour-coded, column-aligned channel row: a dim
// bullet, the bold channel id, a colour-coded rate class, the right-aligned
// message total, the per-source byte breakdown (Topic vs. Cloud Storage), the
// framing breakdown, and a green END marker once the channel's end-of-data has
// arrived.
func panelChannelRow(r channelBreakdownRow) string {
	topicText, topicColor := sourceBytesCell(r.mqttFiles, r.mqttBytes, ui.green)
	cloudText, cloudColor := sourceBytesCell(r.s3Files, r.s3Bytes, ui.magenta)

	var b strings.Builder
	b.WriteString("  " + paint(ui.dim, "-") + " ")
	b.WriteString(padCell(ui.bold, shortID(r.channelID), 8, false))
	b.WriteString("  " + padCell(rateClassColor(r.rateClass), r.rateClass, 9, false))
	b.WriteString("  " + padCell("", fmt.Sprintf("%d", r.files), 6, true))
	b.WriteString("  " + padCell(topicColor, topicText, 9, true))
	b.WriteString("  " + padCell(cloudColor, cloudText, 13, true))
	b.WriteString("  " + formatFramingsPanel(r.framings))
	if r.endReceived {
		b.WriteString("  " + paint(ui.green, "END"))
	}
	return b.String()
}

// getOrCreateChannelStats gets or creates channel stats for a given channel ID
func (s *statsTracker) getOrCreateChannelStats(channelID string) *channelStats {
	s.channelMu.Lock()
	defer s.channelMu.Unlock()
	if s.channelStats[channelID] == nil {
		s.channelStats[channelID] = &channelStats{
			channelID: channelID,
			framings:  make(map[string]*framingCount),
		}
	}
	return s.channelStats[channelID]
}

func (s *statsTracker) AddDownload(bytes int64, source MessageSource) {
	s.markActivity()
	now := time.Now()

	// Track first and last byte timestamps
	s.byteTimeMu.Lock()
	if s.firstByteTime == nil {
		firstTime := now
		s.firstByteTime = &firstTime
	}
	lastTime := now
	s.lastByteTime = &lastTime
	// The download counters share byteTimeMu: per-key telemetry workers call
	// AddDownload concurrently.
	if source == SourceMQTT || source == "" {
		// Default to MQTT if source is empty
		s.mqttDownloadedFiles++
		s.mqttDownloadedBytes += bytes
	} else {
		s.s3DownloadedFiles++
		s.s3DownloadedBytes += bytes
	}
	s.byteTimeMu.Unlock()
}

// downloadTotals returns the per-source download counters, safe against
// concurrent AddDownload calls from the per-key telemetry workers.
func (s *statsTracker) downloadTotals() (mqttFiles int, mqttBytes int64, s3Files int, s3Bytes int64) {
	s.byteTimeMu.RLock()
	defer s.byteTimeMu.RUnlock()
	return s.mqttDownloadedFiles, s.mqttDownloadedBytes, s.s3DownloadedFiles, s.s3DownloadedBytes
}

// AddChannelDownload adds download stats for a specific channel
func (s *statsTracker) AddChannelDownload(
	channelID string,
	framing string,
	bytes int64,
	source MessageSource,
	index int,
) {
	s.markActivity()
	// Announce the first real telemetry (downlink) chunk per base channel. The
	// channelID here can carry a message-type suffix (e.g. ".../downlink",
	// ".../monitoring"); only the downlink stream is telemetry, and the operator
	// wants to see the bare channel UUID, not the internal path.
	if base, ok := downlinkChannelBase(channelID); ok && s.announceOnce("telemetry:"+base) {
		uiDataf(kindTelemetry, "receiving on channel %s", base)
	}
	ch := s.getOrCreateChannelStats(channelID)
	s.channelMu.Lock()
	ch.downloadedFiles++
	ch.downloadedBytes += bytes
	switch source {
	case SourceMQTT:
		ch.mqttFiles++
		ch.mqttBytes += bytes
	case SourceS3:
		ch.s3Files++
		ch.s3Bytes += bytes
	}
	if framing != "" {
		fc := ch.framings[framing]
		if fc == nil {
			fc = &framingCount{}
			ch.framings[framing] = fc
		}
		fc.files++
		fc.bytes += bytes
	}
	if index > 0 {
		ch.lastIndex = index
		// nextIndex should be the next expected index (current max + 1)
		if index >= ch.nextIndex {
			ch.nextIndex = index + 1
		}
	}
	s.channelMu.Unlock()

	// Update advanced statistics if enabled
	if s.enableStats && bytes > 0 {
		now := time.Now()
		s.messageBufferMu.Lock()
		if s.firstByteTimeLocal == nil {
			firstTime := now
			s.firstByteTimeLocal = &firstTime
		}
		lastTime := now
		s.lastByteTimeLocal = &lastTime
		s.totalBytesReceived += bytes
		s.totalMessagesReceived++

		// Add to message buffer for instantaneous throughput stats.
		msg := telemetryWithTimestamp{
			receivedTime: now,
			dataBytes:    int(bytes),
		}
		s.messageBuffer = append(s.messageBuffer, msg)
		cutoffTime := now.Add(-statsInstantWindowDuration)
		for len(s.messageBuffer) > 0 && s.messageBuffer[0].receivedTime.Before(cutoffTime) {
			s.messageBuffer = s.messageBuffer[1:]
		}
		if len(s.messageBuffer) > statsInstantWindowMaxSamples {
			s.messageBuffer = s.messageBuffer[len(s.messageBuffer)-statsInstantWindowMaxSamples:]
		}
		s.messageBufferMu.Unlock()
	}

	// Also update aggregated stats
	s.AddDownload(bytes, source)
}

// SetChannelNextIndex sets the next expected index for a channel (used when writing in order)
func (s *statsTracker) SetChannelNextIndex(channelID string, nextIndex int) {
	ch := s.getOrCreateChannelStats(channelID)
	s.channelMu.Lock()
	ch.nextIndex = nextIndex
	s.channelMu.Unlock()
}

func (s *statsTracker) AddWrite(bytes int64) {
	now := time.Now()

	// Track first and last byte timestamps
	s.byteTimeMu.Lock()
	if s.firstByteTime == nil {
		firstTime := now
		s.firstByteTime = &firstTime
	}
	lastTime := now
	s.lastByteTime = &lastTime
	// The write counters share byteTimeMu, like the download counters above:
	// one worker per (channel, framing) calls this concurrently.
	s.writtenFiles++
	s.writtenBytes += bytes
	s.byteTimeMu.Unlock()
}

// writeTotals returns the write counters, safe against concurrent AddWrite.
func (s *statsTracker) writeTotals() (files int, bytes int64) {
	s.byteTimeMu.RLock()
	defer s.byteTimeMu.RUnlock()
	return s.writtenFiles, s.writtenBytes
}

// AddChannelWrite adds write stats for a specific channel
func (s *statsTracker) AddChannelWrite(channelID string, bytes int64) {
	ch := s.getOrCreateChannelStats(channelID)
	s.channelMu.Lock()
	ch.writtenFiles++
	ch.writtenBytes += bytes
	s.channelMu.Unlock()

	// Also update aggregated stats
	s.AddWrite(bytes)
}

// AddChannelDataForChecksum adds telemetry data to the checksum calculation for a channel
// This should be called with data in order (as it's written)
func (s *statsTracker) AddChannelDataForChecksum(channelID string, data []byte) {
	if !s.diagnosticsEnabled {
		return
	}

	s.checksumMu.Lock()
	defer s.checksumMu.Unlock()

	if s.channelChecksums[channelID] == nil {
		hash := sha256.New()
		s.channelChecksums[channelID] = &channelChecksum{
			channelID: channelID,
			hash:      hash,
		}
	}

	chk := s.channelChecksums[channelID]
	chk.hash.Write(data)
	chk.bytes += int64(len(data))
}

// SetClientID sets the resolved MQTT client ID for diagnostics
func (s *statsTracker) SetClientID(clientID string) {
	s.clientIDMu.Lock()
	defer s.clientIDMu.Unlock()
	s.clientID = clientID
}

// GetClientID gets the resolved MQTT client ID for diagnostics
func (s *statsTracker) GetClientID() string {
	s.clientIDMu.RLock()
	defer s.clientIDMu.RUnlock()
	return s.clientID
}

// AddError records an error for diagnostics
func (s *statsTracker) AddError(err error) {
	if !s.diagnosticsEnabled {
		return
	}

	s.errorsMu.Lock()
	defer s.errorsMu.Unlock()

	s.errors = append(
		s.errors,
		fmt.Sprintf("%s: %v", time.Now().UTC().Format("2006-01-02T15:04:05.000Z"), err),
	)
}

func (s *statsTracker) AddMonitoringMessage(source MessageSource) {
	s.markActivity()
	if s.announceOnce("monitoring") {
		uiDataf(kindMonitoring, "receiving monitoring data")
	}
	s.messageMu.Lock()
	if source == SourceMQTT {
		s.monitoringStats.mqttCount++
	} else {
		s.monitoringStats.s3Count++
	}
	s.monitoringStats.lastIndex = time.Now().UnixNano()
	s.messageMu.Unlock()
}

func (s *statsTracker) AddConfigMessage(source MessageSource) {
	s.markActivity()
	if s.announceOnce("config") {
		uiDataf(kindConfig, "receiving configuration data")
	}
	s.messageMu.Lock()
	if source == SourceMQTT {
		s.configStats.mqttCount++
	} else {
		s.configStats.s3Count++
	}
	s.configStats.lastIndex = time.Now().UnixNano()
	s.messageMu.Unlock()
}

func (s *statsTracker) AddEventMessage(source MessageSource) {
	s.markActivity()
	if s.announceOnce("event") {
		uiDataf(kindEvent, "receiving events")
	}
	s.messageMu.Lock()
	if source == SourceMQTT {
		s.eventStats.mqttCount++
	} else {
		s.eventStats.s3Count++
	}
	s.eventStats.lastIndex = time.Now().UnixNano()
	s.messageMu.Unlock()
}

func (s *statsTracker) AddAckReceived(topic string, payload []byte) {
	s.markActivity()
	s.mu.Lock()
	defer s.mu.Unlock()

	// The station reports rejections on the same ack topic with status NACK;
	// an absent status means the command was accepted. A rejected command
	// must not be recorded as delivered, and a payload that does not decode
	// is neither: a malformed one must not mark an outstanding command
	// delivered.
	rejected, index, err := decodeAckStatusIndex(payload)
	if err != nil {
		s.AddError(fmt.Errorf("undecodable acknowledgement on %s: %w", topic, err))
		return
	}
	if rejected {
		s.nacksReceived++
	} else {
		s.acksReceived++
	}

	// Acks now come on topics like "{env}/pass/{passId}/channel/{channelId}/uplink/ack"
	// We need to map that back to the base topic by removing "/ack" at the end
	baseTopic := strings.TrimSuffix(topic, "/ack")

	// The station always echoes the sender's command index (numbered from 1),
	// so an acknowledgement matches a command strictly by index. A NACK for a
	// payload the station could not decode carries nothing to echo and leaves
	// index unset; such a rejection matches no particular command. Index 0 is
	// also the echo of a command whose sender never set the field; this
	// client numbers from 1, so it matches nothing either. An ACK without a
	// positive index is a tracked error rather than a silent
	// first-outstanding fallback, so a station regression is visible.
	if index <= 0 {
		if !rejected {
			s.AddError(fmt.Errorf(
				"acknowledgement without a command index on %s: the station must echo the index of the command it accepts", topic))
		}
		return
	}
	// An indexed ack that matches no outstanding command is left silent on
	// purpose: another observer of the same channel may acknowledge the same
	// commands, so duplicate indexed acks are normal.
	for _, cmd := range s.sentCommands {
		if cmd.topic != baseTopic || cmd.acked || cmd.rejected {
			continue
		}
		if uint64(cmd.index) != uint64(index) {
			continue
		}
		if rejected {
			cmd.rejected = true
		} else {
			cmd.acked = true
		}
		break
	}
}

func (s *statsTracker) AddReceivedMessage(topic, msgType string, index int, channelID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.receivedMessages = append(s.receivedMessages, &receivedMessage{
		topic:     topic,
		msgType:   msgType,
		index:     index,
		channelID: channelID,
		timestamp: time.Now().UnixNano(),
		acked:     false, // We don't currently send acks, so always false
	})
}

func (s *statsTracker) AddSentCommand(topic, cmdType string, index uint32) {
	s.markActivity()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sentCommands = append(s.sentCommands, &sentCommand{
		topic:     topic,
		cmdType:   cmdType,
		index:     index,
		timestamp: time.Now().UnixNano(),
		acked:     false,
	})
}

// SentCommandCounts returns how many satellite commands ("sat", cmdType
// "uplink") and ground-station config requests ("gs", cmdType "config_request")
// have been sent so far.
func (s *statsTracker) SentCommandCounts() (sat, gs int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cmd := range s.sentCommands {
		switch cmd.cmdType {
		case "uplink":
			sat++
		case "config_request":
			gs++
		}
	}
	return sat, gs
}

func (s *statsTracker) GetUnackedReceivedMessages() []*receivedMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	var unacked []*receivedMessage
	for _, msg := range s.receivedMessages {
		if !msg.acked {
			unacked = append(unacked, msg)
		}
	}
	return unacked
}

func (s *statsTracker) GetUnackedSentCommands() []*sentCommand {
	s.mu.Lock()
	defer s.mu.Unlock()
	var unacked []*sentCommand
	for _, cmd := range s.sentCommands {
		if !cmd.acked && !cmd.rejected {
			unacked = append(unacked, cmd)
		}
	}
	return unacked
}

// GetAllSentCommands returns all sent commands (for diagnostics)
func (s *statsTracker) GetAllSentCommands() []*sentCommand {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Return a copy to avoid race conditions
	cmds := make([]*sentCommand, len(s.sentCommands))
	copy(cmds, s.sentCommands)
	return cmds
}

func (s *statsTracker) MarkReceivedMessageAcked(topic string) {
	s.markActivity()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.acksSent++
	for _, msg := range s.receivedMessages {
		if msg.topic == topic && !msg.acked {
			msg.acked = true
			break
		}
	}
}

// getElapsedTimeBetweenBytes returns the elapsed time in seconds between first and last byte.
// If no bytes have been received yet, returns the time since start.
func (s *statsTracker) getElapsedTimeBetweenBytes(now time.Time) float64 {
	s.byteTimeMu.RLock()
	defer s.byteTimeMu.RUnlock()

	if s.firstByteTime != nil && s.lastByteTime != nil {
		elapsed := s.lastByteTime.Sub(*s.firstByteTime).Seconds()
		if elapsed <= 0 {
			return 1e-9
		}
		return elapsed
	}

	// Fallback to total elapsed time if no bytes received yet
	elapsed := now.Sub(s.start).Seconds()
	if elapsed <= 0 {
		return 1e-9
	}
	return elapsed
}

// LogStats logs overall metrics every second
func (s *statsTracker) LogStats() {
	now := time.Now()
	// Throttle below the 1s ticker cadence so every tick renders (timing jitter
	// against a 1s throttle would otherwise skip alternate ticks, giving ~2s updates).
	if now.Sub(s.lastLogTime) < 500*time.Millisecond {
		return
	}
	s.lastLogTime = now

	// Calculate elapsed time between first and last byte (for accurate bitrate)
	elapsedTotal := s.getElapsedTimeBetweenBytes(now)

	// Calculate total download stats
	mqttFiles, mqttBytes, s3Files, s3Bytes := s.downloadTotals()
	totalDownloadedFiles := mqttFiles + s3Files
	totalDownloadedBytes := mqttBytes + s3Bytes
	avgDlMbps := (float64(totalDownloadedBytes) * 8.0 / 1_000_000) / elapsedTotal

	// Calculate total write stats
	_, writtenBytesTotal := s.writeTotals()
	avgWrMbps := (float64(writtenBytesTotal) * 8.0 / 1_000_000) / elapsedTotal

	// Get message stats
	s.messageMu.RLock()
	monitoringCount := s.monitoringStats.mqttCount + s.monitoringStats.s3Count
	configCount := s.configStats.mqttCount + s.configStats.s3Count
	eventCount := s.eventStats.mqttCount + s.eventStats.s3Count
	s.messageMu.RUnlock()

	// Count unacked messages
	unackedReceived := s.GetUnackedReceivedMessages()
	unackedCommands := s.GetUnackedSentCommands()

	// Get list of channels that have received data
	s.channelMu.RLock()
	channelIDs := make([]string, 0, len(s.channelStats))
	for channelID := range s.channelStats {
		channelIDs = append(channelIDs, channelID)
	}
	s.channelMu.RUnlock()

	// Sort channel IDs for consistent output
	sort.Strings(channelIDs)

	// Format channel list as comma-separated string
	var channelList string
	if len(channelIDs) == 0 {
		channelList = "none"
	} else {
		channelList = strings.Join(channelIDs, ",")
	}

	_ = channelList // channel identities are shown per-channel in verbose mode below

	// Build a friendly one-line dashboard where each data type is colour- and
	// icon-tagged, so an operator can see at a glance what is arriving.
	elapsed := formatClock(time.Since(s.start))
	rate := fmt.Sprintf("%.1f Mb/s", avgDlMbps)
	if s.enableStats {
		rate = fmt.Sprintf("%.1f Mb/s now, %.1f avg", s.getInstantaneousRate(), avgDlMbps)
	}

	// Derive the aggregate telemetry figures from the same per-channel snapshot the
	// rows are built from, so the summary total, byte count and channel count can
	// never disagree with the rows (and stale/phantom channel-stat entries can't
	// inflate them).
	rows := s.channelBreakdown()
	var telFiles int
	var telBytes int64
	channelsWithData := 0
	for _, r := range rows {
		telFiles += r.files
		telBytes += r.bytes
		if r.files > 0 {
			channelsWithData++
		}
	}
	_ = totalDownloadedFiles
	_ = totalDownloadedBytes
	_ = channelIDs

	satCmds, gsCmds := s.SentCommandCounts()
	ackSeg := fmt.Sprintf("%s %d sent, %d received", kindAck.tag(), s.acksSent, s.acksReceived)
	if s.nacksReceived > 0 {
		ackSeg += fmt.Sprintf(", %d rejected", s.nacksReceived)
	}
	segs := []string{
		fmt.Sprintf("%s %s msgs, %s across %s ch, %s",
			kindTelemetry.tag(),
			paint(ui.bold, fmt.Sprintf("%d", telFiles)),
			paint(ui.bold, humanReadableBytes(telBytes)),
			paint(ui.bold, fmt.Sprintf("%d", channelsWithData)),
			paint(ui.bold, rate)),
		fmt.Sprintf("%s %d", kindMonitoring.tag(), monitoringCount),
		fmt.Sprintf("%s %d", kindEvent.tag(), eventCount),
		fmt.Sprintf("%s %d", kindConfig.tag(), configCount),
		fmt.Sprintf("%s %d sat, %d gs", kindUplink.tag(), satCmds, gsCmds),
		ackSeg,
	}
	summary := paint(ui.dim, "elapsed "+elapsed) + "   " + joinSegs(segs)

	// The live dashboard is a bottom-pinned panel: a divider marking its top edge,
	// a summary line, a dim column header, then one colour-coded, column-aligned row
	// per channel (rate class, message/byte totals, source, and the per-framing
	// breakdown). It repaints in place each tick and is skipped entirely when output
	// is not a terminal, to keep piped logs clean.
	lines := []string{panelDivider(), summary}
	if len(rows) > 0 {
		lines = append(lines, panelChannelHeader())
	}
	for _, r := range rows {
		lines = append(lines, panelChannelRow(r))
	}
	renderStatusBlock(lines)

	if uiVerbose {
		if len(unackedReceived) > 0 || len(unackedCommands) > 0 {
			log.Printf("awaiting acks: %d received-msgs, %d sent-cmds", len(unackedReceived), len(unackedCommands))
		}
	}
	_ = avgWrMbps
}

// formatClock renders a duration as mm:ss (or h:mm:ss past an hour).
func formatClock(d time.Duration) string {
	total := int(d.Seconds())
	h := total / 3600
	m := (total % 3600) / 60
	s := total % 60
	if h > 0 {
		return fmt.Sprintf("%d:%02d:%02d", h, m, s)
	}
	return fmt.Sprintf("%02d:%02d", m, s)
}

// joinSegs joins dashboard segments with a dim separator.
func joinSegs(segs []string) string {
	sep := paint(ui.dim, "   ")
	out := ""
	for i, s := range segs {
		if i > 0 {
			out += sep
		}
		out += s
	}
	return out
}

// logPassSummary logs a detailed pass summary (called when plan changes or on exit)
func (s *statsTracker) logPassSummary() {
	if !s.enableStats || s.totalMessagesReceived == 0 {
		return
	}

	elapsed := time.Since(s.start).Seconds()
	if elapsed <= 0 {
		elapsed = 1e-9
	}

	avgRate := float64(s.totalBytesReceived) * 8.0 / elapsed
	avgDelay := int64(0)
	if s.totalMessagesReceived > 0 {
		avgDelay = s.totalDelayNanos / s.totalMessagesReceived
	}

	log.Printf("\n[STATS] %s, Pass summary:\n", time.Now().Format("20060102 15:04:05"))
	log.Printf("  Pass ID   : %s\n", s.passID)
	log.Printf("  Stream ID : %s\n", s.streamID)
	log.Printf("\n")
	log.Printf("  Datatake (Starpass timestamp)\n")
	if s.firstByteTimeProto != nil && s.lastByteTimeProto != nil {
		log.Printf("  First byte received   : %s (UTC %s)\n",
			s.firstByteTimeProto.Format(time.RFC3339),
			s.firstByteTimeProto.UTC().Format(time.RFC3339))
		log.Printf("  Last  byte received   : %s (UTC %s)\n",
			s.lastByteTimeProto.Format(time.RFC3339),
			s.lastByteTimeProto.UTC().Format(time.RFC3339))
		log.Printf("  Duration              : %s\n",
			s.lastByteTimeProto.Sub(*s.firstByteTimeProto))
	}
	log.Printf("\n")
	log.Printf("  CLI data receive (local timestamp)\n")
	if s.firstByteTimeLocal != nil && s.lastByteTimeLocal != nil {
		log.Printf("  First chunk received  : %s\n", s.firstByteTimeLocal.Format(time.RFC3339))
		log.Printf("  Last  chunk received  : %s\n", s.lastByteTimeLocal.Format(time.RFC3339))
	}
	log.Printf(
		"  Total bytes received  : %d (%s)\n",
		s.totalBytesReceived,
		humanReadableBytes(s.totalBytesReceived),
	)
	log.Printf("  Total chunks          : %d\n", s.totalMessagesReceived)
	log.Printf("  Average rate (bits/s)  : %sbps\n", humanReadableCountSI(int64(avgRate)))
	log.Printf("  Average delay          : %s\n", humanReadableNanoSeconds(avgDelay))
	log.Printf("\n")
}

// LogPassSummaryOnExit logs pass summary on exit (public wrapper)
func (s *statsTracker) LogPassSummaryOnExit() {
	s.logPassSummary()
}

// getInstantaneousRate calculates the instantaneous data rate based on recent messages
func (s *statsTracker) getInstantaneousRate() float64 {
	s.messageBufferMu.Lock()
	defer s.messageBufferMu.Unlock()

	if len(s.messageBuffer) < 3 {
		return 0
	}

	bytes := int64(0)
	for i := 1; i < len(s.messageBuffer); i++ {
		bytes += int64(s.messageBuffer[i].dataBytes)
	}
	startTime := s.messageBuffer[0].receivedTime
	endTime := s.messageBuffer[len(s.messageBuffer)-1].receivedTime
	duration := endTime.Sub(startTime).Seconds()
	if duration <= 0 {
		return 0
	}
	return float64(bytes) * 8.0 / duration / 1_000_000 // Mbps
}

// getInstantaneousDelay calculates the instantaneous delay based on recent messages
func (s *statsTracker) getInstantaneousDelay() int64 {
	s.messageBufferMu.Lock()
	defer s.messageBufferMu.Unlock()

	if len(s.messageBuffer) < 2 {
		return 0
	}

	delayNanos := int64(0)
	count := 0
	for _, msg := range s.messageBuffer {
		if msg.timeLastByteReceived != nil {
			delayNanos += msg.receivedTime.Sub(*msg.timeLastByteReceived).Nanoseconds()
			count++
		}
	}
	if count == 0 {
		return 0
	}
	return delayNanos / int64(count)
}

// humanReadableBytes converts bytes to human-readable format (e.g., "1.2 MiB")
func humanReadableBytes(bytes int64) string {
	if bytes < 1024 {
		return fmt.Sprintf("%d B", bytes)
	}
	ci := "KMGTPE"
	idx := 0
	for bytes >= 1024*1024 {
		bytes /= 1024
		idx++
		if idx >= len(ci) {
			break
		}
	}
	return fmt.Sprintf("%.1f %ciB", float64(bytes)/1024.0, ci[idx])
}

// humanReadableCountSI converts count to human-readable SI format (e.g., "1.2 k")
func humanReadableCountSI(count int64) string {
	if count < 1000 {
		return fmt.Sprintf("%d ", count)
	}
	ci := "kMGTPE"
	idx := 0
	for count >= 999950 {
		count /= 1000
		idx++
		if idx >= len(ci) {
			break
		}
	}
	return fmt.Sprintf("%.1f %c", float64(count)/1000.0, ci[idx])
}

// humanReadableNanoSeconds converts nanoseconds to human-readable format
func humanReadableNanoSeconds(nanos int64) string {
	if nanos < 1000 {
		return fmt.Sprintf("%d ns", nanos)
	}
	nanosFloat := float64(nanos) / 1000.0 // microseconds
	if nanosFloat < 1000 {
		return fmt.Sprintf("%.1f µs", nanosFloat)
	}
	nanosFloat /= 1000.0 // milliseconds
	if nanosFloat < 1000 {
		return fmt.Sprintf("%.1f ms", nanosFloat)
	}
	nanosFloat /= 1000.0 // seconds
	if nanosFloat < 60 {
		return fmt.Sprintf("%.1f s", nanosFloat)
	}
	nanosFloat /= 60.0 // minutes
	if nanosFloat < 60 {
		return fmt.Sprintf("%.1f m", nanosFloat)
	}
	nanosFloat /= 60.0 // hours
	return fmt.Sprintf("%.1f h", nanosFloat)
}
