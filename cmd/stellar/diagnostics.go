package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// utcTimestampFormat is the format for UTC timestamps with milliseconds and Z suffix
const utcTimestampFormat = "2006-01-02T15:04:05.000Z"

// formatUTCTimestamp formats a time in UTC with milliseconds and Z suffix
func formatUTCTimestamp(t time.Time) string {
	return t.UTC().Format(utcTimestampFormat)
}

// DiagnosticsData represents the structured diagnostics data
type DiagnosticsData struct {
	StartTime       string                `json:"start_time"`
	EndTime         string                `json:"end_time"`
	DurationSeconds int64                 `json:"duration_seconds"`
	Config          DiagnosticsConfig     `json:"config"`
	Statistics      DiagnosticsStatistics `json:"statistics"`
	Channels        []ChannelStatistics   `json:"channels"`
	Checksums       map[string]string     `json:"checksums,omitempty"`
	Errors          []string              `json:"errors"`
}

// DiagnosticsConfig contains configuration information
type DiagnosticsConfig struct {
	PlanID            string   `json:"plan_id"`
	ClientID          string   `json:"client_id"`
	Environment       string   `json:"environment"`
	Channels          []string `json:"channels"`
	SourceType        string   `json:"source_type"`
	Bucket            string   `json:"bucket"`
	DestinationDir    string   `json:"destination_dir"`
	WriteInOrder      bool     `json:"write_in_order"`
	WindowSize        int      `json:"window_size"`
	EnableDownlink    bool     `json:"enable_downlink"`
	EnableMonitoring  bool     `json:"enable_monitoring"`
	EnableConfigState bool     `json:"enable_config"`
	EnableEvent       bool     `json:"enable_event"`
}

// DiagnosticsStatistics contains overall statistics
type DiagnosticsStatistics struct {
	Commands CommandStats  `json:"commands"`
	Download DownloadStats `json:"download"`
	Write    WriteStats    `json:"write"`
	Messages MessageStats  `json:"messages"`
	Acks     AckStats      `json:"acks"`
}

// DownloadStats contains download statistics
type DownloadStats struct {
	TotalFiles int     `json:"total_files"`
	TotalBytes int64   `json:"total_bytes"`
	TotalMB    float64 `json:"total_mb"`
	AvgMbps    float64 `json:"avg_mbps"`
	MQTTFiles  int     `json:"mqtt_files"`
	MQTTBytes  int64   `json:"mqtt_bytes"`
	MQTTMB     float64 `json:"mqtt_mb"`
	S3Files    int     `json:"s3_files"`
	S3Bytes    int64   `json:"s3_bytes"`
	S3MB       float64 `json:"s3_mb"`
}

// WriteStats contains write statistics
type WriteStats struct {
	TotalFiles int     `json:"total_files"`
	TotalBytes int64   `json:"total_bytes"`
	TotalMB    float64 `json:"total_mb"`
	AvgMbps    float64 `json:"avg_mbps"`
}

// MessageStats contains message type statistics
type MessageStats struct {
	Monitoring MonitoringMessageStats `json:"monitoring"`
	Config     ConfigMessageStats     `json:"config"`
	Event      EventMessageStats      `json:"event"`
}

// MonitoringMessageStats contains monitoring message statistics
type MonitoringMessageStats struct {
	Total int `json:"total"`
	MQTT  int `json:"mqtt"`
	S3    int `json:"s3"`
}

// ConfigMessageStats contains config message statistics
type ConfigMessageStats struct {
	Total int `json:"total"`
	MQTT  int `json:"mqtt"`
	S3    int `json:"s3"`
}

// EventMessageStats contains event message statistics
type EventMessageStats struct {
	Total int `json:"total"`
	MQTT  int `json:"mqtt"`
	S3    int `json:"s3"`
}

// AckStats contains ack statistics
type AckStats struct {
	Sent            int `json:"sent"`
	Received        int `json:"received"`
	UnackedMessages int `json:"unacked_messages"`
	UnackedCommands int `json:"unacked_commands"`
}

// CommandStats contains command sending statistics
type CommandStats struct {
	UplinkStats        UplinkStats        `json:"uplinks"`
	ConfigRequestStats ConfigRequestStats `json:"config_requests"`
}

// UplinkStats contains satellite command statistics
type UplinkStats struct {
	TotalSent int             `json:"total_sent"`
	Acked     int             `json:"acked"`
	Rejected  int             `json:"rejected"`
	Unacked   int             `json:"unacked"`
	Commands  []CommandDetail `json:"commands"`
}

// ConfigRequestStats contains ground station config statistics
type ConfigRequestStats struct {
	TotalSent int             `json:"total_sent"`
	Acked     int             `json:"acked"`
	Rejected  int             `json:"rejected"`
	Unacked   int             `json:"unacked"`
	Commands  []CommandDetail `json:"commands"`
}

// CommandDetail contains details about a single command
type CommandDetail struct {
	Index     uint32 `json:"index"`
	Timestamp string `json:"timestamp"`
	Acked     bool   `json:"acked"`
	Rejected  bool   `json:"rejected"`
}

// ChannelStatistics contains per-channel statistics
type ChannelStatistics struct {
	ChannelID string               `json:"channel_id"`
	Download  ChannelDownloadStats `json:"download"`
	Write     ChannelWriteStats    `json:"write"`
}

// ChannelDownloadStats contains per-channel download statistics
type ChannelDownloadStats struct {
	Files     int     `json:"files"`
	Bytes     int64   `json:"bytes"`
	AvgMbps   float64 `json:"avg_mbps"`
	LastIndex int     `json:"last_index"`
}

// ChannelWriteStats contains per-channel write statistics
type ChannelWriteStats struct {
	Files     int     `json:"files"`
	Bytes     int64   `json:"bytes"`
	AvgMbps   float64 `json:"avg_mbps"`
	NextIndex int     `json:"next_index"`
}

const unknownClientID = "unknown"

func getClientID(stats *statsTracker, cfg Config) string {
	clientID := stats.GetClientID()
	if clientID == "" {
		// Try authorizer credentials (should be set early, but check here if not available)
		if cfg.AuthorizerCreds != nil && cfg.AuthorizerCreds.ClientID != "" {
			clientID = cfg.AuthorizerCreds.ClientID
		}
	}
	if clientID == "" {
		clientID = unknownClientID
	}
	return clientID
}

// overallStats is one consistent snapshot of the transfer counters. Taking
// the per-source values and the totals from a single read keeps the emitted
// report internally consistent: reading twice can observe a concurrent
// AddDownload in between and report a total below the sum of its parts.
type overallStats struct {
	mqttFiles, s3Files, writtenFiles int
	mqttBytes, s3Bytes, writtenBytes int64
	totalDownloadedFiles             int
	totalDownloadedBytes             int64
	avgDlMbps, avgWrMbps             float64
}

func calculateOverallStats(stats *statsTracker, elapsedSeconds float64) overallStats {
	o := overallStats{}
	o.mqttFiles, o.mqttBytes, o.s3Files, o.s3Bytes = stats.downloadTotals()
	o.writtenFiles, o.writtenBytes = stats.writeTotals()
	o.totalDownloadedFiles = o.mqttFiles + o.s3Files
	o.totalDownloadedBytes = o.mqttBytes + o.s3Bytes
	o.avgDlMbps = (float64(o.totalDownloadedBytes) * 8.0 / 1_000_000) / elapsedSeconds
	o.avgWrMbps = (float64(o.writtenBytes) * 8.0 / 1_000_000) / elapsedSeconds
	return o
}

func getMessageStats(stats *statsTracker) (int, int, int) {
	stats.messageMu.RLock()
	defer stats.messageMu.RUnlock()
	monitoringCount := stats.monitoringStats.mqttCount + stats.monitoringStats.s3Count
	configCount := stats.configStats.mqttCount + stats.configStats.s3Count
	eventCount := stats.eventStats.mqttCount + stats.eventStats.s3Count
	return monitoringCount, configCount, eventCount
}

func buildChannelStats(stats *statsTracker, elapsedSeconds float64) []ChannelStatistics {
	stats.channelMu.RLock()
	channels := make([]string, 0, len(stats.channelStats))
	for chID := range stats.channelStats {
		channels = append(channels, chID)
	}
	sort.Strings(channels)
	channelStats := make([]ChannelStatistics, 0, len(channels))
	for _, chID := range channels {
		ch := stats.channelStats[chID]
		chAvgDlMbps := (float64(ch.downloadedBytes) * 8.0 / 1_000_000) / elapsedSeconds
		chAvgWrMbps := (float64(ch.writtenBytes) * 8.0 / 1_000_000) / elapsedSeconds
		channelStats = append(channelStats, ChannelStatistics{
			ChannelID: chID,
			Download: ChannelDownloadStats{
				Files:     ch.downloadedFiles,
				Bytes:     ch.downloadedBytes,
				AvgMbps:   chAvgDlMbps,
				LastIndex: ch.lastIndex,
			},
			Write: ChannelWriteStats{
				Files:     ch.writtenFiles,
				Bytes:     ch.writtenBytes,
				AvgMbps:   chAvgWrMbps,
				NextIndex: ch.nextIndex,
			},
		})
	}
	stats.channelMu.RUnlock()
	return channelStats
}

func buildChecksums(stats *statsTracker, cfg Config) map[string]string {
	checksums := make(map[string]string)
	if !cfg.WriteInOrder || len(stats.channelChecksums) == 0 {
		return checksums
	}
	stats.checksumMu.Lock()
	defer stats.checksumMu.Unlock()
	checksumChannels := make([]string, 0, len(stats.channelChecksums))
	for chID := range stats.channelChecksums {
		checksumChannels = append(checksumChannels, chID)
	}
	sort.Strings(checksumChannels)
	for _, chID := range checksumChannels {
		chk := stats.channelChecksums[chID]
		checksum := chk.hash.Sum(nil)
		checksumHex := hex.EncodeToString(checksum)
		checksums[fmt.Sprintf("channel_%s", chID)] = checksumHex
	}
	return checksums
}

func getErrors(stats *statsTracker) []string {
	stats.errorsMu.Lock()
	defer stats.errorsMu.Unlock()
	errors := make([]string, len(stats.errors))
	copy(errors, stats.errors)
	return errors
}

func buildCommandStats(stats *statsTracker) CommandStats {
	allSentCommands := stats.GetAllSentCommands()
	uplinkStats := UplinkStats{Commands: make([]CommandDetail, 0)}
	configRequestStats := ConfigRequestStats{Commands: make([]CommandDetail, 0)}
	for _, cmd := range allSentCommands {
		cmdDetail := CommandDetail{
			Index:     cmd.index,
			Timestamp: formatUTCTimestamp(time.Unix(0, cmd.timestamp)),
			Acked:     cmd.acked,
			Rejected:  cmd.rejected,
		}
		switch cmd.cmdType {
		case "uplink":
			uplinkStats.TotalSent++
			switch {
			case cmd.acked:
				uplinkStats.Acked++
			case cmd.rejected:
				uplinkStats.Rejected++
			default:
				uplinkStats.Unacked++
			}
			uplinkStats.Commands = append(uplinkStats.Commands, cmdDetail)
		case "config_request":
			configRequestStats.TotalSent++
			switch {
			case cmd.acked:
				configRequestStats.Acked++
			case cmd.rejected:
				configRequestStats.Rejected++
			default:
				configRequestStats.Unacked++
			}
			configRequestStats.Commands = append(configRequestStats.Commands, cmdDetail)
		}
	}
	return CommandStats{
		UplinkStats:        uplinkStats,
		ConfigRequestStats: configRequestStats,
	}
}

func buildDiagnosticsData(
	cfg Config,
	stats *statsTracker,
	clientID string,
	endTime time.Time,
	elapsed time.Duration,
	elapsedSeconds float64,
) DiagnosticsData {
	o := calculateOverallStats(stats, elapsedSeconds)
	monitoringCount, configCount, eventCount := getMessageStats(stats)
	unackedReceived := stats.GetUnackedReceivedMessages()
	unackedCommands := stats.GetUnackedSentCommands()
	channelStats := buildChannelStats(stats, elapsedSeconds)
	checksums := buildChecksums(stats, cfg)
	errors := getErrors(stats)
	commandStats := buildCommandStats(stats)

	diagnostics := DiagnosticsData{
		StartTime:       formatUTCTimestamp(stats.start),
		EndTime:         formatUTCTimestamp(endTime),
		DurationSeconds: int64(elapsed.Round(time.Second).Seconds()),
		Config: DiagnosticsConfig{
			PlanID:            cfg.PassID,
			ClientID:          clientID,
			Environment:       cfg.Environment,
			Channels:          cfg.ChannelIDs,
			SourceType:        string(cfg.SourceType),
			Bucket:            cfg.Bucket,
			DestinationDir:    cfg.DestDir,
			WriteInOrder:      cfg.WriteInOrder,
			WindowSize:        cfg.WindowSize,
			EnableDownlink:    cfg.EnableDownlink,
			EnableMonitoring:  cfg.EnableMonitoring,
			EnableConfigState: cfg.EnableConfigState,
			EnableEvent:       cfg.EnableEvent,
		},
		Statistics: DiagnosticsStatistics{
			Commands: commandStats,
			Download: DownloadStats{
				TotalFiles: o.totalDownloadedFiles,
				TotalBytes: o.totalDownloadedBytes,
				TotalMB:    float64(o.totalDownloadedBytes) / 1_000_000,
				AvgMbps:    o.avgDlMbps,
				MQTTFiles:  o.mqttFiles,
				MQTTBytes:  o.mqttBytes,
				MQTTMB:     float64(o.mqttBytes) / 1_000_000,
				S3Files:    o.s3Files,
				S3Bytes:    o.s3Bytes,
				S3MB:       float64(o.s3Bytes) / 1_000_000,
			},
			Write: WriteStats{
				TotalFiles: o.writtenFiles,
				TotalBytes: o.writtenBytes,
				TotalMB:    float64(o.writtenBytes) / 1_000_000,
				AvgMbps:    o.avgWrMbps,
			},
			Messages: MessageStats{
				Monitoring: MonitoringMessageStats{
					Total: monitoringCount,
					MQTT:  stats.monitoringStats.mqttCount,
					S3:    stats.monitoringStats.s3Count,
				},
				Config: ConfigMessageStats{
					Total: configCount,
					MQTT:  stats.configStats.mqttCount,
					S3:    stats.configStats.s3Count,
				},
				Event: EventMessageStats{
					Total: eventCount,
					MQTT:  stats.eventStats.mqttCount,
					S3:    stats.eventStats.s3Count,
				},
			},
			Acks: AckStats{
				Sent:            stats.acksSent,
				Received:        stats.acksReceived,
				UnackedMessages: len(unackedReceived),
				UnackedCommands: len(unackedCommands),
			},
		},
		Channels: channelStats,
		Errors:   errors,
	}
	if cfg.WriteInOrder && len(checksums) > 0 {
		diagnostics.Checksums = checksums
	}
	return diagnostics
}

func writeDiagnosticsLocally(cfg Config, clientID string, jsonData []byte) {
	localDir := filepath.Join(cfg.DestDir, "diagnostics")
	if err := os.MkdirAll(localDir, 0o750); err != nil {
		uiWarnf("Could not create the diagnostics folder: %v", err)
		return
	}
	localPath := filepath.Join(localDir, fmt.Sprintf("%s.json", clientID))
	if err := os.WriteFile(localPath, jsonData, 0o600); err != nil {
		uiWarnf("Could not save the diagnostics report: %v", err)
	} else {
		uiDimf("Diagnostics report saved to %s", localPath)
	}
}

func uploadDiagnosticsToS3(
	ctx context.Context,
	cfg Config,
	clientID string,
	jsonData []byte,
	s3c *s3.Client,
) {
	// Use authorizer-provided diagnostics location if available
	var bucket, prefix string
	if cfg.AuthorizerCreds != nil && cfg.AuthorizerCreds.DiagnosticsPrefix != "" {
		bucket = cfg.AuthorizerCreds.DiagnosticsBucket
		if bucket == "" {
			bucket = cfg.AuthorizerCreds.S3Bucket
		}
		prefix = cfg.AuthorizerCreds.DiagnosticsPrefix
	} else {
		// Construct prefix manually (shouldn't happen when using authorizer)
		bucket = cfg.Bucket
		if cfg.Environment != "" {
			prefix = fmt.Sprintf("%s/%s/diagnostics/", cfg.Environment, cfg.PassID)
		} else {
			prefix = fmt.Sprintf("%s/diagnostics/", cfg.PassID)
		}
	}

	if bucket == "" || s3c == nil {
		return
	}

	s3Key := fmt.Sprintf("%s%s.json", prefix, clientID)
	_, err := s3c.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(s3Key),
		Body:        bytes.NewReader(jsonData),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		vlogf("failed to upload diagnostics to S3: %v", err)
	} else {
		vlogf("diagnostics uploaded to s3://%s/%s", bucket, s3Key)
	}
}

// writeDiagnosticsFile writes a diagnostics file locally and optionally to S3
func writeDiagnosticsFile(ctx context.Context, cfg Config, stats *statsTracker, s3c *s3.Client) {
	endTime := time.Now()
	// Use elapsed time between first and last byte for accurate bitrate calculation
	elapsedSeconds := stats.getElapsedTimeBetweenBytes(endTime)
	// For duration display, still use total elapsed time
	elapsed := endTime.Sub(stats.start)

	clientID := getClientID(stats, cfg)
	diagnostics := buildDiagnosticsData(cfg, stats, clientID, endTime, elapsed, elapsedSeconds)

	jsonData, err := json.MarshalIndent(diagnostics, "", "  ")
	if err != nil {
		log.Printf("ERROR: Failed to marshal diagnostics to JSON: %v", err)
		return
	}

	writeDiagnosticsLocally(cfg, clientID, jsonData)
	uploadDiagnosticsToS3(ctx, cfg, clientID, jsonData, s3c)
}
