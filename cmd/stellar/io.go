package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

const unknownFraming = "unknown"

// validatePassID rejects a pass ID that is not a plain UUID. Pass and channel
// IDs arrive from the server (authorizer response, MQTT topics, S3 keys) and
// are joined into local file paths, so they are validated before any path is
// built: a compromised server must not be able to steer writes outside the
// destination directory with segments like "..".
func validatePassID(passID string) error {
	if !isCanonicalUUID(passID) {
		return fmt.Errorf("refusing to write received data: pass ID %q is not a valid ID", passID)
	}
	return nil
}

// validateChunkPathSegments checks every server-provided component of a
// telemetry chunk's destination path: pass and channel IDs must be UUIDs and
// the framing must be a known framing name (or empty, which is stored under
// "unknown").
func validateChunkPathSegments(passID, channelID, framing string) error {
	if err := validatePassID(passID); err != nil {
		return err
	}
	if !isValidChannelPathSegment(channelID) {
		return fmt.Errorf("refusing to write received data: channel ID %q is not a valid ID", channelID)
	}
	if framing != "" && framing != unknownFraming && !isKnownFraming(framing) {
		return fmt.Errorf("refusing to write received data: unknown framing type %q", framing)
	}
	return nil
}

// isValidChannelPathSegment accepts the two channel forms the readers emit: a
// bare channel UUID (S3 path) or "<uuid>/downlink" (MQTT low-rate path, which
// mirrors the S3 prefix structure). Anything else is rejected before it is
// joined into a file path.
func isValidChannelPathSegment(channelID string) bool {
	base, rest, found := strings.Cut(channelID, "/")
	if !isCanonicalUUID(base) {
		return false
	}
	return !found || rest == "downlink"
}

// isCanonicalUUID reports whether s is a UUID in the canonical hyphenated
// form. uuid.Parse alone also accepts urn: and braced variants; requiring the
// canonical form keeps every accepted value a single safe path segment.
func isCanonicalUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	_, err := uuid.Parse(s)
	return err == nil
}

// isKnownFraming reports whether framing is one of the framing names the
// platform emits.
func isKnownFraming(framing string) bool {
	for _, known := range getAllFramingTypes() {
		if framing == known {
			return true
		}
	}
	return false
}

// outputFileManager manages output file handles and synchronization
type outputFileManager struct {
	mu                 sync.Mutex
	stdoutMu           sync.Mutex
	handles            map[string]*bufio.Writer
	handlesInitialized bool
}

//nolint:gochecknoglobals // Package-level state for coordinating output file handles across goroutines
var globalOutputFileManager = &outputFileManager{
	handles: make(map[string]*bufio.Writer),
}

// writeToStdout writes data to stdout if enabled
func writeToStdout(data []byte, cfg Config) {
	if !cfg.EnableStdoutOutput {
		return
	}
	globalOutputFileManager.stdoutMu.Lock()
	_, err := os.Stdout.Write(data)
	globalOutputFileManager.stdoutMu.Unlock()
	if err != nil {
		// Log error but continue with file write (non-blocking)
		log.Printf("WARNING: Failed to write to stdout: %v", err)
	}
}

// buildChunkFilePath constructs the file path for a chunk
func buildChunkFilePath(
	destDir, prefix, passID, rateType string,
	index int,
	channelID string,
	framing string,
) (string, error) {
	if err := validateChunkPathSegments(passID, channelID, framing); err != nil {
		return "", err
	}

	key := prefix + strconv.Itoa(index)
	baseName := filepath.Base(key)
	if baseName == "" || baseName == "." || baseName == "/" || baseName == ".." {
		baseName = strings.ReplaceAll(key, "/", "_")
	}

	// <destDir>/pass/<passId>/<rateType>/channel/<channelId>/<framing>
	// If framing is empty, use "unknown"
	framingDir := framing
	if framingDir == "" {
		framingDir = unknownFraming
	}
	channelDir := filepath.Join(
		destDir,
		"pass", passID,
		rateType,
		"channel", channelID,
		framingDir,
	)

	if err := os.MkdirAll(channelDir, 0o750); err != nil {
		return "", fmt.Errorf("create channel dir %s: %w", channelDir, err)
	}

	return filepath.Join(channelDir, baseName), nil
}

// writeChunkData writes data to a file at the given path
func writeChunkData(data []byte, destPath string, stats *statsTracker) error {
	// Use buffered I/O for better write performance
	file, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("open file %s: %w", destPath, err)
	}
	defer file.Close()

	// Use buffered writer with 2MB buffer for maximum throughput with large files (~4.5MB)
	// Since we're not memory-constrained, larger buffer = fewer syscalls = better throughput
	writer := bufio.NewWriterSize(file, 2<<20)
	if _, err := writer.Write(data); err != nil {
		if stats != nil {
			stats.AddError(fmt.Errorf("write file %s: %w", destPath, err))
		}
		return fmt.Errorf("write file %s: %w", destPath, err)
	}
	if err := writer.Flush(); err != nil {
		if stats != nil {
			stats.AddError(fmt.Errorf("flush file %s: %w", destPath, err))
		}
		return fmt.Errorf("flush file %s: %w", destPath, err)
	}
	return nil
}

// writeChunkToFile writes the chunk payload data to destDir
// If stats is provided and diagnostics are enabled, also tracks data for checksum calculation
// Checksums are only calculated in in-order mode (writeInOrder=true) to ensure correct ordering
func writeChunkToFile(
	data []byte,
	destDir, prefix string,
	passID string,
	index int,
	channelID string,
	framing string, // Framing type (e.g., "BITSTREAM", "IQ") - empty string if unknown
	rateType string, // "high-rate" or "low-rate"
	stats *statsTracker, // Optional: for checksum tracking
	writeInOrder bool, // Whether we're writing in order (affects checksum calculation)
	cfg Config, // Config for stdout and output file options
) error {
	// Write to stdout FIRST (time-critical for downstream consumers of the pipe)
	writeToStdout(data, cfg)

	// Forward to proxy if active
	if cfg.ProxyCh != nil {
		cpy := make([]byte, len(data))
		copy(cpy, data)
		select {
		case cfg.ProxyCh <- cpy:
		default:
		}
	}

	// Write to single output file if configured
	if cfg.OutputFile != "" {
		if err := writeToOutputFile(data, channelID, framing, cfg); err != nil {
			log.Printf("WARNING: Failed to write to output file: %v", err)
			// Continue with regular file write
		}
	}

	destPath, err := buildChunkFilePath(
		destDir,
		prefix,
		passID,
		rateType,
		index,
		channelID,
		framing,
	)
	if err != nil {
		return err
	}

	if err := writeChunkData(data, destPath, stats); err != nil {
		return err
	}

	// Track data for checksum calculation (only for telemetry, in in-order mode)
	// In relaxed mode, data is written out of order, so checksums would be incorrect
	if stats != nil && channelID != "" && len(data) > 0 && writeInOrder {
		stats.AddChannelDataForChecksum(channelID, data)
	}

	return nil
}

// writeToOutputFile writes data to output files based on the output file modes
// Multiple modes can be selected, and data will be written to all matching files
func writeToOutputFile(data []byte, channelID string, framing string, cfg Config) error {
	globalOutputFileManager.mu.Lock()
	defer globalOutputFileManager.mu.Unlock()

	if !globalOutputFileManager.handlesInitialized {
		globalOutputFileManager.handles = make(map[string]*bufio.Writer)
		globalOutputFileManager.handlesInitialized = true
	}

	// Normalize framing for file naming
	if framing == "" {
		framing = unknownFraming
	}

	// Determine which files to write to based on selected modes
	fileKeys := make(map[string]bool) // Use map to deduplicate file keys

	for _, mode := range cfg.OutputFileMode {
		var fileKey string
		switch mode {
		case "per-channel":
			fileKey = fmt.Sprintf("%s-ch%s", cfg.OutputFile, channelID)
		case "per-framing":
			fileKey = fmt.Sprintf("%s-%s", cfg.OutputFile, framing)
		case "per-framing-channel":
			fileKey = fmt.Sprintf("%s-%s-ch%s", cfg.OutputFile, framing, channelID)
		case "all-combined":
			fileKey = cfg.OutputFile
		default:
			// Unknown mode, skip it
			continue
		}
		fileKeys[fileKey] = true
	}

	// If no valid modes, default to all-combined
	if len(fileKeys) == 0 {
		fileKeys[cfg.OutputFile] = true
	}

	// Write to all selected files
	for fileKey := range fileKeys {
		file, err := os.OpenFile(fileKey, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			return fmt.Errorf("open output file %s: %w", fileKey, err)
		}

		writer := bufio.NewWriterSize(file, 2<<20) // 2MB buffer

		if _, err := writer.Write(data); err != nil {
			_ = file.Close()
			return fmt.Errorf("write to output file %s: %w", fileKey, err)
		}

		if err := writer.Flush(); err != nil {
			_ = file.Close()
			return fmt.Errorf("flush output file %s: %w", fileKey, err)
		}

		if err := file.Close(); err != nil {
			return fmt.Errorf("close output file %s: %w", fileKey, err)
		}
	}

	return nil
}

// flushOutputFiles flushes all output file handles
func flushOutputFiles() {
	globalOutputFileManager.mu.Lock()
	defer globalOutputFileManager.mu.Unlock()

	for fileKey, writer := range globalOutputFileManager.handles {
		if err := writer.Flush(); err != nil {
			log.Printf("WARNING: Failed to flush output file %s: %v", fileKey, err)
		}
	}
	// Note: We don't close files here as they may be used again
	// Files will be closed when the program exits
}

// writeNonTelemetryMessage writes a non-telemetry message (monitoring, config, or event) to a file as JSON.
func writeNonTelemetryMessage(
	data []byte,
	destDir string,
	passID string,
	msgType string,
) error {
	if err := validatePassID(passID); err != nil {
		return err
	}

	timestamp := time.Now().UnixNano()

	// Keep filenames simple + unique
	baseName := fmt.Sprintf("%d.json", timestamp)

	// <destDir>/pass/<passId>/<msgType>
	fullDestDir := filepath.Join(
		destDir,
		"pass", passID,
		msgType, // "config" | "monitoring" | "event"
	)

	if err := os.MkdirAll(fullDestDir, 0o750); err != nil {
		return fmt.Errorf("create dest dir %s: %w", fullDestDir, err)
	}

	destPath := filepath.Join(fullDestDir, baseName)

	// Use buffered I/O for better write performance
	file, err := os.OpenFile(destPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("open %s message file %s: %w", msgType, destPath, err)
	}
	defer file.Close()

	// Use buffered writer with 2MB buffer for maximum throughput
	writer := bufio.NewWriterSize(file, 2<<20)
	if _, err := writer.Write(data); err != nil {
		return fmt.Errorf("write %s message file %s: %w", msgType, destPath, err)
	}
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush %s message file %s: %w", msgType, destPath, err)
	}
	return nil
}

// isEndMessage checks metadata for messagetype=END (case-insensitive).
// Works for both S3 metadata and MQTT message properties.
func isEndMessage(metadata map[string]string) bool {
	if metadata == nil {
		return false
	}
	if mt, ok := metadata["messagetype"]; ok {
		return strings.EqualFold(mt, "END")
	}
	return false
}
