package main

import (
	"bufio"
	"fmt"
	"io"
)

// Command size limits.
const (
	// mqttMaxPayloadBytes is the hard limit AWS IoT Core places on a single
	// published message; the broker rejects anything larger.
	mqttMaxPayloadBytes = 128 * 1024

	// maxCommandBytes is the largest satellite command payload the CLI
	// transmits. The envelope costs about 120 bytes today, so the transport
	// would carry more, but the limit is deliberately well under it: a
	// published limit can be raised freely and lowered only by breaking
	// callers that sized their commands to it, so the spare room is what lets
	// the envelope gain fields later. TestMaxCommandFitsPayloadLimit pins that
	// a command of this size still publishes.
	maxCommandBytes = 100 * 1024

	// maxCommandLineBytes bounds one line of interactive input: the command
	// keyword, an optional channel ID, and maxCommandBytes rendered as hex.
	// bufio's default line cap is 64 KiB, which would refuse a legal command
	// long before the size check could report it.
	maxCommandLineBytes = maxCommandBytes*2 + 1024

	// commandScanInitialBuf is the starting line buffer. Lines are short in
	// normal use, so start small and let bufio grow towards the cap only for
	// the rare long command.
	commandScanInitialBuf = 64 * 1024
)

// newCommandScanner reads interactive command lines, sized for the longest
// command the CLI accepts.
func newCommandScanner(r io.Reader) *bufio.Scanner {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 0, commandScanInitialBuf), maxCommandLineBytes)
	return s
}

// validateCommandSize rejects a command payload too large to publish. The
// limit applies to the total across commands, since those published together
// share one message.
func validateCommandSize(commands [][]byte) error {
	total := 0
	for _, c := range commands {
		total += len(c)
	}
	if total > maxCommandBytes {
		return fmt.Errorf(
			"command payload is %d bytes, over the %d byte limit; split it into several smaller commands",
			total, maxCommandBytes,
		)
	}
	return nil
}
