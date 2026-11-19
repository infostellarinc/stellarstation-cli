// Copyright (c) 2026 InfoStellar, Inc. All Rights Reserved.

package commandseq

import (
	"fmt"
	"time"
)

// gapWaitNever is the text form of NeverSkip.
const gapWaitNever = "never"

// GapWait is how long a message that arrived after a gap is held for the
// missing indexes before the gap is skipped. The zero value is DefaultGapWait;
// a wait of 0 skips a gap at once, and NeverSkip holds the message until the
// missing index arrives or the lane closes.
//
// Its text form is a duration ("5s", "0s") or "never", so it reads the same in
// execution configurations, events and star-receiver flags.
//
//nolint:recvcheck // Set and UnmarshalText write through a pointer; the rest read a value.
type GapWait struct {
	wait  time.Duration
	set   bool
	never bool
}

// NeverSkip never skips a gap: messages after it are held until the missing
// index arrives, the hold limit refuses more, or the pass ends.
var NeverSkip = GapWait{set: true, never: true}

// WaitFor is a gap wait of d; 0 skips a gap at once.
func WaitFor(d time.Duration) GapWait { return GapWait{wait: d, set: true} }

// ParseGapWait reads the text form. Empty is the default.
func ParseGapWait(s string) (GapWait, error) {
	var g GapWait
	return g, g.Set(s)
}

// timeout returns how long to hold a message, or false to hold it until the
// gap fills.
func (g GapWait) timeout() (time.Duration, bool) {
	switch {
	case g.never:
		return 0, false
	case g.set:
		return g.wait, true
	default:
		return DefaultGapWait, true
	}
}

// String is the text form; empty for the default.
func (g GapWait) String() string {
	switch {
	case g.never:
		return gapWaitNever
	case g.set:
		return g.wait.String()
	default:
		return ""
	}
}

// Set implements pflag.Value.
func (g *GapWait) Set(s string) error {
	switch s {
	case "":
		*g = GapWait{}
	case gapWaitNever:
		*g = NeverSkip
	default:
		d, err := time.ParseDuration(s)
		if err != nil || d < 0 {
			return fmt.Errorf("gap wait %q is neither a duration of 0 or more nor %q", s, gapWaitNever)
		}
		*g = WaitFor(d)
	}
	return nil
}

// Type implements pflag.Value.
func (*GapWait) Type() string { return "gapWait" }

func (g GapWait) MarshalText() ([]byte, error) { return []byte(g.String()), nil }

func (g *GapWait) UnmarshalText(text []byte) error { return g.Set(string(text)) }

// GapWaits holds the gap wait of each kind of command message.
type GapWaits struct {
	// Commands applies to satellite commands.
	Commands GapWait `json:"commands"`
	// Configs applies to ground station configuration requests.
	Configs GapWait `json:"configs"`
}
