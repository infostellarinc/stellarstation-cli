package main

// ui.go provides friendly, colourful terminal output for the stellar CLI so
// that satellite operators, not just StellarStation engineers, can follow
// what the tool is doing. Colours and symbols are automatically disabled when
// output is not a terminal or when NO_COLOR is set, so piped/redirected output
// and CI logs stay clean and greppable.
//
// Everything here writes to stderr. stdout is reserved for machine-readable
// data (command tables in --output json/csv, and raw telemetry under --stdout),
// so the human-friendly narration never corrupts a pipe.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/charmbracelet/x/term"
)

// ANSI colour codes. Populated with escape sequences when colour is enabled and
// left empty otherwise, so format strings can reference them unconditionally.
type palette struct {
	reset, bold, dim                        string
	red, green, yellow, blue, magenta, cyan string
}

//nolint:gochecknoglobals // Process-wide styling state, resolved once at startup.
var (
	ui         palette
	uiColor    bool
	uiInitOnce sync.Once
)

// initUI resolves whether colour output should be used and fills the palette.
// Colour is on when stderr is a terminal and NO_COLOR is unset. Safe to call
// repeatedly; the work happens once.
func initUI() {
	uiInitOnce.Do(func() {
		_, noColor := os.LookupEnv("NO_COLOR")
		uiColor = !noColor && isTerminal(os.Stderr)
		if uiColor {
			ui = palette{
				reset: "\033[0m", bold: "\033[1m", dim: "\033[2m",
				red: "\033[31m", green: "\033[32m", yellow: "\033[33m",
				blue: "\033[34m", magenta: "\033[35m", cyan: "\033[36m",
			}
		}
	})
}

// isTerminal reports whether f is attached to an interactive terminal, using
// only the standard library (no extra dependency) so go.mod is untouched.
func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

// paint wraps s in the given colour code (a no-op when colour is disabled).
func paint(code, s string) string {
	if !uiColor || code == "" {
		return s
	}
	return code + s + ui.reset
}

// uiOut is where human-facing narration goes. Overridable in tests.
//
//nolint:gochecknoglobals
var uiOut io.Writer = os.Stderr

// ---- Bottom-pinned live status panel --------------------------------------
//
// The live stream dashboard is pinned to a fixed block of rows at the bottom of
// the terminal using a DEC scrolling region (DECSTBM, the "CSI top;bottom r"
// sequence). All ordinary output (reader log lines, narration, and the
// interactive command prompt) scrolls in the region ABOVE the panel and never
// touches it, so the per-second metrics stay put in one place while the story of
// the pass scrolls by above them. Everything here is a no-op when stderr is not a
// terminal or colour is disabled, so piped output and CI logs stay clean.

const (
	// panelMinTerminalRows is the smallest screen we will pin a panel on. Below
	// this we fall back to plain scrolling output so the panel can never crowd out
	// the whole screen.
	panelMinTerminalRows = 8
	// panelMinLogRows is how many rows we always keep for scrolling output above
	// the panel, bounding how tall the panel may grow.
	panelMinLogRows = 3
)

// uiMu serializes all writes to the terminal so the stats goroutine (repainting
// the panel) and other goroutines' narration/log output never interleave
// mid-escape-sequence or race the panel geometry below.
//
//nolint:gochecknoglobals
var uiMu sync.Mutex

//nolint:gochecknoglobals // Terminal panel state, guarded by uiMu.
var (
	panelActive bool     // scrolling region installed and a panel is pinned
	panelLines  []string // panel content as last rendered (with colour, uncapped)
	panelRows   int      // rows the panel currently occupies on screen
	termRows    int      // last-observed terminal height
	termCols    int      // last-observed terminal width
)

// terminalSize returns the current (cols, rows) of the stderr terminal, or
// ok=false when the size cannot be determined (e.g. not a terminal). It is a
// package variable so tests can inject a fixed size.
//
//nolint:gochecknoglobals
var terminalSize = func() (cols, rows int, ok bool) {
	w, h, err := term.GetSize(os.Stderr.Fd())
	if err != nil || w <= 0 || h <= 0 {
		return 0, 0, false
	}
	return w, h, true
}

// capPanelLines trims the panel to fit the rows available above panelMinLogRows,
// keeping the summary line and as many channel rows as fit and replacing the
// remainder with a single "and N more" note, so a many-channel pass can never
// grow the panel past its budget.
func capPanelLines(lines []string, rows int) []string {
	budget := rows - panelMinLogRows
	if budget < 1 {
		budget = 1
	}
	if len(lines) <= budget {
		return lines
	}
	kept := make([]string, 0, budget)
	kept = append(kept, lines[:budget-1]...)
	hidden := len(lines) - (budget - 1)
	kept = append(kept, paint(ui.dim, fmt.Sprintf("  ... and %d more", hidden)))
	return kept
}

// installPanelLocked reserves h rows at the bottom of the screen for the panel
// and confines scrolling to the rows above it. Caller holds uiMu.
func installPanelLocked(rows, h int) {
	// Scroll the existing screen up by h lines so the bottom h rows are blank and
	// no prior content is overwritten by the panel.
	fmt.Fprintf(uiOut, "\033[%d;1H", rows) // move to the last row
	fmt.Fprint(uiOut, strings.Repeat("\n", h))
	// Scrolling region = rows 1..(rows-h); the panel owns rows (rows-h+1)..rows.
	fmt.Fprintf(uiOut, "\033[1;%dr", rows-h)
	// Park the cursor at the bottom of the scrolling region so ordinary output
	// lands there and scrolls upward.
	fmt.Fprintf(uiOut, "\033[%d;1H", rows-h)
	panelActive = true
	panelRows = h
	termRows = rows
}

// teardownPanelLocked removes the scrolling region and clears the panel rows so
// closing output prints cleanly where the panel was. Caller holds uiMu.
func teardownPanelLocked() {
	if !panelActive {
		return
	}
	fmt.Fprint(uiOut, "\033[r")                                  // reset region to full screen
	fmt.Fprintf(uiOut, "\033[%d;1H\033[J", termRows-panelRows+1) // clear from panel top down
	panelActive = false
	panelRows = 0
	panelLines = nil
}

// drawPanelLocked repaints the pinned panel rows in place, saving and restoring
// the scrolling-region cursor so ordinary output is undisturbed. Caller holds
// uiMu and must have a panel installed.
func drawPanelLocked() {
	top := termRows - panelRows + 1
	fmt.Fprint(uiOut, "\x1b7") // DECSC: save cursor + attributes
	for i, l := range panelLines {
		fmt.Fprintf(uiOut, "\033[%d;1H\033[K%s", top+i, truncateToWidth(l, termCols))
	}
	fmt.Fprint(uiOut, "\x1b8") // DECRC: restore cursor + attributes
}

// clearStatusBlock removes the live panel (if any). Used on shutdown so the
// closing summary prints where the dashboard was.
func clearStatusBlock() {
	uiMu.Lock()
	defer uiMu.Unlock()
	teardownPanelLocked()
}

// renderStatusBlock repaints the live dashboard in its pinned bottom region,
// installing or resizing the region as the terminal geometry or panel height
// changes. It is a no-op when the terminal is not colour-capable (piped/non-TTY)
// or too small, keeping logs clean.
func renderStatusBlock(lines []string) {
	if !uiColor || len(lines) == 0 {
		return
	}
	uiMu.Lock()
	defer uiMu.Unlock()

	cols, rows, ok := terminalSize()
	if !ok || rows < panelMinTerminalRows {
		// Cannot reserve a region safely; skip the panel. Ordinary output still
		// prints, so nothing is lost; there is just no pinned dashboard.
		if panelActive {
			teardownPanelLocked()
		}
		return
	}

	capped := capPanelLines(lines, rows)
	// (Re)install the region whenever the geometry it depends on changes.
	if !panelActive || rows != termRows || len(capped) != panelRows {
		if panelActive {
			teardownPanelLocked()
		}
		termCols = cols
		installPanelLocked(rows, len(capped))
	}
	termCols, termRows = cols, rows
	panelLines = capped
	drawPanelLocked()
}

// emitLine writes one permanent line into the scrolling region above the panel.
func emitLine(s string) {
	initUI()
	uiMu.Lock()
	defer uiMu.Unlock()
	writePermanentLocked(uiOut, []byte(s+"\n"))
}

// writePermanentLocked writes ordinary output p (a reader log line, narration, or
// the interactive prompt) into the scrolling region above the pinned panel. When
// no panel is active it writes straight through, so setup output and non-TTY runs
// behave exactly as before. Caller holds uiMu.
func writePermanentLocked(w io.Writer, p []byte) (int, error) {
	if !panelActive {
		return w.Write(p)
	}
	// Position at the bottom row of the scrolling region so the write scrolls the
	// log area upward; the panel rows sit below the region and are left untouched.
	fmt.Fprintf(uiOut, "\033[%d;1H", termRows-panelRows)
	return w.Write(p)
}

// statusClearingWriter routes writes (e.g. the standard log package used by the
// readers) into the scrolling region above the live panel, so reader log lines
// scroll cleanly without disturbing the pinned dashboard. Installed via
// log.SetOutput for the duration of a stream.
type statusClearingWriter struct{ w io.Writer }

func (sw statusClearingWriter) Write(p []byte) (int, error) {
	uiMu.Lock()
	defer uiMu.Unlock()
	return writePermanentLocked(sw.w, p)
}

// truncateToWidth shortens s so its visible width (ignoring ANSI escape
// sequences, which have zero width) does not exceed max columns. This keeps every
// panel line on a single physical row: a wrapped line would occupy two rows and
// break the fixed-height region, reintroducing the drift this panel exists to
// avoid. A reset is appended when the string is cut so colour never bleeds.
func truncateToWidth(s string, maxWidth int) string {
	if maxWidth <= 0 {
		return ""
	}
	var b strings.Builder
	width := 0
	i := 0
	truncated := false
	for i < len(s) {
		if s[i] == 0x1b { // ESC: copy the whole escape sequence, counting no width
			j := i + 1
			if j < len(s) && s[j] == '[' { // CSI: ESC [ params ... final byte 0x40-0x7e
				j++
				for j < len(s) && (s[j] < 0x40 || s[j] > 0x7e) {
					j++
				}
				if j < len(s) {
					j++ // include the final byte
				}
			}
			b.WriteString(s[i:j])
			i = j
			continue
		}
		if width+1 > maxWidth {
			truncated = true
			break
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		b.WriteRune(r)
		width++
		i += size
	}
	if truncated {
		b.WriteString("\033[0m")
	}
	return b.String()
}

// The uiXxx helpers all emit one line to stderr with a leading symbol so the
// nature of the line (progress / success / warning / error) is obvious at a
// glance. They take printf-style arguments.

func uiStepf(format string, a ...interface{}) {
	initUI()
	emitLine(paint(ui.cyan, fmt.Sprintf(format, a...)))
}

func uiOKf(format string, a ...interface{}) {
	initUI()
	emitLine(paint(ui.green, fmt.Sprintf(format, a...)))
}

// uiSpinner starts an animated, transient status line for a long-running step
// so the operator can see the CLI is working and not frozen. The message is
// shown with a growing run of trailing dots that cycles "msg." -> "msg.." ->
// "msg..." roughly twice a second. It returns a stop function that halts the
// animation and erases the line; call it exactly once, before emitting the next
// permanent line.
//
// When animation is not appropriate, because colour is disabled (not a
// terminal, or NO_COLOR set) or --verbose/--debug is on (where the line would
// fight with detailed log output), the message is printed once as a plain permanent line
// and stop is a no-op, so piped output and verbose logs stay clean.
func uiSpinner(msg string) (stop func()) {
	initUI()
	if !uiColor || uiVerbose {
		emitLine(paint(ui.cyan, msg+"..."))
		return func() {}
	}
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		dots := 1
		render := func() {
			fmt.Fprintf(uiOut, "\r\033[K%s", paint(ui.cyan, msg+strings.Repeat(".", dots)))
		}
		render()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				if dots++; dots > 3 {
					dots = 1
				}
				render()
			}
		}
	}()
	return func() {
		close(done)
		wg.Wait()
		fmt.Fprint(uiOut, "\r\033[K") // erase the spinner line
	}
}

func uiWarnf(format string, a ...interface{}) {
	initUI()
	emitLine(paint(ui.yellow, "Warning: "+fmt.Sprintf(format, a...)))
}

func uiErrf(format string, a ...interface{}) {
	initUI()
	emitLine(paint(ui.red, "Error: "+fmt.Sprintf(format, a...)))
}

func uiInfof(format string, a ...interface{}) {
	initUI()
	emitLine("  " + fmt.Sprintf(format, a...))
}

func uiDimf(format string, a ...interface{}) {
	initUI()
	emitLine(paint(ui.dim, fmt.Sprintf(format, a...)))
}

// uiCountdownf prints an attention-grabbing, bold+colored line used by the
// idle-shutdown warnings. severity escalates the color: 0 notice (yellow),
// 1 warning (magenta), 2 urgent (red). The bold code precedes the color code so
// both remain active for the whole line (paint appends a single reset).
func uiCountdownf(severity int, format string, a ...interface{}) {
	initUI()
	color := ui.yellow
	switch {
	case severity >= 2:
		color = ui.red
	case severity == 1:
		color = ui.magenta
	}
	emitLine(paint(ui.bold+color, fmt.Sprintf(format, a...)))
}

// uiHeadingf prints a bold section title preceded by a blank line.
func uiHeadingf(format string, a ...interface{}) {
	initUI()
	emitLine("\n" + paint(ui.bold, fmt.Sprintf(format, a...)))
}

// dataKind identifies a stream data type so it can be given a consistent
// coloured label wherever it appears (live counters, per-message notices,
// summary). The label is shown in brackets, e.g. "[Telemetry]".
type dataKind struct {
	label string
	color string
}

//nolint:gochecknoglobals // Fixed catalogue of the stream data types.
var (
	kindTelemetry  = dataKind{"Telemetry", "\033[36m"}  // cyan
	kindMonitoring = dataKind{"Monitoring", "\033[34m"} // blue
	kindEvent      = dataKind{"Events", "\033[35m"}     // magenta
	kindConfig     = dataKind{"Config", "\033[33m"}     // yellow
	kindAck        = dataKind{"Acks", "\033[32m"}       // green
	kindUplink     = dataKind{"Uplink", "\033[35m"}     // magenta
)

// tag renders a coloured "[Label]" badge for a data kind.
func (k dataKind) tag() string {
	initUI()
	return paint(k.color, "["+k.label+"]")
}

// uiDataf prints a one-line notice attributed to a specific data kind, e.g.
//
//	[Telemetry]  receiving on channel abcd
func uiDataf(k dataKind, format string, a ...interface{}) {
	initUI()
	emitLine(k.tag() + " " + fmt.Sprintf(format, a...))
}

// ---- Streaming verbosity ---------------------------------------------------
//
// open-stream is chatty under the hood (credential fetches, MQTT/TLS setup, S3
// scanning). By default a satellite operator only wants the story: connecting,
// connected, receiving data, done. The deep technical detail is kept for
// --verbose / --debug. These package-level switches gate that detail.

//nolint:gochecknoglobals // Set once at the start of a streaming run.
var uiVerbose bool

// configureStreamOutput records the chosen verbosity and tidies the standard
// logger. In normal use log lines carry no noisy date/time prefix (the CLI
// speaks in plain sentences); with --verbose/--debug the timestamp is restored
// because it's useful when diagnosing a live pass.
func configureStreamOutput(verbose, debug bool) {
	initUI()
	uiVerbose = verbose || debug
	if uiVerbose {
		log.SetFlags(log.Ltime)
	} else {
		log.SetFlags(0)
	}
}

// vlogf prints a technical detail line, but only when --verbose/--debug is on.
// Use it for under-the-hood plumbing that would otherwise clutter the output.
func vlogf(format string, a ...interface{}) {
	if uiVerbose {
		log.Printf(format, a...)
	}
}

// humanizeError rewrites cobra's terse built-in messages into plain guidance
// that says what the operator needs to supply. Messages it does not recognise
// are returned unchanged.
func humanizeError(err error) string {
	msg := err.Error()
	const reqPrefix, reqSuffix = "required flag(s) ", " not set"
	if strings.HasPrefix(msg, reqPrefix) && strings.HasSuffix(msg, reqSuffix) {
		inner := strings.TrimSuffix(strings.TrimPrefix(msg, reqPrefix), reqSuffix)
		var names []string
		for _, part := range strings.Split(inner, ",") {
			name := strings.Trim(strings.TrimSpace(part), `"`)
			if name != "" {
				names = append(names, "--"+name)
			}
		}
		switch len(names) {
		case 0:
			// Fall through to the original message.
		case 1:
			return fmt.Sprintf("the %s option is required; add it and run the command again", names[0])
		default:
			return fmt.Sprintf(
				"these options are required; add them and run the command again: %s",
				strings.Join(names, ", "),
			)
		}
	}
	return msg
}

// isCancellation reports whether err is the normal result of the operator
// stopping the stream (Ctrl-C) rather than a genuine failure.
func isCancellation(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, context.Canceled) || strings.Contains(err.Error(), "context canceled")
}

// streamErrf reports a streaming error. When the error is just cancellation
// from a Ctrl-C shutdown it is demoted to verbose-only, so stopping a pass does
// not fill the screen with hundreds of "context canceled" lines.
func streamErrf(err error, format string, a ...interface{}) {
	if isCancellation(err) {
		vlogf(format, a...)
		return
	}
	log.Printf(format, a...)
}

// ---- Streaming banners & summary ------------------------------------------

// uiRule prints a dim horizontal divider the width of a typical terminal line.
func uiRule() {
	initUI()
	fmt.Fprintln(uiOut, paint(ui.dim, strings.Repeat("-", 60)))
}

// uiStreamBanner introduces a streaming session.
func uiStreamBanner(passID string) {
	initUI()
	fmt.Fprintln(uiOut)
	uiRule()
	fmt.Fprintf(uiOut, "%s\n", paint(ui.bold, "Live pass"))
	if passID != "" {
		uiDimf("Pass %s", passID)
	}
	uiRule()
}
