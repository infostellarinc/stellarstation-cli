package main

import (
	"bytes"
	"regexp"
	"strings"
	"testing"
)

// ansiEscape matches CSI/DECSC/DECRC escape sequences so tests can compare the
// visible text a panel line would show, independent of colour codes.
var ansiEscape = regexp.MustCompile(`\x1b(\[[0-9;]*[A-Za-z]|[78])`)

func visible(s string) string { return ansiEscape.ReplaceAllString(s, "") }

// withPanelTest installs a buffer as the UI output and a fixed terminal size,
// with colour forced on, and resets the panel state. It restores everything on
// cleanup so tests do not leak global state into each other.
func withPanelTest(t *testing.T, rows int) *bytes.Buffer {
	t.Helper()
	// Panel width is fixed for these tests; only the row count varies.
	const cols = 80
	// Consume the init-once so a later initUI() call cannot flip uiColor off
	// (stderr is not a terminal under `go test`).
	uiInitOnce.Do(func() {})

	prevColor, prevUI, prevOut, prevSizer := uiColor, ui, uiOut, terminalSize
	prevActive, prevRows, prevLines := panelActive, panelRows, panelLines
	prevTR, prevTC := termRows, termCols

	var buf bytes.Buffer
	uiColor = true
	ui = palette{
		reset: "\033[0m", bold: "\033[1m", dim: "\033[2m",
		red: "\033[31m", green: "\033[32m", yellow: "\033[33m",
		blue: "\033[34m", magenta: "\033[35m", cyan: "\033[36m",
	}
	uiOut = &buf
	terminalSize = func() (int, int, bool) { return cols, rows, true }
	panelActive, panelRows, panelLines, termRows, termCols = false, 0, nil, 0, 0

	t.Cleanup(func() {
		uiColor, ui, uiOut, terminalSize = prevColor, prevUI, prevOut, prevSizer
		panelActive, panelRows, panelLines = prevActive, prevRows, prevLines
		termRows, termCols = prevTR, prevTC
	})
	return &buf
}

func TestRenderStatusBlockInstallsRegionAndDrawsPanel(t *testing.T) {
	buf := withPanelTest(t, 24)

	renderStatusBlock([]string{"SUMMARY", "channel-row"})

	if !panelActive {
		t.Fatal("panel should be active after render")
	}
	if panelRows != 2 {
		t.Fatalf("panelRows = %d, want 2", panelRows)
	}
	if termRows != 24 {
		t.Fatalf("termRows = %d, want 24", termRows)
	}

	out := buf.String()
	// Scrolling region confined to rows 1..22 (24 - 2 panel rows).
	if !strings.Contains(out, "\033[1;22r") {
		t.Errorf("expected scroll region set to 1;22, got:\n%q", out)
	}
	// Panel drawn on rows 23 and 24, each cleared to end of line.
	for _, want := range []string{"\033[23;1H\033[K", "\033[24;1H\033[K"} {
		if !strings.Contains(out, want) {
			t.Errorf("expected panel row draw %q in:\n%q", want, out)
		}
	}
	// Cursor saved/restored around the panel repaint.
	if !strings.Contains(out, "\x1b7") || !strings.Contains(out, "\x1b8") {
		t.Errorf("expected DECSC/DECRC around panel draw, got:\n%q", out)
	}
}

func TestEmitLineWritesIntoScrollRegionAbovePanel(t *testing.T) {
	buf := withPanelTest(t, 24)
	renderStatusBlock([]string{"SUMMARY"}) // panel height 1 -> region 1..23
	buf.Reset()

	emitLine("a log line")

	out := buf.String()
	// The log line is positioned at the bottom of the scroll region (row 23) so it
	// scrolls above the pinned panel, and the panel rows are never targeted.
	if !strings.Contains(out, "\033[23;1H") {
		t.Errorf("expected cursor moved to scroll-region bottom (row 23), got:\n%q", out)
	}
	if !strings.Contains(out, "a log line\n") {
		t.Errorf("expected the log text, got:\n%q", out)
	}
	if strings.Contains(out, "\033[24;1H") {
		t.Errorf("log output must not target the panel row 24, got:\n%q", out)
	}
}

func TestStatusClearingWriterRoutesThroughRegion(t *testing.T) {
	buf := withPanelTest(t, 24)
	renderStatusBlock([]string{"SUMMARY"})
	buf.Reset()

	w := statusClearingWriter{w: uiOut}
	if _, err := w.Write([]byte("reader log\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "\033[23;1H") || !strings.Contains(out, "reader log\n") {
		t.Errorf("reader log should route into the scroll region, got:\n%q", out)
	}
}

func TestClearStatusBlockResetsRegion(t *testing.T) {
	buf := withPanelTest(t, 24)
	renderStatusBlock([]string{"SUMMARY", "row"})
	buf.Reset()

	clearStatusBlock()

	if panelActive {
		t.Fatal("panel should be inactive after clear")
	}
	out := buf.String()
	if !strings.Contains(out, "\033[r") {
		t.Errorf("expected scroll region reset (ESC[r), got:\n%q", out)
	}
	// The panel area is cleared from its top row (23) to end of screen.
	if !strings.Contains(out, "\033[23;1H\033[J") {
		t.Errorf("expected panel area cleared, got:\n%q", out)
	}
}

func TestRenderStatusBlockSkippedOnTinyTerminal(t *testing.T) {
	buf := withPanelTest(t, 5) // below panelMinTerminalRows

	renderStatusBlock([]string{"SUMMARY"})

	if panelActive {
		t.Fatal("panel must not activate on a tiny terminal")
	}
	if buf.Len() != 0 {
		t.Errorf("expected no output on tiny terminal, got:\n%q", buf.String())
	}
}

func TestCapPanelLinesFitsBudgetWithOverflowNote(t *testing.T) {
	withPanelTest(t, 10) // budget = 10 - panelMinLogRows(3) = 7
	lines := make([]string, 10)
	for i := range lines {
		lines[i] = "row"
	}
	got := capPanelLines(lines, 10)
	if len(got) != 7 {
		t.Fatalf("capped length = %d, want 7", len(got))
	}
	last := visible(got[len(got)-1])
	if !strings.Contains(last, "and 4 more") { // 10 - (7-1) = 4 hidden
		t.Errorf("expected overflow note 'and 4 more', got %q", last)
	}
}

func TestCapPanelLinesUnchangedWhenFits(t *testing.T) {
	withPanelTest(t, 24)
	lines := []string{"a", "b", "c"}
	got := capPanelLines(lines, 24)
	if len(got) != 3 {
		t.Fatalf("length = %d, want 3 (no capping)", len(got))
	}
}

func TestTruncateToWidthCountsVisibleRunesOnly(t *testing.T) {
	// 5 visible chars wrapped in colour codes; truncate to 3 visible columns.
	in := "\033[36mHELLO\033[0m"
	got := truncateToWidth(in, 3)
	if v := visible(got); v != "HEL" {
		t.Errorf("visible = %q, want %q", v, "HEL")
	}
	if !strings.Contains(got, "\033[36m") {
		t.Errorf("colour code should be preserved, got %q", got)
	}
	if !strings.HasSuffix(got, "\033[0m") {
		t.Errorf("truncated string should end with a reset, got %q", got)
	}
}

func TestTruncateToWidthLeavesShortStringsIntact(t *testing.T) {
	in := "abc"
	if got := truncateToWidth(in, 10); got != "abc" {
		t.Errorf("got %q, want %q", got, in)
	}
}
