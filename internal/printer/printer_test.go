package printer

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
)

type testRow struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}

var testColumns = []Column{ //nolint:gochecknoglobals
	{Header: "NAME", Extract: func(r interface{}) string { return r.(*testRow).Name }},
	{Header: "AGE", Extract: func(r interface{}) string { return strconv.Itoa(r.(*testRow).Age) }},
}

func makeRows() []interface{} {
	return []interface{}{
		&testRow{Name: "Alice", Age: 30},
		&testRow{Name: "Bob", Age: 25},
	}
}

func TestPrintWide(t *testing.T) {
	var buf bytes.Buffer
	err := Print(&buf, FormatWide, testColumns, makeRows())
	if err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, "NAME") || !strings.Contains(out, "AGE") {
		t.Errorf("missing headers in: %s", out)
	}
	if !strings.Contains(out, "Alice") || !strings.Contains(out, "Bob") {
		t.Errorf("missing row data in: %s", out)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 3 {
		t.Errorf("expected 3 lines (header + 2 rows), got %d", len(lines))
	}
}

func TestPrintCSV(t *testing.T) {
	var buf bytes.Buffer
	err := Print(&buf, FormatCSV, testColumns, makeRows())
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 3 {
		t.Errorf("expected 3 lines, got %d", len(lines))
	}
	if lines[0] != "NAME,AGE" {
		t.Errorf("header = %q, want NAME,AGE", lines[0])
	}
	if lines[1] != "Alice,30" {
		t.Errorf("row1 = %q, want Alice,30", lines[1])
	}
}

func TestPrintJSON(t *testing.T) {
	var buf bytes.Buffer
	err := Print(&buf, FormatJSON, testColumns, makeRows())
	if err != nil {
		t.Fatal(err)
	}
	var result []testRow
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result))
	}
	if result[0].Name != "Alice" || result[0].Age != 30 {
		t.Errorf("row[0] = %+v, want Alice/30", result[0])
	}
}

func TestParseFormat(t *testing.T) {
	tests := []struct {
		input string
		want  Format
		err   bool
	}{
		{"", FormatWide, false},
		{"wide", FormatWide, false},
		{"WIDE", FormatWide, false},
		{"csv", FormatCSV, false},
		{"CSV", FormatCSV, false},
		{"json", FormatJSON, false},
		{"JSON", FormatJSON, false},
		{" json ", FormatJSON, false},
		{"xml", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseFormat(tt.input)
			if (err != nil) != tt.err {
				t.Errorf("ParseFormat(%q) error = %v, wantErr %v", tt.input, err, tt.err)
				return
			}
			if got != tt.want {
				t.Errorf("ParseFormat(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestPrintEmptyRows(t *testing.T) {
	var buf bytes.Buffer
	err := Print(&buf, FormatWide, testColumns, nil)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 {
		t.Errorf("expected 1 line (header only), got %d", len(lines))
	}
}
