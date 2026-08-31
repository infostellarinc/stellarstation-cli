// Package printer provides tabular, CSV, and JSON output formatters for CLI
// commands. Each CRUD subcommand defines a set of Column descriptors and calls
// Print to render the data in the format selected by the --output flag.
package printer

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
)

// Format is an output format identifier.
type Format string

const (
	FormatWide Format = "wide"
	FormatCSV  Format = "csv"
	FormatJSON Format = "json"
)

// ParseFormat normalises a user-supplied string to a Format constant.
// Returns an error for unrecognised values.
func ParseFormat(s string) (Format, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "wide":
		return FormatWide, nil
	case "csv":
		return FormatCSV, nil
	case "json":
		return FormatJSON, nil
	default:
		return "", fmt.Errorf("%q is not a supported display format. Use one of: wide, csv, json", s)
	}
}

// Column describes a single output column. Extract converts a row (typically
// a struct pointer) to its display string.
type Column struct {
	Header  string
	Extract func(row interface{}) string
}

// Print renders rows in the requested format to w. For JSON the raw rows slice
// is marshalled directly, ignoring column definitions.
func Print(w io.Writer, format Format, columns []Column, rows []interface{}) error {
	switch format {
	case FormatJSON:
		return printJSON(w, rows)
	case FormatCSV:
		return printCSV(w, columns, rows)
	case FormatWide:
		return printWide(w, columns, rows)
	default:
		return printWide(w, columns, rows)
	}
}

func printJSON(w io.Writer, rows []interface{}) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(rows)
}

func printCSV(w io.Writer, columns []Column, rows []interface{}) error {
	cw := csv.NewWriter(w)
	headers := make([]string, len(columns))
	for i, c := range columns {
		headers[i] = c.Header
	}
	if err := cw.Write(headers); err != nil {
		return err
	}
	record := make([]string, len(columns))
	for _, row := range rows {
		for i, c := range columns {
			record[i] = c.Extract(row)
		}
		if err := cw.Write(record); err != nil {
			return err
		}
	}
	cw.Flush()
	return cw.Error()
}

func printWide(w io.Writer, columns []Column, rows []interface{}) error {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	headers := make([]string, len(columns))
	for i, c := range columns {
		headers[i] = c.Header
	}
	fmt.Fprintln(tw, strings.Join(headers, "\t"))

	for _, row := range rows {
		vals := make([]string, len(columns))
		for i, c := range columns {
			vals[i] = c.Extract(row)
		}
		fmt.Fprintln(tw, strings.Join(vals, "\t"))
	}
	return tw.Flush()
}
