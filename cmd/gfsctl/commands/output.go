package commands

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// Formatter provides output formatting utilities.
type Formatter struct {
	w      io.Writer
	format string
}

// NewFormatter creates a new formatter with the given writer and format.
func NewFormatter(w io.Writer, format string) *Formatter {
	return &Formatter{w: w, format: format}
}

// IsJSON returns true if the output format is JSON.
func (f *Formatter) IsJSON() bool {
	return f.format == "json"
}

// JSON outputs the value as formatted JSON.
func (f *Formatter) JSON(v interface{}) error {
	enc := json.NewEncoder(f.w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

// Print prints a simple message.
func (f *Formatter) Print(format string, args ...interface{}) {
	fmt.Fprintf(f.w, format, args...)
}

// Println prints a message with a newline.
func (f *Formatter) Println(args ...interface{}) {
	fmt.Fprintln(f.w, args...)
}

// Printf prints a formatted message.
func (f *Formatter) Printf(format string, args ...interface{}) {
	fmt.Fprintf(f.w, format, args...)
}

// Header prints a section header.
func (f *Formatter) Header(title string) {
	fmt.Fprintf(f.w, "\n%s\n", title)
	fmt.Fprintf(f.w, "%s\n", strings.Repeat("─", len(title)))
}

// KeyValue prints a key-value pair with proper alignment.
func (f *Formatter) KeyValue(key string, value interface{}) {
	fmt.Fprintf(f.w, "  %-20s %v\n", key+":", value)
}

// KeyValueColor prints a key-value pair with color for status values.
func (f *Formatter) KeyValueColor(key string, value string, status string) {
	colored := colorize(value, status)
	fmt.Fprintf(f.w, "  %-20s %s\n", key+":", colored)
}

// Table prints a simple table with headers and rows.
type Table struct {
	f       *Formatter
	headers []string
	widths  []int
	rows    [][]string
}

// NewTable creates a new table with the given headers.
func (f *Formatter) NewTable(headers ...string) *Table {
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = len(h)
	}
	return &Table{
		f:       f,
		headers: headers,
		widths:  widths,
	}
}

// AddRow adds a row to the table.
func (t *Table) AddRow(values ...string) {
	// Pad values if fewer than headers
	for len(values) < len(t.headers) {
		values = append(values, "")
	}
	// Truncate if more than headers
	if len(values) > len(t.headers) {
		values = values[:len(t.headers)]
	}
	// Update widths
	for i, v := range values {
		if len(v) > t.widths[i] {
			t.widths[i] = len(v)
		}
	}
	t.rows = append(t.rows, values)
}

// Render prints the table.
func (t *Table) Render() {
	if len(t.rows) == 0 {
		fmt.Fprintln(t.f.w, "  (no data)")
		return
	}

	// Print headers
	fmt.Fprint(t.f.w, "  ")
	for i, h := range t.headers {
		fmt.Fprintf(t.f.w, "%-*s  ", t.widths[i], h)
	}
	fmt.Fprintln(t.f.w)

	// Print separator
	fmt.Fprint(t.f.w, "  ")
	for i := range t.headers {
		fmt.Fprintf(t.f.w, "%s  ", strings.Repeat("─", t.widths[i]))
	}
	fmt.Fprintln(t.f.w)

	// Print rows
	for _, row := range t.rows {
		fmt.Fprint(t.f.w, "  ")
		for i, v := range row {
			fmt.Fprintf(t.f.w, "%-*s  ", t.widths[i], v)
		}
		fmt.Fprintln(t.f.w)
	}
}

// ANSI color codes
const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorCyan   = "\033[36m"
)

// colorize returns a colored string based on the status.
func colorize(value, status string) string {
	switch status {
	case "ok", "healthy", "active", "leader", "mounted", "open":
		return colorGreen + value + colorReset
	case "degraded", "warning", "follower":
		return colorYellow + value + colorReset
	case "unhealthy", "error", "failed", "closed":
		return colorRed + value + colorReset
	case "info":
		return colorCyan + value + colorReset
	default:
		return value
	}
}

// FormatBytes formats bytes in human-readable form.
func FormatBytes(bytes uint64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
	)

	switch {
	case bytes >= TB:
		return fmt.Sprintf("%.2f TB", float64(bytes)/TB)
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// FormatDuration formats seconds in human-readable form.
func FormatDuration(seconds int64) string {
	if seconds < 60 {
		return fmt.Sprintf("%ds", seconds)
	}
	if seconds < 3600 {
		return fmt.Sprintf("%dm %ds", seconds/60, seconds%60)
	}
	if seconds < 86400 {
		hours := seconds / 3600
		mins := (seconds % 3600) / 60
		return fmt.Sprintf("%dh %dm", hours, mins)
	}
	days := seconds / 86400
	hours := (seconds % 86400) / 3600
	return fmt.Sprintf("%dd %dh", days, hours)
}

// FormatPercent formats a percentage with appropriate precision.
func FormatPercent(pct float64) string {
	if pct < 10 {
		return fmt.Sprintf("%.2f%%", pct)
	}
	return fmt.Sprintf("%.1f%%", pct)
}
