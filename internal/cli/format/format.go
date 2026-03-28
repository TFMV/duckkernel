package format

import (
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type TableFormatter struct {
	out     io.Writer
	headers []string
	rows    [][]interface{}
}

func NewTableFormatter(out io.Writer) *TableFormatter {
	return &TableFormatter{out: out}
}

func (t *TableFormatter) SetHeader(headers []string) {
	t.headers = headers
}

func (t *TableFormatter) AppendRow(row []interface{}) {
	t.rows = append(t.rows, row)
}

func (t *TableFormatter) Render() {
	if len(t.headers) == 0 {
		return
	}

	colWidths := make([]int, len(t.headers))
	for i, h := range t.headers {
		colWidths[i] = len(h)
	}

	for _, row := range t.rows {
		for i, cell := range row {
			if i >= len(colWidths) {
				break
			}
			str := formatValue(cell)
			if len(str) > colWidths[i] {
				colWidths[i] = len(str)
			}
		}
	}

	t.printSeparator(colWidths, '+', '-', '+')

	fmt.Fprintf(t.out, "|")
	for i, h := range t.headers {
		fmt.Fprintf(t.out, " %-*s |", colWidths[i], h)
	}
	fmt.Fprintln(t.out)

	t.printSeparator(colWidths, '+', '-', '+')

	for _, row := range t.rows {
		fmt.Fprintf(t.out, "|")
		for i := range t.headers {
			if i < len(row) {
				fmt.Fprintf(t.out, " %-*s |", colWidths[i], formatValue(row[i]))
			} else {
				fmt.Fprintf(t.out, " %-*s |", colWidths[i], "")
			}
		}
		fmt.Fprintln(t.out)
	}

	t.printSeparator(colWidths, '+', '-', '+')
}

func (t *TableFormatter) printSeparator(widths []int, corners, horiz, vert byte) {
	fmt.Fprintf(t.out, "%c", corners)
	for i, w := range widths {
		for j := 0; j < w+2; j++ {
			fmt.Fprintf(t.out, "%c", horiz)
		}
		if i < len(widths)-1 {
			fmt.Fprintf(t.out, "%c", vert)
		}
	}
	fmt.Fprintf(t.out, "%c\n", corners)
}

func formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}

	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", val)
	case uint, uint8, uint16, uint32, uint64:
		return fmt.Sprintf("%d", val)
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case time.Time:
		return val.Format("2006-01-02 15:04:05")
	default:
		s := fmt.Sprintf("%v", val)
		if len(s) > 50 {
			s = s[:47] + "..."
		}
		return s
	}
}

type JSONFormatter struct {
	out io.Writer
}

func NewJSONFormatter(out io.Writer) *JSONFormatter {
	return &JSONFormatter{out: out}
}

func (j *JSONFormatter) Write(data interface{}) error {
	str, err := toJSON(data)
	if err != nil {
		return err
	}
	_, err = j.out.Write([]byte(str))
	return err
}

func toJSON(v interface{}) (string, error) {
	if v == nil {
		return "null", nil
	}

	switch val := v.(type) {
	case string:
		return fmt.Sprintf("%q", val), nil
	case bool:
		return strconv.FormatBool(val), nil
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", val), nil
	case float32, float64:
		return fmt.Sprintf("%v", val), nil
	case []interface{}:
		items := make([]string, 0, len(val))
		for _, item := range val {
			s, err := toJSON(item)
			if err != nil {
				return "", err
			}
			items = append(items, s)
		}
		return "[" + strings.Join(items, ", ") + "]", nil
	case map[string]interface{}:
		pairs := make([]string, 0, len(val))
		for k, item := range val {
			s, err := toJSON(item)
			if err != nil {
				return "", err
			}
			pairs = append(pairs, fmt.Sprintf("%q: %s", k, s))
		}
		return "{" + strings.Join(pairs, ", ") + "}", nil
	default:
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Slice {
			items := make([]string, 0, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				s, err := toJSON(rv.Index(i).Interface())
				if err != nil {
					return "", err
				}
				items = append(items, s)
			}
			return "[" + strings.Join(items, ", ") + "]", nil
		}
		return fmt.Sprintf("%q", fmt.Sprintf("%v", v)), nil
	}
}

type MarkdownFormatter struct {
	out io.Writer
}

func NewMarkdownFormatter(out io.Writer) *MarkdownFormatter {
	return &MarkdownFormatter{out: out}
}

func (m *MarkdownFormatter) Write(headers []string, rows [][]interface{}) {
	if len(headers) == 0 {
		return
	}

	fmt.Fprintf(m.out, "|")
	for _, h := range headers {
		fmt.Fprintf(m.out, " %s |", h)
	}
	fmt.Fprintln(m.out)

	fmt.Fprintf(m.out, "|")
	for range headers {
		fmt.Fprintf(m.out, " --- |")
	}
	fmt.Fprintln(m.out)

	for _, row := range rows {
		fmt.Fprintf(m.out, "|")
		for i := range headers {
			if i < len(row) {
				fmt.Fprintf(m.out, " %s |", formatValue(row[i]))
			} else {
				fmt.Fprintf(m.out, " |")
			}
		}
		fmt.Fprintln(m.out)
	}
}
