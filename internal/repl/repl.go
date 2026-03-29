package repl

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/TFMV/duckkernel/internal/cli/format"
	"github.com/TFMV/duckkernel/internal/execution/runtime"
	"github.com/TFMV/duckkernel/internal/execution/stream"
)

type KernelClient interface {
	CreateDataset(name, sql string) error
	RunDataset(name string) error
	PreviewDataset(name string) (stream.RecordStream, error)
	GetGraph() string
	GetDataset(name string) (string, error)
	ListDatasets() []string
	DropDataset(name string) error
	ExecuteSQL(sql string) (stream.RecordStream, error)
}

type REPL struct {
	client  KernelClient
	in      *bufio.Reader
	out     io.Writer
	vars    map[string]string
	history []string
	debug   bool
}

func New(client KernelClient, in io.Reader, out io.Writer, debug bool) *REPL {
	return &REPL{
		client: client,
		in:     bufio.NewReader(in),
		out:    out,
		vars:   make(map[string]string),
		debug:  debug,
	}
}

func (r *REPL) Run(ctx context.Context) error {
	fmt.Fprintf(r.out, "DuckKernel REPL v0.1 (type 'help' for commands, 'exit' to quit)\n")
	fmt.Fprintf(r.out, "-------------------------------------------\n")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line, err := r.readMultiLine()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		r.addToHistory(line)

		if err := r.processLine(ctx, line); err != nil {
			fmt.Fprintf(r.out, "Error: %v\n", err)
		}
	}
}

func (r *REPL) readMultiLine() (string, error) {
	fmt.Fprintf(r.out, "> ")
	firstLine, err := r.readLine()
	if err != nil {
		return "", err
	}

	firstLine = strings.TrimSpace(firstLine)

	if !r.isMultiLineStart(firstLine) {
		return firstLine, nil
	}

	if strings.HasSuffix(firstLine, ";") {
		return firstLine, nil
	}

	var lines []string
	lines = append(lines, firstLine)

	for {
		fmt.Fprintf(r.out, "| ")
		line, err := r.readLine()
		if err != nil {
			return "", err
		}

		lines = append(lines, line)

		trimmed := strings.TrimSpace(line)
		if strings.HasSuffix(trimmed, ";") {
			break
		}
	}

	return strings.Join(lines, "\n"), nil
}

func (r *REPL) isMultiLineStart(line string) bool {
	upper := strings.ToUpper(strings.TrimSpace(line))
	multiStarts := []string{"WITH ", "INSERT ", "UPDATE ", "DELETE ", "CREATE ", "ALTER "}
	for _, start := range multiStarts {
		if strings.HasPrefix(upper, start) {
			return true
		}
	}
	return false
}

func (r *REPL) addToHistory(line string) {
	r.history = append(r.history, line)
	if len(r.history) > 1000 {
		r.history = r.history[len(r.history)-1000:]
	}
}

func (r *REPL) History() []string {
	return r.history
}

func (r *REPL) readLine() (string, error) {
	line, err := r.in.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(line, "\n"), nil
}

func (r *REPL) processLine(ctx context.Context, line string) error {
	if strings.HasPrefix(line, "\\") || strings.HasPrefix(line, ":") {
		return r.processCommand(ctx, line[1:])
	}

	assignMatch := regexp.MustCompile(`^(\w+)\s*=\s*(.+)$`).FindStringSubmatch(line)
	if assignMatch != nil {
		return r.processAssignment(ctx, assignMatch[1], assignMatch[2])
	}

	return r.processQuery(ctx, line)
}

func (r *REPL) processCommand(ctx context.Context, cmd string) error {
	parts := strings.Fields(cmd)
	if len(parts) == 0 {
		return fmt.Errorf("empty command")
	}

	switch parts[0] {
	case "help", "h":
		return r.printHelp()
	case "exit", "quit", "q":
		return io.EOF
	case "list", "ls":
		return r.listDatasets()
	case "graph", "g":
		return r.showGraph()
	case "clear":
		fmt.Fprint(r.out, "\033[2J\033[H")
		return nil
	case "vars":
		return r.showVars()
	default:
		return fmt.Errorf("unknown command: %s", parts[0])
	}
}

func (r *REPL) processAssignment(ctx context.Context, name, sql string) error {
	sql = strings.TrimSpace(sql)
	if sql == "" {
		return fmt.Errorf("empty SQL assignment")
	}

	r.vars[name] = sql

	err := r.client.CreateDataset(name, sql)
	if err != nil {
		delete(r.vars, name)
		return fmt.Errorf("failed to create dataset: %w", err)
	}

	fmt.Fprintf(r.out, "dataset=%s created\n", name)
	return nil
}

func (r *REPL) processQuery(ctx context.Context, sql string) error {
	sql = strings.TrimSpace(sql)

	stream, err := r.client.ExecuteSQL(sql)
	if err != nil {
		return err
	}
	defer stream.Close()

	cols, err := stream.Columns()
	if err != nil {
		return err
	}

	format := format.NewTableFormatter(r.out)
	format.SetHeader(cols)

	rowCount := 0
	limit := 100
	for stream.Next() && rowCount < limit {
		record := stream.Record()
		if record == nil {
			break
		}
		row := make([]interface{}, len(cols))
		for i, col := range cols {
			row[i] = record[col]
		}
		format.AppendRow(row)
		rowCount++
	}

	if err := stream.Err(); err != nil {
		return err
	}

	format.Render()
	fmt.Fprintf(r.out, "\n(%d rows)\n", rowCount)
	return nil
}

func (r *REPL) printHelp() error {
	help := `
Commands:
  help, h              Show this help
  exit, quit, q        Exit the REPL
  list, ls             List all datasets
  graph, g             Show dataset graph
  clear                Clear screen
  vars                 Show variable bindings

SQL:
  Directly execute SQL queries
  name = SELECT ...    Create named dataset

Examples:
  users = SELECT * FROM read_csv('users.csv')
  SELECT * FROM users WHERE active = true
  preview users
`
	fmt.Fprint(r.out, help)
	return nil
}

func (r *REPL) listDatasets() error {
	datasets := r.client.ListDatasets()
	if len(datasets) == 0 {
		fmt.Fprintln(r.out, "No datasets")
		return nil
	}
	fmt.Fprintln(r.out, "Datasets:")
	for _, ds := range datasets {
		fmt.Fprintf(r.out, "  %s\n", ds)
	}
	return nil
}

func (r *REPL) showGraph() error {
	graph := r.client.GetGraph()
	if graph == "" {
		fmt.Fprintln(r.out, "(empty graph)")
		return nil
	}
	fmt.Fprintln(r.out, graph)
	return nil
}

func (r *REPL) showVars() error {
	if len(r.vars) == 0 {
		fmt.Fprintln(r.out, "No variables")
		return nil
	}
	fmt.Fprintln(r.out, "Variables:")
	for name, sql := range r.vars {
		fmt.Fprintf(r.out, "  %s = %s\n", name, sql)
	}
	return nil
}

func (r *REPL) RunFile(ctx context.Context, path string) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}

		if err := r.processLine(ctx, line); err != nil {
			return fmt.Errorf("error processing line %q: %w", line, err)
		}
	}

	return scanner.Err()
}

type runtimeClient struct {
	rt *runtime.Runtime
}

func NewRuntimeClient(rt *runtime.Runtime) KernelClient {
	return &runtimeClient{rt: rt}
}

func (c *runtimeClient) CreateDataset(name, sql string) error {
	plan := runtime.ExecutionPlan{
		Nodes: []runtime.PlanNode{
			{
				NodeID: name,
				SQL:    sql,
				Action: runtime.ActionCompute,
			},
		},
	}
	_, err := c.rt.ExecutePlan(context.Background(), plan)
	return err
}

func (c *runtimeClient) RunDataset(name string) error {
	plan := runtime.ExecutionPlan{
		Nodes: []runtime.PlanNode{
			{
				NodeID: name,
				SQL:    fmt.Sprintf("SELECT * FROM dk_%s", name),
				Action: runtime.ActionCompute,
			},
		},
	}
	_, err := c.rt.ExecutePlan(context.Background(), plan)
	return err
}

func (c *runtimeClient) PreviewDataset(name string) (stream.RecordStream, error) {
	return c.rt.StreamNode(context.Background(), name)
}

func (c *runtimeClient) GetGraph() string {
	ctx := context.Background()
	stream, err := c.rt.ExecuteSQL(ctx, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' AND table_name LIKE 'dk_%'")
	if err != nil {
		return ""
	}
	defer stream.Close()

	var nodes []string
	cols, _ := stream.Columns()
	if len(cols) > 0 {
		for stream.Next() {
			record := stream.Record()
			if name, ok := record["table_name"]; ok {
				if s, ok := name.(string); ok {
					nodes = append(nodes, s)
				}
			}
		}
	}

	if len(nodes) == 0 {
		return "(empty graph)"
	}

	result := "Datasets:\n"
	for _, n := range nodes {
		result += "  " + n + "\n"
	}
	return result
}

func (c *runtimeClient) GetDataset(name string) (string, error) {
	ctx := context.Background()
	stream, err := c.rt.ExecuteSQL(ctx, fmt.Sprintf("SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'main' AND table_name = '%s'", name))
	if err != nil {
		return "", err
	}
	defer stream.Close()

	cols, _ := stream.Columns()
	if len(cols) > 0 && stream.Next() {
		return fmt.Sprintf("name: %s", name), nil
	}
	return "", fmt.Errorf("dataset not found: %s", name)
}

func (c *runtimeClient) ListDatasets() []string {
	ctx := context.Background()
	stream, err := c.rt.ExecuteSQL(ctx, "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' AND table_name LIKE 'dk_%'")
	if err != nil {
		return []string{}
	}
	defer stream.Close()

	var datasets []string
	cols, _ := stream.Columns()
	if len(cols) > 0 {
		for stream.Next() {
			record := stream.Record()
			if name, ok := record["table_name"]; ok {
				if s, ok := name.(string); ok {
					datasets = append(datasets, s)
				}
			}
		}
	}
	return datasets
}

func (c *runtimeClient) DropDataset(name string) error {
	return nil
}

func (c *runtimeClient) ExecuteSQL(sql string) (stream.RecordStream, error) {
	return c.rt.ExecuteSQL(context.Background(), sql)
}

func StartWithKernel(rt *runtime.Runtime, in io.Reader, out io.Writer, debug bool) error {
	client := NewRuntimeClient(rt)
	repl := New(client, in, out, debug)
	return repl.Run(context.Background())
}

func WaitForInput(ctx context.Context, in *bufio.Reader, timeout time.Duration) (string, error) {
	result := make(chan string, 1)

	go func() {
		line, err := in.ReadString('\n')
		if err != nil {
			return
		}
		result <- strings.TrimSuffix(line, "\n")
	}()

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case line := <-result:
		return line, nil
	case <-time.After(timeout):
		return "", fmt.Errorf("timeout waiting for input")
	}
}
