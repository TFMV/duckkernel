package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/TFMV/duckkernel/internal/cli/format"
	"github.com/TFMV/duckkernel/internal/dataset"
	"github.com/TFMV/duckkernel/internal/execution/runtime"
	"github.com/TFMV/duckkernel/internal/kernel"
	"github.com/TFMV/duckkernel/internal/repl"
)

const defaultDBPath = "duckkernel.db"

func usage() {
	fmt.Fprintf(flag.CommandLine.Output(), `Usage:
  duckkernel [--debug] [--db <path>] <command> [args]

Commands:
  create <name> "<sql>"       Create a named dataset
  transform <name> "<sql>"   Create or update a derived dataset
  show <name>                 Show dataset metadata
  graph                       Print the dataset lineage graph
  recompute <name>            Recompute a dataset and invalidate dependents
  run <name>                  Run a dataset and display results
  list                        List registered datasets
  drop <name>                 Drop a dataset from the kernel
  query "<sql>"               Execute arbitrary SQL query
  preview <name>              Preview dataset (streaming)
  deps <name>                 Show dependencies of a dataset
  lineage <name>              Show downstream lineage
  explain <name>              Explain dataset metadata and plan
  plan <name>                 Show execution plan for dataset
  repl                        Start interactive REPL

Options:
  --debug                     Enable debug output
  --db <path>                 Path to persistent DuckDB file (default: %s)
  --format <format>           Output format: table, json, markdown
`, defaultDBPath)
}

type OutputFormat string

const (
	FormatTable    OutputFormat = "table"
	FormatJSON     OutputFormat = "json"
	FormatMarkdown OutputFormat = "markdown"
)

func Run(args []string) error {
	fs := flag.NewFlagSet("duckkernel", flag.ContinueOnError)
	debug := fs.Bool("debug", false, "enable debug output")
	dbPath := fs.String("db", defaultDBPath, "path to persistent DuckDB file")
	outputFormat := fs.String("format", "table", "output format: table, json, markdown")
	fs.Usage = usage

	if err := fs.Parse(args); err != nil {
		return err
	}

	tail := fs.Args()
	if len(tail) == 0 {
		usage()
		return nil
	}

	logger := log.New(os.Stderr, "duckkernel: ", log.LstdFlags)
	k, err := kernel.New(*dbPath, logger, *debug)
	if err != nil {
		return err
	}
	defer k.Close()

	cmd := tail[0]
	outFormat := parseFormat(*outputFormat)

	switch cmd {
	case "repl":
		return runREPL(*dbPath, logger, *debug)

	case "create", "transform":
		if len(tail) < 3 {
			return fmt.Errorf("%s requires <name> <sql>", cmd)
		}
		name := tail[1]
		sqlText := strings.Join(tail[2:], " ")
		mode := dataset.ModeCached
		ds, err := k.CreateOrUpdate(name, sqlText, mode)
		if err != nil {
			return err
		}
		fmt.Printf("dataset=%s version=%d mode=%s\n", ds.Name, ds.CurrentVersion.Version, ds.CurrentVersion.Mode)
		return nil

	case "show":
		if len(tail) != 2 {
			return errors.New("show requires <name>")
		}
		out, err := k.Show(tail[1])
		if err != nil {
			return err
		}
		fmt.Print(out)
		return nil

	case "graph":
		fmt.Println(k.Graph())
		return nil

	case "recompute":
		if len(tail) < 2 {
			return errors.New("recompute requires <name>")
		}
		force := false
		if len(tail) >= 2 && tail[1] == "--force" {
			force = true
		} else if len(tail) >= 3 && tail[2] == "--force" {
			force = true
		}
		name := tail[1]
		if force {
			name = tail[2]
		}
		var result *kernel.RecomputeResult
		var err error
		if force {
			result, err = k.ForceRecompute(name)
		} else {
			result, err = k.Recompute(name)
		}
		if err != nil {
			return err
		}
		printRecomputePlan(result)
		return nil

	case "list":
		for _, ds := range k.List() {
			fmt.Printf("%s (version=%d mode=%s)\n", ds.Name, ds.CurrentVersion.Version, ds.CurrentVersion.Mode)
		}
		return nil

	case "drop":
		if len(tail) != 2 {
			return errors.New("drop requires <name>")
		}
		if err := k.Drop(tail[1]); err != nil {
			return err
		}
		fmt.Printf("dropped=%s\n", tail[1])
		return nil

	case "query":
		if len(tail) < 2 {
			return errors.New("query requires <sql>")
		}
		sqlText := strings.Join(tail[1:], " ")
		return runQuery(*dbPath, logger, *debug, sqlText, outFormat)

	case "preview":
		if len(tail) != 2 {
			return errors.New("preview requires <name>")
		}
		return runPreview(*dbPath, logger, *debug, tail[1], outFormat)

	case "deps":
		if len(tail) != 2 {
			return errors.New("deps requires <name>")
		}
		return showDependencies(k, tail[1])

	case "lineage":
		if len(tail) != 2 {
			return errors.New("lineage requires <name>")
		}
		return showLineage(k, tail[1])

	case "run":
		if len(tail) != 2 {
			return errors.New("run requires <name>")
		}
		return runDataset(*dbPath, logger, *debug, tail[1])

	case "explain":
		if len(tail) != 2 {
			return errors.New("explain requires <name>")
		}
		return explainDataset(k, tail[1])

	case "plan":
		if len(tail) != 2 {
			return errors.New("plan requires <name>")
		}
		return planDataset(k, tail[1])

	default:
		usage()
		return fmt.Errorf("unknown command: %s", cmd)
	}
}

func runREPL(dbPath string, logger *log.Logger, debug bool) error {
	rt, err := runtime.New(dbPath, logger, debug)
	if err != nil {
		return err
	}
	defer rt.Close()

	return repl.StartWithKernel(rt, os.Stdin, os.Stdout, debug)
}

func runQuery(dbPath string, logger *log.Logger, debug bool, sqlText string, outFormat OutputFormat) error {
	rt, err := runtime.New(dbPath, logger, debug)
	if err != nil {
		return err
	}
	defer rt.Close()

	ctx := context.Background()
	stream, err := rt.ExecuteSQL(ctx, sqlText)
	if err != nil {
		return err
	}
	defer stream.Close()

	cols, err := stream.Columns()
	if err != nil {
		return err
	}

	fmtr := getFormatter(outFormat)
	fmtr.SetHeader(cols)

	rowCount := 0
	limit := 1000
	for stream.Next() && rowCount < limit {
		record := stream.Record()
		if record == nil {
			break
		}
		row := make([]interface{}, len(cols))
		for i, col := range cols {
			row[i] = record[col]
		}
		fmtr.AppendRow(row)
		rowCount++
	}

	if err := stream.Err(); err != nil {
		return err
	}

	fmtr.Render()
	fmt.Fprintf(os.Stdout, "\n(%d rows)\n", rowCount)
	return nil
}

func runPreview(dbPath string, logger *log.Logger, debug bool, name string, outFormat OutputFormat) error {
	rt, err := runtime.New(dbPath, logger, debug)
	if err != nil {
		return err
	}
	defer rt.Close()

	ctx := context.Background()
	stream, err := rt.StreamNode(ctx, name)
	if err != nil {
		return err
	}
	defer stream.Close()

	cols, err := stream.Columns()
	if err != nil {
		return err
	}

	fmtr := getFormatter(outFormat)
	fmtr.SetHeader(cols)

	rowCount := 0
	for stream.Next() {
		record := stream.Record()
		if record == nil {
			break
		}
		row := make([]interface{}, len(cols))
		for i, col := range cols {
			row[i] = record[col]
		}
		fmtr.AppendRow(row)
		rowCount++

		if rowCount%1000 == 0 {
			fmt.Fprintf(os.Stderr, "Streamed %d rows...\n", rowCount)
		}
	}

	if err := stream.Err(); err != nil {
		return err
	}

	fmtr.Render()
	fmt.Fprintf(os.Stdout, "\n(%d rows streamed)\n", rowCount)
	return nil
}

func showDependencies(k *kernel.Kernel, name string) error {
	ds, err := k.Show(name)
	if err != nil {
		return err
	}
	fmt.Print(ds)
	return nil
}

func showLineage(k *kernel.Kernel, name string) error {
	ds, err := k.Show(name)
	if err != nil {
		return err
	}
	fmt.Print(ds)
	return nil
}

func parseFormat(s string) OutputFormat {
	switch strings.ToLower(s) {
	case "json":
		return FormatJSON
	case "markdown", "md":
		return FormatMarkdown
	default:
		return FormatTable
	}
}

func getFormatter(outFormat OutputFormat) format.Formatter {
	switch outFormat {
	case FormatJSON:
		return format.NewJSONFormatter(os.Stdout)
	case FormatMarkdown:
		return format.NewMarkdownFormatter(os.Stdout)
	default:
		return format.NewTableFormatter(os.Stdout)
	}
}

func runDataset(dbPath string, logger *log.Logger, debug bool, name string) error {
	k, err := kernel.New(dbPath, logger, debug)
	if err != nil {
		return err
	}
	defer k.Close()

	result, err := k.EnsureFresh(name)
	if err != nil {
		return fmt.Errorf("failed to ensure fresh: %w", err)
	}

	printExecutionPlan(result)

	rt, err := runtime.New(dbPath, logger, debug)
	if err != nil {
		return err
	}
	defer rt.Close()

	ctx := context.Background()
	stream, err := rt.StreamNode(ctx, name)
	if err != nil {
		return err
	}
	defer stream.Close()

	cols, err := stream.Columns()
	if err != nil {
		return err
	}

	tbl := format.NewTableFormatter(os.Stdout)
	tbl.SetHeader(cols)

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
		tbl.AppendRow(row)
		rowCount++
	}

	if err := stream.Err(); err != nil {
		return err
	}

	tbl.Render()
	fmt.Fprintf(os.Stdout, "\n(%d rows)\n", rowCount)
	return nil
}

func printRecomputePlan(result *kernel.RecomputeResult) {
	if result == nil {
		return
	}
	fmt.Printf("recompute plan:\n")
	printPlanGroup("recompute (requested)", result.Requested)
	printPlanGroup("recomputed due to dependency change", result.RecomputedDueToDependency)
	printPlanGroup("skipped (cached reuse)", result.Skipped)
}

func printExecutionPlan(result *kernel.RecomputeResult) {
	if result == nil {
		return
	}
	fmt.Printf("execution plan:\n")
	printPlanGroup("executing", result.Recomputed)
	printPlanGroup("skipping (cached)", result.Skipped)
	fmt.Printf("\n")
}

func printPlanGroup(label string, items []string) {
	fmt.Printf("  %s:\n", label)
	if len(items) == 0 {
		fmt.Printf("    - (none)\n")
		return
	}
	for _, item := range items {
		fmt.Printf("    - %s\n", item)
	}
}

func explainDataset(k *kernel.Kernel, name string) error {
	ds, err := k.Show(name)
	if err != nil {
		return err
	}

	fmt.Println("=== Dataset Explain ===")
	fmt.Println(ds)
	fmt.Println("=== Execution Plan ===")
	fmt.Printf("SQL: %s\n", extractSQL(ds))
	return nil
}

func planDataset(k *kernel.Kernel, name string) error {
	ds, err := k.Show(name)
	if err != nil {
		return err
	}

	sql := extractSQL(ds)
	if sql == "" {
		return errors.New("no SQL found")
	}

	fmt.Println("=== Execution Plan ===")
	fmt.Printf("Target: %s\n", name)
	fmt.Printf("SQL: %s\n", sql)
	fmt.Println("\nExecution order will be determined by dependency analysis.")

	return nil
}

func extractSQL(output string) string {
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "sql:") {
			return strings.TrimSpace(strings.TrimPrefix(line, "sql:"))
		}
	}
	return ""
}
