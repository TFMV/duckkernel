package cli

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/TFMV/duckkernel/internal/dataset"
	"github.com/TFMV/duckkernel/internal/kernel"
)

const defaultDBPath = "duckkernel.db"

func usage() {
	fmt.Fprintf(flag.CommandLine.Output(), `Usage:
  duckkernel [--debug] <command> [args]

Commands:
  create <name> "<sql>"           Create a named dataset
  transform <name> "<sql>"       Create or update a derived dataset
  show <name>                     Show dataset metadata
  graph                           Print the dataset lineage graph
  recompute <name>                Recompute a dataset and invalidate dependents
  list                            List registered datasets
  drop <name>                     Drop a dataset from the kernel
`)
}

func Run(args []string) error {
	fs := flag.NewFlagSet("duckkernel", flag.ContinueOnError)
	debug := fs.Bool("debug", false, "enable debug output")
	dbPath := fs.String("db", defaultDBPath, "path to persistent DuckDB file")
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
	switch cmd {
	case "create", "transform":
		if len(tail) < 3 {
			return fmt.Errorf("%s requires <name> <sql>", cmd)
		}
		name := tail[1]
		sqlText := strings.Join(tail[2:], " ")
		mode := dataset.ModeCached
		if cmd == "transform" {
			mode = dataset.ModeCached
		}
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
		if len(tail) != 2 {
			return errors.New("recompute requires <name>")
		}
		if err := k.Recompute(tail[1]); err != nil {
			return err
		}
		fmt.Printf("recomputed=%s\n", tail[1])
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
	default:
		usage()
		return fmt.Errorf("unknown command: %s", cmd)
	}
}
