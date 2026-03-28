package main

import (
	"log"
	"os"

	"github.com/TFMV/duckkernel/internal/cli"
)

func main() {
	if err := cli.Run(os.Args[1:]); err != nil {
		log.Fatalf("fatal: %v", err)
	}
}
