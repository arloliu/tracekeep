// Command tracekeep is the traffic-log service, one binary whose first argument names the command.
//
// The design is in docs/specs/tracekeep/; dev and migrate exist and do nothing yet.
package main

import (
	"fmt"
	"os"
)

const usage = "usage: tracekeep <dev|migrate>"

// commands maps each command name to its entry point.
var commands = map[string]func() error{
	"dev":     func() error { return nil },
	"migrate": func() error { return nil },
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, usage)
		os.Exit(2)
	}

	run, ok := commands[os.Args[1]]
	if !ok {
		fmt.Fprintf(os.Stderr, "tracekeep: unknown command %q\n%s\n", os.Args[1], usage)
		os.Exit(2)
	}

	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "tracekeep %s: %v\n", os.Args[1], err)
		os.Exit(1)
	}
}
