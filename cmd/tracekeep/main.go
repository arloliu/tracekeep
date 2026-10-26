// Command tracekeep is the traffic-log service, one binary whose first argument names the role (architecture.md §3.1):
// migrate, ingest, compact, all, dev, or admin with an operator command (operations.md §10.4).
// The design is in docs/specs/tracekeep/; the configuration's keys are in docs/guides/configuration.md.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/arloliu/go-secs/tracepack"

	"github.com/arloliu/tracekeep/internal/admin"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/config"
	"github.com/arloliu/tracekeep/internal/service"
)

const usage = `usage: tracekeep <command> [--config file] [arguments]

commands:
  migrate    ensure the bus's streams and claim bucket and record their incarnations
  ingest     run the ingest role
  compact    run the compact role
  all        run every role in one process
  dev        run a single-process development loop
  admin      run an operator command:
               parked list [capture] | parked terminate <capture> [seq]
               capture terminate <capture> | capture show <capture>
               bus adopt-stream <traffic|parking> | object list [--unattributed|--survivors]`

// errUsage marks a command line the binary cannot run; it exits 2.
var errUsage = errors.New("usage")

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// run runs the command line args and returns the exit code.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	err := dispatch(ctx, args, stdout, stderr)
	switch {
	case err == nil:
		return 0
	case errors.Is(err, errUsage):
		fmt.Fprintf(stderr, "tracekeep: %v\n%s\n", err, usage)
		return 2
	default:
		fmt.Fprintf(stderr, "tracekeep: %v\n", err)
		return 1
	}
}

func dispatch(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("%w: no command", errUsage)
	}
	command := args[0]
	if command == "dev" {
		return runDev(ctx, args[1:], stderr)
	}
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	path := fs.String("config", os.Getenv("TRACEKEEP_CONFIG"), "the configuration file")
	actor := fs.String("actor", os.Getenv("USER"), "the operator named in an admin command's audit row")
	if err := fs.Parse(args[1:]); err != nil {
		return fmt.Errorf("%w: %w", errUsage, err)
	}
	roles := map[string]func(context.Context, *service.Env) error{
		"migrate": service.Migrate, "ingest": service.Ingest, "compact": service.Compact, "all": service.All,
	}
	// The roles each command runs, not ready until each has started.
	readiness := map[string][]string{"ingest": {"ingest"}, "compact": {"compact"}, "all": {"ingest", "compact"}}
	role, isRole := roles[command]
	if !isRole && command != "admin" {
		return fmt.Errorf("%w: unknown command %q", errUsage, command)
	}
	if isRole && fs.NArg() != 0 {
		return fmt.Errorf("%w: %s takes no arguments", errUsage, command)
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(stderr, nil)).With("replica", cfg.Replica)
	e, err := service.Open(ctx, cfg, log)
	if !isRole && errors.Is(err, catalog.ErrSecondWriter) {
		// Admin through the owning process is the api role's, phase 2.
		return fmt.Errorf("%w: a running service holds this SQLite catalog; stop the service to run admin, or run the catalog on MariaDB", err)
	}
	if err != nil {
		return err
	}
	defer func() { _ = e.Close() }()
	if isRole {
		log.Info("tracekeep: starting", "command", command)
		if command != "migrate" {
			if err := e.Observe(ctx, readiness[command]...); err != nil {
				return err
			}
		}

		return role(ctx, e)
	}
	a := &admin.Admin{Catalog: e.Catalog, Bus: e.JS, Config: e.Bus(), Actor: *actor}

	return runAdmin(ctx, a, fs.Args(), stdout)
}

// runAdmin runs one operator command and writes its result to stdout as JSON.
func runAdmin(ctx context.Context, a *admin.Admin, args []string, stdout io.Writer) error {
	if len(args) < 2 {
		return fmt.Errorf("%w: admin needs a command", errUsage)
	}
	var out any
	var err error
	switch cmd := args[0] + " " + args[1]; {
	case cmd == "parked list" && len(args) <= 3:
		var capture *tracepack.UUID
		if len(args) == 3 {
			id, perr := parseCapture(args[2])
			if perr != nil {
				return perr
			}
			capture = &id
		}
		out, err = a.ParkedList(ctx, capture)
	case cmd == "parked terminate" && (len(args) == 3 || len(args) == 4),
		(cmd == "capture terminate" || cmd == "capture show") && len(args) == 3:
		out, err = onCapture(ctx, a, cmd, args[2:])
	case cmd == "bus adopt-stream" && len(args) == 3:
		out, err = a.AdoptStream(ctx, args[2])
	case cmd == "object list" && (len(args) == 2 || len(args) == 3 && (args[2] == "--unattributed" || args[2] == "--survivors")):
		// Unattributed objects and survivors are what retention leaves, which phase 2 brings.
		out = []struct{}{}
	default:
		return fmt.Errorf("%w: admin %v", errUsage, args)
	}
	if err != nil {
		return err
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")

	return enc.Encode(out)
}

// onCapture runs an operator command on one capture, args being the capture and, for parked terminate, a seq.
func onCapture(ctx context.Context, a *admin.Admin, cmd string, args []string) (any, error) {
	capture, err := parseCapture(args[0])
	if err != nil {
		return nil, err
	}
	switch cmd {
	case "capture terminate":
		return a.CaptureTerminate(ctx, capture)
	case "capture show":
		return a.CaptureShow(ctx, capture)
	default: // parked terminate
		var seq *uint64
		if len(args) == 2 {
			n, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("%w: seq %q", errUsage, args[1])
			}
			seq = &n
		}

		return a.ParkedTerminate(ctx, capture, seq)
	}
}

func parseCapture(s string) (tracepack.UUID, error) {
	id, err := tracepack.ParseUUID(s)
	if err != nil {
		return tracepack.UUID{}, fmt.Errorf("%w: capture %q: %w", errUsage, s, err)
	}

	return id, nil
}

// runDev runs tracekeep dev: an embedded bus, SQLite, an fs store, ingest, compact and the loopback simulator, in one process.
func runDev(ctx context.Context, args []string, stderr io.Writer) error {
	fs := flag.NewFlagSet("dev", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	dir := fs.String("data", "", "the directory that keeps the bus, the catalog and the packs; empty for a fresh temporary one")
	port := fs.Int("port", 4222, "the embedded bus's client port, 0 for a free one")
	flush := fs.Duration("flush", 10*time.Second, "ingest's flush interval")
	rate := fs.Float64("rate", 10, "the simulator's forwarded requests per second; 0 runs no simulator")
	listen := fs.String("metrics", "", "where to serve /metrics, /healthz and /readyz; empty serves none")
	if err := fs.Parse(args); err != nil {
		return fmt.Errorf("%w: %w", errUsage, err)
	}
	if fs.NArg() != 0 || *flush <= 0 || *rate < 0 {
		return fmt.Errorf("%w: dev %v", errUsage, args)
	}
	if *dir == "" {
		d, err := os.MkdirTemp("", "tracekeep-dev-")
		if err != nil {
			return err
		}
		*dir = d
	}
	log := slog.New(slog.NewJSONHandler(stderr, nil))
	d, err := service.NewDev(ctx, service.DevOptions{Dir: *dir, Port: *port, Flush: *flush, Rate: *rate, Log: log})
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	d.Env.Config.Metrics.Listen = *listen
	if err := d.Env.Observe(ctx, "ingest", "compact"); err != nil {
		return err
	}
	log.Info("tracekeep dev: running", "data", *dir, "bus", d.URL(), "packs", filepath.Join(*dir, "packs"))
	err = d.Run(ctx)
	if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return nil
	}

	return err
}
