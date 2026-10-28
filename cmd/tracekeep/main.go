// Command tracekeep is the traffic-log service, one binary whose first argument names the role (architecture.md §3.1):
// migrate, ingest, compact, api, all, dev, or admin with an operator command (operations.md §10.4).
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
	"slices"
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
  api        run the api role: the machine API and its downloads
  all        run every role in one process
  status     print the pending captures, then per tool its last record, open captures, deferred messages and backlog
  dev        run a single-process development loop
  admin      run an operator command:
               parked list [capture] | parked terminate <capture> [seq]
               capture terminate <capture> | capture show <capture> | capture close <capture>
               deferred list [--capture <capture>]
               bus adopt-stream <traffic|parking> | object list [--unattributed|--survivors]
               conflict acknowledge <capture> <seq> | scope exclude-input <pack>
               scope merge <capture> <hour, RFC 3339>`

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
		"migrate": service.Migrate, "ingest": service.Ingest, "compact": service.Compact, "api": service.API, "all": service.All,
	}
	// The roles each command runs, not ready until each has started.
	readiness := map[string][]string{"ingest": {"ingest"}, "compact": {"compact"}, "api": {"api"}, "all": {"ingest", "compact", "api"}}
	role, isRole := roles[command]
	if !isRole && command != "admin" && command != "status" {
		return fmt.Errorf("%w: unknown command %q", errUsage, command)
	}
	if (isRole || command == "status") && fs.NArg() != 0 {
		return fmt.Errorf("%w: %s takes no arguments", errUsage, command)
	}
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(stderr, nil)).With("replica", cfg.Replica)
	if command == "migrate" {
		// The catalog first: the bus's part records the streams' incarnations in it.
		if err := service.MigrateCatalog(ctx, &cfg.Catalog); err != nil {
			return err
		}
		log.Info("tracekeep: the catalog is migrated")
	}
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
		if command == "migrate" {
			return role(ctx, e)
		}
		octx, err := e.Observe(ctx, readiness[command]...)
		if err != nil {
			return err
		}
		err = role(octx, e)
		if failed := service.ServeFailed(octx); failed != nil {
			return errors.Join(failed, err)
		}

		return err
	}
	a := &admin.Admin{Catalog: e.Catalog, Bus: e.JS, Config: e.Bus(), Actor: *actor}
	words := fs.Args()
	if command == "status" {
		words = []string{"status"}
	}

	return runAdmin(ctx, a, words, stdout)
}

// runAdmin runs one operator command and writes its result to stdout as JSON.
func runAdmin(ctx context.Context, a *admin.Admin, args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("%w: admin needs a command", errUsage)
	}
	cmd := args[0]
	if len(args) > 1 {
		cmd += " " + args[1]
	}
	out, err := operate(ctx, a, cmd, args)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")

	return enc.Encode(out)
}

// operate runs the operator command cmd, args being the whole command line.
func operate(ctx context.Context, a *admin.Admin, cmd string, args []string) (any, error) {
	switch {
	case cmd == "status" && len(args) == 1,
		cmd == "deferred list" && (len(args) == 2 || len(args) == 4 && args[2] == "--capture"),
		cmd == "parked list" && len(args) <= 3,
		cmd == "object list" && (len(args) == 2 || len(args) == 3 && (args[2] == "--unattributed" || args[2] == "--survivors")):
		return onList(ctx, a, cmd, args)
	case cmd == "parked terminate" && (len(args) == 3 || len(args) == 4),
		slices.Contains([]string{"capture terminate", "capture show", "capture close"}, cmd) && len(args) == 3:
		return onCapture(ctx, a, cmd, args[2:])
	case cmd == "bus adopt-stream" && len(args) == 3:
		return a.AdoptStream(ctx, args[2])
	case (cmd == "conflict acknowledge" || cmd == "scope merge") && len(args) == 4,
		cmd == "scope exclude-input" && len(args) == 3:
		return onScope(ctx, a, cmd, args[2:])
	default:
		return nil, fmt.Errorf("%w: admin %v", errUsage, args)
	}
}

// onList runs a command that only reads: status, deferred list, parked list, object list; operate checked its arguments.
func onList(ctx context.Context, a *admin.Admin, cmd string, args []string) (any, error) {
	var capture *tracepack.UUID
	if (cmd == "deferred list" && len(args) == 4) || (cmd == "parked list" && len(args) == 3) {
		id, err := parseCapture(args[len(args)-1])
		if err != nil {
			return nil, err
		}
		capture = &id
	}
	switch cmd {
	case "status":
		return a.Status(ctx)
	case "deferred list":
		return a.DeferredList(ctx, capture)
	case "parked list":
		return a.ParkedList(ctx, capture)
	default: // object list
		only := ""
		if len(args) == 3 {
			only = args[2]
		}

		return a.ObjectList(ctx, only == "--survivors", only == "--unattributed")
	}
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
	case "capture close":
		return a.CaptureClose(ctx, capture)
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

// onScope runs a command on a scope's conflicts, inputs or merge: args are a capture and a seq or an hour, or a pack.
func onScope(ctx context.Context, a *admin.Admin, cmd string, args []string) (any, error) {
	id, err := tracepack.ParseUUID(args[0])
	if err != nil {
		return nil, fmt.Errorf("%w: %q: %w", errUsage, args[0], err)
	}
	switch cmd {
	case "scope exclude-input":
		return a.ScopeExcludeInput(ctx, id)
	case "scope merge":
		at, err := time.Parse(time.RFC3339, args[1])
		if err != nil {
			return nil, fmt.Errorf("%w: hour %q: %w", errUsage, args[1], err)
		}

		return a.ScopeMerge(ctx, id, tracepack.HourOf(at))
	default: // conflict acknowledge
		seq, err := strconv.ParseUint(args[1], 10, 63)
		if err != nil {
			return nil, fmt.Errorf("%w: seq %q", errUsage, args[1])
		}

		return a.ConflictAcknowledge(ctx, id, seq)
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
	apiAddr := fs.String("api", "127.0.0.1:8080", "where to serve the machine API, over HTTP/2 without TLS")
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
	d, err := service.NewDev(ctx, service.DevOptions{Dir: *dir, Port: *port, Flush: *flush, Rate: *rate, API: *apiAddr, Log: log})
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	d.Env.Config.Metrics.Listen = *listen
	octx, err := d.Env.Observe(ctx, "ingest", "compact", "api")
	if err != nil {
		return err
	}
	log.Info("tracekeep dev: running", "data", *dir, "bus", d.URL(), "packs", filepath.Join(*dir, "packs"),
		"api", "http://"+*apiAddr, "token_file", filepath.Join(*dir, service.DevTokenFile))
	err = d.Run(octx)
	if failed := service.ServeFailed(octx); failed != nil {
		return errors.Join(failed, err)
	}
	if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return nil
	}

	return err
}
