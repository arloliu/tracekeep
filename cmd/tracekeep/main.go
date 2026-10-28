// Command tracekeep is the traffic-log service, one binary whose first argument names the role (architecture.md §3.1):
// migrate, ingest, compact, api, all, dev, or admin with an operator command (operations.md §10.4).
// The design is in docs/specs/tracekeep/; the configuration's keys are in docs/guides/configuration.md.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"syscall"
	"time"

	"connectrpc.com/connect"

	"github.com/arloliu/tracekeep/internal/admin"
	"github.com/arloliu/tracekeep/internal/auth"
	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/internal/config"
	"github.com/arloliu/tracekeep/internal/service"
	tkv1 "github.com/arloliu/tracekeep/proto/tracekeep/v1"
	"github.com/arloliu/tracekeep/proto/tracekeep/v1/tracekeepv1connect"
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
var errUsage = admin.ErrUsage

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
	words := fs.Args()
	if command == "status" {
		words = []string{"status"}
	}
	e, err := service.Open(ctx, cfg, log)
	if !isRole && errors.Is(err, catalog.ErrSecondWriter) {
		// The running service holds its SQLite catalog alone: the command goes through its API (api.md §10.1 *Admin over the API*).
		return remoteAdmin(ctx, cfg, words, stdout)
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

	return runAdmin(ctx, a, words, stdout)
}

// runAdmin runs one operator command and writes its result to stdout as JSON.
func runAdmin(ctx context.Context, a *admin.Admin, args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return fmt.Errorf("%w: admin needs a command", errUsage)
	}
	out, err := admin.Run(ctx, a, args)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")

	return enc.Encode(out)
}

// remoteAdmin sends words to the running service's AdminService and prints its answer:
// authenticated by the configuration's static token holding admin, the service reached at api.public_url,
// its certificate trusted from api.tls_cert_file beside the system's roots. The token's subject is the audited actor, never --actor.
func remoteAdmin(ctx context.Context, cfg *config.Config, words []string, stdout io.Writer) error {
	var token string
	if cfg.Auth.Static.Enabled {
		for _, t := range cfg.Auth.Static.Tokens {
			if slices.Contains(t.Roles, auth.RoleAdmin) {
				token = t.Token
				break
			}
		}
	}
	if token == "" || cfg.API.PublicURL == "" {
		return fmt.Errorf("%w: a running service holds this SQLite catalog, and admin goes through its API, "+
			"which needs api.public_url and an auth.static token holding admin; or stop the service, or run the catalog on MariaDB", catalog.ErrSecondWriter)
	}
	roots, err := x509.SystemCertPool()
	if err != nil {
		roots = x509.NewCertPool()
	}
	if cfg.API.TLSCertFile != "" {
		pem, err := os.ReadFile(cfg.API.TLSCertFile) //nolint:gosec // the operator's own configuration names the file
		if err != nil {
			return err
		}
		roots.AppendCertsFromPEM(pem)
	}
	// No redirect is followed: one could carry the token elsewhere, or to plain text.
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}, ForceAttemptHTTP2: true,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	req := connect.NewRequest(&tkv1.AdminRequest{Words: words})
	req.Header().Set("Authorization", "Bearer "+token)
	resp, err := tracekeepv1connect.NewAdminServiceClient(client, cfg.API.PublicURL).Admin(ctx, req)
	if connect.CodeOf(err) == connect.CodeInvalidArgument {
		return fmt.Errorf("%w: %w", errUsage, err)
	}
	if err != nil {
		return fmt.Errorf("admin through the running service: %w", err)
	}
	_, err = fmt.Fprintln(stdout, resp.Msg.GetJson())

	return err
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
