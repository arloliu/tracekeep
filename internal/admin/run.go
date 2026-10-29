package admin

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/arloliu/go-secs/tracepack"
	"github.com/arloliu/tracekeep/internal/catalog"
)

// ErrUsage marks an operator command the words do not name: a usage error.
var ErrUsage = errors.New("usage")

// reads are the commands that only read, which audit their run here; every other command audits itself in its transaction.
var reads = []string{"status", "deferred list", "parked list", "capture show", "object list", "key verify", "policy list", "policy show", "config list"}

// Run runs the operator command words name (operations.md §10.4) and returns what it prints as JSON:
// the one dispatch tracekeep admin and status share, whether the CLI runs it on the catalog or the api process does (api.md §10.1).
// Every command is audited under a.Actor: one that only reads by a row written here before its answer is returned.
func Run(ctx context.Context, a *Admin, words []string) (any, error) {
	if len(words) == 0 {
		return nil, fmt.Errorf("%w: admin needs a command", ErrUsage)
	}
	cmd := words[0]
	if len(words) > 1 {
		cmd += " " + words[1]
	}
	out, err := operate(ctx, a, cmd, words)
	if err != nil || !slices.Contains(reads, cmd) {
		return out, err
	}
	entry := catalog.AuditEntry{Actor: a.Actor, Action: cmd, Detail: strings.Join(words, " ")}
	if cmd == "capture show" {
		if id, perr := tracepack.ParseUUID(words[2]); perr == nil {
			entry.Capture = &id
		}
	}
	if err := a.Catalog.Audit(ctx, entry); err != nil {
		return nil, fmt.Errorf("admin: audit %s: %w", cmd, err)
	}

	return out, nil
}

// operate runs the operator command cmd, args being the whole command line.
func operate(ctx context.Context, a *Admin, cmd string, args []string) (any, error) {
	switch {
	case listing(cmd, args):
		return onList(ctx, a, cmd, args)
	case cmd == "parked terminate" && (len(args) == 3 || len(args) == 4),
		slices.Contains([]string{"capture terminate", "capture show", "capture close"}, cmd) && len(args) == 3:
		return onCapture(ctx, a, cmd, args[2:])
	case cmd == "bus adopt-stream" && len(args) == 3:
		return a.AdoptStream(ctx, args[2])
	case cmd == "key verify" && len(args) == 3:
		return a.KeyVerify(ctx, args[2])
	case slices.Contains([]string{"policy list", "policy show", "config list", "config activate", "config stage"}, cmd):
		return onConfig(ctx, a, cmd, args)
	case (cmd == "conflict acknowledge" || cmd == "scope merge") && len(args) == 4,
		cmd == "scope exclude-input" && len(args) == 3:
		return onScope(ctx, a, cmd, args[2:])
	default:
		return nil, fmt.Errorf("%w: admin %v", ErrUsage, args)
	}
}

// listing reports whether args are a command that only reads, with the arguments it takes.
func listing(cmd string, args []string) bool {
	switch cmd {
	case "status":
		return len(args) == 1
	case "deferred list":
		return len(args) == 2 || len(args) == 4 && args[2] == "--capture"
	case "parked list":
		return len(args) == 2 || len(args) == 3
	case "object list":
		return len(args) == 2 || len(args) == 3 && (args[2] == "--unattributed" || args[2] == "--survivors")
	default:
		return false
	}
}

// onList runs a command that only reads: status, deferred list, parked list, object list; operate checked its arguments.
func onList(ctx context.Context, a *Admin, cmd string, args []string) (any, error) {
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
func onCapture(ctx context.Context, a *Admin, cmd string, args []string) (any, error) {
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
				return nil, fmt.Errorf("%w: seq %q", ErrUsage, args[1])
			}
			seq = &n
		}

		return a.ParkedTerminate(ctx, capture, seq)
	}
}

// onScope runs a command on a scope's conflicts, inputs or merge: args are a capture and a seq or an hour, or a pack.
func onScope(ctx context.Context, a *Admin, cmd string, args []string) (any, error) {
	id, err := tracepack.ParseUUID(args[0])
	if err != nil {
		return nil, fmt.Errorf("%w: %q: %w", ErrUsage, args[0], err)
	}
	switch cmd {
	case "scope exclude-input":
		return a.ScopeExcludeInput(ctx, id)
	case "scope merge":
		at, err := time.Parse(time.RFC3339, args[1])
		if err != nil {
			return nil, fmt.Errorf("%w: hour %q: %w", ErrUsage, args[1], err)
		}

		return a.ScopeMerge(ctx, id, tracepack.HourOf(at))
	default: // conflict acknowledge
		seq, err := strconv.ParseUint(args[1], 10, 63)
		if err != nil {
			return nil, fmt.Errorf("%w: seq %q", ErrUsage, args[1])
		}

		return a.ConflictAcknowledge(ctx, id, seq)
	}
}

func parseCapture(s string) (tracepack.UUID, error) {
	id, err := tracepack.ParseUUID(s)
	if err != nil {
		return tracepack.UUID{}, fmt.Errorf("%w: capture %q: %w", ErrUsage, s, err)
	}

	return id, nil
}

// onConfig runs a policy or config command, args being the whole command line.
func onConfig(ctx context.Context, a *Admin, cmd string, args []string) (any, error) {
	switch {
	case cmd == "policy list" && len(args) == 2:
		return a.PolicyList(ctx)
	case cmd == "policy show" && len(args) == 4:
		v, err := strconv.ParseInt(args[3], 10, 64)
		if err != nil || v <= 0 {
			return nil, fmt.Errorf("%w: version %q", ErrUsage, args[3])
		}

		return a.PolicyShow(ctx, args[2], v)
	case cmd == "config list" && len(args) == 2:
		return a.ConfigList(ctx)
	case cmd == "config activate" && len(args) == 3:
		id, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil || id <= 0 {
			return nil, fmt.Errorf("%w: configuration %q", ErrUsage, args[2])
		}

		return a.ConfigActivate(ctx, id)
	case cmd == "config stage":
		return onStage(ctx, a, args[2:])
	default:
		return nil, fmt.Errorf("%w: admin %v", ErrUsage, args)
	}
}

// onStage runs config stage <digest key id> <encryption key id> [--policy <file>].
func onStage(ctx context.Context, a *Admin, args []string) (any, error) {
	file := ""
	if n := len(args); n >= 2 && args[n-2] == "--policy" {
		file, args = args[n-1], args[:n-2]
	}
	if len(args) != 2 || slices.Contains(args, "--policy") {
		return nil, fmt.Errorf("%w: config stage <digest key id> <encryption key id> [--policy <file>]", ErrUsage)
	}

	return a.ConfigStage(ctx, args[0], args[1], file)
}
