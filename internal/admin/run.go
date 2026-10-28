package admin

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/arloliu/go-secs/tracepack"
)

// ErrUsage marks an operator command the words do not name: a usage error.
var ErrUsage = errors.New("usage")

// Run runs the operator command words name (operations.md §10.4) and returns what it prints as JSON:
// the one dispatch tracekeep admin and status share, whether the CLI runs it on the catalog or the api process does (api.md §10.1).
func Run(ctx context.Context, a *Admin, words []string) (any, error) {
	if len(words) == 0 {
		return nil, fmt.Errorf("%w: admin needs a command", ErrUsage)
	}
	cmd := words[0]
	if len(words) > 1 {
		cmd += " " + words[1]
	}

	return operate(ctx, a, cmd, words)
}

// operate runs the operator command cmd, args being the whole command line.
func operate(ctx context.Context, a *Admin, cmd string, args []string) (any, error) {
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
		return nil, fmt.Errorf("%w: admin %v", ErrUsage, args)
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
