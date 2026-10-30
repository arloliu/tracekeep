// Command tracekeep-sim is the reference producer on the SDK:
// it drives the SDK exactly as an equipment gateway does, from a go-secs HSMS-SS loopback pair with the SDK's observers,
// and publishes the captures to a traffic bus (docs/ROADMAP.md, phase 0).
//
// In loopback mode the host forwards requests at a steady rate and the equipment answers them;
// in replay mode the pair carries a synthetic or masked tracepack pack's frames, each from the side that sent it.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/arloliu/tracekeep/internal/sim"
	"github.com/arloliu/tracekeep/producer"
)

// stopTimeout bounds the device's Stop, the SDK's End included.
const stopTimeout = 10 * time.Second

type config struct {
	mode     string
	pack     string
	speed    float64
	servers  string
	prefix   string
	bucket   string
	stream   string
	rate     float64
	duration time.Duration
	thinning bool
	rule     producer.SamplingRule
	opts     sim.Options
}

func main() {
	var c config
	flag.StringVar(&c.mode, "mode", "loopback", "loopback, or replay of -pack")
	flag.StringVar(&c.pack, "pack", "", "the tracepack pack replay mode sends; synthetic or masked data only")
	flag.Float64Var(&c.speed, "speed", 1, "replay at the pack's pace times speed; 0 sends as fast as the pair takes it")
	flag.StringVar(&c.servers, "servers", "nats://127.0.0.1:4222", "comma-separated NATS URLs of the traffic bus")
	flag.StringVar(&c.prefix, "prefix", "tpk", "the traffic stream's subject prefix")
	flag.StringVar(&c.bucket, "bucket", "tpk-objects", "the claim bucket")
	flag.StringVar(&c.stream, "stream", "", "the traffic stream; empty takes the SDK's default")
	flag.StringVar(&c.opts.ToolID, "tool", "SIM-TOOL", "the tool_id every capture carries")
	flag.StringVar(&c.opts.DeviceName, "device", "eqp", "the device name every capture carries")
	flag.DurationVar(&c.opts.T3, "t3", 45*time.Second, "the waiter's timeout for a forwarded request")
	flag.Uint64Var(&c.opts.WithholdEvery, "withhold-every", 0, "the equipment leaves every n-th forwarded request unanswered; 0 answers all")
	flag.Float64Var(&c.rate, "rate", 10, "forwarded requests per second")
	flag.DurationVar(&c.duration, "duration", 0, "how long to run; 0 runs until interrupted")
	flag.BoolVar(&c.thinning, "thinning", false, "thin the forwarded interval polls; a replayed request is never thinned")
	flag.IntVar(&c.rule.KeepEveryN, "thinning-keep-every-n", 0, "keep every n-th poll's bodies; 0 takes the SDK's default")
	flag.DurationVar(&c.rule.KeepInterval, "thinning-keep-interval", 0, "keep a poll's bodies at least this often; 0 takes the SDK's default")
	flag.Parse()
	c.opts.SessionID = 1

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, &c)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "tracekeep-sim:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, c *config) error {
	switch {
	case c.mode == "loopback" && c.rate <= 0:
		return fmt.Errorf("rate %v: must be positive", c.rate)
	case c.mode == "replay" && (c.pack == "" || c.speed < 0):
		return errors.New("replay needs -pack and a speed of 0 or more")
	case c.mode != "loopback" && c.mode != "replay":
		return fmt.Errorf("mode %q: want loopback or replay", c.mode)
	default:
	}
	c.opts.Replay = c.mode == "replay"
	if c.thinning {
		c.opts.Thinning = &c.rule
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	p, err := producer.New(producer.Config{
		Servers: strings.Split(c.servers, ","), SubjectPrefix: c.prefix, Stream: c.stream, ClaimBucket: c.bucket,
	}, producer.WithLogger(log))
	if err != nil {
		return err
	}
	defer p.Close()
	l, err := sim.NewLoopback(p, c.opts)
	if err != nil {
		return err
	}
	if err := l.Start(ctx); err != nil {
		_ = l.Close()
		return err
	}
	if c.duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.duration)
		defer cancel()
	}
	var runErr error
	if c.opts.Replay {
		var sent, skipped int
		sent, skipped, runErr = sim.ReplayPack(ctx, l, c.pack, c.speed)
		log.Info("tracekeep-sim: replayed", "frames", sent, "malformed", skipped)
		// The pack's last requests get their waits before the device stops.
		_ = sim.SleepUntil(ctx, time.Now().Add(c.opts.T3))
	} else {
		sim.Forward(ctx, l, c.rate, log)
	}
	sctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
	defer cancel()
	l.Stop(sctx)

	return runErr
}
