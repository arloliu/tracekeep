// Command tracekeep-sim is the reference producer on the SDK:
// it drives the SDK exactly as an equipment gateway does, from a go-secs HSMS-SS loopback pair with the SDK's observers,
// and publishes the captures to a traffic bus (docs/ROADMAP.md, phase 0).
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/arloliu/tracekeep/producer"
)

// stopTimeout bounds the device's Stop, the SDK's End included.
const stopTimeout = 10 * time.Second

type config struct {
	servers  string
	prefix   string
	bucket   string
	stream   string
	rate     float64
	duration time.Duration
	opts     Options
}

func main() {
	var c config
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
	if c.rate <= 0 {
		return fmt.Errorf("rate %v: must be positive", c.rate)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	p, err := producer.New(producer.Config{
		Servers: strings.Split(c.servers, ","), SubjectPrefix: c.prefix, Stream: c.stream, ClaimBucket: c.bucket,
	}, producer.WithLogger(log))
	if err != nil {
		return err
	}
	defer p.Close()
	l, err := NewLoopback(p, c.opts)
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
	tick := time.NewTicker(time.Duration(float64(time.Second) / c.rate))
	defer tick.Stop()
	for done := false; !done; {
		select {
		case <-ctx.Done():
			done = true
		case <-tick.C:
			if err := l.Forward(ctx); err != nil && ctx.Err() == nil {
				log.Warn("tracekeep-sim: forward failed", "error", err)
			}
		}
	}
	sctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
	defer cancel()
	l.Stop(sctx)

	return nil
}
