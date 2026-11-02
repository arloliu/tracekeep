package metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/arloliu/tracekeep/internal/catalog"
	"github.com/arloliu/tracekeep/migrations"
)

// RegisterCatalog registers the catalog's metrics (operations.md §10.4 catalog) on s and its readiness, a ping:
// both pools' usage, the transaction retries by reason (busy on SQLite, deadlock on MariaDB),
// a standalone audit write's wait for its connection by engine and outcome,
// and the schema version this binary was built for, the newest embedded migration.
func RegisterCatalog(s *Server, c *catalog.Catalog) error {
	write, read := c.Pools()
	retries := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "tracekeep", Subsystem: "catalog", Name: "transaction_retries_total",
		Help: "Catalog transactions run again whole after a retryable error, by reason.",
	}, []string{"reason"})
	auditWait := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "tracekeep", Subsystem: "catalog", Name: "audit_wait_seconds",
		Help:    "A standalone audit write's wait for its connection, from the request to its acquisition or failure, by outcome.",
		Buckets: prometheus.ExponentialBuckets(0.0005, 2, 16),
	}, []string{"engine", "outcome"})
	version, err := migrations.Latest(c.Engine())
	if err != nil {
		return err
	}
	migration := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "tracekeep", Subsystem: "catalog", Name: "migration_info",
		Help: "The catalog schema version this binary was built for, the newest embedded migration; always 1.",
	}, []string{"engine", "version"})
	migration.WithLabelValues(c.Engine(), version).Set(1)
	for _, col := range []prometheus.Collector{
		collectors.NewDBStatsCollector(write, "catalog_write"), collectors.NewDBStatsCollector(read, "catalog_read"), retries, auditWait, migration,
	} {
		if err := s.Registry().Register(col); err != nil {
			return fmt.Errorf("metrics: catalog: %w", err)
		}
	}
	c.OnRetry(func(reason string) { retries.WithLabelValues(reason).Inc() })
	engine := c.Engine()
	c.OnAuditWait(func(outcome string, wait time.Duration) {
		auditWait.WithLabelValues(engine, outcome).Observe(wait.Seconds())
	})
	s.Ready("catalog", func(ctx context.Context) error { return c.Ping(ctx) })

	return nil
}
