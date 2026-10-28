// Package config is the service's configuration: one struct loaded with fuda (tracekeep-decisions.md T-22),
// its defaults the service defaults (T-12, T-25), its secrets resolved by reference, read once at start.
// docs/guides/configuration.md describes every key.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/arloliu/fuda"
)

// Config is the configuration of every role.
type Config struct {
	Catalog Catalog `yaml:"catalog"`
	Bus     Bus     `yaml:"bus"`
	Store   Store   `yaml:"store"`
	Ingest  Ingest  `yaml:"ingest"`
	Parking Parking `yaml:"parking"`
	Compact Compact `yaml:"compact"`
	Metrics Metrics `yaml:"metrics"`
	Auth    Auth    `yaml:"auth"`
	Keys    Keys    `yaml:"keys"`
	// Retention is how long whole hours are kept; a record older than the retention horizon is beyond retention.
	Retention time.Duration `yaml:"retention" default:"720h" env:"TRACEKEEP_RETENTION" validate:"gt=0"`
	// Replica names this process in the membership and the compactor lease; empty takes the host name.
	Replica string `yaml:"replica" env:"TRACEKEEP_REPLICA"`
}

// Catalog is the catalog database (catalog.md §7.4).
type Catalog struct {
	// Engine is sqlite or mariadb.
	Engine string `yaml:"engine" default:"sqlite" env:"TRACEKEEP_CATALOG_ENGINE" validate:"oneof=sqlite mariadb"`
	// DSNRef names where the DSN is read from, as a fuda reference (file://, env://); empty reads DSN as written.
	DSNRef string `yaml:"dsn_ref" env:"TRACEKEEP_CATALOG_DSN_REF"`
	// DSN is the SQLite file or the MariaDB DSN, credentials included.
	DSN string `yaml:"dsn" refFrom:"DSNRef" env:"TRACEKEEP_CATALOG_DSN" validate:"required"`
}

// Bus is the traffic cluster: the traffic and parking streams and the claim bucket (architecture.md §3.1, parking.md).
type Bus struct {
	URL string `yaml:"url" env:"TRACEKEEP_BUS_URL" validate:"required"`
	// User and Password authenticate by user and password; PasswordRef names where the password is read from.
	User        string `yaml:"user" env:"TRACEKEEP_BUS_USER"`
	PasswordRef string `yaml:"password_ref" env:"TRACEKEEP_BUS_PASSWORD_REF"`
	Password    string `yaml:"password" refFrom:"PasswordRef"`
	// CredsFile authenticates with a NATS credentials file instead.
	CredsFile string `yaml:"creds_file" env:"TRACEKEEP_BUS_CREDS_FILE"`
	// TLS files: a CA to verify the servers, and a client certificate and key for mutual TLS.
	CAFile   string `yaml:"ca_file" env:"TRACEKEEP_BUS_CA_FILE"`
	CertFile string `yaml:"cert_file" env:"TRACEKEEP_BUS_CERT_FILE"`
	KeyFile  string `yaml:"key_file" env:"TRACEKEEP_BUS_KEY_FILE"`
	// SubjectPrefix is the traffic subjects' prefix, the producers' (producer-sdk.md §4.7).
	SubjectPrefix string `yaml:"subject_prefix" env:"TRACEKEEP_BUS_SUBJECT_PREFIX" validate:"required"`
	// Stream and ParkedStream name the traffic and parking streams; empty takes busconfig's defaults.
	Stream       string `yaml:"stream" env:"TRACEKEEP_BUS_STREAM"`
	ParkedStream string `yaml:"parked_stream" env:"TRACEKEEP_BUS_PARKED_STREAM"`
	// ParkPrefix is the parking subjects' prefix; empty takes busconfig's default.
	ParkPrefix  string `yaml:"park_prefix" env:"TRACEKEEP_BUS_PARK_PREFIX"`
	ClaimBucket string `yaml:"claim_bucket" env:"TRACEKEEP_BUS_CLAIM_BUCKET" validate:"required"`
}

// Store is the object store packs live in (storage.md §5.2).
type Store struct {
	// Kind is s3 or fs.
	Kind string `yaml:"kind" default:"s3" env:"TRACEKEEP_STORE_KIND" validate:"oneof=s3 fs"`
	// KeyPrefix prefixes every pack key; empty for none.
	KeyPrefix string `yaml:"key_prefix" env:"TRACEKEEP_STORE_KEY_PREFIX"`
	// Dir is the fs store's root.
	Dir string `yaml:"dir" env:"TRACEKEEP_STORE_DIR"`
	S3  S3     `yaml:"s3"`
	// Presign lets readers download by presigned URL (T-33); off for an s3 store whose URLs readers cannot use.
	Presign bool `yaml:"presign" default:"true" env:"TRACEKEEP_STORE_PRESIGN"`
	// DownloadLifetime is the longest a download URL lives (T-41); compact.delete_delay exceeds it by more than twice the clock skew.
	DownloadLifetime time.Duration `yaml:"download_lifetime" default:"15m" env:"TRACEKEEP_STORE_DOWNLOAD_LIFETIME" validate:"gt=0"`
}

// S3 is an S3-compatible endpoint (T-34).
type S3 struct {
	// Endpoint is the service's URL; empty for AWS itself.
	Endpoint    string `yaml:"endpoint" env:"TRACEKEEP_S3_ENDPOINT"`
	Region      string `yaml:"region" env:"TRACEKEEP_S3_REGION"`
	Bucket      string `yaml:"bucket" env:"TRACEKEEP_S3_BUCKET"`
	AccessKeyID string `yaml:"access_key_id" env:"TRACEKEEP_S3_ACCESS_KEY_ID"`
	// SecretRef names where the secret access key is read from.
	SecretRef       string `yaml:"secret_access_key_ref" env:"TRACEKEEP_S3_SECRET_ACCESS_KEY_REF"`
	SecretAccessKey string `yaml:"secret_access_key" refFrom:"SecretRef"`
}

// Ingest is the ingest role (ingest.md §6, architecture.md §3.1).
type Ingest struct {
	// FlushInterval is F (T-25).
	FlushInterval time.Duration `yaml:"flush_interval" default:"5m" env:"TRACEKEEP_INGEST_FLUSH_INTERVAL" validate:"gt=0"`
	// AckWait is the consumer's ack wait: above F plus an upload, and below Q minus F with the redelivery slack (T-25).
	AckWait time.Duration `yaml:"ack_wait" default:"7m" env:"TRACEKEEP_INGEST_ACK_WAIT" validate:"gt=0"`
	// MaxAckPending is above the peak message rate times F plus an upload.
	MaxAckPending int `yaml:"max_ack_pending" default:"50000" env:"TRACEKEEP_INGEST_MAX_ACK_PENDING" validate:"gt=0"`
	// FetchBatch bounds one fetch; MaxWait bounds its wait.
	FetchBatch int           `yaml:"fetch_batch" default:"500" env:"TRACEKEEP_INGEST_FETCH_BATCH" validate:"gt=0"`
	MaxWait    time.Duration `yaml:"max_wait" default:"5s" env:"TRACEKEEP_INGEST_MAX_WAIT" validate:"gt=0"`
	// Lease is the membership lease, renewed before every fetch (rollouts.md §6.4).
	Lease time.Duration `yaml:"lease" default:"1m" env:"TRACEKEEP_INGEST_LEASE" validate:"gt=0"`
	// BatchMaxBytes makes a batch due before F once it holds this many payload bytes.
	BatchMaxBytes fuda.ByteSize `yaml:"batch_max_bytes" default:"64MiB" env:"TRACEKEEP_INGEST_BATCH_MAX_BYTES" validate:"gt=0"`
	// WorkDir holds the working files of segments being written; empty takes the system's temporary directory.
	WorkDir string `yaml:"work_dir" env:"TRACEKEEP_INGEST_WORK_DIR"`
	// MaxFrameLen is the processing configuration's max_frame_len, 0 for no ceiling;
	// a change makes another configuration (rollouts.md §6.4).
	MaxFrameLen uint64 `yaml:"max_frame_len" env:"TRACEKEEP_INGEST_MAX_FRAME_LEN"`
}

// Parking is parking (parking.md).
type Parking struct {
	Enabled bool `yaml:"enabled" default:"true" env:"TRACEKEEP_PARKING_ENABLED"`
	// RedeliveryDelay is the parked consumer's nak delay.
	RedeliveryDelay time.Duration `yaml:"redelivery_delay" default:"60s" env:"TRACEKEEP_PARKING_REDELIVERY_DELAY" validate:"gt=0"`
}

// Compact is the compact role: its reconciler (storage.md §5.4), its merges, closes, deletions and retention (compactor.md §8.1–§8.3).
type Compact struct {
	// LeaseTTL is how long a take or renewal holds the compactor lease.
	LeaseTTL time.Duration `yaml:"lease_ttl" default:"1m" env:"TRACEKEEP_COMPACT_LEASE_TTL" validate:"gt=0"`
	// Interval is the time between two reconciler cycles.
	Interval time.Duration `yaml:"interval" default:"5m" env:"TRACEKEEP_COMPACT_INTERVAL" validate:"gt=0"`
	// QuietPeriod is Q: a scope becomes a merge candidate once its hour ended Q ago (catalog.md §7.3, T-12).
	QuietPeriod time.Duration `yaml:"quiet_period" default:"15m" env:"TRACEKEEP_COMPACT_QUIET_PERIOD" validate:"gt=0"`
	// RedeliverySlack is the time Q leaves after F and ack_wait for a redelivered message to be flushed (catalog.md §7.3, T-41).
	RedeliverySlack time.Duration `yaml:"redelivery_slack" default:"2m" env:"TRACEKEEP_COMPACT_REDELIVERY_SLACK" validate:"gte=0"`
	// Inactivity is I: an open capture without a registration for I is closed (compactor.md §8.2, T-12).
	Inactivity time.Duration `yaml:"inactivity" default:"2h" env:"TRACEKEEP_COMPACT_INACTIVITY" validate:"gt=0"`
	// DeleteDelay is D: a merged input is deleted D after its merge, and a shortened retention evicts nothing early for D;
	// it exceeds store.download_lifetime by more than twice two minutes of clock skew (compactor.md §8.3).
	DeleteDelay time.Duration `yaml:"delete_delay" default:"1h" env:"TRACEKEEP_COMPACT_DELETE_DELAY" validate:"gt=0"`
}

// Metrics is the metrics and health listener (operations.md §10.4).
type Metrics struct {
	// Listen is the address /metrics, /healthz and /readyz are served on; empty serves none.
	Listen string `yaml:"listen" default:":9090" env:"TRACEKEEP_METRICS_LISTEN"`
}

// Load reads the configuration from the YAML file path, or from defaults and the environment alone when path is empty,
// resolves its references and validates it.
func Load(path string) (*Config, error) {
	b := fuda.New()
	if path != "" {
		b = b.FromFile(path)
	}
	loader, err := b.WithTimeout(10 * time.Second).Build()
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	var c Config
	if err := loader.Load(&c); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if err := c.check(false); err != nil {
		return nil, err
	}
	if c.Replica == "" {
		if c.Replica, err = os.Hostname(); err != nil {
			return nil, fmt.Errorf("config: replica: %w", err)
		}
	}

	return &c, nil
}

// clockSkew is how far two hosts' clocks may disagree, a minute either way of the true time, which the download margin covers (T-33).
// D leaves twice that beyond a URL's lifetime (compactor.md §8.3): once between the hosts that issue and validate the URL,
// once between those that record a deletion time and act on it.
const clockSkew = 2 * time.Minute

// check holds the rules that span fields; dev admits what only tracekeep dev may use.
func (c *Config) check(dev bool) error {
	errs := c.Auth.check()
	errs = append(errs, c.Keys.check(dev)...)
	if c.Ingest.AckWait <= c.Ingest.FlushInterval {
		errs = append(errs, fmt.Errorf("ingest.ack_wait %s must exceed ingest.flush_interval %s (T-25)", c.Ingest.AckWait, c.Ingest.FlushInterval))
	}
	if bound := c.Ingest.FlushInterval + c.Ingest.AckWait + c.Compact.RedeliverySlack; c.Compact.QuietPeriod <= bound {
		errs = append(errs, fmt.Errorf("compact.quiet_period %s must exceed ingest.flush_interval + ingest.ack_wait + compact.redelivery_slack, %s (T-25, T-41)",
			c.Compact.QuietPeriod, bound))
	}
	if bound := c.Store.DownloadLifetime + 2*clockSkew; c.Compact.DeleteDelay <= bound {
		errs = append(errs, fmt.Errorf("compact.delete_delay %s must exceed store.download_lifetime + twice %s of clock skew, %s (compactor.md §8.3)",
			c.Compact.DeleteDelay, clockSkew, bound))
	}
	switch c.Store.Kind {
	case "fs":
		if c.Store.Dir == "" {
			errs = append(errs, errors.New("store.dir is required for an fs store"))
		}
	case "s3":
		// The store takes static credentials only (T-34).
		for key, v := range map[string]string{
			"store.s3.bucket": c.Store.S3.Bucket, "store.s3.access_key_id": c.Store.S3.AccessKeyID, "store.s3.secret_access_key": c.Store.S3.SecretAccessKey,
		} {
			if v == "" {
				errs = append(errs, fmt.Errorf("%s is required for an s3 store", key))
			}
		}
	default:
		// The kind's own validation names it.
	}
	if (c.Bus.CertFile == "") != (c.Bus.KeyFile == "") {
		errs = append(errs, errors.New("bus.cert_file and bus.key_file go together, for mutual TLS"))
	}
	if c.Bus.CredsFile != "" && c.Bus.User != "" {
		errs = append(errs, errors.New("bus.creds_file and bus.user are exclusive"))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("config: %w", err)
	}

	return nil
}

// Dev is the configuration of tracekeep dev: the defaults, a SQLite catalog and an fs store under dir, the bus at url,
// and a flush interval short enough to watch a capture arrive.
func Dev(dir, url string, flush time.Duration) (*Config, error) {
	var c Config
	if err := fuda.SetDefaults(&c); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	c.Catalog.DSN = filepath.Join(dir, "catalog.db")
	c.Bus.URL, c.Bus.SubjectPrefix, c.Bus.ClaimBucket = url, "tk", "tk-claims"
	c.Store.Kind, c.Store.Dir, c.Store.Presign = "fs", filepath.Join(dir, "packs"), false
	c.Ingest.FlushInterval, c.Ingest.AckWait, c.Ingest.MaxWait = flush, max(time.Minute, 2*flush), time.Second
	c.Compact.Interval = 30 * time.Second
	c.Compact.QuietPeriod = c.Ingest.FlushInterval + c.Ingest.AckWait + c.Compact.RedeliverySlack + time.Minute
	c.Replica, c.Metrics.Listen = "dev", "" // dev serves metrics only where asked
	if err := fuda.Validate(&c); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if err := c.check(true); err != nil {
		return nil, err
	}

	return &c, nil
}
