// Package catalog is the service's database (docs/specs/tracekeep/catalog.md) on SQLite or MariaDB:
// its domain transactions and reads.
// No caller issues a statement, opens a transaction or chooses a lock.
// Every transaction that changes a capture starts with the capture-row prologue,
// so the lock order of §7.2 holds by construction, and retries stay inside the package (§7.4).
package catalog

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	_ "modernc.org/sqlite" // the sqlite driver
)

// The engines a catalog runs on.
const (
	// EngineSQLite is a SQLite file, written by one process.
	EngineSQLite = "sqlite"
	// EngineMariaDB is a MariaDB server.
	EngineMariaDB = "mariadb"
)

// defaultBusyTimeout is how long a SQLite statement waits for a lock another connection holds.
const defaultBusyTimeout = 5 * time.Second

// ErrSecondWriter reports a SQLite catalog another process already writes: only the all process writes one (§7.4).
var ErrSecondWriter = errors.New("catalog: another process writes this SQLite catalog")

// Config says which database the catalog is.
// MariaDB is 11.1 or later, the first with the transaction_isolation variable.
type Config struct {
	// Engine is EngineSQLite or EngineMariaDB.
	Engine string
	// DSN is the database file for SQLite, and a go-sql-driver/mysql DSN for MariaDB.
	DSN string
	// BusyTimeout bounds a SQLite statement's wait for a lock; zero takes five seconds.
	BusyTimeout time.Duration
}

// Catalog is an open catalog.
type Catalog struct {
	d dialect
	// write runs every transaction; on SQLite it holds one connection, the one writer.
	write *sqlx.DB
	// read serves reads outside a transaction; on MariaDB it is write.
	read *sqlx.DB
	// writerLock marks this process as the SQLite catalog's writer while it is open.
	writerLock *os.File
	now        func() int64
	// hooks are the package's test seams, nil otherwise.
	hooks [numHooks]func()
	// testRetryable, set only by the package's tests, is an error a transaction retries like a deadlock.
	testRetryable error
}

// Test hooks: points where a barrier-controlled test holds a transaction.
const (
	// hookRetry runs after an attempt failed with a retryable error, before the next.
	hookRetry = iota
	// hookLocked runs once the prologue holds the capture row.
	hookLocked
	// hookBeforeCommit runs once a transaction's statements succeeded, before its commit.
	hookBeforeCommit
	// hookCompletenessRead runs between the reads of a completeness, inside its snapshot.
	hookCompletenessRead
	numHooks
)

// Open opens the catalog cfg names; its schema must already be migrated.
func Open(ctx context.Context, cfg Config) (*Catalog, error) {
	var c *Catalog
	var err error
	switch cfg.Engine {
	case EngineSQLite:
		c, err = openSQLite(cfg)
	case EngineMariaDB:
		c, err = openMariaDB(cfg)
	default:
		return nil, fmt.Errorf("catalog: unknown engine %q", cfg.Engine)
	}
	if err != nil {
		return nil, err
	}
	c.now = func() int64 { return time.Now().UnixNano() }
	if err := c.write.PingContext(ctx); err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("catalog: %w", err)
	}

	return c, nil
}

// Close closes the catalog.
func (c *Catalog) Close() error {
	err := c.write.Close()
	if c.read != c.write {
		err = errors.Join(err, c.read.Close())
	}
	if c.writerLock != nil {
		err = errors.Join(err, c.writerLock.Close())
	}

	return err
}

// SQLiteURI returns the SQLite URI of the database file at path, its path escaped,
// so a "#", "?" or "%" in it names the file rather than ending the path or starting a parameter.
func SQLiteURI(path string) string {
	return "file:" + (&url.URL{Path: path}).EscapedPath()
}

func openSQLite(cfg Config) (*Catalog, error) {
	// The lock is the file's, so a second process, or a second Open in this one, finds it held;
	// the path is resolved first, so a link to the database names the same lock.
	path, err := filepath.EvalSymlinks(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("catalog: %w", err)
	}
	if path, err = filepath.Abs(path); err != nil {
		return nil, fmt.Errorf("catalog: %w", err)
	}
	lock, err := os.OpenFile(path+".writer", os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("catalog: %w", err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrSecondWriter
		}

		return nil, fmt.Errorf("catalog: %w", err)
	}
	busy := cfg.BusyTimeout
	if busy == 0 {
		busy = defaultBusyTimeout
	}
	q := url.Values{}
	q.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", busy.Milliseconds()))
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(FULL)")
	base := SQLiteURI(path) + "?"
	// Every write transaction begins IMMEDIATE, taking the write lock first, on the one writer connection.
	write, err := sqlx.Open("sqlite", base+q.Encode()+"&_txlock=immediate")
	if err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("catalog: %w", err)
	}
	write.SetMaxOpenConns(1)
	q.Add("_pragma", "query_only(1)")
	read, err := sqlx.Open("sqlite", base+q.Encode())
	if err != nil {
		_ = write.Close()
		_ = lock.Close()
		return nil, fmt.Errorf("catalog: %w", err)
	}

	return &Catalog{d: sqliteDialect, write: write, read: read, writerLock: lock}, nil
}

func openMariaDB(cfg Config) (*Catalog, error) {
	mc, err := mysql.ParseDSN(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("catalog: %w", err)
	}
	if mc.Params == nil {
		mc.Params = map[string]string{}
	}
	mc.Params["transaction_isolation"] = "'READ-COMMITTED'"
	// UNIX_TIMESTAMP(NOW()), the database's clock, goes through the session's time zone, which UTC keeps free of daylight saving.
	mc.Params["time_zone"] = "'+00:00'"
	// Affected rows count the rows an update matched, as on SQLite, so a guard that rewrites a value unchanged still counts.
	mc.ClientFoundRows = true
	db, err := sqlx.Open("mysql", mc.FormatDSN())
	if err != nil {
		return nil, fmt.Errorf("catalog: %w", err)
	}

	return &Catalog{d: mariadbDialect, write: db, read: db}, nil
}

func (c *Catalog) hook(id int) {
	if f := c.hooks[id]; f != nil {
		f()
	}
}
