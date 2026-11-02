package catalog

import (
	"os"
	"testing"
)

// envEngineFacts set to 1 runs TestEngineFacts, as scripts/catalog.sh bench does.
const envEngineFacts = "TRACEKEEP_ENGINE_FACTS"

// TestEngineFacts logs what a baseline records of each engine (650-perf-sec.md), asked of the engine itself on the catalog's write connection:
// SQLite's library version, journal mode and synchronous level, and MariaDB's version and innodb_flush_log_at_trx_commit.
func TestEngineFacts(t *testing.T) {
	t.Parallel()
	if os.Getenv(envEngineFacts) != "1" {
		t.Skipf("%s not set", envEngineFacts)
	}
	engines(t, func(t *testing.T, c *Catalog) {
		q := `SELECT 'sqlite ' || sqlite_version() || ', journal_mode ' || (SELECT journal_mode FROM pragma_journal_mode)
			|| ', synchronous ' || (SELECT synchronous FROM pragma_synchronous)`
		if c.Engine() == EngineMariaDB {
			q = `SELECT CONCAT('mariadb ', VERSION(), ', innodb_flush_log_at_trx_commit ', @@innodb_flush_log_at_trx_commit)`
		}
		var facts string
		if err := c.write.GetContext(t.Context(), &facts, q); err != nil {
			t.Fatal(err)
		}
		t.Logf("engine facts: %s", facts)
	})
}
