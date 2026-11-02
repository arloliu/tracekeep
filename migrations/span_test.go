package migrations_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"math"
	"strings"
	"testing"
	"time"

	"ariga.io/atlas/sql/migrate"

	"github.com/arloliu/tracekeep/migrations"
)

// The span column's migration and the migration of its index and the other four, each the stage of a two-stage change.
const (
	spanStage    = "_span.sql"
	indexesStage = "_span_indexes.sql"
)

// spanIndexes are the indexes the second stage adds.
var spanIndexes = []string{"pack_ranges_capture_span", "conflicts_capture_pack", "conflicts_capture_other_pack", "merges_outcome_delete_at", "merges_capture_hour"}

// stage is the position of the migration whose name ends in suffix among fs.
func stage(t *testing.T, fs []migrate.File, suffix string) int {
	t.Helper()
	for i, f := range fs {
		if strings.HasSuffix(f.Name(), suffix) {
			return i
		}
	}
	t.Fatalf("no migration named *%s", suffix)

	return 0
}

func newID(t *testing.T) []byte {
	t.Helper()
	id := make([]byte, 16)
	_, _ = rand.Read(id)

	return id
}

// insertRange inserts a range naming only the columns a writer older than the span column names (internal/catalog/register.go).
func insertRange(ctx context.Context, db *sql.DB, capture []byte, from, to int64) error {
	pack := make([]byte, 16)
	_, _ = rand.Read(pack)
	_, err := db.ExecContext(ctx, `INSERT INTO pack_ranges (pack_id, capture_id, seq_from, seq_to, ts_from, ts_to) VALUES (?, ?, ?, ?, ?, ?)`,
		pack, capture, from, to, from, to)

	return err
}

// checkSpans asserts every stored range's span is its width and returns how many ranges are stored.
func checkSpans(t *testing.T, db *sql.DB) int {
	t.Helper()
	rows, err := db.QueryContext(t.Context(), `SELECT seq_from, seq_to, span FROM pack_ranges`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var from, to int64
		var span sql.NullInt64
		if err := rows.Scan(&from, &to, &span); err != nil {
			t.Fatal(err)
		}
		if !span.Valid || span.Int64 != to-from {
			t.Errorf("range [%d, %d] has span %v, want %d", from, to, span, to-from)
		}
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}

	return n
}

func revisionOf(t *testing.T, db *sql.DB, f migrate.File) revision {
	t.Helper()
	for _, r := range revisions(t, db) {
		if r.version == f.Version() {
			return r
		}
	}
	t.Fatalf("no revision of %s", f.Name())

	return revision{}
}

// A catalog with stored ranges, migrated through both stages, answers each range's span,
// as it does for a range inserted naming only the old columns, also while the second stage has failed;
// the failed second stage resumes from its failed statement once the cause is gone.
func TestSpanMigration(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, db *sql.DB, engine string) {
		ctx := t.Context()
		all := files(t, engine)
		first := stage(t, all, spanStage)
		second := stage(t, all, indexesStage)
		if second != first+1 {
			t.Fatalf("the stages are migrations %d and %d, want adjacent", first, second)
		}
		if err := migrations.ApplyDir(ctx, db, engine, dirOf(t, all[:first])); err != nil {
			t.Fatal(err)
		}
		capture := newID(t)
		for _, r := range [][2]int64{{0, 0}, {5, 9}, {10, 1 << 40}, {0, math.MaxInt64}} {
			if err := insertRange(ctx, db, capture, r[0], r[1]); err != nil {
				t.Fatal(err)
			}
		}
		// An index of the second stage's name, made by hand, fails that stage after it applied a statement on either engine.
		if _, err := db.ExecContext(ctx, `CREATE INDEX merges_capture_hour ON merges (at)`); err != nil {
			t.Fatal(err)
		}
		if err := migrations.Apply(ctx, db, engine); err == nil {
			t.Fatal("Apply over a clashing index succeeded")
		}
		if r := revisionOf(t, db, all[first]); r.applied != r.total || r.err != "" {
			t.Fatalf("the column's revision = %+v, want applied whole", r)
		}
		r := revisionOf(t, db, all[second])
		if r.applied == 0 || r.applied >= r.total || r.err == "" || !strings.Contains(r.errStmt, "merges_capture_hour") {
			t.Fatalf("the indexes' revision = %+v, want failed at merges_capture_hour after a statement applied", r)
		}
		if n := checkSpans(t, db); n != 4 {
			t.Fatalf("%d ranges after the column, want 4", n)
		}
		if err := insertRange(ctx, db, capture, 100, 150); err != nil {
			t.Fatal(err)
		}
		dropClash(t, db, engine)
		if err := migrations.Apply(ctx, db, engine); err != nil {
			t.Fatalf("Apply once the clash is gone = %v, want the indexes' migration resumed", err)
		}
		if r := revisionOf(t, db, all[second]); r.applied != r.total || r.err != "" {
			t.Errorf("the resumed revision = %+v, want applied whole", r)
		}
		if n := newIndexes(t, db, engine); n != len(spanIndexes) {
			t.Errorf("%d of the %d new indexes exist", n, len(spanIndexes))
		}
		if err := insertRange(ctx, db, capture, 200, 1<<50); err != nil {
			t.Fatal(err)
		}
		if n := checkSpans(t, db); n != 6 {
			t.Errorf("%d ranges after both stages, want 6", n)
		}
	})
}

// dropClash drops the index TestSpanMigration made to fail the second stage.
func dropClash(t *testing.T, db *sql.DB, engine string) {
	t.Helper()
	drop := `DROP INDEX merges_capture_hour`
	if engine == "mariadb" {
		drop += ` ON merges`
	}
	if _, err := db.ExecContext(t.Context(), drop); err != nil {
		t.Fatal(err)
	}
}

// newIndexes counts the second stage's indexes in db.
func newIndexes(t *testing.T, db *sql.DB, engine string) int {
	t.Helper()
	query := `SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name IN (?, ?, ?, ?, ?)`
	if engine == "mariadb" {
		query = `SELECT COUNT(DISTINCT INDEX_NAME) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND INDEX_NAME IN (?, ?, ?, ?, ?)`
	}
	args := make([]any, len(spanIndexes))
	for i, name := range spanIndexes {
		args[i] = name
	}
	var n int
	if err := db.QueryRowContext(t.Context(), query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}

	return n
}

// MariaDB accepts each stage's generated statement with the weakest lock it can be asked for:
// the column instantly, each index in place with LOCK=NONE, which it refuses for a change that needs the table locked.
// The generated statements name no algorithm or lock, so the server takes its default, the least locking it supports.
func TestSpanStagesNeedNoLock(t *testing.T) {
	t.Parallel()
	db := open(t, "mariadb")
	ctx := t.Context()
	all := files(t, "mariadb")
	first := stage(t, all, spanStage)
	if err := migrations.ApplyDir(ctx, db, "mariadb", dirOf(t, all[:first])); err != nil {
		t.Fatal(err)
	}
	for _, s := range []struct {
		file   migrate.File
		clause string
	}{
		{all[first], ", ALGORITHM=INSTANT"},
		{all[stage(t, all, indexesStage)], ", ALGORITHM=INPLACE, LOCK=NONE"},
	} {
		stmts, err := s.file.Stmts()
		if err != nil {
			t.Fatal(err)
		}
		for _, stmt := range stmts {
			stmt = strings.TrimSuffix(strings.TrimSpace(stmt), ";") + s.clause
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				t.Errorf("%s: %v", stmt, err)
			}
		}
	}
}

// insertTimeout bounds each insert the online test makes, a ceiling against a table locked for a whole ALTER, not the stall to expect:
// each ALTER holds the table's metadata lock exclusively while it prepares and while it commits, and inserts wait meanwhile,
// tens of milliseconds on an idle server but seconds on one busy with other work (its commit flushes the new index).
const insertTimeout = 30 * time.Second

// MariaDB runs the indexes' migration while inserts naming the old columns go on in another connection:
// none fails, and inserts into pack_ranges start and return while its ALTER builds the index.
// The test logs the slowest insert and what each metadata-lock wait it saw was waiting behind.
func TestSpanIndexesOnline(t *testing.T) {
	t.Parallel()
	db := open(t, "mariadb")
	ctx := t.Context()
	all := files(t, "mariadb")
	if err := migrations.ApplyDir(ctx, db, "mariadb", dirOf(t, all[:stage(t, all, indexesStage)])); err != nil {
		t.Fatal(err)
	}
	// Enough rows that each ALTER takes a while, from MariaDB's sequence engine.
	capture := newID(t)
	for _, seed := range []string{
		`INSERT INTO pack_ranges (pack_id, capture_id, seq_from, seq_to, ts_from, ts_to)
			SELECT UNHEX(LPAD(HEX(seq), 32, '0')), ?, seq * 10, seq * 10 + seq % 7, seq, seq FROM seq_1_to_200000`,
		`INSERT INTO conflicts (capture_id, seq_from, seq_to, pack_id, other_pack_id, state)
			SELECT ?, seq, seq, UNHEX(LPAD(HEX(seq), 32, '0')), UNHEX(LPAD(HEX(seq + 1), 32, '0')), 'suspect' FROM seq_1_to_50000`,
		`INSERT INTO merges (merge_id, capture_id, hour, output_pack_id, outcome, at, delete_at)
			SELECT UNHEX(LPAD(HEX(seq), 32, '0')), ?, seq % 100, UNHEX(LPAD(HEX(seq), 32, '0')), 'committed', seq, seq FROM seq_1_to_50000`,
	} {
		if _, err := db.ExecContext(ctx, seed, capture); err != nil {
			t.Fatal(err)
		}
	}

	stop, started := make(chan struct{}), make(chan struct{})
	inserted := make(chan []insert, 1)
	go func() { inserted <- writeOld(ctx, db, capture, stop, started) }()
	select {
	case <-started:
	case <-time.After(30 * time.Second):
		t.Fatal("the writer made no insert")
	}
	unwatch := make(chan struct{})
	watched := make(chan alterWatch, 1)
	go func() { watched <- watchAlter(ctx, db, unwatch) }()
	begin := time.Now()
	err := migrations.Apply(ctx, db, "mariadb")
	took := time.Since(begin)
	close(unwatch)
	w := <-watched
	close(stop)
	done := <-inserted
	if err != nil {
		t.Fatal(err)
	}
	if w.err != nil {
		t.Fatal(w.err)
	}

	var worst time.Duration
	during := 0
	for _, ins := range done {
		if ins.err != nil {
			t.Errorf("an insert into %s during the migration failed: %v", ins.table, ins.err)
		}
		worst = max(worst, ins.end.Sub(ins.start))
		if ins.table == "pack_ranges" && w.within(ins) {
			during++
		}
	}
	t.Logf("migration %v, pack_ranges' ALTER seen for %v; %d inserts, %d into pack_ranges within its ALTER, the slowest %v; at most %d sessions seen waiting on a metadata lock",
		took, w.gone.Sub(w.seen), len(done), during, worst, w.lockWaits)
	for s, n := range w.sightings {
		t.Logf("seen %d times: %s", n, s)
	}
	if w.seen.IsZero() {
		t.Fatal("pack_ranges' ALTER never seen running: the seed is too small to show the migration online")
	}
	if during == 0 {
		t.Error("no insert into pack_ranges started and returned while its ALTER ran")
	}
}

// insert is one insert of the online test's writer.
type insert struct {
	table      string
	start, end time.Time
	err        error
}

// writeOld inserts into pack_ranges, conflicts and merges in turn, each insert naming the old columns under insertTimeout,
// closing started after its first insert, until stop is closed.
func writeOld(ctx context.Context, db *sql.DB, capture []byte, stop <-chan struct{}, started chan<- struct{}) []insert {
	var done []insert
	for i := int64(0); ; i++ {
		select {
		case <-stop:
			return done
		default:
		}
		ins := insert{start: time.Now()}
		ictx, cancel := context.WithTimeout(ctx, insertTimeout)
		id := make([]byte, 16)
		_, _ = rand.Read(id)
		switch i % 3 {
		case 0:
			ins.table, ins.err = "pack_ranges", insertRange(ictx, db, capture, i<<20, i<<20+5)
		case 1:
			ins.table = "conflicts"
			_, ins.err = db.ExecContext(ictx, `INSERT INTO conflicts (capture_id, seq_from, seq_to, pack_id, other_pack_id, state) VALUES (?, ?, ?, ?, ?, 'suspect')`,
				capture, i<<20, i<<20, id, id)
		default:
			ins.table = "merges"
			_, ins.err = db.ExecContext(ictx, `INSERT INTO merges (merge_id, capture_id, hour, output_pack_id, outcome, at) VALUES (?, ?, 1, ?, 'committed', ?)`,
				id, capture, id, i)
		}
		cancel()
		ins.end = time.Now()
		done = append(done, ins)
		if len(done) == 1 {
			close(started)
		}
	}
}

// alterWatch is what a watcher of the server's sessions saw:
// when pack_ranges' ALTER was first seen and first seen gone, the most sessions seen waiting on a metadata lock at once,
// and each distinct sighting of such a wait: the waiting statements and the ALTER running then, with its state.
type alterWatch struct {
	seen, gone time.Time
	lockWaits  int
	sightings  map[string]int
	err        error
}

// within reports whether ins started after the ALTER was seen and returned before it was seen gone.
func (w alterWatch) within(ins insert) bool {
	return !w.seen.IsZero() && ins.start.After(w.seen) && (w.gone.IsZero() || ins.end.Before(w.gone))
}

// watchAlter samples db's sessions until stop is closed.
func watchAlter(ctx context.Context, db *sql.DB, stop <-chan struct{}) alterWatch {
	w := alterWatch{sightings: map[string]int{}}
	for {
		var alter, waits int
		var sighting sql.NullString
		w.err = db.QueryRowContext(ctx, `SELECT COALESCE(SUM(INFO LIKE 'ALTER TABLE `+"`pack_ranges`"+`%'), 0), COALESCE(SUM(STATE LIKE '%metadata lock%'), 0),
				CONCAT('waiting: ', GROUP_CONCAT(CASE WHEN STATE LIKE '%metadata lock%' THEN LEFT(INFO, 30) END SEPARATOR ', '),
					'; alter: ', COALESCE(MAX(CASE WHEN INFO LIKE 'ALTER%' THEN CONCAT(LEFT(INFO, 30), ' (', STATE, ')') END), 'none'))
			FROM information_schema.PROCESSLIST WHERE DB = DATABASE()`).Scan(&alter, &waits, &sighting)
		if w.err != nil {
			return w
		}
		now := time.Now()
		if alter > 0 && w.seen.IsZero() {
			w.seen = now
		}
		if alter == 0 && !w.seen.IsZero() && w.gone.IsZero() {
			w.gone = now
		}
		w.lockWaits = max(w.lockWaits, waits)
		if sighting.Valid {
			w.sightings[sighting.String]++
		}
		select {
		case <-stop:
			return w
		default:
		}
		time.Sleep(time.Millisecond) // a sampling interval, not a wait for state
	}
}
