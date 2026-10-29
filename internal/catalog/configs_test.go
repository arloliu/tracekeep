package catalog

import (
	"crypto/sha256"
	"errors"
	"testing"
)

func stage(t *testing.T, c *Catalog, content string) int64 {
	t.Helper()
	id, err := c.StageConfig(t.Context(), Staging{Content: []byte(content)})
	if err != nil {
		t.Fatal(err)
	}

	return id
}

func state(t *testing.T, c *Catalog, id int64) string {
	t.Helper()
	cs, err := c.Configs(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for i := range cs {
		if cs[i].ID == id {
			return cs[i].State()
		}
	}
	t.Fatalf("configuration %d not listed", id)

	return ""
}

func required(t *testing.T, c *Catalog) []int64 {
	t.Helper()
	cs, err := c.RequiredConfigs(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]int64, len(cs))
	for i, s := range cs {
		ids[i] = s.ID
	}

	return ids
}

// A staged configuration is stored and listed, but neither required of members nor bindable; equal content is one row,
// and staging never changes an active one (rollouts.md §6.4).
func TestStagedIsNeitherRequiredNorBindable(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		v1 := gate(t, c)
		s := stage(t, c, "staged")
		if again := stage(t, c, "staged"); again != s {
			t.Fatalf("one content staged as %d and %d", s, again)
		}
		if st := state(t, c, s); st != ConfigStaged {
			t.Errorf("state = %s, want staged", st)
		}
		if got := required(t, c); len(got) != 1 || got[0] != v1 {
			t.Errorf("required = %v, want only the active %d", got, v1)
		}
		if _, err := c.Register(ctx, "other", []int64{v1}, ttl); err != nil {
			t.Errorf("Register without the staged one = %v", err)
		}
		// Every member lists it, and still nothing binds it until it is activated.
		if _, err := c.Register(ctx, "r", []int64{v1, s}, ttl); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Register(ctx, "other", []int64{v1, s}, ttl); err != nil {
			t.Fatal(err)
		}
		if id, ok := bindable(t, c); !ok || id != v1 {
			t.Errorf("bindable = %d, %v; want the active %d", id, ok, v1)
		}
		if again := stage(t, c, "config"); again != v1 || state(t, c, v1) != ConfigActive {
			t.Errorf("staging the active content = %d in %s, want %d left active", again, state(t, c, v1), v1)
		}
	})
}

// Only an activation makes a staged configuration bindable, once every leased member lists it; an active one is unchanged,
// an unknown or retired one refused; each activation writes its audit row with it.
func TestActivateStaged(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		v1 := gate(t, c)
		s := stage(t, c, "next")
		if _, err := c.Register(ctx, "r", []int64{v1, s}, ttl); err != nil {
			t.Fatal(err)
		}
		audit := &AuditEntry{Actor: "op", Action: "config activate", Detail: "config activate"}
		if changed, err := c.ActivateStaged(ctx, s, nil, audit); err != nil || !changed {
			t.Fatalf("ActivateStaged = %v, %v", changed, err)
		}
		if id, ok := bindable(t, c); !ok || id != s {
			t.Errorf("bindable after the activation = %d, %v; want %d", id, ok, s)
		}
		if changed, err := c.ActivateStaged(ctx, s, nil, audit); err != nil || changed {
			t.Errorf("ActivateStaged of an active one = %v, %v; want unchanged", changed, err)
		}
		if _, err := c.ActivateStaged(ctx, 999, nil, audit); !errors.Is(err, ErrUnknownConfig) {
			t.Errorf("ActivateStaged of an unknown id = %v", err)
		}
		r := stage(t, c, "retired")
		if err := c.RetireConfig(ctx, r, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := c.ActivateStaged(ctx, r, nil, audit); !errors.Is(err, ErrConfigRetired) || state(t, c, r) != ConfigRetired {
			t.Errorf("ActivateStaged of a retired one = %v, state %s", err, state(t, c, r))
		}
		if rows, err := c.AuditByActor(ctx, "op"); err != nil || len(rows) != 2 {
			t.Errorf("audit = %+v, %v; want the two answered activations", rows, err)
		}
		// Once active, it is required of members.
		if _, err := c.Register(ctx, "old", []int64{v1}, ttl); !errors.Is(err, ErrConfigNotExecuted) {
			t.Errorf("Register without the activated one = %v, want ErrConfigNotExecuted", err)
		}
	})
}

// A policy version is stored once: the same body again changes nothing, another body under its id and version
// refuses the whole staging, configuration and audit row included.
func TestStagePolicy(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		p := &StoredPolicy{ID: "baseline", Version: 1, Body: []byte(`{"policy_id":"baseline"}`)}
		if _, err := c.StageConfig(ctx, Staging{Content: []byte("masking-1"), Policy: p}); err != nil {
			t.Fatal(err)
		}
		if _, err := c.StageConfig(ctx, Staging{Content: []byte("masking-2"), Policy: p}); err != nil {
			t.Errorf("staging again under the same policy = %v", err)
		}
		other := &StoredPolicy{ID: "baseline", Version: 1, Body: []byte(`{"policy_id":"other"}`)}
		audit := &AuditEntry{Actor: "op", Action: "config stage"}
		if _, err := c.StageConfig(ctx, Staging{Content: []byte("masking-3"), Policy: other, Audit: audit}); !errors.Is(err, ErrPolicyConflict) {
			t.Fatalf("staging under another body = %v, want ErrPolicyConflict", err)
		}
		got, ok, err := c.Policy(ctx, "baseline", 1)
		sum := sha256.Sum256(p.Body)
		if err != nil || !ok || string(got.Body) != string(p.Body) || string(got.Hash) != string(sum[:]) {
			t.Errorf("Policy = %+v, %v, %v; want the first body", got, ok, err)
		}
		cs, err := c.Configs(ctx)
		if err != nil || len(cs) != 2 {
			t.Errorf("configurations = %d, %v; want the refused staging to add none", len(cs), err)
		}
		if rows, _ := c.AuditByActor(ctx, "op"); len(rows) != 0 {
			t.Errorf("audit = %+v, want none for the refused staging", rows)
		}
		if ps, err := c.Policies(ctx); err != nil || len(ps) != 1 {
			t.Errorf("Policies = %+v, %v", ps, err)
		}
		if _, err := c.StageConfig(ctx, Staging{Content: []byte("x"), Policy: &StoredPolicy{ID: "p", Version: 1, Body: []byte("b"), Hash: []byte("not its")}}); err == nil {
			t.Error("a policy whose hash is not its body's was stored")
		}
	})
}

// Configs lists each configuration's state and whether a capture is bound to it.
func TestConfigsListsStatesAndBindings(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		v1 := gate(t, c)
		s := stage(t, c, "staged")
		if _, err := c.Admit(ctx, start(newID(t)), descriptor(t, "d")); err != nil {
			t.Fatal(err)
		}
		cs, err := c.Configs(ctx)
		if err != nil || len(cs) != 2 {
			t.Fatalf("Configs = %+v, %v", cs, err)
		}
		if cs[0].ID != v1 || cs[0].State() != ConfigActive || !cs[0].Bound || string(cs[0].Content) != "config" {
			t.Errorf("the active one = %+v", cs[0])
		}
		if cs[1].ID != s || cs[1].State() != ConfigStaged || cs[1].Bound {
			t.Errorf("the staged one = %+v", cs[1])
		}
	})
}

// An activation and an admission serialize on the membership sentinel: the capture binds the old configuration or the new one, never none.
func TestActivationRacesAnAdmission(t *testing.T) {
	t.Parallel()
	for _, admissionFirst := range []bool{true, false} {
		engines(t, func(t *testing.T, c *Catalog) {
			ctx := t.Context()
			v1 := gate(t, c)
			s := stage(t, c, "next")
			if _, err := c.Register(ctx, "r", []int64{v1, s}, ttl); err != nil {
				t.Fatal(err)
			}
			held, release := holdAt(c, hookBeforeCommit)
			var res Result
			var errA, errB error
			admit := func() { res, errA = c.Admit(ctx, start(newID(t)), descriptor(t, "d")) }
			activateIt := func() { _, errB = c.ActivateStaged(ctx, s, nil, nil) }
			first, second := admit, activateIt
			if !admissionFirst {
				first, second = activateIt, admit
			}
			a := run(first)
			<-held
			waits := c.write.Stats().WaitCount
			b := run(second)
			waitBlocked(t, c, waits, "UPDATE counters SET value", 1)
			close(release)
			<-a
			<-b
			want := s
			if admissionFirst {
				want = v1
			}
			if errA != nil || errB != nil || res.Outcome != Admitted || res.ConfigID != want {
				t.Errorf("admission first %v: admitted %+v (%v), activation %v; want bound to %d", admissionFirst, res, errA, errB, want)
			}
		})
	}
}

// An activation and a registration serialize too: a registration lacking the configuration is refused once it is active,
// and a member that registered before the activation keeps the new configuration unbindable until it lapses or leaves.
func TestActivationRacesARegistration(t *testing.T) {
	t.Parallel()
	for _, registrationFirst := range []bool{true, false} {
		engines(t, func(t *testing.T, c *Catalog) {
			ctx := t.Context()
			v1 := gate(t, c)
			s := stage(t, c, "next")
			if _, err := c.Register(ctx, "r", []int64{v1, s}, ttl); err != nil {
				t.Fatal(err)
			}
			held, release := holdAt(c, hookBeforeCommit)
			var errR, errA error
			register := func() { _, errR = c.Register(ctx, "old", []int64{v1}, ttl) }
			activateIt := func() { _, errA = c.ActivateStaged(ctx, s, nil, nil) }
			first, second := register, activateIt
			if !registrationFirst {
				first, second = activateIt, register
			}
			a := run(first)
			<-held
			waits := c.write.Stats().WaitCount
			b := run(second)
			waitBlocked(t, c, waits, "UPDATE counters SET value", 1)
			close(release)
			<-a
			<-b
			if errA != nil {
				t.Fatalf("activation = %v", errA)
			}
			if !registrationFirst {
				if !errors.Is(errR, ErrConfigNotExecuted) {
					t.Errorf("a registration lacking the active configuration = %v, want ErrConfigNotExecuted", errR)
				}
				return
			}
			if errR != nil {
				t.Fatalf("registration = %v", errR)
			}
			if id, ok := bindable(t, c); !ok || id != v1 {
				t.Errorf("bindable with a member lacking the new one = %d, %v; want %d", id, ok, v1)
			}
		})
	}
}

// Two replicas staging one content at once share one configuration.
func TestConcurrentStagingSharesOneRow(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		held, release := holdAt(c, hookBeforeCommit)
		var id1, id2 int64
		var err1, err2 error
		a := run(func() { id1, err1 = c.StageConfig(t.Context(), Staging{Content: []byte("same")}) })
		<-held
		waits := c.write.Stats().WaitCount
		b := run(func() { id2, err2 = c.StageConfig(t.Context(), Staging{Content: []byte("same")}) })
		waitBlocked(t, c, waits, "UPDATE counters SET value", 1)
		close(release)
		<-a
		<-b
		cs, err := c.Configs(t.Context())
		if err1 != nil || err2 != nil || err != nil || id1 != id2 || len(cs) != 1 {
			t.Errorf("stagings = %d (%v), %d (%v); %d rows (%v); want one shared row", id1, err1, id2, err2, len(cs), err)
		}
	})
}

var errRefused = errors.New("refused by the check")

// An activation's check judges the content inside its transaction: refused, nothing is activated or audited;
// and a configuration staged while the activation waits for the sentinel is judged once it is committed, never skipped.
func TestActivationCheckRunsInTheTransaction(t *testing.T) {
	t.Parallel()
	engines(t, func(t *testing.T, c *Catalog) {
		ctx := t.Context()
		refuseMasking := func(content []byte) error {
			if string(content) == "masking" {
				return errRefused
			}

			return nil
		}
		audit := &AuditEntry{Actor: "op", Action: "config activate"}
		held, release := holdAt(c, hookBeforeCommit)
		var staged int64
		var errS, errA error
		a := run(func() { staged, errS = c.StageConfig(ctx, Staging{Content: []byte("masking")}) })
		<-held
		waits := c.write.Stats().WaitCount
		// The activation names the id the held staging allocates, as a racing operator could.
		b := run(func() { _, errA = c.ActivateStaged(ctx, 1, refuseMasking, audit) })
		waitBlocked(t, c, waits, "UPDATE counters SET value", 1)
		close(release)
		<-a
		<-b
		if errS != nil || staged != 1 || !errors.Is(errA, errRefused) {
			t.Fatalf("staging %d (%v), activation %v; want the activation refused by the check", staged, errS, errA)
		}
		if st := state(t, c, 1); st != ConfigStaged {
			t.Errorf("state = %s, want still staged", st)
		}
		if rows, _ := c.AuditByActor(ctx, "op"); len(rows) != 0 {
			t.Errorf("audit = %+v, want none for the refused activation", rows)
		}
		if changed, err := c.ActivateStaged(ctx, stage(t, c, "plain"), refuseMasking, audit); err != nil || !changed {
			t.Errorf("an activation the check admits = %v, %v", changed, err)
		}
	})
}
