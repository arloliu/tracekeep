# 650 — Performance & Security

## Performance

Hot paths:

- **The producer callback** runs on go-secs's receive and write goroutines,
  the outbound one under the connection's write lock.
  Its critical section allocates nothing and does no I/O;
  the callback's one allocation is the frame copy of an admitted record, made after the lane's budget is reserved.
  Structures the critical section touches are preallocated when the capture is created.
  `producer.md` §4.2 *Observer callback*.
- **The ingest batch**: per (capture, hour), sorted by seq at flush, one segment per flush.
  Pre-allocate slices and maps when the size is known, reuse buffers across batches,
  and keep the per-message path free of catalog round-trips the batch can make once.

Everywhere:

- Profile with `pprof` and benchmark before and after; don't optimize on intuition.
  A benchmark uses `b.Loop` and `b.ReportAllocs`, asserts the work of each iteration and does the same work in each;
  it runs once at its small size in CI, its larger sizes only when `TRACEKEEP_BENCH_SCALE` names them.
  A change regresses a benchmark when the bootstrap interval of its ratio to the base lies above 5% plus the base's repeatability,
  and an interval straddling that allowance is measured again, never passed (T-54).
- Use `sync/atomic` for simple flags and counters, `sync.Mutex` for compound state.
- Bounded queues only: handle the full signal instead of blocking.

## Security

- **Key material** is held in memory only: the service never writes it to disk, a log or a temporary object.
  The `file` key provider only reads files an operator or a Vault Agent places, mode 0600.
  It is resolved, checked against its binding and cached only by `internal/keys`.
- **Clear text** exists only on the traffic and parking streams, in the claim bucket (TLS and encryption at rest)
  and in a consumer's memory before masking.
  Storage holds masked packs only.
- **Originals** are restored only into an API response, never into a file, an export or a temporary object;
  a restore writes its audit row first.
  `masking-keys.md` §9.
- **External input** is validated at the boundary:
  bus messages in the order of `ingest.md` §6.1, layout and length first,
  API requests at the handler, configuration at load.
- **Secrets** (NATS credentials, S3 keys, OIDC client secrets, Vault tokens) are never logged.
- **Download URLs**, presigned or carrying an API download token, are bearer credentials until they expire:
  issued only after the role check, living less than the deletion delay D, never logged.
  `api.md` §10.1 *Downloads*.
- **Transport**: TLS for NATS, S3, Vault and the API.
  A bus or S3 link without TLS is accepted, so no installation stops starting, but outside `tracekeep dev` each role warns at its start once per such link:
  the bus judged by its connection's TLS state (a `nats://` URL the server upgrades is not warned), the store by an `http://` endpoint (an empty one, the provider's default over TLS, is not);
  `all` warns once for its roles, and neither the catalog's DSN, `admin`, `status` nor a reconnect is judged.
- **pprof** is off by default and served only on the metrics listener, which is unauthenticated (`operations.md` §10.4):
  a profile shows the heap and the command line, so it is on only while that listener is reachable by its operators alone.
- **gosec** is enabled; suppress a finding only with `//nolint:gosec // <specific reason>`.
