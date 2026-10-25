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
- Use `sync/atomic` for simple flags and counters, `sync.Mutex` for compound state.
- Bounded queues only: handle the full signal instead of blocking.

## Security

- **Key material** is held in memory only: the service never writes it to disk, a log or a temporary object.
  The `file` key provider only reads files an operator or a Vault Agent places, mode 0600.
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
- **Transport**: TLS for NATS, S3, Vault and the API.
- **gosec** is enabled; suppress a finding only with `//nolint:gosec // <specific reason>`.
