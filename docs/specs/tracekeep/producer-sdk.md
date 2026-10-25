# tracekeep — producer SDK

Status: draft (2026-10-25) — split from seed v21

Covers the producer SDK module:
why it is a separate module, its packages, `Config` and `New`, the publication policy, `Bus`, versions, the producer-side API, `Preflight`, `producertest` and the five guarantees.
From seed v21 §4.7; section numbers are the seed's, and [README.md](README.md) maps each to its file.
Read before designing the SDK's API or changing anything under `producer/`, and before integrating a producer such as eqp-hub.

## Interface

Non-normative summary of what a caller must know.
The sections below are authoritative; a disagreement is a defect in this summary.
Contract changes under discussion are in [P1](proposals/p1-interfaces.md).

- **Module**: `github.com/arloliu/tracekeep/producer`, its own `go.mod`, tagged `producer/vX.Y.Z`;
  it imports tracepack, nats.go and, in `secsobs`, go-secs `hsms`, and never the service module (§4.7 *Why a module*, *No import cycle*).
- **Packages**: `producer` (the algorithm of §4.2–§4.6, `Metrics` and `Logger` interfaces), `producer/secsobs` (go-secs events to records), `producer/busconfig` (the one definition of the stream, bucket and consumer settings), `producer/producertest` (§4.7 *Packages*, *Producer-side API*).
- **Construction**: `New(cfg Config, opts ...Option)`; `Config` names the bus and nothing about driving it;
  every tunable has a default (§4.7 *The SDK owns the NATS client*).
- **Identity**: carried by each capture's descriptor, never by `Config`:
  the SDK derives `recorder_instance_id` from the descriptor's `tool_id` and `recorder_instance_name` (`producer.md` §4.5).
- **Connection**: by default the SDK dials a dedicated connection with every correctness-bearing option applied;
  `WithConn` brings a producer's own connection, which `New` verifies and refuses if it fails;
  `WithBus` brings any `Bus`, which is how `producertest` injects its in-memory bus;
  nothing about the stream is configurable from a producer.
- **Publication policy**: the SDK's on every route: message id `<capture_id>.<seq>`, the expected stream, no client retries;
  the synchronous set for `start` and every reserved record, the asynchronous set for ordinary records (§4.7 *Publication policy*).
- **`Bus`**: the seam the core is written against, exported so `producertest` can implement it, not the primary API.
- **`Preflight`**: runs on the producer's own bus, in the background after the connection is up, never delaying the device's `Start`;
  returns a readable list of what is wrong.
- **What the producer must guarantee**: five items, the whole of an integrator's obligations (§4.7, last paragraph; `.agents/rules/600-invariants.md`).
- **Open**: the producer-side API is a sketch and fixes no signatures;
  *Versions* keeps the SDK's nats.go floor at or below the gateway's v1.40.1,
  which the owner's choice of v1.55.0 contradicts, an open item of the bootstrap plan.

### 4.7 The producer SDK: a separate Go module

**Why a module, and why a separate one.**
Every producer must implement §4.2–§4.6 identically;
eqp-hub should not re-derive them, and a later producer (a test fixture, another gateway) should not either.
It is a **separate Go module** in the tracekeep repository, `github.com/arloliu/tracekeep/producer` (its own `go.mod`, tagged `producer/vX.Y.Z`),
because importing a package of the service module would pull the service's dependency graph (sqlx, minio, oidc, atlas) into every producer.

**No import cycle.**
The bus payload is not tracekeep's:
`tracepack.BusRecord`, `EncodeBusBody`, `BusSubject`, `BusMsgID`, `EncodeDescriptor` and `CheckBusStart` live in `github.com/arloliu/go-secs/tracepack`,
already a separate module that imports no NATS client.
The dependency direction is fixed and one-way:

```
go-secs/tracepack  ←  tracekeep/producer (SDK)  ←  tracekeep (service module)
```

The SDK never imports the service module;
the service imports the SDK for the shared bus configuration and for in-process producers in tests.
A header or convention tracekeep needs beyond the spec goes into the SDK, never into the service,
so the direction holds.

**Packages:**

| Package | Depends on | Holds |
|---|---|---|
| `producer` | tracepack, nats.go `jetstream` | `Producer`, `Capture`, the algorithm of §4.2 (lock, lanes, rotation, `start`, claim-check, retry horizon), the pending-request table and sampler of §4.6, `Metrics` and `Logger` interfaces (no Prometheus dependency) |
| `producer/secsobs` | + go-secs `hsms` | `WireEvent`, `SocketEvent`, `LifecycleEvent`, `TxEvent` → records (tracepack-go.md §5), the frame copy, the socket→epoch map |
| `producer/busconfig` | nats.go `jetstream` | `StreamConfig()`, `ObjectStoreConfig()`, `ConsumerConfig()`, subject prefix and naming: one definition of [STO §4] *Stream settings* shared by producers and `tracekeep migrate` |

**The SDK owns the NATS client by default; injection is the escape hatch.**
A producer's configuration names the bus and nothing about how to drive it:

```go
type Config struct {
    Servers            []string          // NATS URLs of the traffic cluster
    Auth               AuthConfig        // one of: credentials file, NKey seed, user and password, token
    TLS                TLSConfig         // CA, client certificate and key, server name
    SubjectPrefix      string            // the traffic stream's prefix (busconfig knows the stream)
    ClaimBucket        string
    Tunables           Tunables          // lanes, H, τ, sampler; every field has a default
}

func New(cfg Config, opts ...Option) (*Producer, error)
// Options: WithConn(*nats.Conn) brings a connection the producer built itself;
// WithBus(Bus) brings any implementation, which is how producertest injects its in-memory bus.
```

`Config` holds no identity:
it is fixed at `New`, while `toolID` can change through a configuration update,
and every capture's descriptor already carries `tool_id`.
The SDK derives `recorder_instance_id` from the descriptor of each capture it starts or rotates to (`producer.md` §4.5),
so the identity follows whichever lifecycle call starts or rotates a capture.

By default `New` dials a **dedicated connection** to the traffic cluster with every correctness-bearing option applied by the SDK itself:
the reconnect buffer disabled (§4.2), unlimited reconnects, TLS, the publish deadline τ, `WithPublishAsyncMaxPending` and `WithPublishAsyncTimeout` on the JetStream context, the synchronous and asynchronous publish option sets,
and the claim bucket's chunk size from `MaxPayload`;
nothing about the **stream** is configurable from a producer, because the stream is tracekeep's.
`WithConn` is for producers with their own TLS loader, credential rotation, dialer or connection event hooks;
`New` then verifies what it can on the given connection (the reconnect buffer, TLS) and refuses one that fails.
`Bus` remains the internal seam the core is written against and that `producertest` implements;
it is exported for that purpose, not as the primary API.

**Publication policy**, applied identically on the default connection and on an injected one,
since a producer never chooses publish options:
- common to every publish: `WithMsgID("<capture_id>.<seq>")` and `WithExpectStream(<traffic stream>)`,
  which stamps `Nats-Expected-Stream` into the message (the header parking removes, §6.1), and `WithRetryAttempts(0)`:
  the client's default of two retries on `ErrNoResponders` is turned off (nats.go v1.40.1 retries only on that error, and `0` means none),
  and every retry is the SDK's own, under its backoff and the horizon H,
  so a retry is always a deliberate re-publish with the same message id;
- **synchronous set** (`start`, every reserved record, and `PublishMsg` in general):
  the common options plus a context deadline τ, and **no** `WithStallWait`,
  which `PublishMsg` rejects with `ErrInvalidOption`;
- **asynchronous set** (ordinary records, `PublishMsgAsync`):
  the common options plus a short `WithStallWait` (default 100 ms), with `WithPublishAsyncMaxPending` and `WithPublishAsyncTimeout(τ)` set on the JetStream context the SDK builds;
  a stalled or timed-out async publish is handled by the publisher goroutine as a failed attempt, never by the callback.
The `start` therefore uses the synchronous set, as §4.2 requires,
and a producer's phase-0 gate tests the `start` with exactly this set.

```go
// Bus is the seam the core uses; jetstream.JetStream satisfies it.
type Bus interface {
    Conn() *nats.Conn
    PublishMsg(ctx context.Context, m *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error)
    PublishMsgAsync(m *nats.Msg, opts ...jetstream.PublishOpt) (jetstream.PubAckFuture, error)
    PublishAsyncComplete() <-chan struct{}
    ObjectStore(ctx context.Context, bucket string) (jetstream.ObjectStore, error)
    Stream(ctx context.Context, name string) (jetstream.Stream, error) // Preflight's inspection
}
```

**Versions.**
Owning the client isolates nothing at the module level: a Go build holds one version of nats.go,
and minimal version selection picks the **highest** requirement in the whole graph,
so the SDK's requirement is a **floor that can push a producer upward**
(a producer at v1.41.0 and an SDK at v1.55.0 build with v1.55.0, and `go mod tidy` rewrites the producer's go.mod).
The discipline therefore stands:
the SDK's `go.mod` requires the lowest nats.go whose `jetstream` API it needs (at or below eqp-hub's v1.40.1) and never raises it for a feature;
CI runs a sample producer's **resolved** graph (`go list -m all`) at that minimum and at the latest nats.go,
since producers will usually be newer than the floor;
tracepack's own `go.mod` sets a go-secs floor in the same way,
which the SDK's changelog states on every tracepack upgrade.
The header-block length is computed by the SDK from the NATS header wire format, as the spec asks the producer to do.

**Producer-side API** (sketch): `Begin(ctx, CaptureDescriptor)`, `Rotate(ctx, CaptureDescriptor)`, `End(ctx) Report` (bounded, never an error the device must act on),
`Observe(*Record)` (concurrent, non-blocking), `Pending().Register/Complete/Fail(token)`, `Sampler{N, T}`, and:

- `(*Producer).Preflight(ctx) (Report, error)`, run on the producer's own bus
  (`Bus` carries `Stream`, `ObjectStore` and `Conn` for it; `producertest`'s fake implements them):
  checks the stream and bucket exist with the [STO §4] settings that the API exposes
  (retention, storage, limits, compression;
  encryption at rest is a deployment assurance it only reports as unverifiable), `max_payload ≥ 4140`, TLS, the reconnect buffer disabled,
  the capture's `start` message within the header-block and `max_payload` limits
  (through `CheckBusStart` with the real header block), `tool_id` and `recorder_instance_name` set in the descriptor;
  it returns a readable list of what is wrong.
  At startup it runs in the background after the connection is up and never delays the device's `Start` (§4.3);
  it is also an admin-triggered check.
- `producer/producertest`: an in-memory `Bus` and a harness
  that drives disconnects, HotReload, `Stop` under a stalled publisher and fault injection against the integrator's wiring,
  so each producer gets phase 0's chaos tests without rewriting them.

**What the producer must guarantee**, the whole list (everything else is the SDK's):
(1) the traffic cluster's address, credentials and TLS material in `Config`, or a dedicated connection through `WithConn`;
(2) every descriptor carries the configured `toolID` and the configured device name;
(3) `Observe` called from the observers and nowhere else;
(4) `End` called on an orderly stop and never allowed to fail the device;
(5) a rotation on every descriptor change.
