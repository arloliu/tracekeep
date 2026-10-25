# tracekeep — producer SDK

Status: draft (2026-10-25) — split from seed v21

Covers the producer SDK module:
why it is a separate module, its packages, `Config` and `New`, the publication policy, `Bus`, versions, the producer-side API, `Preflight`, `producertest` and the six guarantees.
From seed v21 §4.7; section numbers are the seed's, and [README.md](README.md) maps each to its file.
Read before designing the SDK's API or changing anything under `producer/`, and before integrating a producer such as eqp-hub.

## Interface

Non-normative summary of what a caller must know.
The sections below are authoritative; a disagreement is a defect in this summary.
Contract changes go through proposals, which the subject [README.md](README.md) lists.

- **Module**: `github.com/arloliu/tracekeep/producer`, its own `go.mod`, tagged `producer/vX.Y.Z`;
  it imports tracepack, nats.go and, in `secsobs`, go-secs `hsms`, and never the service module (§4.7 *Why a module*, *No import cycle*).
- **Packages**: `producer` (the algorithm of §4.2–§4.6, `Metrics` and `Logger` interfaces), `producer/secsobs` (go-secs events to records), `producer/busconfig` (the one definition of the stream, bucket and consumer settings), `producer/producertest` (§4.7 *Packages*, *Producer-side API*).
- **Construction**: `New(cfg Config, opts ...Option)`; `Config` names the bus and nothing about driving it;
  every tunable has a default (§4.7 *The SDK owns the NATS client*).
- **Identity**: carried by each capture's descriptor, never by `Config`:
  the SDK derives `recorder_instance_id` from the descriptor's `tool_id` and `recorder_instance_name` (`producer.md` §4.5).
- **Connection**: by default the SDK dials a dedicated connection with every correctness-bearing option applied;
  `WithConn` brings a producer's own connection, which `New` refuses with the reconnect buffer enabled, and on which `Preflight` reports a missing TLS (T-35);
  `WithBus` brings any `Bus`, which is how `producertest` injects its in-memory bus;
  nothing about the stream is configurable from a producer.
- **Publication policy**: the SDK's on every route: message id `<capture_id>.<seq>`, the expected stream, no client retries;
  the synchronous set for `start` and every reserved record, the asynchronous set for ordinary records (§4.7 *Publication policy*).
- **Lifecycle**: `Begin`, `Rotate` and `End`, onto which the device's `Start`, `HotReload` and `Stop` map once (`producer.md` §4.2, T-26).
- **Observers**: the integrator installs `secsobs`'s connection options and lifecycle attachment before the connection opens;
  no record enters the SDK any other way (§4.7 *Producer-side API*).
- **Send path**: `Pending().Register` returns a `Token` on which eqp-hub reports send progress (`Sent`, `SendFailed`, `SubmissionFailed`)
  and the waiter one terminal outcome (`Replied`, `TimedOut` with the effective timeout, `Cancelled`), the first winning;
  eqp-hub's waiter implements `TimeoutSource`, and `producertest` fakes it (§4.7 *Send path*, `producer.md` §4.6).
- **`Bus`**: a narrow seam of the SDK's own types that a connection-free fake implements; NATS details stay private (§4.7).
- **`Preflight`**: runs on the producer's own bus, in the background after the connection is up, never delaying the device's `Start`;
  returns a readable list of what is wrong.
- **What the producer must guarantee**: six items, the whole of an integrator's obligations (§4.7, last paragraph; `.agents/rules/600-invariants.md`).
- **Versions**: the SDK requires nats.go v1.55.0, which raises a producer's nats.go and Go floor (Go 1.26) when it imports the SDK (§4.7 *Versions*, T-19).
- **Open**: the producer-side API is a sketch and fixes no signatures beyond the names above;
  the send-path token protocol behind `Pending()` (`producer.md` §4.6 *Send-path token protocol*) is reviewed with eqp-hub's maintainers before phase G (T-26).

### 4.7 The producer SDK: a separate Go module

**Why a module, and why a separate one.**
Every producer must implement §4.2–§4.6 identically;
eqp-hub should not re-derive them, and a later producer (a test fixture, another gateway) should not either.
It is a **separate Go module** in the tracekeep repository, `github.com/arloliu/tracekeep/producer` (its own `go.mod`, tagged `producer/vX.Y.Z`),
because importing a package of the service module would pull the service's dependency graph (sqlx, the S3 client, oidc, atlas) into every producer.

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
| `producer` | tracepack, nats.go `jetstream` | `Producer`, `Capture`, the algorithm of §4.2 (lock, lanes, rotation, `start`, claim-check, retry horizon), the pending-request table and sampler of §4.6, `Bus`, `Metrics` and `Logger` interfaces (no Prometheus dependency); no exported record entry point |
| `producer/secsobs` | + go-secs `hsms` | the observer options and lifecycle attachment an integrator installs; `WireEvent`, `SocketEvent`, `LifecycleEvent`, `TxEvent` → records (tracepack-go.md §5); it reaches the capture algorithm through `producer/internal/core`, which also holds the frame copy and the socket→epoch map |
| `producer/busconfig` | nats.go `jetstream` | `StreamConfig()`, `ObjectStoreConfig()`, `ConsumerConfig()`, subject prefix and naming: one definition of [STO §4] *Stream settings* shared by producers and `tracekeep migrate`, including `AllowDirect` off on the claim bucket's stream, which `migrate` sets after creating the bucket (T-28) |

**The SDK owns the NATS client by default; injection is the escape hatch.**
A producer's configuration names the bus and nothing about how to drive it:

```go
type Config struct {
    Servers            []string          // NATS URLs of the traffic cluster
    Auth               AuthConfig        // one of: credentials file, NKey seed, user and password, token
    TLS                TLSConfig         // CA, client certificate and key, server name
    SubjectPrefix      string            // the traffic stream's prefix
    Stream             string            // the traffic stream every publish expects; empty takes busconfig's default
    ClaimBucket        string
    Tunables           Tunables          // lanes, H, τ, ε, sampler, the send path's bounds; every field has a default
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
`New` then refuses a given connection whose reconnect buffer is enabled;
a connection without TLS is a `Preflight` finding, never a refusal, so a plain `nats://` test or development bus still works (T-35).
`Bus` is the seam the core is written against and that `producertest` implements;
it is exported for that purpose, not as the primary API,
and `WithBus` stays as an escape hatch whose route the publication policy covers like every other.

`PublishAsyncComplete` may close while a caller waiting for a free async slot is about to publish (nats.go v1.55.0);
only the publisher goroutine, the one caller that publishes, waits on it, and a `Stop` drain never waits on it from another goroutine.

**Publication policy**, applied identically on the default connection and on an injected one,
since a producer never chooses publish options:
- common to every publish: `WithMsgID("<capture_id>.<seq>")` and `WithExpectStream(<traffic stream>)`,
  which stamps `Nats-Expected-Stream` into the message (the header parking removes, §6.1), and `WithRetryAttempts(0)`:
  the client's default of two retries on `ErrNoResponders` is turned off (nats.go v1.55.0 retries only on that error, and `0` means none),
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
// Bus is the narrow seam the core uses (sketch).
// Its types are the SDK's own: the default implementation wraps nats.go,
// and producertest's fake implements it without a connection.
type Bus interface {
    MaxPayload() int                                           // the server's max_payload
    CheckConn() []Problem                                      // reconnect buffer disabled, TLS in use
    Publish(ctx context.Context, m Msg) (Ack, error)           // the synchronous set
    PublishAsync(m Msg) (AckFuture, error)                     // the asynchronous set
    PublishAsyncComplete() <-chan struct{}
    Objects(ctx context.Context, bucket string) (Objects, error) // GetInfo, Put, Get, Delete of claim objects
    StreamInfo(ctx context.Context, name string) (StreamInfo, error) // Preflight's inspection
}
```

`Msg` carries the subject, the headers, the body, the message id and the expected stream the core sets,
so every implementation publishes under the policy above;
nats.go types stay inside the default implementation.
`WithConn(*nats.Conn)` keeps nats.go in the public interface,
so hiding `Bus` would neither remove it nor change the module's version floor.

**Versions.**
Owning the client isolates nothing at the module level: a Go build holds one version of nats.go,
and minimal version selection picks the **highest** requirement in the whole graph,
so the SDK's requirement is a **floor that can push a producer upward**
(a producer at v1.41.0 and an SDK at v1.55.0 build with v1.55.0, and `go mod tidy` rewrites the producer's go.mod).
The SDK requires nats.go v1.55.0 (T-19), above the v1.40.1 the gateway pins,
so importing the SDK **raises the gateway** to v1.55.0 (phase G).
Every `jetstream` API the specs use already exists in v1.40.1;
the floor is the owner's choice of a current release, not a feature's need,
and from v1.55.0 on the SDK raises it only for stability and upstream fixes, never for a feature (T-28).
The raise carries more with it:
nats.go v1.55.0's `go.mod` declares `go 1.26.0`, so a producer builds with Go 1.26 or later, the line the gateway is on (T-14),
and its own requirements rise (nkeys, `klauspost/compress`, `golang.org/x/crypto`).
nats-server is required by the service module (`internal/testbus`) and the SDK's test-only bus module (T-35), never by the SDK, so no producer pulls it in.
CI runs a sample producer's **resolved** graph (`go list -m all`) at that minimum and at the latest nats.go,
since producers will usually be newer than the floor;
tracepack's own `go.mod` sets a go-secs floor in the same way,
which the SDK's changelog states on every tracepack upgrade.
The header-block length is computed by the SDK from the NATS header wire format, as the spec asks the producer to do.

**Producer-side API** (sketch): `Begin(ctx, CaptureDescriptor)`, `Rotate(ctx, CaptureDescriptor)`, `End(ctx) Report` (bounded, never an error the device must act on;
the `Report` says which capture it closed, whether its `stop` was queued, whether it was abandoned, whether the drain finished in time and how many records it dropped, and whether another lifecycle call held the lock past the deadline;
the first `End` to take the lock owns the capture and its `Report`, a later one returns an empty `Report`, and the figures are those at its return, T-31),
which the device's `Start`, `HotReload` and `Stop` map onto (`producer.md` §4.2 *Device verbs and SDK calls*, T-26);
`Pending()` and `TimeoutSource` (*Send path*, below), `Sampler{N, T}`, and:

- **Send path** (sketch; the protocol is `producer.md` §4.6 *Send-path token protocol*,
  **reviewed with the eqp-hub maintainers before phase G**, which may reshape these signatures):

  ```go
  // Register records a forwarded W-bit primary before it is sent and returns its token;
  // the capture reference and the thin decision are the SDK's, taken inside Register.
  func (p *Pending) Register(sessionID uint16, systemBytes uint32, channel string) Token

  // Token is one registration's handle: safe from any goroutine, never blocking on anything external.
  type Token interface {
      Sent()                            // progress, from the forwarding call; the three exclude each other
      SendFailed()                      // removes a live entry by token
      SubmissionFailed()                // removes a live entry by token
      Awaiting(timeout time.Duration)   // the waiter opened the wait; the entry's expiry follows it (T-31)
      Replied()                         // terminal outcomes, from the waiter; the first seals the entry
      TimedOut(effective time.Duration) // the effective timeout of that wait; the only source of T3
      Cancelled()
  }

  // TimeoutSource is the response waiter's side, which eqp-hub's waiter implements.
  type TimeoutSource interface {
      // Await opens the wait for a registered request, reports Awaiting on t as it opens it,
      // and then reports exactly one terminal outcome on t.
      Await(t Token, timeout time.Duration)
  }
  ```

  A `TimeoutSource` reports exactly one terminal outcome per `Await` and never blocks its caller;
  it calls `TimedOut` only from its timer branch, with the timeout the wait was opened with,
  so a HotReload's new timeout applies to new waits only (`producer.md` §4.4).

- **Observers**, from `secsobs` (go-secs v2.8.0, the version tracepack v0.9.0 requires):
  `ConnOptions() []hsms.ConnOption` returns the wire, socket and transaction observers
  (`hsms.WithWireObserver`, `WithSocketObserver`, `WithTransactionObserver`),
  which an HSMS-SS integrator passes through `hsmsss.WithConnectionOption` when it builds the connection;
  `AttachLifecycle(hsms.Connection) (cancel func())` subscribes through `Connection.SubscribeLifecycle`,
  called before the connection opens and cancelled after `End`.
  Both are installed before traffic starts.
  The observers feed the capture algorithm of §4.2 directly:
  no queue before seq allocation, no frame retained past the callback, no lifecycle or network work in a callback.
  There is no exported `Observe`: an integrator cannot feed records by any other route.

- `(*Producer).Preflight(ctx) ([]Problem, error)`, each `Problem` a `Check` and a `Detail`, run on the producer's own bus
  (`Bus` carries `StreamInfo`, `Objects`, `MaxPayload` and `CheckConn` for it; `producertest`'s fake implements them):
  checks the stream and bucket exist with the [STO §4] settings that the API exposes
  (retention, storage, limits, compression, `AllowDirect` off on the claim bucket's stream;
  encryption at rest is a deployment assurance no client can see, so it is left out rather than reported on every run), `max_payload ≥ 4140`, TLS, the reconnect buffer disabled,
  the capture's `start` message within the header-block and `max_payload` limits
  (through `CheckBusStart` with the real header block), `tool_id` and `recorder_instance_name` set in the descriptor;
  it returns a readable list of what is wrong.
  At startup it runs in the background after the connection is up and never delays the device's `Start` (§4.3);
  it is also an admin-triggered check.
- `producer/producertest`: an in-memory `Bus` and a harness
  that drives disconnects, HotReload, `Stop` under a stalled publisher and fault injection against the integrator's wiring,
  so each producer gets phase 0's chaos tests without rewriting them;
  it also holds the `TimeoutSource` fixture, a fake waiter on a controllable clock that produces a reply, a timeout, a cancellation and the competing reports of `producer.md` §4.6,
  and a conformance check an integrator's `TimeoutSource` runs.

**What the producer must guarantee**, the whole list (everything else is the SDK's):
(1) the traffic cluster's address, credentials and TLS material in `Config`, or a dedicated connection through `WithConn`;
(2) every descriptor carries the configured `toolID` and the configured device name;
(3) `secsobs`'s connection options and lifecycle attachment installed on the device's connection before it opens;
(4) `End` called on an orderly stop and never allowed to fail the device;
(5) `Rotate` called on every descriptor change;
(6) the send path: every forwarded W-bit primary registered through `Pending()` before it is sent, its progress reported on its token,
and a `TimeoutSource` that reports `Awaiting` and the terminal outcomes as `producer.md` §4.6 states, which the conformance check covers (T-31).
