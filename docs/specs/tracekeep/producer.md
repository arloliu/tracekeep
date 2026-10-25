# tracekeep — producer

Status: draft (2026-10-25) — split from seed v21

Covers the gateway producer:
where it runs, the capture/seq/clock algorithm, the rules that keep it off the tool's critical path, transport events, the capture descriptor and FDC thinning.
From seed v21 §4.1–§4.6; section numbers are the seed's, and [README.md](README.md) maps each to its file.
Read before touching `producer/` (with `.agents/rules/600-invariants.md`) or the eqp-hub integration.

## Interface

Non-normative summary of what a caller must know.
The sections below are authoritative; a disagreement is a defect in this summary.
Contract changes go through proposals, which the subject [README.md](README.md) lists.

- **Placement and inputs**: the producer runs inside the `eqp-hsms` plugin process;
  its inputs are go-secs observers on the tool's connection: wire, socket and lifecycle, and transaction (§4.1).
- **Seq before loss**: every observation of an open capture gets a seq before any decision that can lose it,
  so every loss inside a capture is a seq gap (§4.2 *Observer callback*).
- **Callback contract**: a callback never blocks on anything external;
  its one wait is for another callback to leave the capture's critical section,
  which holds no I/O, no allocation, no logging and no call into go-secs (§4.2 *Capture object*, §4.3);
  the wait is the residual the owner accepted under LS-14 (T-9).
- **Borrowed frame**: go-secs lends `Frame` for the callback only;
  the producer reserves lane capacity first and copies the frame only for an admitted record (§4.2 *Observer callback* steps 7–8).
- **Rotation**: at `Start`, `Stop`, a `HotReload` that changes any descriptor value, and after a fault;
  its commit point is the install of the new capture pointer while the old capture's lock is held,
  and nothing is acquired after it (§4.2 *Rotation*, *Lifecycle serialization*).
- **Lifecycle bounds**: `Start` is idempotent; `Stop` is bounded by its deadline and never returns a trace error (§4.2).
- **Dependency barrier**: a record of seq s is published only when every reserved seq below s is accepted (§4.2 *Dependency barrier*).
- **Off the critical path**: bus, bucket and consumer faults become counted drops, never a stalled send;
  the producer never creates or alters the stream or bucket and never touches device health (§4.3).
- **Descriptor**: `tool_id` is the hub's `toolID`;
  `recorder_instance_name` is the `eqp-hsms` device name, and `recorder_instance_id` is derived from the two;
  one capture series per device, which a reader selects by (`tool_id`, device) (§4.5).
- **Send path**: every forwarded W-bit primary is registered as a pending request before it is sent (§4.6 *Association protocol*);
  T3 of a forwarded request comes from eqp-hub's waiter through `TimeoutSource` (§4.4);
  eqp-hub reports each request's fate on its token:
  send progress from the forwarding call and one terminal outcome (reply, timeout, cancellation) from the waiter,
  where the first terminal outcome wins and expiry is no outcome (§4.6 *Send-path token protocol*, reviewed with eqp-hub's maintainers before phase G);
  thinning is off by default and touches only positively identified FDC interval polls (§4.6).
- **Device verbs and SDK calls**: this file states the rules under the device's verbs (`Start`, `Stop`, `HotReload`);
  the SDK's calls are `Begin`, `Rotate` and `End`, and each rule holds for the call its verb maps to (§4.2 *Device verbs and SDK calls*, T-26).

## 4. The gateway producer (requirements 1 and 2)

The producer is the gateway project's work (G5-200);
tracekeep supplies the mechanics as the SDK of §4.7
and this section is the contract between them.

### 4.1 Where it lives

- The `eqp-hsms` device runs as a go-plugin child process (`devices/eqp_hsms/plugin/main.go`) and owns the tool's `hsms.Connection`.
  The producer runs **inside that process**:
  the device constructs the SDK from the traffic bus configuration (§4.7),
  the SDK owns the dedicated connection to the traffic cluster by default (injection through `WithConn` is an optional integration choice),
  and the device wires the SDK's observers into the HSMS connection when enabled.
- Its inputs are go-secs v2.6.0 observers on that connection:
  `hsms.WithWireObserver` for every frame in both directions, rejected frames included,
  `hsms.WithSocketObserver` and lifecycle events for sockets, epochs and causes,
  and `hsms.WithTransactionObserver` for the outcomes of sends go-secs itself awaits (tracepack-go.md §5).
  With the wire observer the capture is `raw-stream`, records `wire-exact` (tracepack-go.md §2, §8 Q1);
  choosing wire observation decides that, and LS-1's `decoded-message` wording is amended with it (§13).
- The frames it sees are the tool's side of the connection, session ID as the tool sees it,
  because the gateway rewrites the session ID before it forwards to the EAP (V-8);
  so LS-2 is answered as "the tool's side", and a record carries one header.
  The gateway's own traffic toward the tool (its `S1F1`, `S1F13`, `S6F23`, auto-replies, FDC polls and health pings) crosses the same connection and is observed (LS-5).

### 4.2 Capture, seq and clock: one algorithm

**States a record passes through**, named because the proofs below depend on telling them apart:
*allocated* (it has a seq), *queued* (it is in a lane), *published* (handed to the client), *accepted* (the bus acked it), *staged* (a registered pack holds it, §6).

- **Capture object.**
  A `capture` holds its id, descriptor, subject, and a **lock** that guards exactly these fields:
  the seq counter, the clock anchor, the `closed` flag, the socket→epoch map, and the **reservation set** (reserved seqs allocated but not yet queued).
  The critical section contains no I/O, no allocation, no logging and no call into go-secs;
  it is a few dozen instructions.
  A callback can wait on this lock for another callback
  (go-secs runs inbound and outbound observers concurrently:
  the outbound wire observer runs on the writing goroutine **under the connection's write lock**,
  the inbound one on the receive goroutine that T8 bounds, `hsms/connection_config.go` v2.6.0,
  so the wait sits on the link itself);
  the wait depends on the holder being scheduled, not on the bus or any other system,
  and the design has no algorithm that removes it.
  This is a **residual under LS-14**, which the owner accepted (T-9, §13);
  the design states it rather than claims a bound.
  The structures the section touches are preallocated at capture creation:
  the socket→epoch map holds 1024 sockets and the reservation set as many entries as the reserved lane;
  exhaustion of either is a capture fault (below).
  `Stop` adds its own escape (below).
  The producer keeps the current capture in an atomic pointer.
  Every record carries a reference to its capture, so a queued record never acquires another capture's identity.
- **Observer callback** (hot path).
  Every observation of an open capture gets a seq **before** any decision that can lose it,
  so every loss inside a capture is a seq gap (G5-218):
  1. load the capture pointer;
     nil (after `Stop`) → the observation lies after the capture's `stop`, outside every capture by construction:
     count `after_stop` and return;
     `lock`; if `closed` **because of a fault** (`faulted` set) → `unlock`, count `dropped{reason=capture_fault}` and return:
     the fault window is covered by the consumer's close of that capture and by the successor's `start` (fault handling, below),
     and the callback never retries a faulted capture;
     if `closed` because of a rotation → `unlock`, reload the pointer and repeat:
     a rotation installs the new capture before closing the old,
     so the loop ends at an open capture, a faulted one, or nil within as many iterations as rotations in progress, and never waits on anything else;
  2. **epoch**: look the event's `Socket` up in the socket→epoch map;
     a socket seen for the first time, by a wire or a socket callback alike, takes the next epoch number
     (go-secs may deliver a wire event before the socket event that opened its socket);
  3. **clock check** ([SEM §4]) inside the lock: drift = (wall − anchor_wall) − (mono − anchor_mono);
     when |drift| exceeds the tolerance ε (the descriptor's `clock_step_tolerance_ns`, default one second, T-29), take the next seq for a `clock-step`, **add it to the reservation set**,
     then set the anchor to (wall, mono);
  4. take the record's seq from the counter, inside the same lock,
     so no record under the new anchor can precede the step and no record can follow a `stop` (G5-218, [SEM §4]);
  5. `unlock`; stamp `ts_utc_ns`, `mono_ns`, `epoch`;
  6. push the clock-step, if any, into the **reserved lane first**,
     which removes its seq from the reservation set under the lock;
     this happens whatever becomes of the ordinary record;
  7. **reserve capacity**: atomically reserve one slot and the frame's length in the ordinary lane's count and byte budgets;
     failure drops the record **with its seq**, a gap by construction, before any copy;
  8. **copy the frame** into the reserved budget
     (go-secs lends `Frame` for the duration of the callback;
     the copy is the only allocation and happens only for an admitted record)
     and push the record;
     a push that fails releases the reservation and drops with the seq.
  The entry is wrapped in a recover; the unlock is deferred, so a panic never leaves the lock held.
  A panic after seq allocation leaves a seq gap;
  a panic after a reservation and before step 6 leaves a reservation the publisher turns into a fault;
  a panic **before** allocation (steps 1–3, a bug with no identity) is counted and **faults the capture**,
  so the loss is bounded by the fault window instead of being invisible.
- **Lanes.**
  The ordinary lane is bounded by record count and by bytes (frames can be MiBs) and drops the newest arrival on overflow.
  The reserved lane holds `start`, `stop` and `clock-step` records, is bounded by count (default 4096; the records are tens of bytes), and **never drops**;
  its bound counts a reserved record until the publisher has it accepted, so the publisher's backlog cannot grow past it.
  **Fault handling**: when the reserved lane is full, a reservation stays unqueued longer than a bound (default 1 s),
  the publisher cannot materialize a reserved record, a preallocated structure is exhausted, or a callback panics before allocation,
  the capture is marked `closed` + `faulted` under its lock, an alert is raised, and the publisher **signals the lifecycle goroutine**,
  which rotates to a fresh capture (`previous_capture_id` = the faulted one) as §4.2 *Rotation* does, without a `stop` for the faulted one;
  callbacks between the fault and the new `start` are counted (`dropped{reason=capture_fault}`);
  the faulted capture ends `stop-unclean` on the consumer side,
  whose close bounds the fault window between the last staged record and the successor's `start` ([STO §4]).
  A fault therefore costs a bounded, visible window and never a stall:
  no callback waits on or retries a faulted capture;
  a fresh capture that faults again within a minute pauses rotation for a backoff, counting meanwhile,
  so a persistent fault cannot spin.
- **Dependency barrier** (the rule a reader's anchor check relies on, [SEM §4]):
  the publisher publishes a record of seq s, ordinary **or reserved**,
  only when every reserved seq below s is *accepted*, and the reservation set holds no seq below s.
  Allocation and reservation happen under one lock,
  so a record allocated after a step is never published before the step is queued and accepted;
  a reserved seq that is never queued becomes the fault above instead of a silent gap.
- **Lifecycle serialization.**
  Every rotation, whatever its cause (`Start`, `Stop`, `HotReload`, fault), runs on the device's lifecycle goroutine under one **lifecycle lock** with a **lifecycle generation**:
  a fault signal carries the generation and the capture it saw;
  the lifecycle goroutine discards a signal whose generation is stale or whose capture is no longer current,
  so a fault rotation prepared before a `Stop` or a `HotReload` can never install a capture after them.
  Every capture records the **lifecycle generation** it was created in.
  `Stop` first attempts a normal bounded rotation to nil (so the `stop` boundary is published when the deadline allows);
  if the deadline passes it **abandons** by advancing the generation, a permanent state:
  a capture whose generation is below the current one is abandoned for good, whatever a later `Start` does,
  since generations only grow.
  `Stop` sets `stopping` **before** it takes the lifecycle lock,
  so an operation already inside the lock (a HotReload blocked on a capture lock) sees it and abandons;
  every capture-lock acquisition inside a rotation, whatever its cause, is a `TryLock` loop bounded by that operation's deadline and happens **before** the pointer is installed,
  so an abandoned rotation installs nothing:
  its prepared capture is discarded and the pointer is left to `Stop`.
  The whole shutdown is therefore bounded even when an observer holds a capture lock:
  `Stop` waits at most one bounded rotation plus its own bounded step.
  Only a later `Start` installs a capture again.
- **Rotation** (at `Start`, at `Stop`, at a `HotReload` that changes any descriptor value, `hsms_timers` and thinning parameters included, and after a fault):
  1. build the new capture, with a fresh `capture_id` the SDK generates as a UUIDv7 (RFC 9562), so capture ids sort by creation time;
     under its own lock, which nothing else can hold before it is installed, take seq 0 for its `start`, reserve it, and push the `start` into the reserved lane
     (the lane is the producer's, not the capture's, and a push never blocks), then unlock;
  2. take two places in the reserved lane's bound for the old capture's `clock-step` and `stop`, so a rotation that commits never loses a boundary
     (no room abandons the rotation as a timeout does);
     take the **old** capture's lock with a `TryLock` loop bounded by the operation's deadline,
     and check the deadline and cancellation once more before step 3;
     a timeout here **abandons the rotation with nothing installed**:
     the prepared capture is discarded and its queued `start` is withdrawn from the lane by the publisher,
     which skips records of a capture that was never installed;
  3. with the old lock held, **install** the new capture in the pointer (or nil at `Stop`):
     this is the commit point, nothing before it is visible;
     then, still holding the old lock, set `closed`,
     take the seqs for a `clock-step` if the anchor rule needs one and for the `stop` ([STO §4] *Producer duties*),
     reserve them, **push them into the reserved lane and retire their reservations**, and only then unlock.
  Every lock the rotation needs is held before the commit point and released once after it;
  there is no post-install acquisition, so no post-install deadline exists.
  A callback that loaded the old pointer waits for the brief step 3, finds `closed` and retries on the new capture;
  the old capture's `stop` is its last seq.
  Past the shutdown deadline `Stop` abandons the old capture (no `stop`, counted, closed by the consumer), advances the generation and installs nil without the old lock.
  **In-flight callbacks under abandonment** are allowed to finish:
  a callback that holds the old lock, or that allocated before the generation advanced, completes its allocation and enqueue against the old capture's retained resources
  (the capture object outlives abandonment until its queue is empty);
  the publisher, which checks every dequeued record's capture generation against the current one,
  **drops** records of an abandoned capture and counts them as `dropped{reason=abandoned}`, inside the loss window the consumer's close already covers.
  A callback that takes the lock after the generation advanced finds the mismatch and returns without allocating.
  A prepared `start` has three terminal states, **pending** (queued, its capture not yet installed: the publisher waits), **installed** (publish) and **discarded** (the rotation was abandoned before the commit point: the publisher skips it);
  discard is recorded on the capture object, never inferred from the pointer.
  Seq 2^63 − 4 is the last ordinary seq; the producer rotates before passing it (G5-231):
  a capture whose seq passes a margin below it asks the lifecycle goroutine once for a rotation with the same descriptor,
  and only a capture that reaches it anyway faults.
- **Device verbs and SDK calls** (T-26).
  This section names the device's verbs; the SDK's API (`producer-sdk.md` §4.7) names its own, and the integrator maps them once:
  `Start` → `Begin(ctx, descriptor)`;
  a `HotReload` that changes any descriptor value → `Rotate(ctx, descriptor)`, and one that changes none calls nothing;
  `Stop` → `End(ctx)`, which returns a `Report` and never an error;
  a fault rotation is the SDK's own, on its lifecycle goroutine, and has no call.
  Every rule stated here for a device verb holds for the SDK call it maps to.
- **`Start` is idempotent**: a `Start` on an already-open connection does not open a second capture.
- **`Stop`** rotates to nil, then gives the publisher a bounded deadline (a fraction of `closeConnTimeout`) to drain and publish the `stop`;
  what is left is dropped and counted;
  the whole trace shutdown, rotation and lock acquisition included, is bounded by that deadline.
  `Stop` **never returns a trace error**: eqp-hub's manager treats any non-nil `Stop` as an unsafe demotion.
- **Publisher goroutine** (the only place that waits):
  pops, encodes with `tracepack.NewBusRecord` and `EncodeBusBody`, publishes on the dedicated connection under the barrier above.
  Its synchronous points:
  the `start` (below), the acceptance of a reserved record before anything above it, and claim-check puts.
- **`start` publication**: preflight with `CheckBusStart`;
  publish synchronously (`PublishMsg` with a context deadline and the synchronous option set of §4.7) with retry and backoff until acked;
  ordinary records of the capture queue meanwhile and overflow as usual.
  A capture whose `start` the bus never accepted is never admitted by tracekeep (§6.1);
  a `start` the bus accepted but whose ack the producer lost is admitted like any other,
  and its retries are duplicates;
  it becomes visible only through the next capture of the same tool and recorder instance,
  which `ListCaptures` reports as an unregistered interval (§7.3).
  The loss-visibility guarantee is therefore stated for **durably admitted captures**;
  this qualifies LS-15 and needs the owner's acceptance (§13).
- **Large records** (G5-217):
  a body above `max_payload − 4096` is put as `<capture_id>.<seq>` into the claim bucket, a NATS object store on the same traffic cluster and connection,
  then published header-only with `Tpk-Payload-Ref` and `Tpk-Payload-Digest`;
  the put runs on the publisher goroutine under a timeout.
  A put or publish that fails past the producer's **retry horizon** H (a deployment setting, default 5 minutes) drops the record:
  a seq gap from a failed publish, which the ruling allows, not a policy.
  The traffic connection runs with the reconnect buffer disabled (`nats.ReconnectBufSize(-1)`, enforced by the SDK at `New`),
  so a publish while disconnected fails at once instead of being flushed minutes later,
  and every publish carries a deadline τ (default 10 s).
  These bound when the producer **attempts** a publish, not when the server **accepts** one:
  bytes already written to the socket can be processed after the client's deadline, and no client setting retracts them.
  §8.4 therefore does not rely on an acceptance bound; it relies on receipts and on the stream's own pending state.
  Object-store facts of nats.go v1.55.0 the SDK builds on (unchanged since v1.40.1, T-19):
  `Put(ctx, ObjectMeta, io.Reader)` publishes chunks through its own internal JetStream publisher, inherits none of the SDK's publish options, and observes only its context;
  so every object operation carries a context deadline,
  the chunk size is set per object through `ObjectMetaOptions.ChunkSize` from `Conn().MaxPayload()` (the default 128 KiB chunk would exceed a small `max_payload`),
  and the metadata message is checked to fit.
  The SDK **never overwrites** a claim object with another version:
  before a put it reads `GetInfo(name)`,
  which the stream's leader answers because the claim bucket's stream has `AllowDirect` off (T-28):
  nats.go creates every object store with `AllowDirect` on (v1.55.0 `jetstream/object.go`) and routes a read by the handle's cached setting (`jetstream/stream.go`),
  so a direct read could reach a lagging replica, and a stale "not found" would let a put replace an existing object;
  an existing object with the same payload digest and the same record header
  (the SDK stores the 44-byte header and the record's `ts_utc_ns`, hex, in `ObjectMeta.Metadata` under `tpk-record-header` and `tpk-ts-utc-ns`) is a candidate for reuse;
  one with another digest or header is a producer fault:
  the record is dropped as a gap with alert `claim-conflict`.
  **Uncertain uploads** (`Put` returned an error or timed out):
  nats.go publishes the object's metadata before every chunk is confirmed and purges only the chunks on a late chunk error,
  so matching metadata does not prove a complete object.
  Before reusing a candidate the SDK therefore **reads it back in full** and checks length and digest;
  a candidate that fails is deleted by the SDK (the only writer of that name under G5-200) and put again;
  past H the record is dropped as a gap.
  A reference is published only after a put the SDK itself saw complete or a read-back it verified.
  Claim objects are **not swept early**:
  an object lives until the hour of its record is evicted by retention (§8.3, §8.4),
  so a late reference within retention, and a conflicting one from an SDK producer, always finds its payload
  (a non-SDK producer's conflicting version in another hour is outside the guarantee, §13).
  The traffic cluster raises `max_payload` toward 8 MiB so that few records take this path.
- `recorder_instance_id` is derived from configuration (`toolID` and the device name, §4.5), never from a hostname or pod name,
  so it is stable across restarts and leadership changes ([FMT I-7]).
  `previous_capture_id` is known within one process lifetime and absent after a restart,
  since the gateway keeps no durable state (G5-200).

### 4.3 Rules that keep it off the critical path

| Fault | Behaviour | Never |
|---|---|---|
| Traffic NATS unreachable at `Start` | connect in the background with backoff; `Start` returns as today | wait for the bus |
| Traffic NATS down while running | ordinary lane fills, then drops with `reason=bus_down`; reserved lane holds its few records | block an observer, stall a send |
| Stream or claim bucket missing or misconfigured | publishes fail, drops counted, one alert log per state change | create or alter them from the producer |
| Claim bucket slow | put under a timeout on the publisher goroutine; past H the record drops | hold the ring |
| tracekeep consumers down | nothing changes for the gateway: the stream backlogs (G5-213) | — |
| Producer bug or panic | recovered at each callback entry with the lock released; an unqueued reservation or a full reserved lane puts the capture in the fault state; it ends `stop-unclean` on the consumer side | propagate to the device |
| `Stop` drain deadline passed | remainder dropped and counted; `Stop` returns the connection's result only | return a trace error from `Stop` |

Its metrics live in their own namespace (`tracepub_records_observed`, `_published`, `_dropped{reason}`, `_ring_depth`, `_ring_bytes`, `_bus_connected`, `_claim_puts`)
and it **never** calls `IncDeviceErrors` or changes device state:
eqp-hub's circuit breakers and election act on device health,
so counting bus faults there would make the bus critical by the back door.

### 4.4 Transport events and timeouts (LS-3)

| Evidence | Source | Record |
|---|---|---|
| connect, select, deselect, separate, linktest, T6, T7, socket close and its cause | socket and lifecycle observers | transport events per tracepack-go.md §5.1–§5.3 |
| T3 of a send go-secs awaited (its own `S1F1`, `S1F13`, `S6F23`) | `TxEvent{Outcome: TxT3Timeout}` from `WithTransactionObserver` | transport event `timer-expiry`, timer 3, at the time the event is received (tracepack-go.md §5.4) |
| T3 of a request forwarded for a channel peer | eqp-hub's response waiter: `ForwardDataMessage` opens no reply wait in go-secs (`TxSent`, `ReplyWaited=false`), so the waiter's **timer branch** in `waiterWorker` is the evidence. `responseT3Timeout` is **not** a usable hook: `responseHandler` reaches it for any error, including a worker-pool submission failure before the send | the same transport event, emitted from a typed timeout outcome the waiter exposes (an eqp-hub change: distinguish timeout, cancellation, submission failure and send failure, reported on the request's token as §4.6 *Send-path token protocol* states), by a non-blocking enqueue; its `epoch` is the socket recorded for the request when its outbound frame was observed: **every** forwarded W-bit primary is registered in the pending-request table of §4.6, not only polls; a request whose outbound frame was never observed produces no T3 event; the event's `detail` names the effective timeout. eqp-hub's HotReload today updates go-secs's T3 but not the waiter's stored timeout (`eqp_hsms.go` HotReload calls only `SetCapacity`), so phase G includes updating the waiter's timeout for new waits; existing waits keep theirs; phase 0 defines the `TimeoutSource` interface (`producer-sdk.md` §4.7 *Send path*) and exercises it with a fixture |
| malformed frames | wire observer reports frames go-secs rejects | `wire-exact` data record, classifier marks it |

A request is awaited by exactly one of the two mechanisms, so no timeout is recorded twice.

### 4.5 Capture descriptor (LS-6)

`tool_id` = the hub's configured **`toolID`** (top-level config, the identity FDC also receives), passed to the plugin as a device property;
**not** the device name, which the templates set to `eqp` for every tool;
`recorder_instance_name` = the `eqp-hsms` device name (`devices[].name`), unchanged and never empty
(tracepack G5-270, tag 0x0034; it counts toward the 4096-byte header block of the `start` message);
`recorder_instance_id` = UUIDv5 (RFC 9562), under one fixed namespace UUID the SDK defines,
of `tool_id` and the device name, each prefixed by its byte length as a 4-byte big-endian integer, so no choice of characters can make two pairs collide;
the SDK derives it from each descriptor it starts or rotates to and refuses a descriptor that sets it;
the gateway's `AppID` is not part of it, so a deployment rename does not break a device's series;
nothing in eqp-hub stops two hubs, under one `AppID` or two, from serving one `toolID` at once
(leader election is off by default and `AppID` only names its lease, `develop` at fc67e5f),
so such hubs write overlapping captures of one device, which `catalog.md` §7.3 *Overlap* reports;
`device_id` from `deviceID`;
`hsms_timers` from the go-secs properties (a timer HotReload therefore rotates the capture, §4.2);
`equipment_connect_mode` from `mode`; `equipment_endpoint` from `hostPort`; `host_software` = eqp-hub version;
`equipment_model` and `equipment_sw_rev` only if configured; `thinning` entries per §4.6.
The SDK fixes the values its way of observing decides (G5-274):
transport HSMS-SS, capture method `raw-stream`, time source capture clock, lifecycle coverage subscribed,
vantage **host**, since the gateway is the host endpoint of the connection it records,
and `quality_evaluated` true:
the SDK sets correlation-incomplete on a record of epoch 0, ordering-uncertain on one observed before its predecessor in the capture,
and never direction-inferred, since the wire observer reports the direction;
`recorder` is `tracekeep-producer/<module version> go-secs/<version>`,
and `clock_step_tolerance_ns` is ε, an operator tunable (T-29) the SDK writes into every descriptor.
One hub has one `toolID` and several `eqp-hsms` devices
(fourteen in eqp-hub's `examples/tfdc_common_parallel.yaml`:
`eqp`, `tlp`, the sensor connections `dpm`, `fac`, `lsc`, `sgs`, each with its own `hostPort`, and in parallel mode the `pdf_*` connections to the legacy host system),
so a tool has **one capture series per device**, told apart by `recorder_instance_name` and the `recorder_instance_id` derived from it;
`device_id` (which two connections may share) and `equipment_endpoint` (which an address change alters) do not identify a device;
the producer is enabled **per device**,
so a connection whose peer is a host-side system is captured only if its operators want it,
and the VE's reader selects the device it plays.

### 4.6 FDC polling: thin only what is positively identified (requirement 1)

The mechanism is fixed:
a thinned record keeps its header, length, direction and time, its message text zero-filled, with `quality.unobserved`,
and the descriptor declares the stream and function in `thinning` (G5-208, [STO §4] *Unobserved records*).
Dropping is rejected by V-27 and LS-9.
Thinning anything the gateway cannot positively identify as the FDC service's interval polling is rejected by G5-225;
**the conforming default is no thinning, and phase 0 ships with thinning disabled**.

**Provenance in eqp-hub today** (eqp-hub-analysis and source, `develop` fc67e5f):

- In **pure mode** every `fdc_grpc` request to the tool travels on one request-response channel (`fdc-eqp`, built by `SecsCommand.BuildChannelName`),
  and `device.Message` carries no marker.
- The aggregator's `buildRequest` consolidates every **due context** into one `S1F3`,
  and `RouteProxyRequest` merges a matching proxy request into the active plan,
  so one request can carry interval and non-interval purposes.
  Moving `buildRequest` to another channel is therefore **not** positive provenance.
- In **parallel mode** `fdc_grpc` sends nothing;
  the polls the tool sees come from the legacy host device (`pdf`) on its own channel,
  and the gateway cannot tell that device's interval polls from its other queries.
- The hub's channel validation counts request-response channels per source
  and `fdc_grpc` advertises support for several,
  so a second FDC-source channel is admissible.

**The eqp-hub change that gives provenance** (a prerequisite for enabling thinning, not for phase 0):
`fdc_grpc` builds **interval-only requests**
(a request holding only interval-poll contexts;
a batch that contains any proxy or event context is sent whole on the ordinary channel)
and sends them, and their drain resends, on a dedicated channel (`fdc_poll-eqp`).
`eqp-hsms` already receives `msgChannel` in `doSendToTarget`.
The producer's configuration names such channels (`pollChannels`); without one it thins nothing and declares nothing.
In parallel mode nothing is thinned at all:
the legacy host's channel is not polling-only by construction,
since `fdc_grpc` itself expects MES queries, event-triggered post-event queries and interval `S1F3` on that shadow
(`service/message_handler.go:202-231`, `event_dispatcher.go:397-417`),
so no declaration can make it so.
Whether timer-triggered (cron) collection contexts,
which accumulate into the same bucket as interval contexts (`service/timer_handler.go:57`),
count as interval polling under G5-225 or must stay on the ordinary channel is the FDC maintainers' and the owner's to say.

**Association protocol** (the one piece of state shared between the send path and the record pipeline;
it also serves §4.4's T3 attribution):

- `doSendToTarget` registers **every forwarded W-bit primary** before `ForwardDataMessage` as a **pending request**:
  an immutable **token** (unique per send), the key `(session id, System Bytes)`, the capture reference, the sending channel,
  and, when the channel is in `pollChannels`, the **thin decision** for the pair taken right there from the sampling rule.
  A failed send removes the entry **by token**;
  an entry expires at T3 plus a margin;
  a completed entry is retained until its expiry so a late callback still finds it.
- **Ambiguity rule, over the process's lifetime, decided at registration**:
  the table keeps one exact set of every key ever **registered** in this producer process, inserted at registration time, before the send and before any socket is known
  (System Bytes are 4 bytes;
  the set is capped, default 4 million entries, about 46 days of one-second polling;
  past the cap thinning is off until the process restarts, and the flag is checked at dequeue,
  so queued thin-selected records are kept in full too).
  A registration whose key is already in the set marks the new entry **and** any live or unresolved entry of that key ambiguous;
  an ambiguous entry never thins.
  Inserting at registration closes the pre-binding interval:
  a delayed outbound or reply callback of an earlier request with the same key can only ever find an entry already marked ambiguous.
  Observer delivery has no bound on delay (tracepack-go.md §5.5),
  so no window shorter than the process's life excludes an old callback;
  a wire frame carries only the key, so no token can tell an old reply from a new one under reuse.
  Full bodies are the only safe answer, and System Bytes reuse is a protocol anomaly anyway.
- **Socket binding**: the outbound observation records the socket the frame went out on;
  a reply matches an entry only on the **same socket** and while the entry is live or completed;
  a reply on another socket, or after expiry, finds nothing and is kept in full;
  a reconnect between registration and the outbound observation marks the entry ambiguous.
  The socket is also the epoch source of §4.4.
- The wire callback looks the outbound primary and the inbound reply up by key (a map read, no body inspection) and attaches the **token** to the record.
  A reply callback that arrives before the outbound callback (go-secs delivers observations out of causal order) finds the entry,
  since it exists before the frame is written, but has **no socket binding yet**:
  it is attached provisionally,
  and the publisher zero-fills only if the binding has been established and matches by the time the reply is dequeued;
  otherwise the reply is kept in full.
- On the publisher goroutine:
  a record is zero-filled only if its token's decision is *thin*, the entry is not ambiguous, the socket binding matches,
  **and the record's own capture declares the stream and function in `thinning`**;
  the last condition covers a rotation between the send and the reply.
  An `S1F4` whose text is empty or holds a zero-length item is kept in full whatever the decision (abnormal reply, V-27);
  `S9Fx` is never touched.
  `WireEvent.Frame` holds the 4-byte length prefix, the 10-byte header and the message text,
  so the zero-fill covers bytes 14 onward;
  phase 0 keeps a regression test on that offset.
- **Send-path token protocol** (P1 item 5; **reviewed with the eqp-hub maintainers before phase G**, T-26, so its shape may change there while the rules above do not):
  one protocol serves the two places in eqp-hub that learn a forwarded request's fate,
  the forwarding call in `doSendToTarget` and the response waiter in `waiterWorker`.
  The token registration returns is the only handle either holds;
  `doSendToTarget` hands it to the waiter with the wait it opens.
  The SDK's calls are in `producer-sdk.md` §4.7 *Send path*.
  - **Registration** is unconditional:
    every forwarded W-bit primary is registered, whether or not its channel is a poll channel and past the ambiguity set's cap as well,
    because §4.4's T3 attribution needs the entry even where nothing is thinned.
  - **Send progress**, reported once per token by whichever eqp-hub code learns it:
    `Sent`, the forwarding call returned without error;
    `SendFailed`, it returned an error;
    `SubmissionFailed`, the request never reached `ForwardDataMessage` (a worker-pool submission failure).
    The three exclude each other, so a second progress report is a counted no-op.
    Either failure removes the entry by token (above), whatever was reported before it, and its key stays in the ambiguity set.
    `Sent` decides nothing: the socket binding comes from the outbound observation alone.
  - **Terminal outcomes**, reported by the waiter:
    `Replied`;
    `TimedOut`, only from the waiter's timer branch, carrying the effective timeout of that wait;
    `Cancelled`, the wait ended any other way (the device stopping, a cancelled context).
    The first terminal outcome makes the entry *completed*, retained until its expiry (above);
    every later terminal outcome is a counted no-op,
    so a reply racing a timeout yields one T3 event or none, never a T3 event after a reply.
  - **Competing reports**: a `Sent` after a terminal outcome, and any report on a token whose entry was removed or has expired, are counted no-ops.
    A waiter that times out after a send failure or a submission failure therefore emits nothing:
    the entry is gone, which is the case `responseT3Timeout` cannot tell apart (§4.4).
  - **Expiry is not an outcome**: a token that never receives a terminal outcome expires at T3 plus the margin and emits nothing,
    so T3 of a forwarded request comes only from the waiter's typed timeout.
  - **T3**: a `TimedOut` that is the token's first terminal outcome, on an entry the outbound observation has bound to a socket,
    emits §4.4's `timer-expiry` event by a non-blocking enqueue,
    with that socket's epoch and the effective timeout in `detail`;
    one on an entry with no binding is counted and emits nothing.
    The binding precedes any timeout in practice,
    since the outbound wire observer runs on the writing goroutine under the connection's write lock (§4.2 *Capture object*),
    before the forwarding call returns.
  - **Calls**: every report is safe from any goroutine and never blocks on anything external (§4.3).
    Reports add no thinning condition:
    a record is zero-filled only under the publisher's conditions above.
- **Sampling rule** (LS-7, decided 2026-10-25): a poll keeps its bodies when `count mod N == 0` or `now − last_kept ≥ T`;
  defaults `N = 60`, `T = 60 s`, **configurable per poll channel** in the producer's configuration (`pollChannels[].keep_every_n`, `keep_interval`), never compiled in;
  a change rotates the capture, since the declaration lives in the descriptor.
  A `thinning` entry carries only `stream`, `function` and `source` ([FMT §5] nested tags),
  so the producer writes the rule into `source` with a **key=value convention tracekeep owns**
  (e.g. `fdc interval polling on channel fdc_poll-eqp; keep_every_n=60; keep_interval_ms=60000; abnormal=kept`):
  the SDK writes it, the catalog parses it at registration, tracepack treats it as text;
  the tracepack maintainers advise against a structured nested tag (2026-10-25):
  tracepack never evaluates a sampling rule, each record's `quality.unobserved` is that record's truth,
  and a new nested tag would force every consumer to upgrade first (G5-252) for a field only tracekeep's catalog reads.

Other periodic traffic (the `fdc_grpc` health ping `S1F1` every minute in pure mode, HSMS linktest control frames every minute) is small and is **not** thinned:
G5-225 allows nothing else.
