# tracekeep — positioning

Status: draft (2026-10-25) — split from seed v21

Covers what tracekeep is and is not, its four operational goals, what tracepack and VE rulings already settle, and the `ve-log-service.md` rows the design settles.
From seed v21 §1, §2, §14; section numbers are the seed's, and [README.md](README.md) maps each to its file.
Read first, before any other spec file; read again when a change touches a ruling or an LS row.

tracekeep is the traffic log service of the VE design
(`veq/docs/specs/ve/ve-log-service.md`, decisions V-12, V-29, V-30):
it consumes the traffic the equipment gateway (eqp-hub) publishes on a NATS JetStream bus,
writes it as tracepack captures into the fab's object storage,
and serves it back for VE learning and for troubleshooting.

## 1. Positioning (requirement 6)

- **Collects** every record the gateway publishes, one capture per tool connection and gateway leadership term (G5-200).
- **Keeps** them as tracepack segments and hourly archives under [STO §3] keys, one month by default (G5-200), masked at write under a policy (G5-207).
- **Serves** two readers:
  the VE generation service, which reads the packs of a tool and a time window with their completeness (LS-10, LS-11),
  and people troubleshooting a tool,
  who query, filter and export records and, with privilege, restore an encrypted original.
- **Provides** the producer side as a Go module,
  so eqp-hub and any later producer publish correctly without reimplementing the bus (§4.7).
- **Is not** on any tool's critical path:
  a dead tracekeep or a dead traffic bus costs records, never a tool's conversation (LS-14, §4).
- **Is designed to four operational goals**, checked in every section:
  *producer-friendly* (one SDK, a short list of guarantees the producer keeps, a preflight that says what is wrong before traffic flows, §4.7);
  *operation-friendly* (one binary, named incidents each with a runbook and an admin command, few tunables in one table, §10.4);
  *never stuck* (a producer fault, a disconnect or a seq gap costs that capture's records and nothing else:
  dispositions are per message, isolation is per capture, no component waits for a missing seq, §6);
  *transparent* (every state machine exports its counters, ages and last success;
  data-level loss is a metric, not only a catalog fact, §10.4).
- **Is not** a general log platform: no free-text logs, no metrics, no alerting beyond its own health.

## 2. What is already settled

| Topic | Settled by | tracekeep's obligation |
|---|---|---|
| Producer, bus record, subject, `Nats-Msg-Id`, descriptor header; `seq` starts at 0 and is assigned in capture order | G5-200, G5-216, [FMT I-12], [STO §4] *Bus message* | consume with `tracepack.CheckBusMessage`, `DeriveBusRecord`; never reassign anything; completeness counts from seq 0 |
| Records above the inline limit travel by claim-check, never dropped as policy | G5-217, [STO §4] *Large records* | producer puts then publishes; consumer fetches, checks, writes; the service sweeps objects under the lifetime invariant |
| Durability point is the bus ack after the segment is durable | [STO §4] *Durability* | ack a message only after the put and the catalog insert of a segment holding it |
| Stream: work-queue, file storage, no age, size or count limit, TLS, encryption at rest; the same for the claim bucket | G5-213, [STO §4] *Clear-text protection* | own the stream and bucket configuration; size the bus disk for the outage budget |
| Masking at write, deterministic, per-capture policy, classifier and keys, AES-SIV per capture, one digest key across captures, rotated rarely by declaration; baseline rules name the recipe carriers | G5-207, G5-214, G5-220, G5-222, G5-250, [SEM §8] *Baseline rules*, `tracepack/redact` | bind an immutable processing configuration per capture; hold the keys; defer what a consumer cannot execute; the default policy covers the baseline carriers |
| Thinning: only polls the gateway positively identifies as FDC interval polling, by a means other than stream and function; everything else in full | V-27, G5-208, G5-224, G5-225 | store as received; a merge refuses an unobserved record the descriptor does not declare |
| Scope = UTC hour; segments interleave; merge per scope; deletion only after a verified merge whose output is retained; conflicting copies of one seq are reported, never resolved by a writer | [STO §4] *Scope and order*, *Merge*, *Deletion*, [FMT I-12] | catalog records every merge with its inputs; deletion reads that relation; conflicts are kept, both versions |
| Closing a capture left open: one `stop-unclean` at seq 2^63 − 1, recorded before written, byte-identical on retry; seqs after the last producer record stay an unknown tail | [STO §4] *Closing a capture left open*, `tracepack.WriteClose` | durable close intent with every byte-affecting input fixed |
| Catalog is the service's database; tracepack defines no view | G5-201, [STO §5] | catalog answers "packs of a tool and window" and completeness |
| Export schema follows the pack's `format_minor`: `tracepack-jsonl/1` for 0, `/2` for 1, `/3` for 2; the canonical exporter takes one pack; extracts are a v1.0 non-goal | G5-233, G5-270, G5-198, tracepack-go.md §3 | pack-granular export bundles (§10.1) |
| Traffic bus is a separate NATS cluster | V-30 | tracekeep never touches the gateway's own bus |
| The standalone recorder's spool is gone; a bus consumer writes with `tracepack.Writer` directly | G5-227, tracepack-go.md §3 *Masking preparation* | §5.1 |
| A wire observer lends `Frame` for the callback only; `Frame` holds the 4-byte length prefix, the 10-byte header and the message text | go-secs `hsms.WithWireObserver`, `WireEvent.Frame` | the producer copies it before returning (§4.2); the zero-fill of §4.6 starts at byte 14 |

## 14. Rows of `ve-log-service.md` this design settles once agreed

LS-2 (the tool's side, one header), LS-3 (§4.4), LS-4 and LS-5 (observers see every frame and socket, rejected and self-originated included),
LS-6 (§4.5), LS-7 (the rule of §4.6, pending the interval-only request origin, T-42), LS-10 and LS-11 (§7.3, §10.1), LS-14 (§4.3), LS-15 as qualified in §13 item 2.
Still open there: LS-8's fields beyond recipe bodies, LS-13.
