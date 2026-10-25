# tracekeep — storage

Status: draft (2026-10-25) — split from seed v21

Covers the write and read seams, the `ObjectStore` interface and its backends, ranged reads and presigned URLs, and the reconciler.
From seed v21 §5; section numbers are the seed's, and [README.md](README.md) maps each to its file.
Read before touching `internal/store` or the reconciler, or anything that puts, reads or deletes a pack.

## Interface

Non-normative summary of what a caller must know.
The sections below are authoritative; a disagreement is a defect in this summary.
Contract changes go through proposals, which the subject [README.md](README.md) lists.

- **Write seam**: a consumer masks its batch, then writes with `tracepack.NewWriter` over an `io.Writer` with a `Syncer` on a temporary file;
  after `Close` the file is uploaded whole, with one put under its [STO §3] key (§5.1).
- **Working directory**: every temporary file lives under `storage.work_dir`, holds masked bytes only,
  and is disposable: a role removes what it left behind at start (§5.1).
- **Read seam**: `io.ReaderAt` plus size, for `tracepack.Open`, `Verify` and `MergeInput`,
  which may issue parallel `ReadAt` calls (§5.1, §5.3).
- **`ObjectStore`**: `Put`, `Open`, `Stat`, `List`, `Delete`; backends `s3`, `fs`, `mem`;
  one shared conformance suite (§5.2).
- **`Put` postconditions**: the object is visible only when complete; nil means durable;
  an error means nothing is visible, or the caller treats the key as unknown and the reconciler decides (§5.2).
- **Keys**: exactly [STO §3] for packs, `exports/` for export bundles, never overwritten;
  bucket lifecycle rules may change the storage class under `archive/` and never expire anything (§5.2).
- **Not this interface**: the claim bucket is a NATS object store on the traffic cluster (§5.2).
- **Reconciler**: verifies an unknown key in full, quarantines one that is not `finalized-consistent` or has writer defects, and registers the rest with `ingest`'s transaction under the compactor lease;
  only an admitted capture's orphan is adopted;
  a tombstoned or beyond-retention key is deleted again (§5.4).
- **Downloads**: presigning is an optional capability, `Presigner`, that `s3` implements and `fs` and `mem` do not;
  without it, or with `storage.presign` off, the API serves the download itself (`api.md` §10.1 *Downloads*).
  Either way a download URL expires before its object can be deleted (§5.2 *Downloads*).

## 5. Storage (requirement 3)

### 5.1 The seam

- Writing: a bus consumer masks its flush batch and then constructs `tracepack.NewWriter(io.Writer, …)` (tracepack-go.md §3 *Masking preparation*);
  `SegmentWriter` and `SegmentSink` are the standalone recorder's layer, whose spool G5-227 removed.
  The seam is therefore an `io.Writer` with a `Syncer` over a temporary file;
  after `Close` returns, the file is uploaded whole with one put under its [STO §3] key.
  `Writer.Close` finalizes the bytes; durability is the put's.
- **Working files**: `ingest`'s flush output and `compact`'s merge and close output are written to temporary files under one working directory,
  `storage.work_dir` in the service configuration (T-22;
  default: the operating system's temporary directory; on Kubernetes an `emptyDir` with a `sizeLimit`).
  Files are created with mode 0600 and removed once their put returns or their batch is abandoned;
  at start each role removes the working files it left behind.
  A lost working file costs nothing:
  its messages were not acknowledged (`ingest`), or its scope is merged or its close written again next cycle (`compact`).
  The directory holds one segment per concurrent flush, plus one archive per concurrent merge for `compact`;
  open batches are held in memory until flush.
  A working file holds masked bytes only, the same as the pack it becomes
  (`masking-keys.md`: originals are never written to a file).
- Reading: `io.ReaderAt` + size for `tracepack.Open`, `tracepack.Verify` and `tracepack.MergeInput`;
  all may issue parallel `ReadAt` calls.

### 5.2 ObjectStore

```go
type ObjectStore interface {
    Put(ctx context.Context, key string, r io.Reader, size int64) error
    Open(ctx context.Context, key string) (Object, error) // Object: io.ReaderAt, Size(), Close()
    Stat(ctx context.Context, key string) (ObjectInfo, error)
    List(ctx context.Context, prefix string, fn func(ObjectInfo) error) error
    Delete(ctx context.Context, key string) error
}
```

`Put` postconditions, per backend, required of every implementation and tested by one shared conformance suite:
the object is visible under its key only when complete; a returned nil means durable;
a returned error means nothing is visible, or the caller treats the key as unknown and the reconciler (§5.4) decides.

- `s3` (aws-sdk-go-v2 `service/s3`, T-34; any S3-compatible store): one `PutObject` with a seekable body, atomic and durable on success;
  keys are never overwritten ([STO §3]).
  With a custom endpoint it uses path-style addressing and sets `RequestChecksumCalculation` and `ResponseChecksumValidation` to `WhenRequired`,
  since the SDK's default checksums break some S3-compatible stores;
  `Presigner` uses the SDK's presign client.
  Deployments run MinIO and Ceph RGW, so the conformance suite runs the `s3` backend against both, each in a container, in CI (T-34).
- `fs` (standalone pilots, tests): write to `<root>/.partial/`, fsync the file, rename into place, fsync the directory.
- `mem` (tests).

Keys of packs are exactly [STO §3];
export bundles are tracekeep's own and live under `exports/` (`api.md` §10.1), outside [STO §3]'s key space;
bucket lifecycle rules may change the storage class under `archive/` and must never expire anything.
The claim bucket is a NATS object store on the traffic cluster, not this interface.

**Downloads.**
`PackURL` and export bundles hand a reader a URL (§10.1), which `ObjectStore` cannot produce;
presigning is therefore an optional capability of a store:

```go
type Presigner interface {
    PresignGet(ctx context.Context, key string, lifetime time.Duration) (string, error)
}
```

- `s3` implements it with a presigned GET;
  `fs` and `mem` do not, so a standalone pilot has no presigned URLs.
- The `api` role finds the capability once at start, by type assertion on its store;
  without it, the API serves downloads itself (§10.1 *Downloads*).
- `storage.presign` (default on) is the one configuration that chooses between the two (T-33):
  off, an `s3` store's downloads also go through the API,
  for a store whose presigned URLs readers cannot use (a proxy or a different host name in front of the store, a policy refusing query-string authentication).
  With it on, the `api` role checks at start by presigning a GET of a key that does not exist and fetching it:
  not-found means the store accepts the signature, and a refused signature is reported with the advice to turn presigning off.
- A presigned URL, like an API download token, expires before its object can be deleted (`api.md` §10.1 *Downloads*, §8.3).

### 5.3 ReaderAt

Ranged GETs with a small read-ahead;
tracepack's reader keeps its bootstrap bytes and reads packs below a few MiB in one request (G5-211).
Presigned URLs (§10.1) give the VE service the same objects without tracekeep in the data path;
on a store without presigning the API is in that path, serving ranged reads through `Open` (§10.1 *Downloads*).

### 5.4 Reconciler

The flush order is put → catalog registration → ack (§6.3).
`compact` lists `staging/` and `archive/` periodically.
For a key the catalog does not know and that has no **tombstone** (§8.3):

1. `tracepack.Verify` the object in full;
   one that is not `finalized-consistent`,
   **or whose `VerifyReport.WriterDefects` is not empty** (cross-block seq order, hour span, `seq_start` defects, which `Verify` reports beside the outcome), is quarantined:
   reported, never registered, never deleted automatically;
2. read every record (block by block, through the `Reader`) to obtain the **exact seq set** with the time of each run's endpoints, the capture, hour, `format_minor` and coverage;
   block summaries carry only first and last seq, which do not give membership;
3. check that the key spells the pack's capture and `pack_id`, and for a segment key its `seq_first` (an archive key has none, [STO §3]);
4. register it with the same idempotent transaction `ingest` uses (§6.3), **under the compactor lease**:
   the transaction's tombstone predicate, hour-retention guard and lease check decide, not the listing's earlier look.
   An orphan is adopted only for an **admitted** capture:
   a `pending` row (§6.1) lets the transaction's prologue succeed but is not admission,
   so such a pack stays unregistered, reported with its capture id, until a `start` admits the capture;
   adoption never turns a placeholder into an apparently registered capture.
   An orphan holding a seq whose `parked` row is `terminated`, or of a capture with a termination order, is never adopted:
   the transaction tombstones its key and the object is deleted after the commit (T-32),
   which loses nothing, since an orphan's messages were never acknowledged and are decided again at their redelivery.

No receipt is reconstructed for an orphan:
a masked pack holds neither the bus header nor the original payload, and none is needed.
The reconciler passes no delivery evidence, so its registration writes coverage only, never a receipt or a `parked` state (`catalog.md` §7.2),
and an adopted pack never gains them later (T-32):
when the adoption wins against `ingest`'s own registration of the same key, that registration finds the pack `adopted` and leaves its batch unacknowledged, so the redelivery writes the evidence into another segment.
A claim object without a receipt is deleted only with its record's hour (§8.4),
so a redelivered claim-check message of an orphan's record fetches its object, is staged again, byte-identical, and writes the receipt then.
A key with a tombstone, or whose hour is beyond the retention horizon, is deleted again
if it reappears (a delayed upload after retention), never registered.
A reconciled archive is a `staged` pack like any other and an input of the scope's next merge;
it is never deleted by the worker whose merge it came from (§8.1).
