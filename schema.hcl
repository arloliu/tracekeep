# The catalog's schema (docs/specs/tracekeep/catalog.md §7.1, parking.md §7.1), one file for both engines:
# the types the engines spell differently are variables atlas.hcl sets per engine.
# Times are UTC nanoseconds, hours are hours since the Unix epoch, seqs fit a signed BIGINT.
# On MariaDB every string compares byte for byte, as on SQLite: scripts/catalog.sh generates against a server whose default collation is utf8mb4_nopad_bin (a binary collation that, unlike utf8mb4_bin, keeps trailing spaces significant).
# Edit this file, then run `make migrate-diff NAME=<change>`; never edit a generated migration.

variable "uuid" {
  type = string
}
variable "hash" {
  type = string
}
variable "header" {
  type = string
}
variable "bytes" {
  type = string
}

schema "main" {}

table "captures" {
  schema = schema.main
  column "capture_id" {
    type = sql(var.uuid)
  }
  # pending: a placeholder without descriptor that parking inserts before any start (ingest.md §6.1).
  column "state" {
    type = varchar(16)
  }
  # The start's descriptor bytes; a rejected capture keeps them for the operator; none on a pending row.
  column "descriptor" {
    type = sql(var.bytes)
    null = true
  }
  column "tool_id" {
    type = varchar(255)
    null = true
  }
  column "recorder_instance_id" {
    type = sql(var.uuid)
    null = true
  }
  # The descriptor's recorder_instance_name, decoded at admission.
  column "device" {
    type = varchar(255)
    null = true
  }
  column "origin_ns" {
    type = bigint
    null = true
  }
  column "previous_capture_id" {
    type = sql(var.uuid)
    null = true
  }
  # The processing configuration bound at admission (rollouts.md §6.4).
  column "config_id" {
    type = bigint
    null = true
  }
  column "data_revision" {
    type    = bigint
    default = 0
  }
  # The highest producer seq registered and the time of that record.
  column "highest_seq" {
    type = bigint
    null = true
  }
  column "highest_seq_ts" {
    type = bigint
    null = true
  }
  column "stop_seen" {
    type    = bool
    default = false
  }
  column "last_registered_at" {
    type = bigint
    null = true
  }
  column "created_at" {
    type = bigint
  }
  primary_key {
    columns = [column.capture_id]
  }
  index "captures_successor" {
    columns = [column.tool_id, column.recorder_instance_id, column.origin_ns]
  }
  index "captures_device" {
    columns = [column.tool_id, column.device, column.origin_ns]
  }
  # The captures bound to a configuration, which a retirement reads under the membership sentinel.
  index "captures_config" {
    columns = [column.config_id]
  }
  check "captures_state" {
    expr = "state IN ('pending', 'open', 'stopped', 'closed', 'rejected')"
  }
}

table "capture_closes" {
  schema = schema.main
  column "capture_id" {
    type = sql(var.uuid)
  }
  column "trigger_kind" {
    type = varchar(16)
  }
  column "triggered_at" {
    type = bigint
  }
  column "highest_seq" {
    type = bigint
    null = true
  }
  column "highest_seq_ts" {
    type = bigint
    null = true
  }
  # The crash gap the close declares.
  column "gap_start" {
    type = bigint
    null = true
  }
  column "gap_end" {
    type = bigint
    null = true
  }
  column "pack_id" {
    type = sql(var.uuid)
  }
  column "writer_time" {
    type = bigint
  }
  column "pack_meta" {
    type = sql(var.bytes)
  }
  column "codec" {
    type = varchar(32)
  }
  column "state" {
    type = varchar(16)
  }
  primary_key {
    columns = [column.capture_id]
  }
  check "capture_closes_trigger" {
    expr = "trigger_kind IN ('inactivity', 'successor')"
  }
  check "capture_closes_state" {
    expr = "state IN ('pending', 'published', 'abandoned')"
  }
}

table "packs" {
  schema = schema.main
  column "pack_id" {
    type = sql(var.uuid)
  }
  column "capture_id" {
    type = sql(var.uuid)
  }
  column "tool_id" {
    type = varchar(255)
  }
  column "hour" {
    type = bigint
  }
  column "role" {
    type = varchar(16)
  }
  column "object_key" {
    type = varchar(512)
  }
  column "size" {
    type = bigint
  }
  column "seq_start" {
    type = bigint
  }
  column "format_minor" {
    type = int
  }
  column "state" {
    type = varchar(16)
  }
  # Registered by the reconciler, without delivery evidence (storage.md §5.4).
  column "adopted" {
    type    = bool
    default = false
  }
  column "registered_at" {
    type = bigint
  }
  primary_key {
    columns = [column.pack_id]
  }
  index "packs_tool_hour" {
    columns = [column.tool_id, column.hour]
  }
  index "packs_capture_hour_state" {
    columns = [column.capture_id, column.hour, column.state]
  }
  index "packs_object_key" {
    columns = [column.object_key]
  }
  check "packs_role" {
    expr = "role IN ('segment', 'conflict', 'archive', 'close')"
  }
  check "packs_state" {
    expr = "state IN ('staged', 'merged', 'excluded', 'deleted')"
  }
}

# The exact seq runs a pack holds, with the times of each run's first and last record; never the close sentinel.
table "pack_ranges" {
  schema = schema.main
  column "pack_id" {
    type = sql(var.uuid)
  }
  column "capture_id" {
    type = sql(var.uuid)
  }
  column "seq_from" {
    type = bigint
  }
  column "seq_to" {
    type = bigint
  }
  column "ts_from" {
    type = bigint
  }
  column "ts_to" {
    type = bigint
  }
  primary_key {
    columns = [column.pack_id, column.seq_from]
  }
  index "pack_ranges_capture_seq" {
    columns = [column.capture_id, column.seq_from, column.seq_to]
  }
}

table "evicted_ranges" {
  schema = schema.main
  column "capture_id" {
    type = sql(var.uuid)
  }
  column "hour" {
    type = bigint
  }
  column "seq_from" {
    type = bigint
  }
  column "seq_to" {
    type = bigint
  }
  column "ts_from" {
    type = bigint
  }
  column "ts_to" {
    type = bigint
  }
  primary_key {
    columns = [column.capture_id, column.seq_from]
  }
}

# A pack's coverage entries: its declared losses, each field as the entry has it (a carried entry may lack any).
table "pack_losses" {
  schema = schema.main
  column "pack_id" {
    type = sql(var.uuid)
  }
  column "ord" {
    type = int
  }
  column "seq_from" {
    type = bigint
    null = true
  }
  # Absent when the upper bound of the lost seqs is unknown.
  column "seq_to" {
    type = bigint
    null = true
  }
  column "ts_from" {
    type = bigint
    null = true
  }
  column "ts_to" {
    type = bigint
    null = true
  }
  primary_key {
    columns = [column.pack_id, column.ord]
  }
}

# The receipts of records that travelled by claim-check, one row per version.
table "claim_records" {
  schema = schema.main
  column "capture_id" {
    type = sql(var.uuid)
  }
  column "seq" {
    type = bigint
  }
  column "header" {
    type = sql(var.header)
  }
  column "payload_sha256" {
    type = sql(var.hash)
  }
  column "hour" {
    type = bigint
  }
  column "first_pack_id" {
    type = sql(var.uuid)
  }
  primary_key {
    columns = [column.capture_id, column.seq, column.header, column.payload_sha256]
  }
  index "claim_records_capture_hour" {
    columns = [column.capture_id, column.hour]
  }
}

# A run of seqs two packs of one capture both hold: suspect until compact compares them, or confirmed when they differ.
table "conflicts" {
  schema = schema.main
  column "capture_id" {
    type = sql(var.uuid)
  }
  column "seq_from" {
    type = bigint
  }
  column "seq_to" {
    type = bigint
  }
  column "pack_id" {
    type = sql(var.uuid)
  }
  column "other_pack_id" {
    type = sql(var.uuid)
  }
  column "state" {
    type = varchar(16)
  }
  primary_key {
    columns = [column.capture_id, column.seq_from, column.seq_to, column.pack_id, column.other_pack_id]
  }
  check "conflicts_state" {
    expr = "state IN ('suspect', 'confirmed')"
  }
}

table "claim_objects" {
  schema = schema.main
  column "object_key" {
    type = varchar(512)
  }
  column "capture_id" {
    type = sql(var.uuid)
  }
  column "seq" {
    type = bigint
  }
  column "hour" {
    type = bigint
  }
  column "size" {
    type = bigint
  }
  # live, or a survivor retention skipped with its evicted hour, then deletable (compactor.md §8.4).
  column "state" {
    type = varchar(16)
  }
  column "evicted_hour" {
    type = bigint
    null = true
  }
  primary_key {
    columns = [column.object_key]
  }
  index "claim_objects_capture_hour" {
    columns = [column.capture_id, column.hour]
  }
  check "claim_objects_state" {
    expr = "state IN ('live', 'survivor', 'deletable')"
  }
}

# Ingest replicas and their membership leases (rollouts.md §6.4).
table "consumers" {
  schema = schema.main
  column "replica_id" {
    type = varchar(128)
  }
  column "lease_until" {
    type = bigint
  }
  # Advanced by every registration, so a delivery is checked against the membership it was pulled under.
  column "generation" {
    type = bigint
  }
  column "registered_at" {
    type = bigint
  }
  primary_key {
    columns = [column.replica_id]
  }
}

table "consumer_configs" {
  schema = schema.main
  column "replica_id" {
    type = varchar(128)
  }
  column "config_id" {
    type = bigint
  }
  primary_key {
    columns = [column.replica_id, column.config_id]
  }
}

table "processing_configs" {
  schema = schema.main
  column "config_id" {
    type = bigint
  }
  column "content" {
    type = sql(var.bytes)
  }
  column "content_sha256" {
    type = sql(var.hash)
  }
  column "created_at" {
    type = bigint
  }
  column "activated_at" {
    type = bigint
    null = true
  }
  column "retired_at" {
    type = bigint
    null = true
  }
  primary_key {
    columns = [column.config_id]
  }
  index "processing_configs_content" {
    unique  = true
    columns = [column.content_sha256]
  }
}

table "policies" {
  schema = schema.main
  column "policy_id" {
    type = varchar(128)
  }
  column "version" {
    type = bigint
  }
  column "content_sha256" {
    type = sql(var.hash)
  }
  column "body" {
    type = sql(var.bytes)
  }
  column "created_at" {
    type = bigint
  }
  primary_key {
    columns = [column.policy_id, column.version]
  }
}

table "key_bindings" {
  schema = schema.main
  column "key_id" {
    type = varchar(128)
  }
  column "kind" {
    type = varchar(16)
  }
  column "provider_ref" {
    type = varchar(255)
  }
  column "provider_version" {
    type = varchar(64)
  }
  column "fingerprint" {
    type = varchar(64)
  }
  column "bound_at" {
    type = bigint
  }
  primary_key {
    columns = [column.key_id]
  }
  check "key_bindings_kind" {
    expr = "kind IN ('digest', 'master', 'session', 'download')"
  }
}

table "scopes" {
  schema = schema.main
  column "capture_id" {
    type = sql(var.uuid)
  }
  column "hour" {
    type = bigint
  }
  column "state" {
    type = varchar(16)
  }
  column "revision" {
    type = bigint
  }
  column "archive_pack_id" {
    type = sql(var.uuid)
    null = true
  }
  column "merged_at" {
    type = bigint
    null = true
  }
  primary_key {
    columns = [column.capture_id, column.hour]
  }
  check "scopes_state" {
    expr = "state IN ('open', 'settled', 'reopened')"
  }
}

# The change cursor (catalog.md §7.3): seq comes from counters('scope_events'), allocated last in its transaction.
table "scope_events" {
  schema = schema.main
  column "seq" {
    type = bigint
  }
  column "capture_id" {
    type = sql(var.uuid)
  }
  column "hour" {
    type = bigint
  }
  column "event" {
    type = varchar(16)
  }
  column "revision" {
    type = bigint
  }
  column "at" {
    type = bigint
  }
  primary_key {
    columns = [column.seq]
  }
  check "scope_events_event" {
    expr = "event IN ('created', 'settled', 'reopened', 'conflict', 'terminated')"
  }
}

# Named counters: 'scope_events' allocates event seqs;
# 'membership' counts registrations, which number the memberships, and its row lock serializes admission with membership changes.
table "counters" {
  schema = schema.main
  column "name" {
    type = varchar(32)
  }
  column "value" {
    type    = bigint
    default = 0
  }
  column "low_water" {
    type    = bigint
    default = 0
  }
  column "epoch" {
    type    = bigint
    default = 0
  }
  primary_key {
    columns = [column.name]
  }
}

table "merges" {
  schema = schema.main
  column "merge_id" {
    type = sql(var.uuid)
  }
  column "capture_id" {
    type = sql(var.uuid)
  }
  column "hour" {
    type = bigint
  }
  column "output_pack_id" {
    type = sql(var.uuid)
  }
  column "outcome" {
    type = varchar(16)
  }
  column "at" {
    type = bigint
  }
  primary_key {
    columns = [column.merge_id]
  }
  index "merges_output" {
    columns = [column.output_pack_id]
  }
  check "merges_outcome" {
    expr = "outcome IN ('committed', 'failed')"
  }
}

table "merge_inputs" {
  schema = schema.main
  column "merge_id" {
    type = sql(var.uuid)
  }
  column "pack_id" {
    type = sql(var.uuid)
  }
  primary_key {
    columns = [column.merge_id, column.pack_id]
  }
  index "merge_inputs_pack" {
    columns = [column.pack_id]
  }
}

table "tombstones" {
  schema = schema.main
  column "object_key" {
    type = varchar(512)
  }
  column "reason" {
    type = varchar(32)
  }
  column "at" {
    type = bigint
  }
  primary_key {
    columns = [column.object_key]
  }
}

table "leases" {
  schema = schema.main
  column "name" {
    type = varchar(64)
  }
  column "holder" {
    type = varchar(128)
  }
  column "fence_token" {
    type = bigint
  }
  column "until_ns" {
    type = bigint
  }
  primary_key {
    columns = [column.name]
  }
}

table "audit_log" {
  schema = schema.main
  column "audit_id" {
    type = sql(var.uuid)
  }
  column "at" {
    type = bigint
  }
  column "actor" {
    type = varchar(255)
  }
  column "action" {
    type = varchar(64)
  }
  column "action_id" {
    type = varchar(64)
    null = true
  }
  column "capture_id" {
    type = sql(var.uuid)
    null = true
  }
  column "detail" {
    type = text
    null = true
  }
  primary_key {
    columns = [column.audit_id]
  }
  index "audit_log_at" {
    columns = [column.at]
  }
}

# Parking (parking.md §7.1): one row per version, kept for the capture's lifetime.
table "parked" {
  schema = schema.main
  column "capture_id" {
    type = sql(var.uuid)
  }
  column "seq" {
    type = bigint
  }
  column "header" {
    type = sql(var.header)
  }
  column "payload_digest" {
    type = sql(var.hash)
  }
  column "state" {
    type = varchar(16)
  }
  column "reason" {
    type = varchar(32)
  }
  column "hour" {
    type = bigint
  }
  column "object_key" {
    type = varchar(512)
    null = true
  }
  column "terminated_by" {
    type = varchar(64)
    null = true
  }
  column "parked_at" {
    type = bigint
  }
  column "resolved_at" {
    type = bigint
    null = true
  }
  # Metadata only: when the last known copy was acknowledged; never a protection predicate.
  column "acked_at" {
    type = bigint
    null = true
  }
  primary_key {
    columns = [column.capture_id, column.seq, column.header, column.payload_digest]
  }
  index "parked_capture_hour" {
    columns = [column.capture_id, column.hour]
  }
  check "parked_state" {
    expr = "state IN ('parking', 'parked', 'evicted-hour', 'staged', 'terminated')"
  }
}

# One row per message of a parked version, on either stream.
table "parked_copies" {
  schema = schema.main
  column "incarnation_id" {
    type = bigint
  }
  column "stream_seq" {
    type = bigint
  }
  column "capture_id" {
    type = sql(var.uuid)
  }
  column "seq" {
    type = bigint
  }
  column "header" {
    type = sql(var.header)
  }
  column "payload_digest" {
    type = sql(var.hash)
  }
  column "state" {
    type = varchar(16)
  }
  primary_key {
    columns = [column.incarnation_id, column.stream_seq]
  }
  index "parked_copies_version" {
    columns = [column.capture_id, column.seq, column.header, column.payload_digest]
  }
  check "parked_copies_state" {
    expr = "state IN ('waiting', 'acked', 'lost')"
  }
}

table "bus_streams" {
  schema = schema.main
  column "incarnation_id" {
    type = bigint
  }
  column "role" {
    type = varchar(16)
  }
  column "stream_name" {
    type = varchar(255)
  }
  # StreamInfo.Created, in nanoseconds.
  column "created_ns" {
    type = bigint
  }
  column "last_seq_seen" {
    type = bigint
  }
  column "retired_at" {
    type = bigint
    null = true
  }
  primary_key {
    columns = [column.incarnation_id]
  }
  check "bus_streams_role" {
    expr = "role IN ('traffic', 'parking')"
  }
}

table "termination_orders" {
  schema = schema.main
  column "action_id" {
    type = varchar(64)
  }
  column "capture_id" {
    type = sql(var.uuid)
  }
  column "incarnation_id" {
    type = bigint
  }
  column "cut_off" {
    type = bigint
  }
  column "state" {
    type = varchar(16)
  }
  primary_key {
    columns = [column.action_id]
  }
  index "termination_orders_capture" {
    columns = [column.capture_id, column.state]
  }
  check "termination_orders_state" {
    expr = "state IN ('active', 'spent')"
  }
}
