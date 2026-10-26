-- Create "captures" table
CREATE TABLE `captures` (
  `capture_id` blob NOT NULL,
  `state` varchar NOT NULL,
  `descriptor` blob NULL,
  `tool_id` varchar NULL,
  `recorder_instance_id` blob NULL,
  `device` varchar NULL,
  `origin_ns` bigint NULL,
  `previous_capture_id` blob NULL,
  `config_id` bigint NULL,
  `data_revision` bigint NOT NULL DEFAULT 0,
  `highest_seq` bigint NULL,
  `highest_seq_ts` bigint NULL,
  `stop_seen` bool NOT NULL DEFAULT false,
  `last_registered_at` bigint NULL,
  `created_at` bigint NOT NULL,
  PRIMARY KEY (`capture_id`),
  CONSTRAINT `captures_state` CHECK (state IN ('pending', 'open', 'stopped', 'closed', 'rejected'))
);
-- Create index "captures_successor" to table: "captures"
CREATE INDEX `captures_successor` ON `captures` (`tool_id`, `recorder_instance_id`, `origin_ns`);
-- Create index "captures_device" to table: "captures"
CREATE INDEX `captures_device` ON `captures` (`tool_id`, `device`, `origin_ns`);
-- Create "capture_closes" table
CREATE TABLE `capture_closes` (
  `capture_id` blob NOT NULL,
  `trigger_kind` varchar NOT NULL,
  `triggered_at` bigint NOT NULL,
  `highest_seq` bigint NULL,
  `highest_seq_ts` bigint NULL,
  `gap_start` bigint NULL,
  `gap_end` bigint NULL,
  `pack_id` blob NOT NULL,
  `writer_time` bigint NOT NULL,
  `pack_meta` blob NOT NULL,
  `codec` varchar NOT NULL,
  `state` varchar NOT NULL,
  PRIMARY KEY (`capture_id`),
  CONSTRAINT `capture_closes_trigger` CHECK (trigger_kind IN ('inactivity', 'successor')),
  CONSTRAINT `capture_closes_state` CHECK (state IN ('pending', 'published', 'abandoned'))
);
-- Create "packs" table
CREATE TABLE `packs` (
  `pack_id` blob NOT NULL,
  `capture_id` blob NOT NULL,
  `tool_id` varchar NOT NULL,
  `hour` bigint NOT NULL,
  `role` varchar NOT NULL,
  `object_key` varchar NOT NULL,
  `size` bigint NOT NULL,
  `seq_start` bigint NOT NULL,
  `format_minor` int NOT NULL,
  `state` varchar NOT NULL,
  `adopted` bool NOT NULL DEFAULT false,
  `registered_at` bigint NOT NULL,
  PRIMARY KEY (`pack_id`),
  CONSTRAINT `packs_role` CHECK (role IN ('segment', 'conflict', 'archive', 'close')),
  CONSTRAINT `packs_state` CHECK (state IN ('staged', 'merged', 'excluded', 'deleted'))
);
-- Create index "packs_tool_hour" to table: "packs"
CREATE INDEX `packs_tool_hour` ON `packs` (`tool_id`, `hour`);
-- Create index "packs_capture_hour_state" to table: "packs"
CREATE INDEX `packs_capture_hour_state` ON `packs` (`capture_id`, `hour`, `state`);
-- Create index "packs_object_key" to table: "packs"
CREATE INDEX `packs_object_key` ON `packs` (`object_key`);
-- Create "pack_ranges" table
CREATE TABLE `pack_ranges` (
  `pack_id` blob NOT NULL,
  `capture_id` blob NOT NULL,
  `seq_from` bigint NOT NULL,
  `seq_to` bigint NOT NULL,
  `ts_from` bigint NOT NULL,
  `ts_to` bigint NOT NULL,
  PRIMARY KEY (`pack_id`, `seq_from`)
);
-- Create index "pack_ranges_capture_seq" to table: "pack_ranges"
CREATE INDEX `pack_ranges_capture_seq` ON `pack_ranges` (`capture_id`, `seq_from`);
-- Create "evicted_ranges" table
CREATE TABLE `evicted_ranges` (
  `capture_id` blob NOT NULL,
  `hour` bigint NOT NULL,
  `seq_from` bigint NOT NULL,
  `seq_to` bigint NOT NULL,
  `ts_from` bigint NOT NULL,
  `ts_to` bigint NOT NULL,
  PRIMARY KEY (`capture_id`, `seq_from`)
);
-- Create "pack_losses" table
CREATE TABLE `pack_losses` (
  `pack_id` blob NOT NULL,
  `ord` int NOT NULL,
  `seq_from` bigint NOT NULL,
  `seq_to` bigint NOT NULL,
  `ts_from` bigint NULL,
  `ts_to` bigint NULL,
  `reason` varchar NOT NULL,
  PRIMARY KEY (`pack_id`, `ord`)
);
-- Create "claim_records" table
CREATE TABLE `claim_records` (
  `capture_id` blob NOT NULL,
  `seq` bigint NOT NULL,
  `header` blob NOT NULL,
  `payload_sha256` blob NOT NULL,
  `hour` bigint NOT NULL,
  `first_pack_id` blob NOT NULL,
  PRIMARY KEY (`capture_id`, `seq`, `header`, `payload_sha256`)
);
-- Create index "claim_records_capture_hour" to table: "claim_records"
CREATE INDEX `claim_records_capture_hour` ON `claim_records` (`capture_id`, `hour`);
-- Create "conflicts" table
CREATE TABLE `conflicts` (
  `capture_id` blob NOT NULL,
  `seq` bigint NOT NULL,
  `pack_id` blob NOT NULL,
  `state` varchar NOT NULL,
  PRIMARY KEY (`capture_id`, `seq`, `pack_id`),
  CONSTRAINT `conflicts_state` CHECK (state IN ('suspect', 'confirmed'))
);
-- Create "claim_objects" table
CREATE TABLE `claim_objects` (
  `object_key` varchar NOT NULL,
  `capture_id` blob NOT NULL,
  `seq` bigint NOT NULL,
  `hour` bigint NOT NULL,
  `size` bigint NOT NULL,
  `state` varchar NOT NULL,
  `evicted_hour` bigint NULL,
  PRIMARY KEY (`object_key`),
  CONSTRAINT `claim_objects_state` CHECK (state IN ('live', 'survivor', 'deletable'))
);
-- Create index "claim_objects_capture_hour" to table: "claim_objects"
CREATE INDEX `claim_objects_capture_hour` ON `claim_objects` (`capture_id`, `hour`);
-- Create "consumers" table
CREATE TABLE `consumers` (
  `replica_id` varchar NOT NULL,
  `lease_until` bigint NOT NULL,
  `generation` bigint NOT NULL,
  `registered_at` bigint NOT NULL,
  PRIMARY KEY (`replica_id`)
);
-- Create "consumer_configs" table
CREATE TABLE `consumer_configs` (
  `replica_id` varchar NOT NULL,
  `config_id` bigint NOT NULL,
  PRIMARY KEY (`replica_id`, `config_id`)
);
-- Create "processing_configs" table
CREATE TABLE `processing_configs` (
  `config_id` bigint NOT NULL,
  `content` blob NOT NULL,
  `content_sha256` blob NOT NULL,
  `created_at` bigint NOT NULL,
  `activated_at` bigint NULL,
  `retired_at` bigint NULL,
  PRIMARY KEY (`config_id`)
);
-- Create index "processing_configs_content" to table: "processing_configs"
CREATE UNIQUE INDEX `processing_configs_content` ON `processing_configs` (`content_sha256`);
-- Create "policies" table
CREATE TABLE `policies` (
  `policy_id` varchar NOT NULL,
  `version` bigint NOT NULL,
  `content_sha256` blob NOT NULL,
  `body` blob NOT NULL,
  `created_at` bigint NOT NULL,
  PRIMARY KEY (`policy_id`, `version`)
);
-- Create "key_bindings" table
CREATE TABLE `key_bindings` (
  `key_id` varchar NOT NULL,
  `kind` varchar NOT NULL,
  `provider_ref` varchar NOT NULL,
  `provider_version` varchar NOT NULL,
  `fingerprint` varchar NOT NULL,
  `bound_at` bigint NOT NULL,
  PRIMARY KEY (`key_id`),
  CONSTRAINT `key_bindings_kind` CHECK (kind IN ('digest', 'master', 'session', 'download'))
);
-- Create "scopes" table
CREATE TABLE `scopes` (
  `capture_id` blob NOT NULL,
  `hour` bigint NOT NULL,
  `state` varchar NOT NULL,
  `revision` bigint NOT NULL,
  `archive_pack_id` blob NULL,
  `merged_at` bigint NULL,
  PRIMARY KEY (`capture_id`, `hour`),
  CONSTRAINT `scopes_state` CHECK (state IN ('open', 'settled', 'reopened'))
);
-- Create "scope_events" table
CREATE TABLE `scope_events` (
  `seq` bigint NOT NULL,
  `capture_id` blob NOT NULL,
  `hour` bigint NOT NULL,
  `event` varchar NOT NULL,
  `revision` bigint NOT NULL,
  `at` bigint NOT NULL,
  PRIMARY KEY (`seq`),
  CONSTRAINT `scope_events_event` CHECK (event IN ('created', 'settled', 'reopened', 'conflict', 'terminated'))
);
-- Create "counters" table
CREATE TABLE `counters` (
  `name` varchar NOT NULL,
  `value` bigint NOT NULL DEFAULT 0,
  `low_water` bigint NOT NULL DEFAULT 0,
  `epoch` bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (`name`)
);
-- Create "merges" table
CREATE TABLE `merges` (
  `merge_id` blob NOT NULL,
  `capture_id` blob NOT NULL,
  `hour` bigint NOT NULL,
  `output_pack_id` blob NOT NULL,
  `outcome` varchar NOT NULL,
  `at` bigint NOT NULL,
  PRIMARY KEY (`merge_id`),
  CONSTRAINT `merges_outcome` CHECK (outcome IN ('committed', 'failed'))
);
-- Create index "merges_output" to table: "merges"
CREATE INDEX `merges_output` ON `merges` (`output_pack_id`);
-- Create "merge_inputs" table
CREATE TABLE `merge_inputs` (
  `merge_id` blob NOT NULL,
  `pack_id` blob NOT NULL,
  PRIMARY KEY (`merge_id`, `pack_id`)
);
-- Create index "merge_inputs_pack" to table: "merge_inputs"
CREATE INDEX `merge_inputs_pack` ON `merge_inputs` (`pack_id`);
-- Create "tombstones" table
CREATE TABLE `tombstones` (
  `object_key` varchar NOT NULL,
  `reason` varchar NOT NULL,
  `at` bigint NOT NULL,
  PRIMARY KEY (`object_key`)
);
-- Create "leases" table
CREATE TABLE `leases` (
  `name` varchar NOT NULL,
  `holder` varchar NOT NULL,
  `fence_token` bigint NOT NULL,
  `until_ns` bigint NOT NULL,
  PRIMARY KEY (`name`)
);
-- Create "audit_log" table
CREATE TABLE `audit_log` (
  `audit_id` blob NOT NULL,
  `at` bigint NOT NULL,
  `actor` varchar NOT NULL,
  `action` varchar NOT NULL,
  `action_id` varchar NULL,
  `capture_id` blob NULL,
  `detail` text NULL,
  PRIMARY KEY (`audit_id`)
);
-- Create index "audit_log_at" to table: "audit_log"
CREATE INDEX `audit_log_at` ON `audit_log` (`at`);
-- Create "parked" table
CREATE TABLE `parked` (
  `capture_id` blob NOT NULL,
  `seq` bigint NOT NULL,
  `header` blob NOT NULL,
  `payload_digest` blob NOT NULL,
  `state` varchar NOT NULL,
  `reason` varchar NOT NULL,
  `hour` bigint NOT NULL,
  `object_key` varchar NULL,
  `terminated_by` varchar NULL,
  `parked_at` bigint NOT NULL,
  `resolved_at` bigint NULL,
  `acked_at` bigint NULL,
  PRIMARY KEY (`capture_id`, `seq`, `header`, `payload_digest`),
  CONSTRAINT `parked_state` CHECK (state IN ('parking', 'parked', 'evicted-hour', 'staged', 'terminated'))
);
-- Create index "parked_capture_hour" to table: "parked"
CREATE INDEX `parked_capture_hour` ON `parked` (`capture_id`, `hour`);
-- Create "parked_copies" table
CREATE TABLE `parked_copies` (
  `incarnation_id` bigint NOT NULL,
  `stream_seq` bigint NOT NULL,
  `capture_id` blob NOT NULL,
  `seq` bigint NOT NULL,
  `header` blob NOT NULL,
  `payload_digest` blob NOT NULL,
  `state` varchar NOT NULL,
  PRIMARY KEY (`incarnation_id`, `stream_seq`),
  CONSTRAINT `parked_copies_state` CHECK (state IN ('waiting', 'acked', 'lost'))
);
-- Create index "parked_copies_version" to table: "parked_copies"
CREATE INDEX `parked_copies_version` ON `parked_copies` (`capture_id`, `seq`, `header`, `payload_digest`);
-- Create "bus_streams" table
CREATE TABLE `bus_streams` (
  `incarnation_id` bigint NOT NULL,
  `role` varchar NOT NULL,
  `stream_name` varchar NOT NULL,
  `created_ns` bigint NOT NULL,
  `last_seq_seen` bigint NOT NULL,
  `retired_at` bigint NULL,
  PRIMARY KEY (`incarnation_id`),
  CONSTRAINT `bus_streams_role` CHECK (role IN ('traffic', 'parking'))
);
-- Create "termination_orders" table
CREATE TABLE `termination_orders` (
  `action_id` varchar NOT NULL,
  `capture_id` blob NOT NULL,
  `incarnation_id` bigint NOT NULL,
  `cut_off` bigint NOT NULL,
  `state` varchar NOT NULL,
  PRIMARY KEY (`action_id`),
  CONSTRAINT `termination_orders_state` CHECK (state IN ('active', 'spent'))
);
-- Create index "termination_orders_capture" to table: "termination_orders"
CREATE INDEX `termination_orders_capture` ON `termination_orders` (`capture_id`, `state`);
