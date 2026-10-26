-- Create "audit_log" table
CREATE TABLE `audit_log` (
  `audit_id` binary(16) NOT NULL,
  `at` bigint NOT NULL,
  `actor` varchar(255) NOT NULL,
  `action` varchar(64) NOT NULL,
  `action_id` varchar(64) NULL,
  `capture_id` binary(16) NULL,
  `detail` text NULL,
  PRIMARY KEY (`audit_id`),
  INDEX `audit_log_at` (`at`)
) COLLATE utf8mb4_uca1400_ai_ci;
-- Create "bus_streams" table
CREATE TABLE `bus_streams` (
  `incarnation_id` bigint NOT NULL,
  `role` varchar(16) NOT NULL,
  `stream_name` varchar(255) NOT NULL,
  `created_ns` bigint NOT NULL,
  `last_seq_seen` bigint NOT NULL,
  `retired_at` bigint NULL,
  PRIMARY KEY (`incarnation_id`),
  CONSTRAINT `bus_streams_role` CHECK (`role` in ('traffic','parking'))
) COLLATE utf8mb4_uca1400_ai_ci;
-- Create "captures" table
CREATE TABLE `captures` (
  `capture_id` binary(16) NOT NULL,
  `state` varchar(16) NOT NULL,
  `descriptor` mediumblob NULL,
  `tool_id` varchar(255) NULL,
  `recorder_instance_id` binary(16) NULL,
  `device` varchar(255) NULL,
  `origin_ns` bigint NULL,
  `previous_capture_id` binary(16) NULL,
  `config_id` bigint NULL,
  `data_revision` bigint NOT NULL DEFAULT 0,
  `highest_seq` bigint NULL,
  `highest_seq_ts` bigint NULL,
  `stop_seen` bool NOT NULL DEFAULT 0,
  `last_registered_at` bigint NULL,
  `created_at` bigint NOT NULL,
  PRIMARY KEY (`capture_id`),
  INDEX `captures_device` (`tool_id`, `device`, `origin_ns`),
  INDEX `captures_successor` (`tool_id`, `recorder_instance_id`, `origin_ns`),
  CONSTRAINT `captures_state` CHECK (`state` in ('pending','open','stopped','closed','rejected'))
) COLLATE utf8mb4_uca1400_ai_ci;
-- Create "capture_closes" table
CREATE TABLE `capture_closes` (
  `capture_id` binary(16) NOT NULL,
  `trigger_kind` varchar(16) NOT NULL,
  `triggered_at` bigint NOT NULL,
  `highest_seq` bigint NULL,
  `highest_seq_ts` bigint NULL,
  `gap_start` bigint NULL,
  `gap_end` bigint NULL,
  `pack_id` binary(16) NOT NULL,
  `writer_time` bigint NOT NULL,
  `pack_meta` mediumblob NOT NULL,
  `codec` varchar(32) NOT NULL,
  `state` varchar(16) NOT NULL,
  PRIMARY KEY (`capture_id`),
  CONSTRAINT `capture_closes_state` CHECK (`state` in ('pending','published','abandoned')),
  CONSTRAINT `capture_closes_trigger` CHECK (`trigger_kind` in ('inactivity','successor'))
) COLLATE utf8mb4_uca1400_ai_ci;
-- Create "claim_objects" table
CREATE TABLE `claim_objects` (
  `object_key` varchar(512) NOT NULL,
  `capture_id` binary(16) NOT NULL,
  `seq` bigint NOT NULL,
  `hour` bigint NOT NULL,
  `size` bigint NOT NULL,
  `state` varchar(16) NOT NULL,
  `evicted_hour` bigint NULL,
  PRIMARY KEY (`object_key`),
  INDEX `claim_objects_capture_hour` (`capture_id`, `hour`),
  CONSTRAINT `claim_objects_state` CHECK (`state` in ('live','survivor','deletable'))
) COLLATE utf8mb4_uca1400_ai_ci;
-- Create "claim_records" table
CREATE TABLE `claim_records` (
  `capture_id` binary(16) NOT NULL,
  `seq` bigint NOT NULL,
  `header` binary(44) NOT NULL,
  `payload_sha256` binary(32) NOT NULL,
  `hour` bigint NOT NULL,
  `first_pack_id` binary(16) NOT NULL,
  PRIMARY KEY (`capture_id`, `seq`, `header`, `payload_sha256`),
  INDEX `claim_records_capture_hour` (`capture_id`, `hour`)
) COLLATE utf8mb4_uca1400_ai_ci;
-- Create "conflicts" table
CREATE TABLE `conflicts` (
  `capture_id` binary(16) NOT NULL,
  `seq_from` bigint NOT NULL,
  `seq_to` bigint NOT NULL,
  `pack_id` binary(16) NOT NULL,
  `other_pack_id` binary(16) NOT NULL,
  `state` varchar(16) NOT NULL,
  PRIMARY KEY (`capture_id`, `seq_from`, `pack_id`, `other_pack_id`),
  CONSTRAINT `conflicts_state` CHECK (`state` in ('suspect','confirmed'))
) COLLATE utf8mb4_uca1400_ai_ci;
-- Create "consumers" table
CREATE TABLE `consumers` (
  `replica_id` varchar(128) NOT NULL,
  `lease_until` bigint NOT NULL,
  `generation` bigint NOT NULL,
  `registered_at` bigint NOT NULL,
  PRIMARY KEY (`replica_id`)
) COLLATE utf8mb4_uca1400_ai_ci;
-- Create "consumer_configs" table
CREATE TABLE `consumer_configs` (
  `replica_id` varchar(128) NOT NULL,
  `config_id` bigint NOT NULL,
  PRIMARY KEY (`replica_id`, `config_id`)
) COLLATE utf8mb4_uca1400_ai_ci;
-- Create "counters" table
CREATE TABLE `counters` (
  `name` varchar(32) NOT NULL,
  `value` bigint NOT NULL DEFAULT 0,
  `low_water` bigint NOT NULL DEFAULT 0,
  `epoch` bigint NOT NULL DEFAULT 0,
  PRIMARY KEY (`name`)
) COLLATE utf8mb4_uca1400_ai_ci;
-- Create "evicted_ranges" table
CREATE TABLE `evicted_ranges` (
  `capture_id` binary(16) NOT NULL,
  `hour` bigint NOT NULL,
  `seq_from` bigint NOT NULL,
  `seq_to` bigint NOT NULL,
  `ts_from` bigint NOT NULL,
  `ts_to` bigint NOT NULL,
  PRIMARY KEY (`capture_id`, `seq_from`)
) COLLATE utf8mb4_uca1400_ai_ci;
-- Create "key_bindings" table
CREATE TABLE `key_bindings` (
  `key_id` varchar(128) NOT NULL,
  `kind` varchar(16) NOT NULL,
  `provider_ref` varchar(255) NOT NULL,
  `provider_version` varchar(64) NOT NULL,
  `fingerprint` varchar(64) NOT NULL,
  `bound_at` bigint NOT NULL,
  PRIMARY KEY (`key_id`),
  CONSTRAINT `key_bindings_kind` CHECK (`kind` in ('digest','master','session','download'))
) COLLATE utf8mb4_uca1400_ai_ci;
-- Create "leases" table
CREATE TABLE `leases` (
  `name` varchar(64) NOT NULL,
  `holder` varchar(128) NOT NULL,
  `fence_token` bigint NOT NULL,
  `until_ns` bigint NOT NULL,
  PRIMARY KEY (`name`)
) COLLATE utf8mb4_uca1400_ai_ci;
-- Create "merges" table
CREATE TABLE `merges` (
  `merge_id` binary(16) NOT NULL,
  `capture_id` binary(16) NOT NULL,
  `hour` bigint NOT NULL,
  `output_pack_id` binary(16) NOT NULL,
  `outcome` varchar(16) NOT NULL,
  `at` bigint NOT NULL,
  PRIMARY KEY (`merge_id`),
  INDEX `merges_output` (`output_pack_id`),
  CONSTRAINT `merges_outcome` CHECK (`outcome` in ('committed','failed'))
) COLLATE utf8mb4_uca1400_ai_ci;
-- Create "merge_inputs" table
CREATE TABLE `merge_inputs` (
  `merge_id` binary(16) NOT NULL,
  `pack_id` binary(16) NOT NULL,
  PRIMARY KEY (`merge_id`, `pack_id`),
  INDEX `merge_inputs_pack` (`pack_id`)
) COLLATE utf8mb4_uca1400_ai_ci;
-- Create "packs" table
CREATE TABLE `packs` (
  `pack_id` binary(16) NOT NULL,
  `capture_id` binary(16) NOT NULL,
  `tool_id` varchar(255) NOT NULL,
  `hour` bigint NOT NULL,
  `role` varchar(16) NOT NULL,
  `object_key` varchar(512) NOT NULL,
  `size` bigint NOT NULL,
  `seq_start` bigint NOT NULL,
  `format_minor` int NOT NULL,
  `state` varchar(16) NOT NULL,
  `adopted` bool NOT NULL DEFAULT 0,
  `registered_at` bigint NOT NULL,
  PRIMARY KEY (`pack_id`),
  INDEX `packs_capture_hour_state` (`capture_id`, `hour`, `state`),
  INDEX `packs_object_key` (`object_key`),
  INDEX `packs_tool_hour` (`tool_id`, `hour`),
  CONSTRAINT `packs_role` CHECK (`role` in ('segment','conflict','archive','close')),
  CONSTRAINT `packs_state` CHECK (`state` in ('staged','merged','excluded','deleted'))
) COLLATE utf8mb4_uca1400_ai_ci;
-- Create "pack_losses" table
CREATE TABLE `pack_losses` (
  `pack_id` binary(16) NOT NULL,
  `ord` int NOT NULL,
  `seq_from` bigint NOT NULL,
  `seq_to` bigint NOT NULL,
  `ts_from` bigint NULL,
  `ts_to` bigint NULL,
  `reason` varchar(64) NOT NULL,
  PRIMARY KEY (`pack_id`, `ord`)
) COLLATE utf8mb4_uca1400_ai_ci;
-- Create "pack_ranges" table
CREATE TABLE `pack_ranges` (
  `pack_id` binary(16) NOT NULL,
  `capture_id` binary(16) NOT NULL,
  `seq_from` bigint NOT NULL,
  `seq_to` bigint NOT NULL,
  `ts_from` bigint NOT NULL,
  `ts_to` bigint NOT NULL,
  PRIMARY KEY (`pack_id`, `seq_from`),
  INDEX `pack_ranges_capture_seq` (`capture_id`, `seq_from`)
) COLLATE utf8mb4_uca1400_ai_ci;
-- Create "parked" table
CREATE TABLE `parked` (
  `capture_id` binary(16) NOT NULL,
  `seq` bigint NOT NULL,
  `header` binary(44) NOT NULL,
  `payload_digest` binary(32) NOT NULL,
  `state` varchar(16) NOT NULL,
  `reason` varchar(32) NOT NULL,
  `hour` bigint NOT NULL,
  `object_key` varchar(512) NULL,
  `terminated_by` varchar(64) NULL,
  `parked_at` bigint NOT NULL,
  `resolved_at` bigint NULL,
  `acked_at` bigint NULL,
  PRIMARY KEY (`capture_id`, `seq`, `header`, `payload_digest`),
  INDEX `parked_capture_hour` (`capture_id`, `hour`),
  CONSTRAINT `parked_state` CHECK (`state` in ('parking','parked','evicted-hour','staged','terminated'))
) COLLATE utf8mb4_uca1400_ai_ci;
-- Create "parked_copies" table
CREATE TABLE `parked_copies` (
  `incarnation_id` bigint NOT NULL,
  `stream_seq` bigint NOT NULL,
  `capture_id` binary(16) NOT NULL,
  `seq` bigint NOT NULL,
  `header` binary(44) NOT NULL,
  `payload_digest` binary(32) NOT NULL,
  `state` varchar(16) NOT NULL,
  PRIMARY KEY (`incarnation_id`, `stream_seq`),
  INDEX `parked_copies_version` (`capture_id`, `seq`, `header`, `payload_digest`),
  CONSTRAINT `parked_copies_state` CHECK (`state` in ('waiting','acked','lost'))
) COLLATE utf8mb4_uca1400_ai_ci;
-- Create "policies" table
CREATE TABLE `policies` (
  `policy_id` varchar(128) NOT NULL,
  `version` bigint NOT NULL,
  `content_sha256` binary(32) NOT NULL,
  `body` mediumblob NOT NULL,
  `created_at` bigint NOT NULL,
  PRIMARY KEY (`policy_id`, `version`)
) COLLATE utf8mb4_uca1400_ai_ci;
-- Create "processing_configs" table
CREATE TABLE `processing_configs` (
  `config_id` bigint NOT NULL,
  `content` mediumblob NOT NULL,
  `content_sha256` binary(32) NOT NULL,
  `created_at` bigint NOT NULL,
  `activated_at` bigint NULL,
  `retired_at` bigint NULL,
  PRIMARY KEY (`config_id`),
  UNIQUE INDEX `processing_configs_content` (`content_sha256`)
) COLLATE utf8mb4_uca1400_ai_ci;
-- Create "scopes" table
CREATE TABLE `scopes` (
  `capture_id` binary(16) NOT NULL,
  `hour` bigint NOT NULL,
  `state` varchar(16) NOT NULL,
  `revision` bigint NOT NULL,
  `archive_pack_id` binary(16) NULL,
  `merged_at` bigint NULL,
  PRIMARY KEY (`capture_id`, `hour`),
  CONSTRAINT `scopes_state` CHECK (`state` in ('open','settled','reopened'))
) COLLATE utf8mb4_uca1400_ai_ci;
-- Create "scope_events" table
CREATE TABLE `scope_events` (
  `seq` bigint NOT NULL,
  `capture_id` binary(16) NOT NULL,
  `hour` bigint NOT NULL,
  `event` varchar(16) NOT NULL,
  `revision` bigint NOT NULL,
  `at` bigint NOT NULL,
  PRIMARY KEY (`seq`),
  CONSTRAINT `scope_events_event` CHECK (`event` in ('created','settled','reopened','conflict','terminated'))
) COLLATE utf8mb4_uca1400_ai_ci;
-- Create "termination_orders" table
CREATE TABLE `termination_orders` (
  `action_id` varchar(64) NOT NULL,
  `capture_id` binary(16) NOT NULL,
  `incarnation_id` bigint NOT NULL,
  `cut_off` bigint NOT NULL,
  `state` varchar(16) NOT NULL,
  PRIMARY KEY (`action_id`),
  INDEX `termination_orders_capture` (`capture_id`, `state`),
  CONSTRAINT `termination_orders_state` CHECK (`state` in ('active','spent'))
) COLLATE utf8mb4_uca1400_ai_ci;
-- Create "tombstones" table
CREATE TABLE `tombstones` (
  `object_key` varchar(512) NOT NULL,
  `reason` varchar(32) NOT NULL,
  `at` bigint NOT NULL,
  PRIMARY KEY (`object_key`)
) COLLATE utf8mb4_uca1400_ai_ci;
