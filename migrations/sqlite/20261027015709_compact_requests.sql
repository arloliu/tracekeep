-- Disable the enforcement of foreign-keys constraints
PRAGMA foreign_keys = off;
-- Create "new_conflicts" table
CREATE TABLE `new_conflicts` (
  `capture_id` blob NOT NULL,
  `seq_from` bigint NOT NULL,
  `seq_to` bigint NOT NULL,
  `pack_id` blob NOT NULL,
  `other_pack_id` blob NOT NULL,
  `state` varchar NOT NULL,
  PRIMARY KEY (`capture_id`, `seq_from`, `seq_to`, `pack_id`, `other_pack_id`),
  CONSTRAINT `conflicts_state` CHECK (state IN ('suspect', 'confirmed', 'acknowledged'))
);
-- Copy rows from old table "conflicts" to new temporary table "new_conflicts"
INSERT INTO `new_conflicts` (`capture_id`, `seq_from`, `seq_to`, `pack_id`, `other_pack_id`, `state`) SELECT `capture_id`, `seq_from`, `seq_to`, `pack_id`, `other_pack_id`, `state` FROM `conflicts`;
-- Drop "conflicts" table after copying rows
DROP TABLE `conflicts`;
-- Rename temporary table "new_conflicts" to "conflicts"
ALTER TABLE `new_conflicts` RENAME TO `conflicts`;
-- Create "new_scope_events" table
CREATE TABLE `new_scope_events` (
  `seq` bigint NOT NULL,
  `capture_id` blob NOT NULL,
  `hour` bigint NOT NULL,
  `event` varchar NOT NULL,
  `revision` bigint NOT NULL,
  `at` bigint NOT NULL,
  PRIMARY KEY (`seq`),
  CONSTRAINT `scope_events_event` CHECK (event IN ('created', 'settled', 'reopened', 'conflict', 'terminated', 'excluded'))
);
-- Copy rows from old table "scope_events" to new temporary table "new_scope_events"
INSERT INTO `new_scope_events` (`seq`, `capture_id`, `hour`, `event`, `revision`, `at`) SELECT `seq`, `capture_id`, `hour`, `event`, `revision`, `at` FROM `scope_events`;
-- Drop "scope_events" table after copying rows
DROP TABLE `scope_events`;
-- Rename temporary table "new_scope_events" to "scope_events"
ALTER TABLE `new_scope_events` RENAME TO `scope_events`;
-- Create "compact_requests" table
CREATE TABLE `compact_requests` (
  `request_id` blob NOT NULL,
  `kind` varchar NOT NULL,
  `capture_id` blob NOT NULL,
  `hour` bigint NULL,
  `requested_by` varchar NOT NULL,
  `requested_at` bigint NOT NULL,
  `state` varchar NOT NULL,
  `outcome` text NULL,
  `done_at` bigint NULL,
  PRIMARY KEY (`request_id`),
  CONSTRAINT `compact_requests_kind` CHECK (kind IN ('merge', 'close')),
  CONSTRAINT `compact_requests_state` CHECK (state IN ('pending', 'done', 'failed'))
);
-- Create index "compact_requests_state" to table: "compact_requests"
CREATE INDEX `compact_requests_state` ON `compact_requests` (`state`, `requested_at`);
-- Enable back the enforcement of foreign-keys constraints
PRAGMA foreign_keys = on;
