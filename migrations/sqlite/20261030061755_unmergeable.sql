-- Disable the enforcement of foreign-keys constraints
PRAGMA foreign_keys = off;
-- Create "new_scopes" table
CREATE TABLE `new_scopes` (
  `capture_id` blob NOT NULL,
  `hour` bigint NOT NULL,
  `state` varchar NOT NULL,
  `revision` bigint NOT NULL,
  `archive_pack_id` blob NULL,
  `merged_at` bigint NULL,
  PRIMARY KEY (`capture_id`, `hour`),
  CONSTRAINT `scopes_state` CHECK (state IN ('open', 'settled', 'reopened', 'unmergeable'))
);
-- Copy rows from old table "scopes" to new temporary table "new_scopes"
INSERT INTO `new_scopes` (`capture_id`, `hour`, `state`, `revision`, `archive_pack_id`, `merged_at`) SELECT `capture_id`, `hour`, `state`, `revision`, `archive_pack_id`, `merged_at` FROM `scopes`;
-- Drop "scopes" table after copying rows
DROP TABLE `scopes`;
-- Rename temporary table "new_scopes" to "scopes"
ALTER TABLE `new_scopes` RENAME TO `scopes`;
-- Create "new_scope_events" table
CREATE TABLE `new_scope_events` (
  `seq` bigint NOT NULL,
  `capture_id` blob NOT NULL,
  `hour` bigint NOT NULL,
  `event` varchar NOT NULL,
  `revision` bigint NOT NULL,
  `at` bigint NOT NULL,
  PRIMARY KEY (`seq`),
  CONSTRAINT `scope_events_event` CHECK (event IN ('created', 'settled', 'reopened', 'conflict', 'terminated', 'excluded', 'unmergeable'))
);
-- Copy rows from old table "scope_events" to new temporary table "new_scope_events"
INSERT INTO `new_scope_events` (`seq`, `capture_id`, `hour`, `event`, `revision`, `at`) SELECT `seq`, `capture_id`, `hour`, `event`, `revision`, `at` FROM `scope_events`;
-- Drop "scope_events" table after copying rows
DROP TABLE `scope_events`;
-- Rename temporary table "new_scope_events" to "scope_events"
ALTER TABLE `new_scope_events` RENAME TO `scope_events`;
-- Enable back the enforcement of foreign-keys constraints
PRAGMA foreign_keys = on;
