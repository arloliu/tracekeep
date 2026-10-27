-- Disable the enforcement of foreign-keys constraints
PRAGMA foreign_keys = off;
-- Create "new_evicted_ranges" table
CREATE TABLE `new_evicted_ranges` (
  `capture_id` blob NOT NULL,
  `hour` bigint NOT NULL,
  `seq_from` bigint NOT NULL,
  `seq_to` bigint NOT NULL,
  `ts_from` bigint NOT NULL,
  `ts_to` bigint NOT NULL,
  PRIMARY KEY (`capture_id`, `hour`, `seq_from`)
);
-- Copy rows from old table "evicted_ranges" to new temporary table "new_evicted_ranges"
INSERT INTO `new_evicted_ranges` (`capture_id`, `hour`, `seq_from`, `seq_to`, `ts_from`, `ts_to`) SELECT `capture_id`, `hour`, `seq_from`, `seq_to`, `ts_from`, `ts_to` FROM `evicted_ranges`;
-- Drop "evicted_ranges" table after copying rows
DROP TABLE `evicted_ranges`;
-- Rename temporary table "new_evicted_ranges" to "evicted_ranges"
ALTER TABLE `new_evicted_ranges` RENAME TO `evicted_ranges`;
-- Add column "delete_at" to table: "merges"
ALTER TABLE `merges` ADD COLUMN `delete_at` bigint NOT NULL DEFAULT 0;
-- A merge committed before the column existed deletes its inputs the default D, 1 hour, after it (hand-edited, rehashed).
UPDATE `merges` SET `delete_at` = `at` + 3600000000000 WHERE `delete_at` = 0;
-- Create "retention_periods" table
CREATE TABLE `retention_periods` (
  `recorded_at` bigint NOT NULL,
  `period_ns` bigint NOT NULL,
  `held_ns` bigint NOT NULL,
  `held_until` bigint NOT NULL,
  PRIMARY KEY (`recorded_at`)
);
-- Create "unattributed_objects" table
CREATE TABLE `unattributed_objects` (
  `object_key` varchar NOT NULL,
  `size` bigint NOT NULL,
  `seen_at` bigint NOT NULL,
  PRIMARY KEY (`object_key`)
);
-- Enable back the enforcement of foreign-keys constraints
PRAGMA foreign_keys = on;
