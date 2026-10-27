-- Modify "evicted_ranges" table
ALTER TABLE `evicted_ranges` DROP PRIMARY KEY, ADD PRIMARY KEY (`capture_id`, `hour`, `seq_from`);
-- Modify "merges" table
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
) CHARSET utf8mb4 COLLATE utf8mb4_nopad_bin;
-- Create "unattributed_objects" table
CREATE TABLE `unattributed_objects` (
  `object_key` varchar(512) NOT NULL,
  `size` bigint NOT NULL,
  `seen_at` bigint NOT NULL,
  PRIMARY KEY (`object_key`)
) CHARSET utf8mb4 COLLATE utf8mb4_nopad_bin;
