-- Modify "conflicts" table
ALTER TABLE `conflicts` ADD INDEX `conflicts_capture_other_pack` (`capture_id`, `other_pack_id`), ADD INDEX `conflicts_capture_pack` (`capture_id`, `pack_id`);
-- Modify "merges" table
ALTER TABLE `merges` ADD INDEX `merges_capture_hour` (`capture_id`, `hour`), ADD INDEX `merges_outcome_delete_at` (`outcome`, `delete_at`, `at`);
-- Modify "pack_ranges" table
ALTER TABLE `pack_ranges` ADD INDEX `pack_ranges_capture_span` (`capture_id`, `span`);
