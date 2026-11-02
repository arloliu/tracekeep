-- Create index "pack_ranges_capture_span" to table: "pack_ranges"
CREATE INDEX `pack_ranges_capture_span` ON `pack_ranges` (`capture_id`, `span`);
-- Create index "merges_outcome_delete_at" to table: "merges"
CREATE INDEX `merges_outcome_delete_at` ON `merges` (`outcome`, `delete_at`, `at`);
-- Create index "merges_capture_hour" to table: "merges"
CREATE INDEX `merges_capture_hour` ON `merges` (`capture_id`, `hour`);
-- Create index "conflicts_capture_pack" to table: "conflicts"
CREATE INDEX `conflicts_capture_pack` ON `conflicts` (`capture_id`, `pack_id`);
-- Create index "conflicts_capture_other_pack" to table: "conflicts"
CREATE INDEX `conflicts_capture_other_pack` ON `conflicts` (`capture_id`, `other_pack_id`);
