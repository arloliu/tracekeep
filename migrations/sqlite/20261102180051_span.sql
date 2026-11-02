-- Add column "span" to table: "pack_ranges"
ALTER TABLE `pack_ranges` ADD COLUMN `span` bigint NULL AS (seq_to - seq_from) VIRTUAL;
