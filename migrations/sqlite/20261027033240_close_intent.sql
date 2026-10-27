-- Disable the enforcement of foreign-keys constraints
PRAGMA foreign_keys = off;
-- Create "new_capture_closes" table
CREATE TABLE `new_capture_closes` (
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
  `state` varchar NOT NULL,
  PRIMARY KEY (`capture_id`),
  CONSTRAINT `capture_closes_trigger_kind` CHECK (trigger_kind IN ('inactivity', 'successor', 'operator')),
  CONSTRAINT `capture_closes_state` CHECK (state IN ('pending', 'published', 'abandoned'))
);
-- Copy rows from old table "capture_closes" to new temporary table "new_capture_closes"
INSERT INTO `new_capture_closes` (`capture_id`, `trigger_kind`, `triggered_at`, `highest_seq`, `highest_seq_ts`, `gap_start`, `gap_end`, `pack_id`, `writer_time`, `pack_meta`, `state`) SELECT `capture_id`, `trigger_kind`, `triggered_at`, `highest_seq`, `highest_seq_ts`, `gap_start`, `gap_end`, `pack_id`, `writer_time`, `pack_meta`, `state` FROM `capture_closes`;
-- Drop "capture_closes" table after copying rows
DROP TABLE `capture_closes`;
-- Rename temporary table "new_capture_closes" to "capture_closes"
ALTER TABLE `new_capture_closes` RENAME TO `capture_closes`;
-- Enable back the enforcement of foreign-keys constraints
PRAGMA foreign_keys = on;
