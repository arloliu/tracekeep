-- Two live incarnations of a role fail here, named by a bad JSON path's error, before the unique index refuses them unnamed;
-- adopting the role's stream retires them, and the migration resumes from this statement (hand-edited, rehashed).
SELECT json_extract('{}', 'two live incarnations of ' || role || ': ' || group_concat(incarnation_id, ', ' ORDER BY incarnation_id)) FROM bus_streams WHERE retired_at IS NULL GROUP BY role HAVING COUNT(*) > 1;
-- Disable the enforcement of foreign-keys constraints
PRAGMA foreign_keys = off;
-- Create "new_bus_streams" table
CREATE TABLE `new_bus_streams` (
  `incarnation_id` bigint NOT NULL,
  `role` varchar NOT NULL,
  `stream_name` varchar NOT NULL,
  `created_ns` bigint NOT NULL,
  `last_seq_seen` bigint NOT NULL,
  `retired_at` bigint NULL,
  `live_role` varchar NULL AS (CASE WHEN retired_at IS NULL THEN role END) VIRTUAL,
  PRIMARY KEY (`incarnation_id`),
  CONSTRAINT `bus_streams_role` CHECK (role IN ('traffic', 'parking'))
);
-- Copy rows from old table "bus_streams" to new temporary table "new_bus_streams"
INSERT INTO `new_bus_streams` (`incarnation_id`, `role`, `stream_name`, `created_ns`, `last_seq_seen`, `retired_at`) SELECT `incarnation_id`, `role`, `stream_name`, `created_ns`, `last_seq_seen`, `retired_at` FROM `bus_streams`;
-- Drop "bus_streams" table after copying rows
DROP TABLE `bus_streams`;
-- Rename temporary table "new_bus_streams" to "bus_streams"
ALTER TABLE `new_bus_streams` RENAME TO `bus_streams`;
-- Create index "bus_streams_live_role" to table: "bus_streams"
CREATE UNIQUE INDEX `bus_streams_live_role` ON `bus_streams` (`live_role`);
-- Enable back the enforcement of foreign-keys constraints
PRAGMA foreign_keys = on;
