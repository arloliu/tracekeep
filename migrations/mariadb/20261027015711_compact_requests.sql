-- Atlas writes MySQL's DROP CHECK, which MariaDB refuses; DROP CONSTRAINT is MariaDB's spelling (hand-edited, rehashed).
-- Modify "conflicts" table
ALTER TABLE `conflicts` DROP CONSTRAINT `conflicts_state`, ADD CONSTRAINT `conflicts_state` CHECK (`state` in ('suspect','confirmed','acknowledged'));
-- Modify "scope_events" table
ALTER TABLE `scope_events` DROP CONSTRAINT `scope_events_event`, ADD CONSTRAINT `scope_events_event` CHECK (`event` in ('created','settled','reopened','conflict','terminated','excluded'));
-- Create "compact_requests" table
CREATE TABLE `compact_requests` (
  `request_id` binary(16) NOT NULL,
  `kind` varchar(16) NOT NULL,
  `capture_id` binary(16) NOT NULL,
  `hour` bigint NULL,
  `requested_by` varchar(255) NOT NULL,
  `requested_at` bigint NOT NULL,
  `state` varchar(16) NOT NULL,
  `outcome` text NULL,
  `done_at` bigint NULL,
  PRIMARY KEY (`request_id`),
  INDEX `compact_requests_state` (`state`, `requested_at`),
  CONSTRAINT `compact_requests_kind` CHECK (`kind` in ('merge','close')),
  CONSTRAINT `compact_requests_state` CHECK (`state` in ('pending','done','failed'))
) CHARSET utf8mb4 COLLATE utf8mb4_nopad_bin;
