-- Modify "captures" table
ALTER TABLE `captures` ADD COLUMN `capture_method` varchar(32) NULL AFTER `source`, ADD COLUMN `vantage` varchar(16) NULL AFTER `capture_method`, ADD COLUMN `source_dialect` varchar(64) NULL AFTER `vantage`, ADD COLUMN `stop_requested_at` bigint NULL;
-- Create "logfeed_captures" table
CREATE TABLE `logfeed_captures` (
  `source` varchar(128) NOT NULL,
  `equipment_id` varchar(255) NOT NULL,
  `device` varchar(255) NOT NULL,
  `file` varchar(128) NOT NULL,
  `capture_id` binary(16) NOT NULL,
  `lease_group` int NOT NULL,
  PRIMARY KEY (`source`, `equipment_id`, `device`, `file`),
  UNIQUE INDEX `logfeed_captures_capture` (`capture_id`),
  INDEX `logfeed_captures_group` (`lease_group`)
) CHARSET utf8mb4 COLLATE utf8mb4_nopad_bin;
-- Create "logfeed_files" table
CREATE TABLE `logfeed_files` (
  `source` varchar(128) NOT NULL,
  `equipment_id` varchar(255) NOT NULL,
  `file` varchar(128) NOT NULL,
  `watermark` bigint NOT NULL,
  `stopped` bool NOT NULL DEFAULT 0,
  PRIMARY KEY (`source`, `equipment_id`, `file`)
) CHARSET utf8mb4 COLLATE utf8mb4_nopad_bin;
