-- Add column "capture_method" to table: "captures"
ALTER TABLE `captures` ADD COLUMN `capture_method` varchar NULL;
-- Add column "vantage" to table: "captures"
ALTER TABLE `captures` ADD COLUMN `vantage` varchar NULL;
-- Add column "source_dialect" to table: "captures"
ALTER TABLE `captures` ADD COLUMN `source_dialect` varchar NULL;
-- Add column "stop_requested_at" to table: "captures"
ALTER TABLE `captures` ADD COLUMN `stop_requested_at` bigint NULL;
-- Create "logfeed_captures" table
CREATE TABLE `logfeed_captures` (
  `source` varchar NOT NULL,
  `equipment_id` varchar NOT NULL,
  `device` varchar NOT NULL,
  `file` varchar NOT NULL,
  `capture_id` blob NOT NULL,
  `lease_group` int NOT NULL,
  PRIMARY KEY (`source`, `equipment_id`, `device`, `file`)
);
-- Create index "logfeed_captures_capture" to table: "logfeed_captures"
CREATE UNIQUE INDEX `logfeed_captures_capture` ON `logfeed_captures` (`capture_id`);
-- Create index "logfeed_captures_group" to table: "logfeed_captures"
CREATE INDEX `logfeed_captures_group` ON `logfeed_captures` (`lease_group`);
-- Create "logfeed_files" table
CREATE TABLE `logfeed_files` (
  `source` varchar NOT NULL,
  `equipment_id` varchar NOT NULL,
  `file` varchar NOT NULL,
  `watermark` bigint NOT NULL,
  `stopped` bool NOT NULL DEFAULT false,
  PRIMARY KEY (`source`, `equipment_id`, `file`)
);
