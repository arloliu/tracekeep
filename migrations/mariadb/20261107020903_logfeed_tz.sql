-- Modify "logfeed_captures" table
ALTER TABLE `logfeed_captures` ADD COLUMN `tz` varchar(64) NOT NULL DEFAULT '';
