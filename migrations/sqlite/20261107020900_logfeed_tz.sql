-- Add column "tz" to table: "logfeed_captures"
ALTER TABLE `logfeed_captures` ADD COLUMN `tz` varchar NOT NULL DEFAULT '';
