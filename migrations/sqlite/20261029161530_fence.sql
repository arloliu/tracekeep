-- The fence of the release that masks (tracekeep-decisions.md T-43): the membership tables and the compactor lease row
-- move to names an earlier release does not use, so its ingest replicas and compactors fail closed.
-- Disable the enforcement of foreign-keys constraints
PRAGMA foreign_keys = off;
-- Drop "consumers" table
DROP TABLE `consumers`;
-- Drop "consumer_configs" table
DROP TABLE `consumer_configs`;
-- Create "members" table
CREATE TABLE `members` (
  `replica_id` varchar NOT NULL,
  `lease_until` bigint NOT NULL,
  `generation` bigint NOT NULL,
  `registered_at` bigint NOT NULL,
  PRIMARY KEY (`replica_id`)
);
-- Create "member_configs" table
CREATE TABLE `member_configs` (
  `replica_id` varchar NOT NULL,
  `config_id` bigint NOT NULL,
  PRIMARY KEY (`replica_id`, `config_id`)
);
-- Enable back the enforcement of foreign-keys constraints
PRAGMA foreign_keys = on;
-- Move the compactor lease to the row only this release names.
UPDATE `leases` SET `name` = 'compactor/2' WHERE `name` = 'compactor';
