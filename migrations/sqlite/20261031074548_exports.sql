-- Create "export_jobs" table
CREATE TABLE `export_jobs` (
  `job_id` blob NOT NULL,
  `owner` varchar NOT NULL,
  `request` text NOT NULL,
  `state` varchar NOT NULL,
  `fence` bigint NOT NULL,
  `holder` varchar NULL,
  `lease_until` bigint NOT NULL,
  `attempts` bigint NOT NULL,
  `object_key` varchar NULL,
  `size` bigint NULL,
  `created_at` bigint NOT NULL,
  `ended_at` bigint NULL,
  `delete_at` bigint NULL,
  `reason` text NULL,
  PRIMARY KEY (`job_id`),
  CONSTRAINT `export_jobs_state` CHECK (state IN ('queued', 'running', 'done', 'failed', 'cancelled'))
);
-- Create index "export_jobs_claim" to table: "export_jobs"
CREATE INDEX `export_jobs_claim` ON `export_jobs` (`state`, `lease_until`);
-- Create index "export_jobs_owner" to table: "export_jobs"
CREATE INDEX `export_jobs_owner` ON `export_jobs` (`owner`, `created_at`);
-- Create "export_objects" table
CREATE TABLE `export_objects` (
  `object_key` varchar NOT NULL,
  `job_id` blob NOT NULL,
  `fence` bigint NOT NULL,
  `put_deadline` bigint NOT NULL,
  `state` varchar NOT NULL,
  PRIMARY KEY (`object_key`),
  CONSTRAINT `export_objects_state` CHECK (state IN ('intended', 'bundle', 'deleting'))
);
-- Create index "export_objects_job" to table: "export_objects"
CREATE INDEX `export_objects_job` ON `export_objects` (`job_id`);
-- Create index "export_objects_state" to table: "export_objects"
CREATE INDEX `export_objects_state` ON `export_objects` (`state`, `put_deadline`);
