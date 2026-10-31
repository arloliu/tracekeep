-- Create "export_jobs" table
CREATE TABLE `export_jobs` (
  `job_id` binary(16) NOT NULL,
  `owner` varchar(512) NOT NULL,
  `request` text NOT NULL,
  `state` varchar(16) NOT NULL,
  `fence` bigint NOT NULL,
  `holder` varchar(128) NULL,
  `lease_until` bigint NOT NULL,
  `attempts` bigint NOT NULL,
  `object_key` varchar(512) NULL,
  `size` bigint NULL,
  `created_at` bigint NOT NULL,
  `ended_at` bigint NULL,
  `delete_at` bigint NULL,
  `reason` text NULL,
  PRIMARY KEY (`job_id`),
  INDEX `export_jobs_claim` (`state`, `lease_until`),
  INDEX `export_jobs_owner` (`owner`, `created_at`),
  CONSTRAINT `export_jobs_state` CHECK (`state` in ('queued','running','done','failed','cancelled'))
) CHARSET utf8mb4 COLLATE utf8mb4_nopad_bin;
-- Create "export_objects" table
CREATE TABLE `export_objects` (
  `object_key` varchar(512) NOT NULL,
  `job_id` binary(16) NOT NULL,
  `fence` bigint NOT NULL,
  `put_deadline` bigint NOT NULL,
  `state` varchar(16) NOT NULL,
  PRIMARY KEY (`object_key`),
  INDEX `export_objects_job` (`job_id`),
  INDEX `export_objects_state` (`state`, `put_deadline`),
  CONSTRAINT `export_objects_state` CHECK (`state` in ('intended','bundle','deleting'))
) CHARSET utf8mb4 COLLATE utf8mb4_nopad_bin;
