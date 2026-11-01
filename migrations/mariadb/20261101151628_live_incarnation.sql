-- Two live incarnations of a role fail this statement whole, its duplicate-entry error naming the role;
-- adopting the role's stream retires them, and the migration resumes from it (hand-written, hashed).
-- Modify "bus_streams" table
ALTER TABLE `bus_streams` ADD COLUMN `live_role` varchar(16) AS (CASE WHEN `retired_at` IS NULL THEN `role` END) VIRTUAL, ADD UNIQUE INDEX `bus_streams_live_role` (`live_role`);
