-- Modify "capture_closes" table
ALTER TABLE `capture_closes` DROP CONSTRAINT `capture_closes_trigger`, ADD CONSTRAINT `capture_closes_trigger_kind` CHECK (`trigger_kind` in ('inactivity','successor','operator')), DROP COLUMN `codec`;
