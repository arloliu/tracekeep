-- The identity reset (catalog.md §7.4): rename tool_id to equipment_id and add the source.
-- tracekeep migrate applies it only to a catalog without captures.
ALTER TABLE `captures` RENAME COLUMN `tool_id` TO `equipment_id`;
ALTER TABLE `captures` ADD COLUMN `source` varchar NULL;
ALTER TABLE `packs` RENAME COLUMN `tool_id` TO `equipment_id`;
DROP INDEX `packs_tool_hour`;
CREATE INDEX `packs_equipment_hour` ON `packs` (`equipment_id`, `hour`);
