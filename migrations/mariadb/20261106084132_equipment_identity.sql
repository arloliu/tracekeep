-- The identity reset (catalog.md §7.4): rename tool_id to equipment_id and add the source.
-- tracekeep migrate applies it only to a catalog without captures.
ALTER TABLE `captures` RENAME COLUMN `tool_id` TO `equipment_id`, ADD COLUMN `source` varchar(255) NULL;
ALTER TABLE `packs` RENAME COLUMN `tool_id` TO `equipment_id`, RENAME INDEX `packs_tool_hour` TO `packs_equipment_hour`;
