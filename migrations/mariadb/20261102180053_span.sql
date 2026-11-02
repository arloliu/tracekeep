-- Modify "pack_ranges" table
ALTER TABLE `pack_ranges` ADD COLUMN `span` bigint AS (`seq_to` - `seq_from`) VIRTUAL;
