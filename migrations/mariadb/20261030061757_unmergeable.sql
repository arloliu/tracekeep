-- Atlas writes MySQL's DROP CHECK, which MariaDB refuses; DROP CONSTRAINT is MariaDB's spelling (hand-edited, rehashed).
-- Modify "scopes" table
ALTER TABLE `scopes` DROP CONSTRAINT `scopes_state`, ADD CONSTRAINT `scopes_state` CHECK (`state` in ('open','settled','reopened','unmergeable'));
-- Modify "scope_events" table
ALTER TABLE `scope_events` DROP CONSTRAINT `scope_events_event`, ADD CONSTRAINT `scope_events_event` CHECK (`event` in ('created','settled','reopened','conflict','terminated','excluded','unmergeable'));
