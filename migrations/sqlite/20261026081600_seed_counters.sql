-- The counters every catalog transaction expects (catalog.md §7.3): the event seq and the membership sentinel.
INSERT INTO `counters` (`name`) VALUES ('scope_events'), ('membership');
