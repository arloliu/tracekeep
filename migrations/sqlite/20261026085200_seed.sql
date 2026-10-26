-- The rows every catalog transaction expects (catalog.md §7.2, §7.3): the event seq, the membership sentinel, and the compactor's lease, free.
INSERT INTO `counters` (`name`) VALUES ('scope_events'), ('membership');
INSERT INTO `leases` (`name`, `holder`, `fence_token`, `until_ns`) VALUES ('compactor', '', 0, 0);
