// Package migrationstest gives tests a migrated SQLite catalog without migrating it:
// a copy of a template each test process migrates once and keeps in memory.
// It imports no catalog package, so internal/catalog's own tests can use it as catalogtest does.
package migrationstest
