// Package web holds the frontend's build for the api role to serve (api.md §10.3):
// web/dist when built with -tags webdist, which make web writes, and otherwise the fallback page saying the frontend was not built.
package web

import "io/fs"

// FS is the build's files, index.html at its root.
func FS() fs.FS {
	sub, err := fs.Sub(files, root)
	if err != nil {
		panic(err) // the embedded root exists, or the package does not compile
	}

	return sub
}

// Built tells whether the frontend's build is embedded, not the fallback.
func Built() bool { return built }
