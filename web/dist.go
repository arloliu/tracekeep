//go:build webdist

package web

import "embed"

// A build with the tag and no web/dist fails here, so a release never ships the fallback.
//
//go:embed all:dist
var files embed.FS

const (
	root  = "dist"
	built = true
)
