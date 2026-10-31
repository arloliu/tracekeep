//go:build !webdist

package web

import "embed"

//go:embed fallback
var files embed.FS

const (
	root  = "fallback"
	built = false
)
