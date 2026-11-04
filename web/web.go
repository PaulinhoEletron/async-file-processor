// Package web embeds the browser UI served by the API.
package web

import (
	"embed"
	"io/fs"
)

//go:embed index.html app.js style.css
var assets embed.FS

// FS returns the UI files rooted at the package directory.
func FS() fs.FS { return assets }
