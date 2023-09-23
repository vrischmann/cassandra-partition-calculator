//go:build release

package assets

import (
	"embed"
	"net/http"
)

//go:embed *.css *.js
var fs embed.FS

func init() {
	server := http.FileServer(http.FS(fs))
	FileServer = http.StripPrefix("/assets", server)
}
