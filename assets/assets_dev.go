//go:build !release

package assets

import (
	"net/http"
	"os"
)

var Mode = "dev"

func init() {
	fs := os.DirFS("assets")
	server := http.FileServer(http.FS(fs))

	FileServer = http.StripPrefix("/assets", server)
}
