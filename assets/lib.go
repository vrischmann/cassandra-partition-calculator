package assets

import "net/http"

// FileServer serves assets either from:
// * the local file system in non-release builds
// * a embedded file system in release builds
var FileServer http.Handler
