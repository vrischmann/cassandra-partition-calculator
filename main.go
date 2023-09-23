package main

import (
	"net/http"

	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewDevelopment()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
	})

	if err := http.ListenAndServe(":"); err != nil {
		logger.Fatal("unable to listen and serve", zap.Error(err))
	}
}
