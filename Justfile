gen:
	templ generate .

build: gen
	go build ./...
