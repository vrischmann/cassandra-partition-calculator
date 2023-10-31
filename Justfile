tool_templ := "github.com/a-h/templ/cmd/templ@latest"

gen:
	@printf "\x1b[34m===>\x1b[m  Running templ generate\n"
	go run {{tool_templ}} generate

build: gen
	@printf "\x1b[34m===>\x1b[m  Running go build\n"
	go build

build-release: gen
	@printf "\x1b[34m===>\x1b[m  Running go build\n"
	GOOS=linux GOARCH=amd64 go build -tags=release

watch-gen:
	watchexec --print-events -f "*.templ" just gen

watch-build:
	watchexec --print-events --debounce 1s -e go go build

watch-run:
	watchexec --no-project-ignore -f cassandra-partition-calculator -n -r ./cassandra-partition-calculator serve
