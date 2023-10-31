package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/a-h/templ"
	"github.com/dustin/go-humanize"
	"github.com/peterbourgon/ff/v3/ffcli"
	"github.com/vrischmann/hutil/v3"
	"go.uber.org/zap"
	"golang.org/x/exp/constraints"
	"golang.org/x/text/language"
	"golang.org/x/text/message"

	"rischmann.fr/cassandra-partition-calculator/assets"
	"rischmann.fr/cassandra-partition-calculator/cassandra"
	"rischmann.fr/cassandra-partition-calculator/cql"
	"rischmann.fr/cassandra-partition-calculator/ui"
	"rischmann.fr/cassandra-partition-calculator/ui/fragments"
)

type rootCommandConfig struct {
	logger *zap.Logger
}

func newRootCommand() (*rootCommandConfig, *ffcli.Command) {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal(err)
	}

	cfg := &rootCommandConfig{
		logger: logger,
	}

	return cfg, &ffcli.Command{
		Name:       "cassandra-partition-calculator",
		ShortUsage: "cassandra-partition-calculator [flags] <subcommand>",
		Exec: func(_ context.Context, _ []string) error {
			return flag.ErrHelp
		},
	}
}

type serveCommandConfig struct {
	root *rootCommandConfig

	listenAddr string
	baseURL    string
}

func newServeCommandConfig(root *rootCommandConfig) *ffcli.Command {
	cfg := &serveCommandConfig{
		root:       root,
		listenAddr: ":8909",
		baseURL:    "",
	}

	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.Func("listen-addr", "Listen address", func(data string) error {
		_, _, err := net.SplitHostPort(data)
		if err != nil {
			return err
		}
		cfg.listenAddr = data
		return nil
	})
	fs.StringVar(&cfg.baseURL, "base-url", "", "The base URL of the application")

	return &ffcli.Command{
		Name:       "serve",
		ShortUsage: "serve [flags]",
		ShortHelp:  `serve the UI and API`,
		FlagSet:    fs,
		Exec:       cfg.Exec,
	}
}

func (c *serveCommandConfig) Exec(ctx context.Context, args []string) error {
	var middlewares hutil.MiddlewareStack
	middlewares.Use(hutil.NewLoggingMiddleware(c.root.logger))

	mux := http.NewServeMux()
	mux.Handle("/assets/", assets.FileServer)
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		schemaCQL := `CREATE TABLE events(
	tenant_key bigint,
	user_id uuid,
	event_category text,
	event_id timeuuid,
	event_data blob,
	PRIMARY KEY ((tenant_key, user_id, event_category), event_id)
);`

		// TODO(vincent): stop hardcoding this
		schema := ui.SchemaComponent(c.baseURL, schemaCQL)

		page := ui.MainPage(c.baseURL, "Cassandra Partition Calculator", schema)
		page.Render(req.Context(), w)
	})
	mux.HandleFunc("/evaluate", c.evaluateHandler)

	c.root.logger.Info("serving UI and API",
		zap.String("listen_addr", c.listenAddr),
		zap.String("base_url", c.baseURL),
		zap.String("assets_mode", assets.Mode),
	)

	return http.ListenAndServe(c.listenAddr, middlewares.Handler(mux))
}

var errFieldEmpty = errors.New("field empty")

type validationError struct {
	field string
	err   error
}

func (e *validationError) Error() string {
	return fmt.Sprintf("field %q is invalid because of error: %s", e.field, e.err)
}
func (e *validationError) Unwrap() error {
	return e.err
}

type evaluationSchema struct {
	rows   int64
	schema cql.Schema
}

func (c *serveCommandConfig) parseEvaluateRequest(req *http.Request) (res evaluationSchema, err error) {
	// Parse the form data
	//
	// From this we get:
	// * the number of rows
	// * the schema
	// * maybe some size estimates for the columns

	if err = req.ParseForm(); err != nil {
		return res, fmt.Errorf("unable to parse form, err: %w", err)
	}
	form := req.Form

	rowsStr := form.Get("rows")
	if rowsStr == "" {
		return res, &validationError{
			field: "rows",
			err:   errFieldEmpty,
		}
	}

	res.rows, err = strconv.ParseInt(rowsStr, 10, 64)
	if err != nil {
		return res, &validationError{
			field: "rows",
			err:   err,
		}
	}

	schemaStr := form.Get("schema")
	if schemaStr == "" {
		return res, &validationError{
			field: "schema",
			err:   errFieldEmpty,
		}
	}

	res.schema, err = cql.ParseSchema(schemaStr)
	if err != nil {
		return res, &validationError{
			field: "schema",
			err:   err,
		}
	}

	// Parse the size estimates provided in the form
	//
	// This is not available in the first submission of the form because it depends
	// on the schema provided being parsed.

	for name, value := range form {
		const prefix = "size::"
		if strings.HasPrefix(name, prefix) {
			columnName := name[len(prefix):]

			sizeEstimate, err := strconv.Atoi(value[0])
			if err != nil {
				return res, &validationError{
					field: name,
					err:   err,
				}
			}

			res.schema = res.schema.
				WithColumnSizeEstimate(columnName, sizeEstimate)
		}
	}

	return
}

func formatInt[T constraints.Integer](language language.Tag, n T) string {
	printer := message.NewPrinter(language)
	return printer.Sprintf("%d", n)
}

func formatFloat[T constraints.Float](language language.Tag, n T) string {
	printer := message.NewPrinter(language)
	return printer.Sprintf("%f", n)
}

func formatIF[T constraints.Float | constraints.Integer](language language.Tag, n T) string {
	printer := message.NewPrinter(language)
	return printer.Sprintf("%v", n)
}

func (c *serveCommandConfig) evaluateHandler(w http.ResponseWriter, req *http.Request) {
	// Parse the request

	languageTag := message.MatchLanguage(req.Header.Get("Accept-Language"), "en")

	// Parse the form data

	res, err := c.parseEvaluateRequest(req)
	if err != nil {
		c.root.logger.Error("unable to parse evaluate request", zap.Error(err))

		if isHTMXRequest(req) {
			component := fragments.Results(fragments.ResultsData{
				ErrorMessages: []string{err.Error()},
			})
			component.Render(req.Context(), w)

		} else {
			// TODO(vincent): flash message
			http.Redirect(w, req, "/", http.StatusTemporaryRedirect)
		}

		return
	}

	// Get an estimation

	estimation, err := cassandra.Estimate(res.schema, res.rows)
	if err != nil {
		c.root.logger.Error("unable to estimate", zap.Error(err))

		if isHTMXRequest(req) {
			component := fragments.Results(fragments.ResultsData{
				ErrorMessages: []string{err.Error()},
			})
			component.Render(req.Context(), w)
		} else {
			// TODO(vincent): flash message
			http.Redirect(w, req, "/", http.StatusTemporaryRedirect)
		}

		return
	}

	// Render the results

	if isHTMXRequest(req) {
		estimation := fragments.Estimation{
			Values: formatIF(languageTag, estimation.Values),
			Bytes:  fmt.Sprintf("%s bytes (%s)", formatIF(languageTag, estimation.Bytes), humanize.IBytes(uint64(estimation.Bytes))),
		}

		component := fragments.Results(fragments.ResultsData{
			Estimation: estimation,
			Schema:     res.schema,
		})
		component.Render(req.Context(), w)

		return
	} else {
		// TODO(vincent): redirect + session storage ?
		_ = req
	}
}

func isHTMXRequest(req *http.Request) bool {
	value := req.Header.Get("HX-Request")
	return value == "true"
}

type concatComponents []templ.Component

func (l concatComponents) Render(ctx context.Context, w io.Writer) error {
	for _, component := range l {
		err := component.Render(ctx, w)
		if err != nil {
			return err
		}
	}
	return nil
}

var _ templ.Component = (concatComponents)(nil)

func main() {
	var (
		rootCfg, rootCmd = newRootCommand()
		serveCmd         = newServeCommandConfig(rootCfg)
		evaluateCmd      = newEvaluateCommandConfig(rootCfg)
	)

	rootCmd.Subcommands = []*ffcli.Command{
		serveCmd,
		evaluateCmd,
	}

	//

	err := rootCmd.ParseAndRun(context.Background(), os.Args[1:])
	switch {
	case errors.Is(err, flag.ErrHelp):
		os.Exit(1)
	case err != nil:
		fmt.Fprintf(os.Stderr, "%s", err)
		os.Exit(1)
	}
}
