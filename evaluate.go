package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/davecgh/go-spew/spew"
	"github.com/peterbourgon/ff/v3/ffcli"

	"rischmann.fr/cassandra-partition-calculator/cql"
)

type evaluateCommandConfig struct {
	root *rootCommandConfig
}

func newEvaluateCommandConfig(root *rootCommandConfig) *ffcli.Command {
	cfg := &evaluateCommandConfig{
		root: root,
	}

	fs := flag.NewFlagSet("evaluate", flag.ContinueOnError)

	return &ffcli.Command{
		Name:       "evaluate",
		ShortUsage: "evaluate [flags] <schema file>",
		ShortHelp:  `evaluate a CQL schema`,
		FlagSet:    fs,
		Exec:       cfg.Exec,
	}
}

func (c *evaluateCommandConfig) Exec(ctx context.Context, args []string) error {
	if len(args) < 1 {
		return flag.ErrHelp
	}

	input, err := os.ReadFile(args[0])
	if err != nil {
		return fmt.Errorf("unable to read input file, err: %w", err)
	}

	schema, err := cql.ParseSchema(string(input))
	if err != nil {
		return fmt.Errorf("unable to parse schema, err: %w", err)
	}

	spew.Dump(schema)

	return nil
}
