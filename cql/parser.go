package cql

import (
	"errors"
	"fmt"
	"strings"
)

var (
	errInvalidType = errors.New("invalid type")
)

type invalidTokenError struct {
	expected string
	got      string
}

func (e *invalidTokenError) Error() string {
	return fmt.Sprintf("expected %q, got %q", e.expected, e.got)
}

func equalsIgnoreCase[A ~string, B ~string](a A, b B) bool {
	return strings.EqualFold(string(a), string(b))
}

type parser struct {
	lexer *lexer
}

func parseStrings(p *parser, expectedTokens ...string) error {
	for _, expectedToken := range expectedTokens {
		token, err := p.lexer.Next()
		if err != nil {
			return err
		}
		if !equalsIgnoreCase(token, expectedToken) {
			return &invalidTokenError{
				expected: expectedToken,
				got:      token.String(),
			}
		}
	}

	return nil
}

func parseOptionalStrings(p *parser, expectedTokens ...string) bool {
	p.lexer.ResetUndo()

	for _, expectedToken := range expectedTokens {
		token, err := p.lexer.Next()
		if err != nil {
			return false
		}

		if !equalsIgnoreCase(token, expectedToken) {
			p.lexer.Undo()
			return false
		}
	}

	return true
}

func parseNextStringInto[T ~string](p *parser, dest *T) error {
	tmp, err := p.lexer.Next()
	if err != nil {
		return err
	}

	*dest = T(tmp)

	return nil
}

func (p *parser) parseCreateTable() (tableName string, err error) {
	// 1. Parse the CREATE TABLE
	if err = parseStrings(p, "CREATE", "TABLE"); err != nil {
		return "", err
	}

	// 2. Eat the IF NOT EXISTS if present
	parseOptionalStrings(p, "IF", "NOT", "EXISTS")

	// 3. Get the table name
	if err = parseNextStringInto(p, &tableName); err != nil {
		return "", err
	}

	return tableName, nil
}

func (p *parser) parseColumnDefinitions() (columns ColumnDefinitions, primaryKey PrimaryKey, err error) {
	// Eat the (
	if err = parseStrings(p, "("); err != nil {
		return
	}

loop:
	for {
		// In this loop we can process two things: a column definition and a primary key definition
		// There are 1 or more column definitions. The primary key definition is optional.

		switch {
		case parseOptionalStrings(p, "PRIMARY", "KEY"):
			if primaryKey, err = p.parsePrimaryKey(columns); err != nil {
				return
			}

		default:
			var (
				columnDefinition ColumnDefinition
				isPrimaryKey     bool
			)
			if columnDefinition, isPrimaryKey, err = p.parseColumnDefinition(); err != nil {
				break loop
			}

			// Check if the column is a primary key

			if isPrimaryKey {
				// It's an error if we already have a primary key
				if len(primaryKey.Columns()) > 0 {
					err = fmt.Errorf("multiple primary keys defined")
					return
				}

				primaryKey = PrimaryKey{
					PartitionKey: PartitionKey{
						Columns: ColumnDefinitions{columnDefinition},
					},
				}
			}

			columns = append(columns, columnDefinition)
		}

		// Eat the possible ,
		// Not having a comma here indicates the end of the column definitions block
		if !parseOptionalStrings(p, ",") {
			break loop
		}
	}

	// Eat the )
	if err = parseStrings(p, ")"); err != nil {
		return
	}

	return
}

func (p *parser) parseColumnDefinition() (res ColumnDefinition, primaryKey bool, err error) {
	// Parse the name
	if err = parseNextStringInto(p, &res.Name); err != nil {
		return
	}

	// Parse the type
	if res.Type, err = p.parseTypeDefinition(); err != nil {
		return
	}

	// Parse the optional PRIMARY KEY
	primaryKey = parseOptionalStrings(p, "PRIMARY", "KEY")

	// Parse the optional STATIC
	res.Static = parseOptionalStrings(p, "STATIC")

	return
}

func (p *parser) parseTypeDefinition() (res DataType, err error) {
	// 1. Get the type name
	if err = parseNextStringInto(p, &res.Name); err != nil {
		return
	}

	// 2. Return early if it's not a collection
	if res.Name != "set" && res.Name != "map" && res.Name != "list" {
		return
	}

	// It is a collection, continue parsing.
	// We parse differently if it's a set/list or a map

	var (
		token token
		level int
	)
loop:
	for {
		if token, err = p.lexer.Next(); err != nil {
			break loop
		}

		res.Name += token.String()

		// The level allows us to know when we're done parsing the full collection type
		switch token {
		case "<":
			level++

		case ">":
			level--

			if level == 0 {
				break loop
			}
		}
	}

	return
}

func (p *parser) parsePrimaryKey(columns ColumnDefinitions) (primaryKey PrimaryKey, err error) {
	// We got a primary key definition, continue

	// Eat the (
	if err = parseStrings(p, "("); err != nil {
		return
	}

	parseColumnsRun := func(columnDefinitions ColumnDefinitions) (res ColumnDefinitions, err error) {
		res = columnDefinitions
		var token token

		for {
			if token, err = p.lexer.Next(); err != nil {
				return
			}

			switch token {
			case ",":
				continue
			case ")":
				return
			default:
				columnName := token.String()
				columnDefinition, ok := columns.FindByName(columnName)
				if !ok {
					return res, fmt.Errorf("invalid column %q in primary key", columnName)
				}

				res = append(res, columnDefinition)
			}
		}
	}

	//
	// Parse the partition key
	//

	// Check the first token to see if we have a compound partition key
	var token token
	if token, err = p.lexer.Next(); err != nil {
		return
	}

	switch token {
	case "(":
		// We have a compound partition key, parse columns until a )
		if primaryKey.PartitionKey.Columns, err = parseColumnsRun(primaryKey.PartitionKey.Columns); err != nil {
			return
		}

		// The rest is the clustering key
		if primaryKey.ClusteringKey.Columns, err = parseColumnsRun(primaryKey.ClusteringKey.Columns); err != nil {
			return
		}

	default:
		// Not a compound partition key, first column is the partition key
		columnName := token.String()
		columnDefinition, ok := columns.FindByName(columnName)
		if !ok {
			return primaryKey, fmt.Errorf("invalid column %q in primary key", columnName)
		}

		primaryKey.PartitionKey.Columns = append(primaryKey.ClusteringKey.Columns, columnDefinition)

		// The rest is the clustering key
		if primaryKey.ClusteringKey.Columns, err = parseColumnsRun(primaryKey.ClusteringKey.Columns); err != nil {
			return
		}
	}

	return
}

func (p *parser) parse() (schema Schema, err error) {
	if schema.TableName, err = p.parseCreateTable(); err != nil {
		return
	}

	if schema.Columns, schema.PrimaryKey, err = p.parseColumnDefinitions(); err != nil {
		return
	}

	return
}

func ParseSchema(schema string) (Schema, error) {
	parser := &parser{
		lexer: newLexer(schema),
	}
	return parser.parse()
}

type DataType struct {
	Name string
}

func (t DataType) IsFixedSize() bool {
	switch t.Name {
	case "uuid", "timeuuid",
		"timestamp", "int", "bigint",
		"float", "double",
		"boolean":
		return true
	default:
		return false
	}
}

func (t DataType) Size() int {
	switch t.Name {
	case "uuid", "timeuuid":
		return 16
	case "timestamp", "bigint", "double":
		return 8
	case "float", "int":
		return 4
	case "boolean":
		return 1
	default:
		panic(fmt.Errorf("invalid call to Size for a non-fixed size type: %q", t.Name))
	}
}

type ColumnDefinition struct {
	Name   string
	Type   DataType
	Static bool

	sizeEstimate int
}

func (c ColumnDefinition) Size() int {
	if c.Type.IsFixedSize() {
		return c.Type.Size()
	}
	return c.sizeEstimate
}

type ColumnDefinitions []ColumnDefinition

func (d ColumnDefinitions) GetStaticColumns() ColumnDefinitions {
	res := make(ColumnDefinitions, 0, len(d))
	for _, cd := range d {
		if cd.Static {
			res = append(res, cd)
		}
	}
	return res
}

func (d ColumnDefinitions) FindByName(name string) (ColumnDefinition, bool) {
	for _, cd := range d {
		if cd.Name == name {
			return cd, true
		}
	}

	return ColumnDefinition{}, false
}

type PartitionKey struct {
	Columns ColumnDefinitions
}
type ClusteringKey struct {
	Columns ColumnDefinitions
}

type PrimaryKey struct {
	PartitionKey  PartitionKey
	ClusteringKey ClusteringKey
}

func (c PrimaryKey) Columns() ColumnDefinitions {
	return append(
		c.PartitionKey.Columns,
		c.ClusteringKey.Columns...,
	)
}

type Schema struct {
	TableName  string
	Columns    ColumnDefinitions
	PrimaryKey PrimaryKey
}

func (s Schema) WithColumnSizeEstimate(name string, sizeEstimate int) Schema {
	update := func(column *ColumnDefinition) {
		if column.Name == name {
			if column.Type.IsFixedSize() {
				panic(fmt.Errorf("can't set a size estimate on a fixed size column"))
			}
			column.sizeEstimate = sizeEstimate
		}
	}

	for i := range s.Columns {
		update(&s.Columns[i])
	}
	for i := range s.PrimaryKey.PartitionKey.Columns {
		update(&s.PrimaryKey.PartitionKey.Columns[i])
	}
	for i := range s.PrimaryKey.ClusteringKey.Columns {
		update(&s.PrimaryKey.ClusteringKey.Columns[i])
	}

	return s
}
