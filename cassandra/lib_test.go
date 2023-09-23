package cassandra

import (
	"fmt"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/require"

	"rischmann.fr/cassandra-partition-calculator/cql"
)

func TestEstimate(t *testing.T) {
	const cqlSchema = `CREATE TABLE events(
				user_id uuid,
				partition int,
				event_category tinyint,
				event_id timeuuid,
				event_data blob,
				PRIMARY KEY ((user_id, partition), event_category, event_id)
			);`

	schema, err := cql.ParseSchema(cqlSchema)
	require.NoError(t, err)

	schema = schema.
		WithColumnSizeEstimate("event_data", 100)

	result, err := Estimate(schema, 5_000_000)
	require.NoError(t, err)

	spew.Dump(result)
	fmt.Printf("values: %d, bytes: %s\n", result.Values, humanize.Bytes(uint64(result.Bytes)))
}
