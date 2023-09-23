package cql

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func mkColumnDef(name string, typ string) ColumnDefinition {
	return ColumnDefinition{
		Name: name,
		Type: DataType{Name: typ},
	}
}

func TestParser(t *testing.T) {
	testCases := []struct {
		input  string
		exp    Schema
		modify func(schema *Schema)
	}{
		{
			input: `CREATE TABLE events(
				user_id uuid,
				event_data blob,
				PRIMARY KEY (user_id)
			);`,
			exp: Schema{
				TableName: "events",
				Columns: ColumnDefinitions{
					mkColumnDef("user_id", "uuid"),
					mkColumnDef("event_data", "blob"),
				},
				PrimaryKey: PrimaryKey{
					PartitionKey: PartitionKey{
						Columns: ColumnDefinitions{
							mkColumnDef("user_id", "uuid"),
						},
					},
				},
			},
		},
		{
			input: `CREATE TABLE events(
				user_id uuid,
				partition int,
				event_category tinyint,
				event_id timeuuid,
				event_data blob,
				PRIMARY KEY ((user_id, partition), event_category, event_id)
			);`,
			exp: Schema{
				TableName: "events",
				Columns: ColumnDefinitions{
					mkColumnDef("user_id", "uuid"),
					mkColumnDef("partition", "int"),
					mkColumnDef("event_category", "tinyint"),
					mkColumnDef("event_id", "timeuuid"),
					mkColumnDef("event_data", "blob"),
				},
				PrimaryKey: PrimaryKey{
					PartitionKey: PartitionKey{
						Columns: ColumnDefinitions{
							mkColumnDef("user_id", "uuid"),
							mkColumnDef("partition", "int"),
						},
					},
					ClusteringKey: ClusteringKey{
						Columns: ColumnDefinitions{
							mkColumnDef("event_category", "tinyint"),
							mkColumnDef("event_id", "timeuuid"),
						},
					},
				},
			},
		},
		{
			input: `CREATE TABLE events(
				user_id uuid PRIMARY KEY,
				event_data blob
			);`,
			exp: Schema{
				TableName: "events",
				Columns: ColumnDefinitions{
					mkColumnDef("user_id", "uuid"),
					mkColumnDef("event_data", "blob"),
				},
			},
			modify: func(exp *Schema) {
				exp.PrimaryKey.PartitionKey.Columns = ColumnDefinitions{exp.Columns[0]}
			},
		},
		{
			input: `CREATE TABLE events(
				user_id uuid PRIMARY KEY,
				event_data text STATIC
			);`,
			exp: Schema{
				TableName: "events",
				Columns: ColumnDefinitions{
					mkColumnDef("user_id", "uuid"),
					mkColumnDef("event_data", "text"),
				},
			},
			modify: func(exp *Schema) {
				exp.Columns[1].Static = true
				exp.PrimaryKey.PartitionKey.Columns = ColumnDefinitions{exp.Columns[0]}
			},
		},
		{
			input: `CREATE TABLE events(
				user_id uuid PRIMARY KEY,
				event_data map<bigint, text>
			);`,
			exp: Schema{
				TableName: "events",
				Columns: ColumnDefinitions{
					mkColumnDef("user_id", "uuid"),
					mkColumnDef("event_data", "map<bigint,text>"),
				},
			},
			modify: func(exp *Schema) {
				exp.PrimaryKey.PartitionKey.Columns = ColumnDefinitions{exp.Columns[0]}
			},
		},
		{
			input: `CREATE TABLE IF NOT EXISTS events(
				user_id uuid PRIMARY KEY,
				name text
			);`,
			exp: Schema{
				TableName: "events",
				Columns: ColumnDefinitions{
					mkColumnDef("user_id", "uuid"),
					mkColumnDef("name", "text"),
				},
			},
			modify: func(exp *Schema) {
				exp.PrimaryKey.PartitionKey.Columns = ColumnDefinitions{exp.Columns[0]}
			},
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			if tc.modify != nil {
				tc.modify(&tc.exp)
			}

			schema, err := ParseSchema(tc.input)
			require.NoError(t, err)
			require.Equal(t, tc.exp, schema)
		})
	}
}
