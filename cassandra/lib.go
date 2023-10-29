package cassandra

import (
	"errors"

	"rischmann.fr/cassandra-partition-calculator/cql"
)

type Estimation struct {
	Values int
	Bytes  int
}

func sumColumnsSize(columns cql.ColumnDefinitions) int64 {
	var result int64

	for _, column := range columns {
		result += int64(column.Size())
	}

	return result
}

var (
	ErrMissingEstimatedColumn = errors.New("missing estimated column")
)

func Estimate(schema cql.Schema, rows int64) (res Estimation, err error) {
	// Formula as defined in the article above:
	//   Nv = Nr * (Nc - Npk - Ns) + Ns
	// where
	//   Nr  = number of rows
	//   Nc  = total number of columns
	//   Npk = number of columns in the primary key
	//   Ns  = number of static columns

	nbColumns := int64(len(schema.Columns))
	nbPrimaryKeyColumns := int64(len(schema.PrimaryKey.Columns()))
	nbStaticColumns := int64(len(schema.Columns.GetStaticColumns()))

	values := rows*(nbColumns-nbPrimaryKeyColumns-nbStaticColumns) + nbStaticColumns

	partitionKeySize := sumColumnsSize(schema.PrimaryKey.PartitionKey.Columns)
	clusteringKeySize := sumColumnsSize(schema.PrimaryKey.ClusteringKey.Columns)

	columnsNotInPrimaryKey := schema.Columns.NotIn(schema.PrimaryKey.Columns())
	columnsSize := sumColumnsSize(columnsNotInPrimaryKey)

	rowsSize := (clusteringKeySize + columnsSize) * rows
	metadataSize := (values * 8) + (rows * 8)
	totalSize := partitionKeySize + clusteringKeySize + metadataSize + rowsSize

	res.Values = int(values)
	res.Bytes = int(totalSize)

	return res, nil
}
