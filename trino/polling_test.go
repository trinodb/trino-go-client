package trino

import (
	"database/sql"
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertQueryArgsToDriverArgs(t *testing.T) {
	t.Run("converts named args and positional args", func(t *testing.T) {
		result := convertQueryArgsToDriverArgs(
			sql.Named("X-Trino-User", "my-user"),
			sql.Named("X-Trino-Source", "my-source"),
			1,
			"a",
		)

		expected := []driver.NamedValue{
			{Name: "X-Trino-User", Ordinal: 1, Value: "my-user"},
			{Name: "X-Trino-Source", Ordinal: 2, Value: "my-source"},
			{Name: "", Ordinal: 3, Value: 1},
			{Name: "", Ordinal: 4, Value: "a"},
		}

		assert.Equal(t, expected, result)
	})

	t.Run("handles driver.NamedValue directly", func(t *testing.T) {
		result := convertQueryArgsToDriverArgs(
			driver.NamedValue{Name: "test", Value: "value"},
			&driver.NamedValue{Name: "test2", Value: "value2"},
		)

		assert.Equal(t, "test", result[0].Name)
		assert.Equal(t, "value", result[0].Value)
		assert.Equal(t, 1, result[0].Ordinal)

		assert.Equal(t, "test2", result[1].Name)
		assert.Equal(t, "value2", result[1].Value)
		assert.Equal(t, 2, result[1].Ordinal)
	})

	t.Run("handles nil pointer", func(t *testing.T) {
		var nilNamedArg *sql.NamedArg
		result := convertQueryArgsToDriverArgs(nilNamedArg)

		assert.Equal(t, 1, result[0].Ordinal)
		assert.Nil(t, result[0].Value)
	})
}

func TestPollingRows(t *testing.T) {
	t.Run("Next and Scan iterate through rows", func(t *testing.T) {
		pr := newPollingRows(
			[]string{"id", "name"},
			[]*PollingColumnType{
				{name: "id", databaseType: "INTEGER"},
				{name: "name", databaseType: "VARCHAR"},
			},
			[][]interface{}{
				{int64(1), "Alice"},
				{int64(2), "Bob"},
				{int64(3), "Charlie"},
			},
		)

		// Read first row
		require.True(t, pr.Next())
		var id int64
		var name string
		err := pr.Scan(&id, &name)
		require.NoError(t, err)
		assert.Equal(t, int64(1), id)
		assert.Equal(t, "Alice", name)

		// Read second row
		require.True(t, pr.Next())
		err = pr.Scan(&id, &name)
		require.NoError(t, err)
		assert.Equal(t, int64(2), id)
		assert.Equal(t, "Bob", name)

		// Read third row
		require.True(t, pr.Next())
		err = pr.Scan(&id, &name)
		require.NoError(t, err)
		assert.Equal(t, int64(3), id)
		assert.Equal(t, "Charlie", name)

		// No more rows
		require.False(t, pr.Next())
	})

	t.Run("Scan returns error for mismatched column count", func(t *testing.T) {
		pr := newPollingRows(
			[]string{"id", "name"},
			[]*PollingColumnType{
				{name: "id", databaseType: "INTEGER"},
				{name: "name", databaseType: "VARCHAR"},
			},
			[][]interface{}{
				{int64(1), "Alice"},
			},
		)

		require.True(t, pr.Next())

		var id int64
		err := pr.Scan(&id) // Only one destination, but two columns
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected 2 destination arguments")
	})

	t.Run("Columns returns column names", func(t *testing.T) {
		pr := newPollingRows(
			[]string{"id", "name", "age"},
			[]*PollingColumnType{},
			[][]interface{}{},
		)

		columns, err := pr.Columns()
		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name", "age"}, columns)
	})
}
