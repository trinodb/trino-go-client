package trino

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"net/http"
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

func TestDirectPollingRows(t *testing.T) {
	validQresp := &queryResponse{
		ID: "query123",
		Columns: []queryColumn{
			{
				Name: "id",
				Type: "bigint",
				TypeSignature: typeSignature{
					RawType: "bigint",
				},
			},
			{
				Name: "name",
				Type: "varchar",
				TypeSignature: typeSignature{
					RawType: "varchar",
				},
			},
		},
		Data: []interface{}{
			[]interface{}{json.Number("1"), "Alice"},
			[]interface{}{json.Number("2"), "Bob"},
		},
	}
	t.Run("parses direct protocol response correctly", func(t *testing.T) {
		rows, err := newDirectPollingRows(validQresp, validQresp.Data.([]interface{}))
		require.NoError(t, err)
		require.NotNil(t, rows)

		// Verify columns
		cols, err := rows.Columns()
		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name"}, cols)

		// Verify data
		require.True(t, rows.Next())
		var id int64
		var name string
		err = rows.Scan(&id, &name)
		require.NoError(t, err)
		assert.Equal(t, int64(1), id)
		assert.Equal(t, "Alice", name)

		require.True(t, rows.Next())
		err = rows.Scan(&id, &name)
		require.NoError(t, err)
		assert.Equal(t, int64(2), id)
		assert.Equal(t, "Bob", name)

		require.False(t, rows.Next())
	})

	t.Run("Scan returns error for mismatched column count", func(t *testing.T) {
		rows, err := newDirectPollingRows(validQresp, validQresp.Data.([]interface{}))
		require.NoError(t, err)
		require.NotNil(t, rows)

		require.True(t, rows.Next())

		var id int64
		err = rows.Scan(&id) // Only one destination, but two columns
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected 2 destination arguments")
	})

	t.Run("returns error for invalid row data", func(t *testing.T) {
		invalidQresp := &queryResponse{
			ID: "query123",
			Columns: []queryColumn{
				{
					Name: "id",
					Type: "bigint",
					TypeSignature: typeSignature{
						RawType: "bigint",
					},
				},
			},
			Data: []interface{}{
				"invalid_row_data", // Should be []interface{}
			},
		}

		rows, err := newDirectPollingRows(invalidQresp, invalidQresp.Data.([]interface{}))
		assert.Error(t, err)
		assert.Nil(t, rows)
		assert.Contains(t, err.Error(), "unexpected data type for row")
	})
}

func TestNewSpoolingPollingRowsFromResponse(t *testing.T) {
	t.Run("parses spooling protocol response structure correctly", func(t *testing.T) {
		qresp := &queryResponse{
			ID: "query123",
			Columns: []queryColumn{
				{
					Name: "id",
					Type: "bigint",
					TypeSignature: typeSignature{
						RawType: "bigint",
					},
				},
				{
					Name: "name",
					Type: "varchar",
					TypeSignature: typeSignature{
						RawType: "varchar",
					},
				},
			},
			Data: map[string]interface{}{
				"encoding": "json",
				"segments": []interface{}{
					map[string]interface{}{
						"type":   "spooled",
						"uri":    "http://example.com/segment/0",
						"ackUri": "http://example.com/ack/0",
						"metadata": map[string]interface{}{
							"rowOffset":   json.Number("0"),
							"rowsCount":   json.Number("2"),
							"segmentSize": json.Number("100"),
						},
					},
				},
			},
		}

		conn := &PollingConn{conn: &Conn{httpClient: http.Client{}}}
		ctx := context.Background()

		rows, err := newSpoolingPollingRows(conn, ctx, qresp, qresp.Data.(map[string]interface{}))
		require.NoError(t, err)
		require.NotNil(t, rows)

		cols, err := rows.Columns()
		require.NoError(t, err)
		assert.Equal(t, []string{"id", "name"}, cols)
	})
}
