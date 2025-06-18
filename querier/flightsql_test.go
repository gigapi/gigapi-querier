package querier

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/assert"
)

func TestConvertResultsToArrow(t *testing.T) {
	tests := []struct {
		name     string
		results  []map[string]interface{}
		wantType arrow.DataType
		check    func(t *testing.T, record arrow.Record)
	}{
		{
			name: "Nanosecond timestamp from time column",
			results: []map[string]interface{}{
				{
					"time": int64(1704067200000000000), // 2024-01-01T00:00:00Z in nanoseconds
				},
			},
			wantType: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"},
			check: func(t *testing.T, record arrow.Record) {
				assert.Equal(t, int64(1), record.NumRows())
				assert.Equal(t, int64(1), record.NumCols())

				col := record.Column(0)
				assert.IsType(t, &array.Timestamp{}, col)

				ts := col.(*array.Timestamp)
				assert.False(t, ts.IsNull(0))
				assert.Equal(t, arrow.Timestamp(1704067200000000000), ts.Value(0))
			},
		},
		{
			name: "Multiple timestamp formats",
			results: []map[string]interface{}{
				{
					"time":     int64(1704067200000000000), // 2024-01-01T00:00:00Z in nanoseconds
					"time_str": int64(1704067200000000000), // Using int64 since we want timestamps
					"time_int": int64(1704067200000000000),
				},
			},
			wantType: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"},
			check: func(t *testing.T, record arrow.Record) {
				assert.Equal(t, int64(1), record.NumRows())
				assert.Equal(t, int64(3), record.NumCols())

				// Check all fields are timestamp type
				for i := 0; i < int(record.NumCols()); i++ {
					field := record.Schema().Field(i)
					assert.IsType(t, &arrow.TimestampType{}, field.Type)
					assert.Equal(t, arrow.Nanosecond, field.Type.(*arrow.TimestampType).Unit)
					assert.Equal(t, "UTC", field.Type.(*arrow.TimestampType).TimeZone)
				}

				// Check time column (nanoseconds)
				timeCol := record.Column(0)
				assert.IsType(t, &array.Timestamp{}, timeCol)
				ts := timeCol.(*array.Timestamp)
				assert.False(t, ts.IsNull(0))
				assert.Equal(t, arrow.Timestamp(1704067200000000000), ts.Value(0))

				// Check time_str column
				timeStrCol := record.Column(1)
				assert.IsType(t, &array.Timestamp{}, timeStrCol)
				tsStr := timeStrCol.(*array.Timestamp)
				assert.False(t, tsStr.IsNull(0))
				assert.Equal(t, arrow.Timestamp(1704067200000000000), tsStr.Value(0))

				// Check time_int column
				timeIntCol := record.Column(2)
				assert.IsType(t, &array.Timestamp{}, timeIntCol)
				tsInt := timeIntCol.(*array.Timestamp)
				assert.False(t, tsInt.IsNull(0))
				assert.Equal(t, arrow.Timestamp(1704067200000000000), tsInt.Value(0))
			},
		},
		{
			name: "Null timestamp handling",
			results: []map[string]interface{}{
				{
					"time": nil,
				},
			},
			wantType: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"},
			check: func(t *testing.T, record arrow.Record) {
				assert.Equal(t, int64(1), record.NumRows())
				assert.Equal(t, int64(1), record.NumCols())

				col := record.Column(0)
				assert.IsType(t, &array.Timestamp{}, col)

				ts := col.(*array.Timestamp)
				assert.True(t, ts.IsNull(0))
			},
		},
		{
			name: "Mixed data types with time column",
			results: []map[string]interface{}{
				{
					"time":    int64(1704067200000000000),
					"count":   int64(42),
					"value":   float64(3.14),
					"active":  true,
					"message": "test",
				},
			},
			wantType: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"},
			check: func(t *testing.T, record arrow.Record) {
				assert.Equal(t, int64(1), record.NumRows())
				assert.Equal(t, int64(5), record.NumCols())

				// Find columns by name
				timeCol := findColumnByName(record, "time")
				countCol := findColumnByName(record, "count")
				valueCol := findColumnByName(record, "value")
				activeCol := findColumnByName(record, "active")
				messageCol := findColumnByName(record, "message")

				// Check time column
				assert.IsType(t, &array.Timestamp{}, timeCol)
				ts := timeCol.(*array.Timestamp)
				assert.False(t, ts.IsNull(0))
				assert.Equal(t, arrow.Timestamp(1704067200000000000), ts.Value(0))

				// Check count column
				assert.IsType(t, &array.Int64{}, countCol)
				count := countCol.(*array.Int64)
				assert.False(t, count.IsNull(0))
				assert.Equal(t, int64(42), count.Value(0))

				// Check value column
				assert.IsType(t, &array.Float64{}, valueCol)
				value := valueCol.(*array.Float64)
				assert.False(t, value.IsNull(0))
				assert.Equal(t, float64(3.14), value.Value(0))

				// Check active column
				assert.IsType(t, &array.Boolean{}, activeCol)
				active := activeCol.(*array.Boolean)
				assert.False(t, active.IsNull(0))
				assert.Equal(t, true, active.Value(0))

				// Check message column
				assert.IsType(t, &array.String{}, messageCol)
				message := messageCol.(*array.String)
				assert.False(t, message.IsNull(0))
				assert.Equal(t, "test", message.Value(0))
			},
		},
		{
			name: "Invalid timestamp string",
			results: []map[string]interface{}{
				{
					"time": "invalid-timestamp",
				},
			},
			wantType: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"},
			check: func(t *testing.T, record arrow.Record) {
				assert.Equal(t, int64(1), record.NumRows())
				assert.Equal(t, int64(1), record.NumCols())

				col := record.Column(0)
				assert.IsType(t, &array.Timestamp{}, col)

				ts := col.(*array.Timestamp)
				assert.True(t, ts.IsNull(0))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schema, record, err := convertResultsToArrow(tt.results)
			assert.NoError(t, err)
			assert.NotNil(t, schema)
			assert.NotNil(t, record)

			// Check schema of time column
			timeField := schema.Field(0)
			assert.Equal(t, "time", timeField.Name)
			assert.Equal(t, tt.wantType, timeField.Type)

			// Run custom checks
			tt.check(t, record)
		})
	}
}

func TestInferTypeFromColumn(t *testing.T) {
	tests := []struct {
		name     string
		column   string
		results  []map[string]interface{}
		wantType arrow.DataType
	}{
		{
			name:   "Time column always returns timestamp",
			column: "time",
			results: []map[string]interface{}{
				{"time": nil},
				{"time": "not a timestamp"},
				{"time": 42},
			},
			wantType: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"},
		},
		{
			name:   "Time-like column returns timestamp",
			column: "time_str",
			results: []map[string]interface{}{
				{"time_str": int64(1704067200000000000)},
			},
			wantType: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"},
		},
		{
			name:   "Int64 column",
			column: "count",
			results: []map[string]interface{}{
				{"count": int64(42)},
			},
			wantType: arrow.PrimitiveTypes.Int64,
		},
		{
			name:   "Float64 column",
			column: "value",
			results: []map[string]interface{}{
				{"value": float64(3.14)},
			},
			wantType: arrow.PrimitiveTypes.Float64,
		},
		{
			name:   "String column",
			column: "message",
			results: []map[string]interface{}{
				{"message": "test"},
			},
			wantType: arrow.BinaryTypes.String,
		},
		{
			name:   "Boolean column",
			column: "active",
			results: []map[string]interface{}{
				{"active": true},
			},
			wantType: arrow.FixedWidthTypes.Boolean,
		},
		{
			name:   "All null values defaults to string",
			column: "unknown",
			results: []map[string]interface{}{
				{"unknown": nil},
				{"unknown": nil},
			},
			wantType: arrow.BinaryTypes.String,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType := inferTypeFromColumn(tt.column, tt.results)
			assert.Equal(t, tt.wantType, gotType)
		})
	}
}

// Helper function to find a column by name
func findColumnByName(record arrow.Record, name string) arrow.Array {
	for i := 0; i < int(record.NumCols()); i++ {
		if record.Schema().Field(i).Name == name {
			return record.Column(i)
		}
	}
	return nil
}

// Helper function to find a field by name
func findFieldByName(schema *arrow.Schema, name string) arrow.Field {
	for i := 0; i < schema.NumFields(); i++ {
		if schema.Field(i).Name == name {
			return schema.Field(i)
		}
	}
	return arrow.Field{}
}
