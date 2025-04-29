package querier

import (
	"testing"
	"time"
)

func TestExtractTimeRange(t *testing.T) {
	tests := []struct {
		name        string
		whereClause string
		wantStart   string
		wantEnd     string
	}{
		{
			name:        "Simple timestamp comparison",
			whereClause: "time >= '2023-01-01T00:00:00Z' AND time <= '2023-01-02T00:00:00Z'",
			wantStart:   "2023-01-01T00:00:00Z",
			wantEnd:     "2023-01-02T00:00:00Z",
		},
		{
			name:        "Cast timestamp comparison",
			whereClause: "time >= cast('2023-01-01T00:00:00Z' as timestamp) AND time <= cast('2023-01-02T00:00:00Z' as timestamp)",
			wantStart:   "2023-01-01T00:00:00Z",
			wantEnd:     "2023-01-02T00:00:00Z",
		},
		{
			name:        "Epoch_ns timestamp comparison",
			whereClause: "time >= epoch_ns('2023-01-01T00:00:00'::TIMESTAMP) AND time <= epoch_ns('2023-01-02T00:00:00'::TIMESTAMP)",
			wantStart:   "2023-01-01T00:00:00Z",
			wantEnd:     "2023-01-02T00:00:00Z",
		},
		{
			name:        "Epoch_ns with cast",
			whereClause: "time >= epoch_ns(cast('2023-01-01T00:00:00' as timestamp)::TIMESTAMP) AND time <= epoch_ns(cast('2023-01-02T00:00:00' as timestamp)::TIMESTAMP)",
			wantStart:   "2023-01-01T00:00:00Z",
			wantEnd:     "2023-01-02T00:00:00Z",
		},
		{
			name:        "Single timestamp comparison",
			whereClause: "time = '2023-01-01T00:00:00Z'",
			wantStart:   "2023-01-01T00:00:00Z",
			wantEnd:     "2023-01-01T00:00:00Z",
		},
		{
			name:        "Between timestamp",
			whereClause: "time BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z'",
			wantStart:   "2023-01-01T00:00:00Z",
			wantEnd:     "2023-01-02T00:00:00Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &QueryClient{}
			got := q.extractTimeRange(tt.whereClause)

			// Check if we got a time range
			if got.Start == nil || got.End == nil {
				t.Errorf("extractTimeRange() got nil time range")
				return
			}

			// Convert timestamps to strings for comparison
			gotStart := time.Unix(0, *got.Start).UTC().Format(time.RFC3339)
			gotEnd := time.Unix(0, *got.End).UTC().Format(time.RFC3339)

			if gotStart != tt.wantStart {
				t.Errorf("extractTimeRange() start = %v, want %v", gotStart, tt.wantStart)
			}
			if gotEnd != tt.wantEnd {
				t.Errorf("extractTimeRange() end = %v, want %v", gotEnd, tt.wantEnd)
			}
		})
	}
}

func TestParseQuery(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		dbName  string
		want    *ParsedQuery
		wantErr bool
	}{
		{
			name:   "Simple count query with time range",
			query:  "SELECT COUNT(*) AS value FROM hep.hep_1 WHERE time >= '2023-01-01T00:00:00Z' AND time <= '2023-01-02T00:00:00Z'",
			dbName: "hep",
			want: &ParsedQuery{
				Columns:     "COUNT(*) AS value",
				DbName:      "hep",
				Measurement: "hep_1",
				TimeRange: TimeRange{
					Start:         ptr(int64(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())),
					End:           ptr(int64(time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC).UnixNano())),
					TimeCondition: "time >= epoch_ns('2023-01-01T00:00:00Z'::TIMESTAMP) AND time <= epoch_ns('2023-01-02T00:00:00Z'::TIMESTAMP)",
				},
				WhereConditions: "time >= '2023-01-01T00:00:00Z' AND time <= '2023-01-02T00:00:00Z'",
			},
			wantErr: false,
		},
		{
			name:   "Query with cast timestamps",
			query:  "SELECT COUNT(*) AS value FROM hep.hep_1 WHERE time >= cast('2023-01-01T00:00:00Z' as timestamp) AND time <= cast('2023-01-02T00:00:00Z' as timestamp)",
			dbName: "hep",
			want: &ParsedQuery{
				Columns:     "COUNT(*) AS value",
				DbName:      "hep",
				Measurement: "hep_1",
				TimeRange: TimeRange{
					Start:         ptr(int64(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())),
					End:           ptr(int64(time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC).UnixNano())),
					TimeCondition: "time >= epoch_ns('2023-01-01T00:00:00Z'::TIMESTAMP) AND time <= epoch_ns('2023-01-02T00:00:00Z'::TIMESTAMP)",
				},
				WhereConditions: "time >= cast('2023-01-01T00:00:00Z' as timestamp) AND time <= cast('2023-01-02T00:00:00Z' as timestamp)",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &QueryClient{}
			got, err := q.ParseQuery(tt.query, tt.dbName)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Compare the parsed query
			if got.Columns != tt.want.Columns {
				t.Errorf("ParseQuery() columns = %v, want %v", got.Columns, tt.want.Columns)
			}
			if got.DbName != tt.want.DbName {
				t.Errorf("ParseQuery() dbName = %v, want %v", got.DbName, tt.want.DbName)
			}
			if got.Measurement != tt.want.Measurement {
				t.Errorf("ParseQuery() measurement = %v, want %v", got.Measurement, tt.want.Measurement)
			}

			// Compare time range
			if got.TimeRange.Start == nil || tt.want.TimeRange.Start == nil {
				if got.TimeRange.Start != tt.want.TimeRange.Start {
					t.Errorf("ParseQuery() timeRange.Start = %v, want %v", got.TimeRange.Start, tt.want.TimeRange.Start)
				}
			} else if *got.TimeRange.Start != *tt.want.TimeRange.Start {
				t.Errorf("ParseQuery() timeRange.Start = %v, want %v", *got.TimeRange.Start, *tt.want.TimeRange.Start)
			}

			if got.TimeRange.End == nil || tt.want.TimeRange.End == nil {
				if got.TimeRange.End != tt.want.TimeRange.End {
					t.Errorf("ParseQuery() timeRange.End = %v, want %v", got.TimeRange.End, tt.want.TimeRange.End)
				}
			} else if *got.TimeRange.End != *tt.want.TimeRange.End {
				t.Errorf("ParseQuery() timeRange.End = %v, want %v", *got.TimeRange.End, *tt.want.TimeRange.End)
			}
		})
	}
}

// Helper function to create a pointer to an int64
func ptr(i int64) *int64 {
	return &i
} 