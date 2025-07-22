package querier

import (
	"strings"
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

func TestParseQueryCaseInsensitive(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		dbName  string
		want    *ParsedQuery
		wantErr bool
	}{
		{
			name:   "Mixed case SELECT and FROM",
			query:  "select COUNT(*) AS value from hep.hep_1 WHERE time >= '2023-01-01T00:00:00Z'",
			dbName: "hep",
			want: &ParsedQuery{
				Columns:     "COUNT(*) AS value",
				DbName:      "hep",
				Measurement: "hep_1",
				TimeRange: TimeRange{
					Start:         ptr(int64(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())),
					End:           nil,
					TimeCondition: "time >= epoch_ns('2023-01-01T00:00:00Z'::TIMESTAMP)",
				},
				WhereConditions: "time >= '2023-01-01T00:00:00Z'",
			},
			wantErr: false,
		},
		{
			name:   "All lowercase keywords",
			query:  "select count(*) from hep.hep_1 where time >= '2023-01-01T00:00:00Z' order by time limit 10",
			dbName: "hep",
			want: &ParsedQuery{
				Columns:     "count(*)",
				DbName:      "hep",
				Measurement: "hep_1",
				TimeRange: TimeRange{
					Start:         ptr(int64(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())),
					End:           nil,
					TimeCondition: "time >= epoch_ns('2023-01-01T00:00:00Z'::TIMESTAMP)",
				},
				WhereConditions: "time >= '2023-01-01T00:00:00Z'",
				OrderBy:         "time",
				Limit:           10,
			},
			wantErr: false,
		},
		{
			name:   "All uppercase keywords",
			query:  "SELECT COUNT(*) FROM HEP.HEP_1 WHERE TIME >= '2023-01-01T00:00:00Z' GROUP BY TIME ORDER BY TIME LIMIT 10",
			dbName: "hep",
			want: &ParsedQuery{
				Columns:     "COUNT(*)",
				DbName:      "HEP",
				Measurement: "HEP_1",
				TimeRange: TimeRange{
					Start:         ptr(int64(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())),
					End:           nil,
					TimeCondition: "time >= epoch_ns('2023-01-01T00:00:00Z'::TIMESTAMP)",
				},
				WhereConditions: "TIME >= '2023-01-01T00:00:00Z'",
				GroupBy:         "TIME",
				OrderBy:         "TIME",
				Limit:           10,
			},
			wantErr: false,
		},
		{
			name:   "Mixed case with complex clauses",
			query:  "Select count(*) as value From hep.hep_1 Where time >= '2023-01-01T00:00:00Z' And time <= '2023-01-02T00:00:00Z' Group By hour Order By time Limit 100",
			dbName: "hep",
			want: &ParsedQuery{
				Columns:     "count(*) as value",
				DbName:      "hep",
				Measurement: "hep_1",
				TimeRange: TimeRange{
					Start:         ptr(int64(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())),
					End:           ptr(int64(time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC).UnixNano())),
					TimeCondition: "time >= epoch_ns('2023-01-01T00:00:00Z'::TIMESTAMP) AND time <= epoch_ns('2023-01-02T00:00:00Z'::TIMESTAMP)",
				},
				WhereConditions: "time >= '2023-01-01T00:00:00Z' And time <= '2023-01-02T00:00:00Z'",
				GroupBy:         "hour",
				OrderBy:         "time",
				Limit:           100,
			},
			wantErr: false,
		},
		{
			name:   "Mixed case with BETWEEN",
			query:  "select * from hep.hep_1 where time between '2023-01-01T00:00:00Z' and '2023-01-02T00:00:00Z'",
			dbName: "hep",
			want: &ParsedQuery{
				Columns:     "*",
				DbName:      "hep",
				Measurement: "hep_1",
				TimeRange: TimeRange{
					Start:         ptr(int64(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())),
					End:           ptr(int64(time.Date(2023, 1, 2, 0, 0, 0, 0, time.UTC).UnixNano())),
					TimeCondition: "time >= epoch_ns('2023-01-01T00:00:00Z'::TIMESTAMP) AND time <= epoch_ns('2023-01-02T00:00:00Z'::TIMESTAMP)",
				},
				WhereConditions: "time between '2023-01-01T00:00:00Z' and '2023-01-02T00:00:00Z'",
			},
			wantErr: false,
		},
		{
			name:   "Mixed case with CAST",
			query:  "Select count(*) From hep.hep_1 Where time >= cast('2023-01-01T00:00:00Z' as timestamp)",
			dbName: "hep",
			want: &ParsedQuery{
				Columns:     "count(*)",
				DbName:      "hep",
				Measurement: "hep_1",
				TimeRange: TimeRange{
					Start:         ptr(int64(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())),
					End:           nil,
					TimeCondition: "time >= epoch_ns('2023-01-01T00:00:00Z'::TIMESTAMP)",
				},
				WhereConditions: "time >= cast('2023-01-01T00:00:00Z' as timestamp)",
			},
			wantErr: false,
		},
		{
			name:   "Mixed case with EPOCH_NS",
			query:  "select * from hep.hep_1 where time >= epoch_ns('2023-01-01T00:00:00'::timestamp)",
			dbName: "hep",
			want: &ParsedQuery{
				Columns:     "*",
				DbName:      "hep",
				Measurement: "hep_1",
				TimeRange: TimeRange{
					Start:         ptr(int64(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())),
					End:           nil,
					TimeCondition: "time >= epoch_ns('2023-01-01T00:00:00Z'::TIMESTAMP)",
				},
				WhereConditions: "time >= epoch_ns('2023-01-01T00:00:00'::timestamp)",
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

			if tt.wantErr {
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
			if got.OrderBy != tt.want.OrderBy {
				t.Errorf("ParseQuery() orderBy = %v, want %v", got.OrderBy, tt.want.OrderBy)
			}
			if got.GroupBy != tt.want.GroupBy {
				t.Errorf("ParseQuery() groupBy = %v, want %v", got.GroupBy, tt.want.GroupBy)
			}
			if got.Limit != tt.want.Limit {
				t.Errorf("ParseQuery() limit = %v, want %v", got.Limit, tt.want.Limit)
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

func TestExtractTimeRangeCaseInsensitive(t *testing.T) {
	tests := []struct {
		name        string
		whereClause string
		wantStart   string
		wantEnd     string
	}{
		{
			name:        "Mixed case TIME keyword",
			whereClause: "TIME >= '2023-01-01T00:00:00Z' AND TIME <= '2023-01-02T00:00:00Z'",
			wantStart:   "2023-01-01T00:00:00Z",
			wantEnd:     "2023-01-02T00:00:00Z",
		},
		{
			name:        "Mixed case BETWEEN",
			whereClause: "time BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z'",
			wantStart:   "2023-01-01T00:00:00Z",
			wantEnd:     "2023-01-02T00:00:00Z",
		},
		{
			name:        "Mixed case CAST",
			whereClause: "time >= CAST('2023-01-01T00:00:00Z' AS TIMESTAMP) AND time <= CAST('2023-01-02T00:00:00Z' AS TIMESTAMP)",
			wantStart:   "2023-01-01T00:00:00Z",
			wantEnd:     "2023-01-02T00:00:00Z",
		},
		{
			name:        "Mixed case EPOCH_NS",
			whereClause: "time >= EPOCH_NS('2023-01-01T00:00:00'::TIMESTAMP) AND time <= EPOCH_NS('2023-01-02T00:00:00'::TIMESTAMP)",
			wantStart:   "2023-01-01T00:00:00Z",
			wantEnd:     "2023-01-02T00:00:00Z",
		},
		{
			name:        "Mixed case EPOCH_NS with CAST",
			whereClause: "time >= EPOCH_NS(CAST('2023-01-01T00:00:00' AS TIMESTAMP)::TIMESTAMP) AND time <= EPOCH_NS(CAST('2023-01-02T00:00:00' AS TIMESTAMP)::TIMESTAMP)",
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

func TestDetectTimeColumn(t *testing.T) {
	tests := []struct {
		name        string
		whereClause string
		want        string
	}{
		{
			name:        "Default time column",
			whereClause: "time >= '2023-01-01T00:00:00Z'",
			want:        "time",
		},
		{
			name:        "Timestamp column",
			whereClause: "timestamp >= '2023-01-01T00:00:00Z'",
			want:        "timestamp",
		},
		{
			name:        "Double underscore timestamp",
			whereClause: "__timestamp >= 1747803600000000000",
			want:        "__timestamp",
		},
		{
			name:        "DateTime column",
			whereClause: "datetime BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z'",
			want:        "datetime",
		},
		{
			name:        "Complex query with __timestamp",
			whereClause: "nodename = 'sniff03' AND iface = 'ens6f0np0' AND (__timestamp >= 1747803600000000000 AND __timestamp <= 1747825200000000000)",
			want:        "__timestamp",
		},
		{
			name:        "No time column",
			whereClause: "nodename = 'test' AND value > 100",
			want:        "time",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &QueryClient{}
			got := q.detectTimeColumn(tt.whereClause)
			if got != tt.want {
				t.Errorf("detectTimeColumn() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExtractTimeRangeWithDynamicColumn(t *testing.T) {
	tests := []struct {
		name        string
		whereClause string
		wantStart   string
		wantEnd     string
		wantColumn  string
	}{
		{
			name:        "Numeric __timestamp range",
			whereClause: "__timestamp >= 1747803600000000000 AND __timestamp <= 1747825200000000000",
			wantStart:   "2025-05-21T05:00:00Z",
			wantEnd:     "2025-05-21T11:00:00Z",
			wantColumn:  "__timestamp",
		},
		{
			name:        "Numeric __timestamp BETWEEN",
			whereClause: "__timestamp BETWEEN 1747803600000000000 AND 1747825200000000000",
			wantStart:   "2025-05-21T05:00:00Z",
			wantEnd:     "2025-05-21T11:00:00Z",
			wantColumn:  "__timestamp",
		},
		{
			name:        "String timestamp range",
			whereClause: "timestamp >= '2023-01-01T00:00:00Z' AND timestamp <= '2023-01-02T00:00:00Z'",
			wantStart:   "2023-01-01T00:00:00Z",
			wantEnd:     "2023-01-02T00:00:00Z",
			wantColumn:  "timestamp",
		},
		{
			name:        "DateTime column",
			whereClause: "datetime BETWEEN '2023-01-01T00:00:00Z' AND '2023-01-02T00:00:00Z'",
			wantStart:   "2023-01-01T00:00:00Z",
			wantEnd:     "2023-01-02T00:00:00Z",
			wantColumn:  "datetime",
		},
		{
			name:        "Query with __timestamp and other conditions",
			whereClause: "nodename = 'sniff03' AND iface = 'ens6f0np0' AND (__timestamp >= 1747803600000000000 AND __timestamp <= 1747825200000000000)",
			wantStart:   "2025-05-21T05:00:00Z",
			wantEnd:     "2025-05-21T11:00:00Z",
			wantColumn:  "__timestamp",
		},
		{
			name:        "Query with time and other conditions",
			whereClause: "time >= 1747803600000000000 AND time <= 1747825200000000000 AND nodename = 'sniff03' AND iface = 'ens6f0np0'",
			wantStart:   "2025-05-21T05:00:00Z",
			wantEnd:     "2025-05-21T11:00:00Z",
			wantColumn:  "time",
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

			// Check that the time condition uses the correct column name
			if !strings.Contains(got.TimeCondition, tt.wantColumn) {
				t.Errorf("extractTimeRange() time condition = %v, should contain column %v", got.TimeCondition, tt.wantColumn)
			}
		})
	}
}