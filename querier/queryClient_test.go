package querier

import (
	"testing"
	"time"
)

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

func TestParseQuery_TimeExtraction(t *testing.T) {
	tests := []struct {
		name      string
		query     string
		dbName    string
		wantStart int64
		wantEnd   int64
		wantField string
		wantErr   bool
	}{
		{
			name:   ">= and <= time range (time)",
			query:  "SELECT * FROM mydb.mytable WHERE time >= '2024-01-01T00:00:00Z' AND time <= '2024-01-02T00:00:00Z'",
			dbName: "mydb",
			wantStart: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
			wantEnd:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC).UnixNano(),
			wantField: "time",
			wantErr: false,
		},
		{
			name:   "= time equality (timestamp)",
			query:  "SELECT * FROM mydb.mytable WHERE timestamp = '2024-01-01T12:00:00Z'",
			dbName: "mydb",
			wantStart: time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano(),
			wantEnd:   time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC).UnixNano(),
			wantField: "timestamp",
			wantErr: false,
		},
		{
			name:   "BETWEEN time range (__timestamp)",
			query:  "SELECT * FROM mydb.mytable WHERE __timestamp BETWEEN 1747738272042000000 AND 1748343072042000000",
			dbName: "mydb",
			wantStart: 1747738272042000000,
			wantEnd:   1748343072042000000,
			wantField: "__timestamp",
			wantErr: false,
		},
		{
			name:   ">= and <= time range (date)",
			query:  "SELECT * FROM mydb.mytable WHERE date >= '2024-01-01T00:00:00Z' AND date <= '2024-01-02T00:00:00Z'",
			dbName: "mydb",
			wantStart: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
			wantEnd:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC).UnixNano(),
			wantField: "date",
			wantErr: false,
		},
		{
			name:   "BETWEEN time range (record_datetime)",
			query:  "SELECT * FROM mydb.mytable WHERE record_datetime BETWEEN 1747738272042000000 AND 1748343072042000000",
			dbName: "mydb",
			wantStart: 1747738272042000000,
			wantEnd:   1748343072042000000,
			wantField: "record_datetime",
			wantErr: false,
		},
		{
			name:   ">= and <= with cast (regex fallback)",
			query:  "SELECT * FROM mydb.mytable WHERE date >= cast('2024-01-01T00:00:00Z' as timestamp) AND date <= cast('2024-01-02T00:00:00Z' as timestamp)",
			dbName: "mydb",
			wantStart: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
			wantEnd:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC).UnixNano(),
			wantField: "date",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &QueryClient{}
			parsed, err := q.ParseQuery(tt.query, tt.dbName)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseQuery() error = %v, wantErr %v", err, tt.wantErr)
			}
			if parsed == nil {
				t.Fatalf("ParseQuery() returned nil parsed query")
			}
			if parsed.TimeRange.Start == nil || *parsed.TimeRange.Start != tt.wantStart {
				t.Errorf("Start = %v, want %v", parsed.TimeRange.Start, tt.wantStart)
			}
			if parsed.TimeRange.End == nil || *parsed.TimeRange.End != tt.wantEnd {
				t.Errorf("End = %v, want %v", parsed.TimeRange.End, tt.wantEnd)
			}
			if parsed.TimeField != tt.wantField {
				t.Errorf("TimeField = %v, want %v", parsed.TimeField, tt.wantField)
			}
		})
	}
}

// Helper function to create a pointer to an int64
func ptr(i int64) *int64 {
	return &i
} 