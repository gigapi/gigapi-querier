// queryClient.go
package querier

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/metadata"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/gigapi/gigapi-querier/core"
	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow"
)

var db *sql.DB

// Ensure QueryClient implements core.QueryClient interface
var _ core.QueryClient = (*QueryClient)(nil)

// QueryClient handles parsing SQL and querying parquet files
type QueryClient struct {
	DataDir          string
	DB               *sql.DB
	DefaultTimeRange int64 // 10 minutes in nanoseconds
}

// NewQueryClient creates a new QueryClient
func NewQueryClient(dataDir string) *QueryClient {
	return &QueryClient{
		DataDir:          dataDir,
		DefaultTimeRange: 10 * 60 * 1000000000, // 10 minutes in nanoseconds
	}
}

// Initialize sets up the DuckDB connection
func (q *QueryClient) Initialize() error {
	var err error
	db, err = sql.Open("duckdb", "?access_mode=READ_WRITE&allow_unsigned_extensions=1")
	if err != nil {
		return fmt.Errorf("failed to initialize DuckDB: %v", err)
	}
	q.DB = db
	return nil
}

// ParsedQuery contains the parsed components of a SQL query
type ParsedQuery struct {
	Columns         string
	DbName          string
	Measurement     string
	TimeRange       TimeRange
	WhereConditions string
	OrderBy         string
	GroupBy         string
	Having          string
	Limit           int
}

// TimeRange represents a query time range
type TimeRange struct {
	Start         *int64
	End           *int64
	TimeCondition string
}

// Parse SQL query to extract components
func (q *QueryClient) ParseQuery(sql, dbName string) (*ParsedQuery, error) {
	// Normalize whitespace
	sql = regexp.MustCompile(`\s+`).ReplaceAllString(sql, " ")
	sql = strings.TrimSpace(sql)

	log.Printf("Parsing query: %s", sql)

	// Extract columns
	columnsPattern := regexp.MustCompile(`(?i)SELECT\s+(.*?)\s+FROM`)
	columnsMatch := columnsPattern.FindStringSubmatch(sql)
	columns := "*"
	if len(columnsMatch) > 1 {
		columns = strings.TrimSpace(columnsMatch[1])
	}

	// Extract measurement name
	fromPattern := regexp.MustCompile(`(?i)FROM\s+(?:(\w+)\.)?(\w+)`)
	fromMatch := fromPattern.FindStringSubmatch(sql)
	if len(fromMatch) < 3 {
		return nil, fmt.Errorf("invalid query: FROM clause not found or invalid")
	}

	// If db name is in the query, use it, otherwise use the provided dbName
	queryDbName := dbName
	if fromMatch[1] != "" {
		queryDbName = fromMatch[1]
	}
	measurement := fromMatch[2]

	// Extract WHERE clause
	whereClause := ""
	whereParts := strings.Split(sql, " WHERE ")
	if len(whereParts) >= 2 {
		whereClause = whereParts[1]

		// Remove other clauses
		for _, clause := range []string{" GROUP BY ", " ORDER BY ", " LIMIT ", " HAVING "} {
			if idx := strings.Index(strings.ToUpper(whereClause), clause); idx != -1 {
				whereClause = whereClause[:idx]
			}
		}
	}

	// log.Printf("Extracted WHERE clause: %s", whereClause)

	// Extract time range
	timeRange := q.extractTimeRange(whereClause)
	if timeRange.Start != nil || timeRange.End != nil {
		log.Printf("Detected time range: %v to %v",
			time.Unix(0, *timeRange.Start).Format(time.RFC3339Nano),
			time.Unix(0, *timeRange.End).Format(time.RFC3339Nano))
	} else {
		log.Printf("No time range detected in WHERE clause")
	}

	// Extract other clauses
	orderBy := ""
	orderByPattern := regexp.MustCompile(`(?i)ORDER\s+BY\s+(.*?)(?:\s+(?:LIMIT|GROUP|HAVING|$))`)
	orderByMatch := orderByPattern.FindStringSubmatch(sql)
	if len(orderByMatch) > 1 {
		orderBy = strings.TrimSpace(orderByMatch[1])
	}

	groupBy := ""
	groupByPattern := regexp.MustCompile(`(?i)GROUP\s+BY\s+(.*?)(?:\s+(?:ORDER|LIMIT|HAVING|$))`)
	groupByMatch := groupByPattern.FindStringSubmatch(sql)
	if len(groupByMatch) > 1 {
		groupBy = strings.TrimSpace(groupByMatch[1])
	}

	having := ""
	havingPattern := regexp.MustCompile(`(?i)HAVING\s+(.*?)(?:\s+(?:ORDER|LIMIT|$))`)
	havingMatch := havingPattern.FindStringSubmatch(sql)
	if len(havingMatch) > 1 {
		having = strings.TrimSpace(havingMatch[1])
	}

	limit := 0
	limitPattern := regexp.MustCompile(`(?i)LIMIT\s+(\d+)`)
	limitMatch := limitPattern.FindStringSubmatch(sql)
	if len(limitMatch) > 1 {
		fmt.Sscanf(limitMatch[1], "%d", &limit)
	}

	return &ParsedQuery{
		Columns:         columns,
		DbName:          queryDbName,
		Measurement:     measurement,
		TimeRange:       timeRange,
		WhereConditions: whereClause,
		OrderBy:         orderBy,
		GroupBy:         groupBy,
		Having:          having,
		Limit:           limit,
	}, nil
}

// Extract time range from WHERE clause
func (q *QueryClient) extractTimeRange(whereClause string) TimeRange {
	timeRange := TimeRange{
		Start:         nil,
		End:           nil,
		TimeCondition: "",
	}

	if whereClause == "" {
		return timeRange
	}

	// log.Printf("Extracting time range from WHERE clause: %s", whereClause)

	// Match time patterns including both simple timestamps and epoch_ns with various formats
	timePatterns := []*regexp.Regexp{
		// Simple timestamp format
		regexp.MustCompile(`time\s*(>=|>)\s*'([^']+)'`),                    // time >= '2023-01-01T00:00:00Z'
		regexp.MustCompile(`time\s*(<=|<)\s*'([^']+)'`),                    // time <= '2023-01-01T00:00:00Z'
		regexp.MustCompile(`time\s*=\s*'([^']+)'`),                         // time = '2023-01-01T00:00:00Z'
		regexp.MustCompile(`time\s+BETWEEN\s+'([^']+)'\s+AND\s+'([^']+)'`), // time BETWEEN '...' AND '...'

		// Cast format
		regexp.MustCompile(`time\s*(>=|>)\s*cast\s*\(\s*'([^']+)'\s+as\s+timestamp\s*\)`),                                                      // time >= cast('2023-01-01T00:00:00' as timestamp)
		regexp.MustCompile(`time\s*(<=|<)\s*cast\s*\(\s*'([^']+)'\s+as\s+timestamp\s*\)`),                                                      // time <= cast('2023-01-01T00:00:00' as timestamp)
		regexp.MustCompile(`time\s*=\s*cast\s*\(\s*'([^']+)'\s+as\s+timestamp\s*\)`),                                                           // time = cast('2023-01-01T00:00:00' as timestamp)
		regexp.MustCompile(`time\s+BETWEEN\s+cast\s*\(\s*'([^']+)'\s+as\s+timestamp\s*\)\s+AND\s+cast\s*\(\s*'([^']+)'\s+as\s+timestamp\s*\)`), // time BETWEEN cast('...') AND cast('...')

		// Epoch_ns format
		regexp.MustCompile(`time\s*(>=|>)\s*epoch_ns\s*\(\s*'([^']+)'(?:::TIMESTAMP)?\s*\)`),                                                         // time >= epoch_ns('2023-01-01T00:00:00'::TIMESTAMP)
		regexp.MustCompile(`time\s*(<=|<)\s*epoch_ns\s*\(\s*'([^']+)'(?:::TIMESTAMP)?\s*\)`),                                                         // time <= epoch_ns('2023-01-01T00:00:00'::TIMESTAMP)
		regexp.MustCompile(`time\s*=\s*epoch_ns\s*\(\s*'([^']+)'(?:::TIMESTAMP)?\s*\)`),                                                              // time = epoch_ns('2023-01-01T00:00:00'::TIMESTAMP)
		regexp.MustCompile(`time\s+BETWEEN\s+epoch_ns\s*\(\s*'([^']+)'(?:::TIMESTAMP)?\s*\)\s+AND\s+epoch_ns\s*\(\s*'([^']+)'(?:::TIMESTAMP)?\s*\)`), // time BETWEEN epoch_ns('...') AND epoch_ns('...')

		// Epoch_ns with cast format
		regexp.MustCompile(`time\s*(>=|>)\s*epoch_ns\s*\(\s*cast\s*\(\s*'([^']+)'\s+as\s+timestamp\s*\)(?:::TIMESTAMP)?\s*\)`),                                                                                           // time >= epoch_ns(cast('2023-01-01T00:00:00' as timestamp)::TIMESTAMP)
		regexp.MustCompile(`time\s*(<=|<)\s*epoch_ns\s*\(\s*cast\s*\(\s*'([^']+)'\s+as\s+timestamp\s*\)(?:::TIMESTAMP)?\s*\)`),                                                                                           // time <= epoch_ns(cast('2023-01-01T00:00:00' as timestamp)::TIMESTAMP)
		regexp.MustCompile(`time\s*=\s*epoch_ns\s*\(\s*cast\s*\(\s*'([^']+)'\s+as\s+timestamp\s*\)(?:::TIMESTAMP)?\s*\)`),                                                                                                // time = epoch_ns(cast('2023-01-01T00:00:00' as timestamp)::TIMESTAMP)
		regexp.MustCompile(`time\s+BETWEEN\s+epoch_ns\s*\(\s*cast\s*\(\s*'([^']+)'\s+as\s+timestamp\s*\)(?:::TIMESTAMP)?\s*\)\s+AND\s+epoch_ns\s*\(\s*cast\s*\(\s*'([^']+)'\s+as\s+timestamp\s*\)(?:::TIMESTAMP)?\s*\)`), // time BETWEEN epoch_ns(cast('...')::TIMESTAMP) AND epoch_ns(cast('...')::TIMESTAMP)
	}

	var startTime, endTime time.Time
	var startOp, endOp string
	var err error

	for i, pattern := range timePatterns {
		matches := pattern.FindStringSubmatch(whereClause)
		// log.Printf("Trying pattern: %s", pattern.String())
		if len(matches) > 0 {
			log.Printf("Found matches: %v", matches)

			// Handle BETWEEN patterns
			if i == 3 || i == 7 || i == 11 || i == 15 { // BETWEEN patterns
				startTimestamp := matches[1]
				endTimestamp := matches[2]
				// log.Printf("BETWEEN clause: start=%s, end=%s", startTimestamp, endTimestamp)

				startTime, err = time.Parse(time.RFC3339Nano, startTimestamp)
				if err != nil {
					startTime, err = time.Parse("2006-01-02T15:04:05", startTimestamp)
					if err != nil {
						log.Printf("Error parsing start timestamp %s: %v", startTimestamp, err)
						continue
					}
				}

				endTime, err = time.Parse(time.RFC3339Nano, endTimestamp)
				if err != nil {
					endTime, err = time.Parse("2006-01-02T15:04:05", endTimestamp)
					if err != nil {
						log.Printf("Error parsing end timestamp %s: %v", endTimestamp, err)
						continue
					}
				}

				startOp = ">="
				endOp = "<="
				break
			}

			// Handle = patterns
			if i == 2 || i == 6 || i == 10 || i == 14 { // = patterns
				timestamp := matches[1]
				// log.Printf("Equal timestamp: %s", timestamp)

				parsedTime, err := time.Parse(time.RFC3339Nano, timestamp)
				if err != nil {
					parsedTime, err = time.Parse("2006-01-02T15:04:05", timestamp)
					if err != nil {
						log.Printf("Error parsing timestamp %s: %v", timestamp, err)
						continue
					}
				}

				startTime = parsedTime
				endTime = parsedTime
				startOp = ">="
				endOp = "<="
				break
			}

			// Handle >= and <= patterns
			if len(matches) == 3 {
				timestamp := matches[2]
				op := matches[1]
				// log.Printf("Single timestamp comparison: op=%s, timestamp=%s", op, timestamp)

				parsedTime, err := time.Parse(time.RFC3339Nano, timestamp)
				if err != nil {
					parsedTime, err = time.Parse("2006-01-02T15:04:05", timestamp)
					if err != nil {
						log.Printf("Error parsing timestamp %s: %v", timestamp, err)
						continue
					}
				}

				if op == ">=" || op == ">" {
					startTime = parsedTime
					startOp = op
				} else if op == "<=" || op == "<" {
					endTime = parsedTime
					endOp = op
				}
			}
		}
	}

	if !startTime.IsZero() {
		startNano := startTime.UnixNano()
		timeRange.Start = &startNano
		timeRange.TimeCondition = fmt.Sprintf("time %s epoch_ns('%s'::TIMESTAMP)", startOp, startTime.Format(time.RFC3339))
	}

	if !endTime.IsZero() {
		endNano := endTime.UnixNano()
		timeRange.End = &endNano
		if timeRange.TimeCondition != "" {
			timeRange.TimeCondition = fmt.Sprintf("%s AND time %s epoch_ns('%s'::TIMESTAMP)",
				timeRange.TimeCondition, endOp, endTime.Format(time.RFC3339))
		} else {
			timeRange.TimeCondition = fmt.Sprintf("time %s epoch_ns('%s'::TIMESTAMP)",
				endOp, endTime.Format(time.RFC3339))
		}
	}

	if timeRange.Start != nil || timeRange.End != nil {
		log.Printf("Found time range: start=%v, end=%v", startTime, endTime)
		log.Printf("Time condition: %s", timeRange.TimeCondition)
	} else {
		log.Printf("No time range found")
	}

	return timeRange
}

// MetadataFile represents a metadata.json file structure
type MetadataFile struct {
	Type             string        `json:"type"`
	ParquetSizeBytes int           `json:"parquet_size_bytes"`
	RowCount         int           `json:"row_count"`
	MinTime          int64         `json:"min_time"`
	MaxTime          int64         `json:"max_time"`
	Files            []ParquetFile `json:"files"`
}

// ParquetFile represents a single parquet file entry in metadata
type ParquetFile struct {
	Path      string `json:"path"`
	SizeBytes int    `json:"size_bytes"`
	RowCount  int    `json:"row_count"`
	MinTime   int64  `json:"min_time"`
	MaxTime   int64  `json:"max_time"`
}

func (q *QueryClient) enumFolderWithMetadata(metadataPath string, timeRange TimeRange) ([]string, error) {
	metadataBytes, err := os.ReadFile(metadataPath)
	if err != nil {
		return nil, err
	}

	var metadata MetadataFile
	if err := json.Unmarshal(metadataBytes, &metadata); err != nil {
		return nil, err
	}

	// Skip if metadata time range doesn't overlap with requested time range
	if timeRange.Start != nil && timeRange.End != nil &&
		(metadata.MaxTime < *timeRange.Start || metadata.MinTime > *timeRange.End) {
		return []string{}, nil
	}

	var res []string

	// Check each file in metadata
	for _, file := range metadata.Files {
		// Skip if file time range doesn't overlap with requested time range
		if timeRange.Start != nil && timeRange.End != nil &&
			(file.MaxTime < *timeRange.Start || file.MinTime > *timeRange.End) {
			continue
		}

		// Check if the file exists at the given path
		if _, err := os.Stat(file.Path); err == nil {
			res = append(res, file.Path)
		}
	}
	return res, nil
}

func (q *QueryClient) enumFolderNoMetadata(path string) ([]string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	res := make([]string, len(entries))
	for i, entry := range entries {
		name := entry.Name()
		if strings.HasSuffix(name, ".parquet") {
			res[i] = filepath.Join(path, entries[i].Name())
		}
	}
	return res, nil
}

func getIndex(db, measurement string) (metadata.TableIndex, error) {
	switch config.Config.Gigapi.Metadata.Type {
	case "json":
		return metadata.NewJSONIndex(config.Config.Gigapi.Root, db, measurement), nil
	case "redis":
		return metadata.NewRedisIndex(config.Config.Gigapi.Metadata.URL, db, measurement)
	}
	return nil, fmt.Errorf("Unsupported index %s", config.Config.Gigapi.Metadata.Type)
}

// Find relevant parquet files based on time range
func (q *QueryClient) FindRelevantFiles(ctx context.Context, dbName, measurement string,
	timeRange TimeRange) ([]string, error) {
	// If no time range specified, get all files
	var relevantFiles []string
	// log.Printf("Getting relevant files for %s.%s within time range %v to %v", dbName, measurement,
	// time.Unix(0, *timeRange.Start), time.Unix(0, *timeRange.End))
	start := time.Now()
	defer func() {
		log.Printf("Found %d files in: %v", len(relevantFiles), time.Since(start))
	}()

	// Convert nanosecond timestamps to time.Time for directory parsing
	var opts metadata.QueryOptions
	if timeRange.Start != nil {
		opts.After = time.Unix(0, *timeRange.Start)
	}

	if timeRange.End != nil {
		opts.Before = time.Unix(0, *timeRange.End)
	}

	idx, err := getIndex(dbName, measurement)
	if err != nil {
		return nil, err
	}

	ies, err := idx.GetQuerier().Query(opts)
	for _, ie := range ies {
		relevantFiles = append(relevantFiles, filepath.Join(config.Config.Gigapi.Root, dbName, measurement, "data", ie.Path))
	}

	if len(relevantFiles) == 0 {
		log.Printf("No files found in any directory for %s.%s", dbName, measurement)
	}

	return relevantFiles, nil
}

// Find all files for a measurement
func (q *QueryClient) findAllFiles(ctx context.Context, dbName, measurement string) ([]string, error) {
	var allFiles []string
	basePath := filepath.Join(q.DataDir, dbName, measurement)

	// log.Printf("Getting all files for %s.%s", dbName, measurement)
	start := time.Now()
	defer func() {
		log.Printf("Found %d files in: %v", len(allFiles), time.Since(start))
	}()

	if _, err := os.Stat(basePath); os.IsNotExist(err) {
		return allFiles, nil
	}

	// Recursively find all parquet files
	err := filepath.Walk(basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip errors
		}

		if info.Name() == "tmp" && info.IsDir() {
			// Skipping tmp directory as it may include half-created parquet files
			return filepath.SkipDir
		}

		if info.IsDir() {
			if _, err := os.Stat(filepath.Join(path, "metadata.json")); err == nil {
				metadataBytes, err := os.ReadFile(filepath.Join(path, "metadata.json"))
				if err == nil {
					var metadata MetadataFile
					if err := json.Unmarshal(metadataBytes, &metadata); err == nil {
						for _, file := range metadata.Files {
							if _, err := os.Stat(file.Path); err == nil {
								allFiles = append(allFiles, file.Path)
							} else {
								// Try relative path
								relPath := filepath.Join(filepath.Dir(path), filepath.Base(file.Path))
								if _, err := os.Stat(relPath); err == nil {
									allFiles = append(allFiles, relPath)
								}
							}
						}
					}
				}
				return filepath.SkipDir
			}
		}

		if !info.IsDir() {
			if strings.HasSuffix(info.Name(), ".parquet") {
				// Direct parquet file
				allFiles = append(allFiles, path)
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return allFiles, nil
}

// Get date directories in range
func (q *QueryClient) getDateDirectoriesInRange(dbName, measurement string, startDate, endDate time.Time) ([]string, error) {
	basePath := filepath.Join(q.DataDir, dbName, measurement)
	if _, err := os.Stat(basePath); os.IsNotExist(err) {
		return nil, err
	}

	entries, err := os.ReadDir(basePath)
	if err != nil {
		return nil, err
	}

	var dateDirs []string
	datePattern := regexp.MustCompile(`^date=(.+)$`)

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		matches := datePattern.FindStringSubmatch(entry.Name())
		if len(matches) < 2 {
			continue
		}

		dateStr := matches[1]
		dirDate, err := time.Parse("2006-01-02", dateStr)
		if err != nil {
			continue
		}

		// Check if directory date is within range
		dirDateOnly := time.Date(dirDate.Year(), dirDate.Month(), dirDate.Day(), 0, 0, 0, 0, time.UTC)
		startDateOnly := time.Date(startDate.Year(), startDate.Month(), startDate.Day(), 0, 0, 0, 0, time.UTC)
		endDateOnly := time.Date(endDate.Year(), endDate.Month(), endDate.Day(), 0, 0, 0, 0, time.UTC)

		if !dirDateOnly.Before(startDateOnly) && !dirDateOnly.After(endDateOnly) {
			dateDirs = append(dateDirs, entry.Name())
		}
	}

	return dateDirs, nil
}

// Get hour directories in range
func (q *QueryClient) getHourDirectoriesInRange(datePath string, startDate, endDate time.Time) ([]string, error) {
	if _, err := os.Stat(datePath); os.IsNotExist(err) {
		return nil, err
	}

	entries, err := os.ReadDir(datePath)
	if err != nil {
		return nil, err
	}

	var hourDirs []string
	hourPattern := regexp.MustCompile(`^hour=(\d+)$`)
	datePattern := regexp.MustCompile(`^date=(.+)$`)

	dateMatches := datePattern.FindStringSubmatch(filepath.Base(datePath))
	if len(dateMatches) < 2 {
		return hourDirs, nil
	}

	dateStr := dateMatches[1]
	dirDate, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return hourDirs, nil
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		matches := hourPattern.FindStringSubmatch(entry.Name())
		if len(matches) < 2 {
			continue
		}

		hour := 0
		fmt.Sscanf(matches[1], "%d", &hour)

		// Same day comparison logic
		if dirDate.Year() == startDate.Year() && dirDate.Month() == startDate.Month() && dirDate.Day() == startDate.Day() &&
			dirDate.Year() == endDate.Year() && dirDate.Month() == endDate.Month() && dirDate.Day() == endDate.Day() {
			// Same day, filter by hour
			if hour >= startDate.Hour() && hour <= endDate.Hour() {
				hourDirs = append(hourDirs, entry.Name())
			}
		} else if dirDate.Year() == startDate.Year() && dirDate.Month() == startDate.Month() && dirDate.Day() == startDate.Day() {
			// Start date, include hours >= start hour
			if hour >= startDate.Hour() {
				hourDirs = append(hourDirs, entry.Name())
			}
		} else if dirDate.Year() == endDate.Year() && dirDate.Month() == endDate.Month() && dirDate.Day() == endDate.Day() {
			// End date, include hours <= end hour
			if hour <= endDate.Hour() {
				hourDirs = append(hourDirs, entry.Name())
			}
		} else {
			// Not start or end date, include all hours
			hourDirs = append(hourDirs, entry.Name())
		}
	}

	return hourDirs, nil
}

// Query executes a query against the database
func (c *QueryClient) Query(ctx context.Context, query, dbName string) ([]map[string]interface{}, error) {
	// Ensure we have a context
	if ctx == nil {
		ctx = context.Background()
	}

	// Clean up the query string
	query = strings.TrimSpace(query)
	query = strings.ReplaceAll(query, "\n", " ")
	query = strings.ReplaceAll(query, "\r", " ")
	query = regexp.MustCompile(`\s+`).ReplaceAllString(query, " ")
	upperQuery := strings.ToUpper(query)

	// Handle special commands
	switch upperQuery {
	case "SHOW DATABASES":
		entries, err := os.ReadDir(c.DataDir)
		if err != nil {
			return nil, fmt.Errorf("failed to read data directory: %v", err)
		}

		results := make([]map[string]interface{}, 0)
		for _, entry := range entries {
			if entry.IsDir() {
				results = append(results, map[string]interface{}{
					"database_name": entry.Name(),
				})
			}
		}
		return results, nil

	case "SHOW TABLES":
		// List directories inside the database folder
		dbPath := filepath.Join(c.DataDir, dbName)
		entries, err := os.ReadDir(dbPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read database directory: %v", err)
		}

		results := make([]map[string]interface{}, 0)
		for _, entry := range entries {
			if entry.IsDir() {
				results = append(results, map[string]interface{}{
					"table_name": entry.Name(),
				})
			}
		}
		return results, nil
	}

	// Parse the query
	parsed, err := c.ParseQuery(query, dbName)
	if err != nil {
		return nil, err
	}

	// Find relevant files
	files, err := c.FindRelevantFiles(ctx, parsed.DbName, parsed.Measurement, parsed.TimeRange)
	if err != nil || len(files) == 0 {
		return nil, fmt.Errorf("no relevant files found for query")
	}

	start := time.Now()

	// Build the DuckDB query
	var filesList strings.Builder
	for i, file := range files {
		if i > 0 {
			filesList.WriteString(", ")
		}
		filesList.WriteString(fmt.Sprintf("'%s'", file))
	}

	// Split the original query and rebuild with file list
	originalParts := strings.SplitN(query, " FROM ", 2)
	var duckdbQuery string

	if len(originalParts) >= 2 {
		// Extract table name pattern to replace
		tablePattern := fmt.Sprintf(`(?:%s\.)?%s\b`, parsed.DbName, parsed.Measurement)
		tableRegex := regexp.MustCompile(tablePattern)
		restOfQuery := tableRegex.ReplaceAllString(originalParts[1], "")

		// Replace any simple timestamp comparisons with epoch_ns
		timestampRegex := regexp.MustCompile(`time\s*(>=|<=|=|>|<)\s*cast\('([^']+)'\s+as\s+timestamp\)`)
		restOfQuery = timestampRegex.ReplaceAllString(restOfQuery, "time $1 epoch_ns('$2'::TIMESTAMP)")

		log.Printf("Modified query part: %s", restOfQuery)

		// Simply replace the FROM clause with our parquet files
		duckdbQuery = fmt.Sprintf("%s FROM read_parquet([%s], union_by_name=true)%s",
			originalParts[0], filesList.String(), restOfQuery)
	} else {
		// Fallback to manually constructing the query
		duckdbQuery = fmt.Sprintf("SELECT %s FROM read_parquet([%s], union_by_name=true)",
			parsed.Columns, filesList.String())
	}

	log.Printf("Created DuckDB query in: %v", time.Since(start))
	start = time.Now()

	// Execute the query
	stmt, err := c.DB.Prepare(duckdbQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare query: %v", err)
	}
	defer stmt.Close()

	rows, err := stmt.Query()
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %v", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %v", err)
	}

	// Prepare result structure
	var result []map[string]interface{}

	// Process rows
	for rows.Next() {
		// Create a slice of interface{} to hold the values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))

		// Set up pointers to each interface{}
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan the row into the values
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("error scanning row: %v", err)
		}

		// Create a map for this row
		row := make(map[string]interface{})

		// Set each column in the map
		for i, col := range columns {
			val := values[i]

			// Handle special cases for counts
			if strings.Contains(col, "count") && val == nil {
				row[col] = 0
			} else {
				row[col] = val
			}
		}

		result = append(result, row)
	}

	log.Printf("Got query result in: %v", time.Since(start))

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %v", err)
	}

	return result, nil
}

// QueryArrow executes a query against DuckDB and returns an Arrow RecordReader and schema
func (c *QueryClient) QueryArrow(ctx context.Context, query string) (array.RecordReader, *arrow.Schema, error) {
	// TODO: Implement Arrow streaming for the current DuckDB Go driver version
	return nil, nil, fmt.Errorf("Arrow streaming not implemented for this DuckDB Go driver version")
}

// Close releases resources
func (q *QueryClient) Close() error {
	if q.DB != nil {
		return q.DB.Close()
	}
	return nil
}
