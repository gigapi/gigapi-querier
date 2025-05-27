// queryClient.go
package querier

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gigapi/gigapi-querier/core"
	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/parser/test_driver"
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
	TimeField       string // The field used for time boundaries
}

// TimeRange represents a query time range
type TimeRange struct {
	Start         *int64
	End           *int64
	TimeCondition string
	TimeField     string // The field used for time boundaries
}

// Parse SQL query to extract components
func (q *QueryClient) ParseQuery(sql, dbName string) (*ParsedQuery, error) {
	// Normalize whitespace
	sql = regexp.MustCompile(`\s+`).ReplaceAllString(sql, " ")
	sql = strings.TrimSpace(sql)

	log.Printf("Parsing query: %s", sql)

	p := parser.New()
	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil || len(stmtNodes) == 0 {
		log.Printf("TiDB parser failed: %v, falling back to regex", err)
		// fallback to old logic
		return q.parseQueryRegex(sql, dbName)
	}
	stmt := stmtNodes[0]

	parsed := &ParsedQuery{
		Columns:         "*",
		DbName:          dbName,
		Measurement:     "",
		TimeRange:       TimeRange{},
		WhereConditions: "",
	}

	// Extract table and db from AST
	tableName, schemaName := "", ""
	if selectStmt, ok := stmt.(*ast.SelectStmt); ok {
		if selectStmt.From != nil {
			switch node := selectStmt.From.TableRefs.Left.(type) {
			case *ast.TableSource:
				if tn, ok := node.Source.(*ast.TableName); ok {
					tableName = tn.Name.O
					if tn.Schema.O != "" {
						schemaName = tn.Schema.O
					}
				}
			}
		}
		if schemaName != "" {
			parsed.DbName = schemaName
		}
		parsed.Measurement = tableName
		// Extract columns
		if len(selectStmt.Fields.Fields) > 0 {
			var cols []string
			for _, f := range selectStmt.Fields.Fields {
				cols = append(cols, f.Text())
			}
			parsed.Columns = strings.Join(cols, ", ")
		}
		// Extract WHERE clause as string
		if selectStmt.Where != nil {
			parsed.WhereConditions = selectStmt.Where.Text()
		}
		// Extract time range from WHERE
		tr := extractTimeRangeFromAST(selectStmt.Where)
		parsed.TimeRange = tr
		parsed.TimeField = tr.TimeField
	}

	return parsed, nil
}

// Fallback: old regex-based logic
func (q *QueryClient) parseQueryRegex(sql, dbName string) (*ParsedQuery, error) {
	columnsPattern := regexp.MustCompile(`(?i)SELECT\s+(.*?)\s+FROM`)
	columnsMatch := columnsPattern.FindStringSubmatch(sql)
	columns := "*"
	if len(columnsMatch) > 1 {
		columns = strings.TrimSpace(columnsMatch[1])
	}

	fromPattern := regexp.MustCompile(`(?i)FROM\s+(?:(\w+)\.)?(\w+)`)
	fromMatch := fromPattern.FindStringSubmatch(sql)
	if len(fromMatch) < 3 {
		return nil, fmt.Errorf("invalid query: FROM clause not found or invalid")
	}

	queryDbName := dbName
	if fromMatch[1] != "" {
		queryDbName = fromMatch[1]
	}
	measurement := fromMatch[2]

	whereClause := ""
	whereParts := strings.Split(sql, " WHERE ")
	if len(whereParts) >= 2 {
		whereClause = whereParts[1]
		for _, clause := range []string{" GROUP BY ", " ORDER BY ", " LIMIT ", " HAVING "} {
			if idx := strings.Index(strings.ToUpper(whereClause), clause); idx != -1 {
				whereClause = whereClause[:idx]
			}
		}
	}

	log.Printf("parseQueryRegex: whereClause = %q", whereClause)
	// Minimal regex-based time extraction
	timeRange := TimeRange{}
	var timeField string
	// Support for multiple time field matches (e.g., >= and <= with cast for the same field)
	// We'll scan for all >= and <= (with and without cast) and set the field if both are present for the same field
	fields := make(map[string]int)
	// >= with cast
	geCastRe := regexp.MustCompile(`([a-zA-Z0-9_]+)\s*>=\s*cast\('([\w\-:T\.Z]+)'\s+as\s+timestamp\)`)
	for _, m := range geCastRe.FindAllStringSubmatch(whereClause, -1) {
		fields[m[1]]++
		if start, ok := parseTimeString(m[2]); ok {
			timeRange.Start = &start
		}
	}
	// <= with cast
	leCastRe := regexp.MustCompile(`([a-zA-Z0-9_]+)\s*<=\s*cast\('([\w\-:T\.Z]+)'\s+as\s+timestamp\)`)
	for _, m := range leCastRe.FindAllStringSubmatch(whereClause, -1) {
		fields[m[1]]++
		if end, ok := parseTimeString(m[2]); ok {
			timeRange.End = &end
		}
	}
	// >=
	geRe := regexp.MustCompile(`([a-zA-Z0-9_]+)\s*>=\s+'?([\w\-:T\.Z]+)'?`)
	for _, m := range geRe.FindAllStringSubmatch(whereClause, -1) {
		fields[m[1]]++
		if start, ok := parseTimeString(m[2]); ok {
			timeRange.Start = &start
		}
	}
	// <=
	leRe := regexp.MustCompile(`([a-zA-Z0-9_]+)\s*<=\s+'?([\w\-:T\.Z]+)'?`)
	for _, m := range leRe.FindAllStringSubmatch(whereClause, -1) {
		fields[m[1]]++
		if end, ok := parseTimeString(m[2]); ok {
			timeRange.End = &end
		}
	}
	// BETWEEN (no backreferences)
	betweenRe := regexp.MustCompile(`([a-zA-Z0-9_]+)\s+BETWEEN\s+'?([\w\-:T\.Z]+)'?\s+AND\s+'?([\w\-:T\.Z]+)'?`)
	if m := betweenRe.FindStringSubmatch(whereClause); len(m) == 4 {
		fields[m[1]] += 2
		if start, ok := parseTimeString(m[2]); ok {
			timeRange.Start = &start
		}
		if end, ok := parseTimeString(m[3]); ok {
			timeRange.End = &end
		}
	}
	// = with cast
	eqCastRe := regexp.MustCompile(`([a-zA-Z0-9_]+)\s*=\s*cast\('([\w\-:T\.Z]+)'\s+as\s+timestamp\)`)
	if m := eqCastRe.FindStringSubmatch(whereClause); len(m) == 3 {
		fields[m[1]] += 2
		if val, ok := parseTimeString(m[2]); ok {
			timeRange.Start = &val
			timeRange.End = &val
		}
	}
	// =
	eqRe := regexp.MustCompile(`([a-zA-Z0-9_]+)\s*=\s+'?([\w\-:T\.Z]+)'?`)
	if m := eqRe.FindStringSubmatch(whereClause); len(m) == 3 {
		fields[m[1]] += 2
		if val, ok := parseTimeString(m[2]); ok {
			timeRange.Start = &val
			timeRange.End = &val
		}
	}
	log.Printf("parseQueryRegex: fields = %+v", fields)
	// Pick the field with the highest count (most likely the time field)
	maxCount := 0
	for f, count := range fields {
		if count > maxCount {
			timeField = f
			maxCount = count
		}
	}

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

	timeRange.TimeField = timeField

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
		TimeField:       timeField,
	}, nil
}

// Helper for regex fallback: parse time string as RFC3339 or int64
func parseTimeString(s string) (int64, bool) {
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UnixNano(), true
	}
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return i, true
	}
	return 0, false
}

// AST-based time range extraction
func extractTimeRangeFromAST(where ast.ExprNode) TimeRange {
	tr := TimeRange{}
	if where == nil {
		return tr
	}
	var start, end *int64
	var timeField string

	var walk func(n ast.Node)
	walk = func(n ast.Node) {
		if n == nil {
			return
		}
		// Handle binary operations (>=, <=, =, AND, OR)
		if binOp, ok := n.(*ast.BinaryOperationExpr); ok {
			if col, ok := binOp.L.(*ast.ColumnNameExpr); ok {
				if ts, ok := isValueExpr(binOp.R); ok {
					timeField = col.Name.Name.O
					switch binOp.Op {
					case opcode.GE, opcode.GT:
						start = &ts
					case opcode.LE, opcode.LT:
						end = &ts
					case opcode.EQ:
						start = &ts
						end = &ts
					}
				}
			}
			// Always recurse into L and R for all binary ops
			walk(binOp.L)
			walk(binOp.R)
			return
		}
		// Handle BETWEEN
		if between, ok := n.(*ast.BetweenExpr); ok {
			if col, ok := between.Expr.(*ast.ColumnNameExpr); ok {
				if low, ok := isValueExpr(between.Left); ok {
					if high, ok2 := isValueExpr(between.Right); ok2 {
						timeField = col.Name.Name.O
						start = &low
						end = &high
					}
				}
			}
			walk(between.Expr)
			walk(between.Left)
			walk(between.Right)
			return
		}
		// Handle IN/NOT IN (not used for time, but for completeness)
		if inExpr, ok := n.(*ast.PatternInExpr); ok {
			walk(inExpr.Expr)
			for _, v := range inExpr.List {
				walk(v)
			}
			return
		}
		// Handle unary operations
		if logic, ok := n.(*ast.UnaryOperationExpr); ok {
			walk(logic.V)
			return
		}
		// Handle function calls
		if logic, ok := n.(*ast.FuncCallExpr); ok {
			for _, arg := range logic.Args {
				walk(arg)
			}
			return
		}
	}
	walk(where)
	tr.Start = start
	tr.End = end
	tr.TimeField = timeField
	return tr
}

// Helper to check if node is a ValueExpr and extract int64 or RFC3339 string
func isValueExpr(n ast.Node) (int64, bool) {
	valExpr, ok := n.(*test_driver.ValueExpr)
	if !ok || valExpr == nil {
		return 0, false
	}
	v := valExpr.Datum.GetValue()
	switch t := v.(type) {
	case string:
		tm, err := time.Parse(time.RFC3339, t)
		if err == nil {
			return tm.UnixNano(), true
		}
		if i, err := strconv.ParseInt(t, 10, 64); err == nil {
			return i, true
		}
		return 0, false
	case int64:
		return t, true
	case uint64:
		return int64(t), true
	case float64:
		return int64(t), true
	}
	return 0, false
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

// Find relevant parquet files based on time range
func (q *QueryClient) FindRelevantFiles(ctx context.Context, dbName, measurement string,
	timeRange TimeRange) ([]string, error) {
	// If no time range specified, get all files
	if timeRange.Start == nil && timeRange.End == nil {
		log.Printf("No time range specified, getting all files for %s.%s", dbName, measurement)
		return q.findAllFiles(ctx, dbName, measurement)
	}

	var relevantFiles []string
	// log.Printf("Getting relevant files for %s.%s within time range %v to %v", dbName, measurement,
	// time.Unix(0, *timeRange.Start), time.Unix(0, *timeRange.End))
	start := time.Now()
	defer func() {
		log.Printf("Found %d files in: %v", len(relevantFiles), time.Since(start))
	}()

	// Convert nanosecond timestamps to time.Time for directory parsing
	var startDate, endDate time.Time
	if timeRange.Start != nil {
		startDate = time.Unix(0, *timeRange.Start)
	} else {
		startDate = time.Unix(0, 0) // Beginning of epoch
	}

	if timeRange.End != nil {
		endDate = time.Unix(0, *timeRange.End)
	} else {
		endDate = time.Now() // Current time
	}

	// Get all date directories that might contain relevant data
	// log.Printf("Looking for date directories between %v and %v", startDate, endDate)
	dateDirectories, err := q.getDateDirectoriesInRange(dbName, measurement, startDate, endDate)
	if err != nil {
		log.Printf("Failed to get date directories: %v", err)
		return nil, err
	}
	// log.Printf("Found %d date directories", len(dateDirectories))

	for _, dateDir := range dateDirectories {
		// For each date directory, get all hour directories
		datePath := filepath.Join(q.DataDir, dbName, measurement, dateDir)
		// log.Printf("Processing date directory: %s", datePath)

		hourDirs, err := q.getHourDirectoriesInRange(datePath, startDate, endDate)
		if err != nil {
			log.Printf("Failed to get hour directories for %s: %v", datePath, err)
			continue // Skip this directory on error
		}
		// log.Printf("Found %d hour directories in %s", len(hourDirs), dateDir)

		for _, hourDir := range hourDirs {
			hourPath := filepath.Join(datePath, hourDir)
			// log.Printf("Processing hour directory: %s", hourPath)

			// Read metadata.json
			metadataPath := filepath.Join(hourPath, "metadata.json")
			if _, err := os.Stat(metadataPath); err == nil {
				// log.Printf("Found metadata.json in %s", hourPath)
				_relevantFiles, err := q.enumFolderWithMetadata(metadataPath, timeRange)
				if err == nil {
					// log.Printf("Found %d files in metadata.json", len(_relevantFiles))
					relevantFiles = append(relevantFiles, _relevantFiles...)
					continue
				}
				log.Printf("Failed to read metadata.json: %v", err)
			}

			_relevantFiles, err := q.enumFolderNoMetadata(hourPath)
			if err == nil {
				// log.Printf("Found %d files without metadata", len(_relevantFiles))
				relevantFiles = append(relevantFiles, _relevantFiles...)
				continue
			}
			log.Printf("Failed to enumerate folder: %v", err)
		}
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

// Close releases resources
func (q *QueryClient) Close() error {
	if q.DB != nil {
		return q.DB.Close()
	}
	return nil
}
