package querier

import (
	"context"
	"fmt"
	"log"
	"net"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/flight"
	"github.com/apache/arrow/go/v14/arrow/flight/flightsql"
	flightgen "github.com/apache/arrow/go/v14/arrow/flight/gen/flight"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// FlightSQLServer implements the FlightSQL server interface
type FlightSQLServer struct {
	flightgen.UnimplementedFlightServiceServer
	flightsql.BaseServer
	queryClient *QueryClient
	mem         memory.Allocator
	// Add result storage
	results     map[string]arrow.Record
	resultsLock sync.RWMutex
}

// mustEmbedUnimplementedFlightServiceServer implements the FlightServiceServer interface
func (s *FlightSQLServer) mustEmbedUnimplementedFlightServiceServer() {}

// NewFlightSQLServer creates a new FlightSQL server instance
func NewFlightSQLServer(queryClient *QueryClient) *FlightSQLServer {
	return &FlightSQLServer{
		queryClient: queryClient,
		mem:         memory.DefaultAllocator,
		results:     make(map[string]arrow.Record),
	}
}

// PollFlightInfo implements the FlightService interface
func (s *FlightSQLServer) PollFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.PollInfo, error) {
	log.Printf("PollFlightInfo called with descriptor type: %v, path: %v, cmd: %v",
		desc.Type, desc.Path, string(desc.Cmd))
	// For now, we don't support polling flight info
	return nil, nil
}

// ListFlights implements the FlightService interface
func (s *FlightSQLServer) ListFlights(criteria *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	log.Printf("ListFlights called with criteria: %v", criteria)
	// For now, we don't support listing flights
	return nil
}

// ListActions implements the FlightService interface
func (s *FlightSQLServer) ListActions(request *flight.Empty, stream flight.FlightService_ListActionsServer) error {
	log.Printf("ListActions called")
	// For now, we don't support any actions
	return nil
}

// Handshake implements the FlightService interface
func (s *FlightSQLServer) Handshake(stream flight.FlightService_HandshakeServer) error {
	log.Printf("Handshake called")
	// For now, we'll just echo back any handshake request
	for {
		req, err := stream.Recv()
		if err != nil {
			log.Printf("Handshake receive error: %v", err)
			return err
		}

		err = stream.Send(&flight.HandshakeResponse{
			Payload: req.Payload,
		})
		if err != nil {
			log.Printf("Handshake send error: %v", err)
			return err
		}
	}
}

// GetSchema implements the FlightService interface
func (s *FlightSQLServer) GetSchema(ctx context.Context, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	log.Printf("GetSchema called with descriptor type: %v, path: %v, cmd: %v",
		desc.Type, desc.Path, string(desc.Cmd))
	// For now, we don't support schema requests
	return nil, fmt.Errorf("schema requests not supported")
}

// GetFlightInfo implements the FlightService interface
func (s *FlightSQLServer) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	log.Printf("GetFlightInfo called with descriptor type: %v, path: %v, cmd: %v",
		desc.Type, desc.Path, string(desc.Cmd))

	// Handle SQL query command
	if desc.Type == flight.DescriptorCMD {
		// Unmarshal the Any message
		any := &anypb.Any{}
		if err := proto.Unmarshal(desc.Cmd, any); err != nil {
			log.Printf("Failed to unmarshal Any message: %v", err)
			return nil, fmt.Errorf("failed to unmarshal command: %w", err)
		}

		// Check if this is a CommandStatementQuery
		if any.TypeUrl == "type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery" {
			// The query is in the Any message's value
			query := string(any.Value)
			// Clean up the query string
			query = strings.TrimSpace(query)
			query = strings.ReplaceAll(query, "\n", " ")
			query = strings.ReplaceAll(query, "\r", " ")
			query = strings.ReplaceAll(query, "\b", "") // Remove backspace characters
			query = regexp.MustCompile(`\s+`).ReplaceAllString(query, " ")
			// Remove any non-printable characters
			query = strings.Map(func(r rune) rune {
				if r < 32 || r > 126 {
					return -1
				}
				return r
			}, query)
			log.Printf("Executing SQL query: %v", query)

			dbName := "default" // Default database name
			if md, ok := metadata.FromIncomingContext(ctx); ok {
				if bucket := md.Get("bucket"); len(bucket) > 0 {
					dbName = bucket[0]
					log.Printf("Using bucket from metadata: %s", dbName)
				} else if namespace := md.Get("database"); len(namespace) > 0 {
					dbName = namespace[0]
					log.Printf("Using database from metadata: %s", dbName)
				} else if namespace := md.Get("namespace"); len(namespace) > 0 {
					dbName = namespace[0]
					log.Printf("Using namespace from metadata: %s", dbName)
				}
			}

			// Use QueryClient.Query which now handles all fallback logic
			results, err := s.queryClient.Query(ctx, query, dbName)
			if err != nil {
				log.Printf("Query execution failed: %v", err)
				return nil, fmt.Errorf("failed to execute query: %w", err)
			}

			// Convert results to Arrow format
			_, recordBatch, err := convertResultsToArrow(results)
			if err != nil {
				log.Printf("Failed to convert results to Arrow format: %v", err)
				return nil, fmt.Errorf("failed to convert results to Arrow format: %w", err)
			}

			// Generate a unique ticket
			ticketID := fmt.Sprintf("query-%d", time.Now().UnixNano())

			// Store the results
			s.resultsLock.Lock()
			s.results[ticketID] = recordBatch
			s.resultsLock.Unlock()

			// Create a ticket for the results
			ticket := &flight.Ticket{
				Ticket: []byte(ticketID),
			}

			// Create the flight info
			info := &flight.FlightInfo{
				FlightDescriptor: desc,
				Endpoint: []*flight.FlightEndpoint{
					{
						Ticket: ticket,
						Location: []*flight.Location{
							{
								Uri: "grpc://localhost:8082",
							},
						},
					},
				},
				TotalRecords: recordBatch.NumRows(),
				TotalBytes:   -1,
				Schema:       []byte{}, // Empty schema, will be sent in DoGet
			}

			log.Printf("Returning flight info with %d records", recordBatch.NumRows())
			return info, nil
		}
	}

	// For now, we don't support any other flight info requests
	return nil, fmt.Errorf("unsupported flight descriptor type: %v", desc.Type)
}

// GetFlightInfoStatement implements the FlightSQL server interface for executing SQL statements
func (s *FlightSQLServer) GetFlightInfoStatement(ctx context.Context, cmd *flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	log.Printf("GetFlightInfoStatement called with descriptor type: %v, path: %v, cmd: %v",
		desc.Type, desc.Path, string(desc.Cmd))

	// Extract query from command
	query := string(desc.Cmd)

	// Execute the query using our existing QueryClient
	results, err := s.queryClient.Query(ctx, query, "default") // Using default database for now
	if err != nil {
		log.Printf("Query execution failed: %v", err)
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// Convert results to Arrow format
	_, recordBatch, err := convertResultsToArrow(results)
	if err != nil {
		log.Printf("Failed to convert results to Arrow format: %v", err)
		return nil, fmt.Errorf("failed to convert results to Arrow format: %w", err)
	}

	// Create a ticket for the results
	ticket := &flight.Ticket{
		Ticket: []byte("query-results"),
	}

	// Create the flight info
	info := &flight.FlightInfo{
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{
			{
				Ticket: ticket,
				Location: []*flight.Location{
					{
						Uri: "grpc://localhost:8082",
					},
				},
			},
		},
		TotalRecords: recordBatch.NumRows(),
		TotalBytes:   -1,
		Schema:       []byte{}, // Empty schema, will be sent in DoGet
	}

	log.Printf("Returning flight info with %d records", recordBatch.NumRows())
	return info, nil
}

// DoGet implements the FlightSQL server interface for retrieving data
func (s *FlightSQLServer) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	log.Printf("DoGet called with ticket: %v", string(ticket.Ticket))

	// Get the results from storage
	s.resultsLock.RLock()
	recordBatch, exists := s.results[string(ticket.Ticket)]
	s.resultsLock.RUnlock()

	if !exists {
		return fmt.Errorf("no results found for ticket: %s", string(ticket.Ticket))
	}

	// Get the schema from the record batch
	schema := recordBatch.Schema()

	// Write the schema
	writer := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	err := writer.Write(recordBatch)
	if err != nil {
		log.Printf("Failed to write record batch: %v", err)
		return fmt.Errorf("failed to write record batch: %w", err)
	}

	// Clean up the stored results
	s.resultsLock.Lock()
	delete(s.results, string(ticket.Ticket))
	s.resultsLock.Unlock()

	log.Printf("Successfully wrote record batch with %d rows", recordBatch.NumRows())
	return writer.Close()
}

// DoPut implements the FlightService interface
func (s *FlightSQLServer) DoPut(stream flight.FlightService_DoPutServer) error {
	log.Printf("DoPut called")
	// We don't support putting data yet
	return fmt.Errorf("put not supported")
}

// DoAction implements the FlightService interface
func (s *FlightSQLServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	log.Printf("DoAction called with action type: %v", action.Type)
	// We don't support any actions yet
	return fmt.Errorf("action %s not supported", action.Type)
}

// DoExchange implements the FlightService interface
func (s *FlightSQLServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	log.Printf("DoExchange called")
	// We don't support exchange yet
	return fmt.Errorf("exchange not supported")
}

// convertResultsToArrow converts our query results to Arrow format
func convertResultsToArrow(results []map[string]interface{}) (*arrow.Schema, arrow.Record, error) {
	if len(results) == 0 {
		return nil, nil, fmt.Errorf("no results to convert")
	}

	// Get column names from the first row, ensuring "time" is first
	var columnNames []string
	hasTime := false
	for columnName := range results[0] {
		if columnName == "time" {
			hasTime = true
			continue
		}
		columnNames = append(columnNames, columnName)
	}
	sort.Strings(columnNames)
	if hasTime {
		columnNames = append([]string{"time"}, columnNames...)
	}

	// Create schema fields
	fields := make([]arrow.Field, len(columnNames))
	for i, columnName := range columnNames {
		dataType := inferTypeFromColumn(columnName, results)
		fields[i] = arrow.Field{Name: columnName, Type: dataType, Nullable: true}
	}
	schema := arrow.NewSchema(fields, nil)

	// Create builders for each column
	builders := make([]array.Builder, len(columnNames))
	for i, field := range schema.Fields() {
		builders[i] = array.NewBuilder(memory.DefaultAllocator, field.Type)
	}

	// Populate builders with data
	for _, row := range results {
		for i, columnName := range columnNames {
			value := row[columnName]
			if value == nil {
				builders[i].AppendNull()
				continue
			}

			switch builder := builders[i].(type) {
			case *array.TimestampBuilder:
				switch value.(type) {
				case int64:
					ts := value.(int64)
					builder.Append(arrow.Timestamp(ts))
				case string:
					str := value.(string)
					if ts, err := parseTimestamp(str); err == nil {
						builder.Append(arrow.Timestamp(ts))
					} else {
						builder.AppendNull()
					}
				default:
					builder.AppendNull()
				}
			case *array.Int64Builder:
				if v, ok := value.(int64); ok {
					builder.Append(v)
				} else {
					builder.AppendNull()
				}
			case *array.Float64Builder:
				if v, ok := value.(float64); ok {
					builder.Append(v)
				} else {
					builder.AppendNull()
				}
			case *array.BooleanBuilder:
				if v, ok := value.(bool); ok {
					builder.Append(v)
				} else {
					builder.AppendNull()
				}
			case *array.StringBuilder:
				if v, ok := value.(string); ok {
					builder.Append(v)
				} else {
					builder.Append(fmt.Sprintf("%v", value))
				}
			default:
				return nil, nil, fmt.Errorf("unsupported builder type for column %s", columnName)
			}
		}
	}

	// Create arrays from builders
	arrays := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
		defer arrays[i].Release()
		builder.Release()
	}

	// Create record
	record := array.NewRecord(schema, arrays, int64(len(results)))
	return schema, record, nil
}

func parseTimestamp(s string) (int64, error) {
	formats := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02T15:04:05.999999999",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t.UnixNano(), nil
		}
	}
	return 0, fmt.Errorf("could not parse timestamp: %s", s)
}

// inferTypeFromColumn attempts to infer the Arrow type for a column by looking at non-null values
func inferTypeFromColumn(columnName string, results []map[string]interface{}) arrow.DataType {
	// Time-related columns should always be timestamps
	if columnName == "time" || columnName == "time_str" || columnName == "time_int" {
		return &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
	}

	// For other columns, infer type from the first non-nil value
	for _, row := range results {
		if value, exists := row[columnName]; exists && value != nil {
			switch value.(type) {
			case int64:
				return arrow.PrimitiveTypes.Int64
			case float64:
				return arrow.PrimitiveTypes.Float64
			case bool:
				return arrow.FixedWidthTypes.Boolean
			case string:
				return arrow.BinaryTypes.String
			default:
				// If we encounter an unknown type, convert it to string
				return arrow.BinaryTypes.String
			}
		}
	}

	// If all values are nil, default to string type
	return arrow.BinaryTypes.String
}

var s *grpc.Server

func StopFlightSQLServer() {
	if s != nil {
		s.Stop()
	}
}

// StartFlightSQLServer starts the FlightSQL server
func StartFlightSQLServer(port int, queryClient *QueryClient) error {
	server := NewFlightSQLServer(queryClient)
	s = grpc.NewServer()
	flightgen.RegisterFlightServiceServer(s, server)
	reflection.Register(s)

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	log.Printf("FlightSQL server listening on port %d", port)
	return s.Serve(lis)
}
