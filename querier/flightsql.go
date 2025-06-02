package querier

import (
	"context"
	"fmt"
	"log"
	"net"
	"regexp"
	"strings"
	"encoding/base64"
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
}

// mustEmbedUnimplementedFlightServiceServer implements the FlightServiceServer interface
func (s *FlightSQLServer) mustEmbedUnimplementedFlightServiceServer() {}

// NewFlightSQLServer creates a new FlightSQL server instance
func NewFlightSQLServer(queryClient *QueryClient) *FlightSQLServer {
	return &FlightSQLServer{
		queryClient: queryClient,
		mem:         memory.DefaultAllocator,
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

	if desc.Type == flight.DescriptorCMD {
		any := &anypb.Any{}
		if err := proto.Unmarshal(desc.Cmd, any); err != nil {
			log.Printf("Failed to unmarshal Any message: %v", err)
			return nil, fmt.Errorf("failed to unmarshal command: %w", err)
		}
		if any.TypeUrl == "type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery" {
			query := string(any.Value)
			query = strings.TrimSpace(query)
			query = strings.ReplaceAll(query, "\n", " ")
			query = strings.ReplaceAll(query, "\r", " ")
			query = strings.ReplaceAll(query, "\b", "")
			query = regexp.MustCompile(`\s+`).ReplaceAllString(query, " ")
			query = strings.Map(func(r rune) rune {
				if r < 32 || r > 126 {
					return -1
				}
				return r
			}, query)
			log.Printf("Executing SQL query: %v", query)

			dbName := "default"
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

			parsed, err := s.queryClient.ParseQuery(query, dbName)
			if err != nil {
				log.Printf("Failed to parse query: %v", err)
				return nil, fmt.Errorf("failed to parse query: %w", err)
			}
			// Find relevant files (side effect: ensures query is valid)
			_, err = s.queryClient.FindRelevantFiles(ctx, parsed.DbName, parsed.Measurement, parsed.TimeRange)
			if err != nil {
				log.Printf("Failed to find relevant files: %v", err)
				return nil, fmt.Errorf("failed to find relevant files: %w", err)
			}

			// Encode the query and dbName as a ticket (base64)
			ticketPayload := fmt.Sprintf("%s|%s", dbName, query)
			ticket := &flight.Ticket{
				Ticket: []byte(base64.StdEncoding.EncodeToString([]byte(ticketPayload))),
			}

			info := &flight.FlightInfo{
				FlightDescriptor: desc,
				Endpoint: []*flight.FlightEndpoint{{
					Ticket: ticket,
					Location: []*flight.Location{{Uri: "grpc://localhost:8082"}},
				}},
				TotalRecords: -1, // Unknown until DoGet
				TotalBytes:   -1,
				Schema:       []byte{},
			}
			return info, nil
		}
	}
	return nil, fmt.Errorf("unsupported flight descriptor type: %v", desc.Type)
}

// DoGet implements the FlightSQL server interface for retrieving data
func (s *FlightSQLServer) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	log.Printf("DoGet called with ticket: %v", string(ticket.Ticket))
	decoded, err := base64.StdEncoding.DecodeString(string(ticket.Ticket))
	if err != nil {
		return fmt.Errorf("invalid ticket encoding: %w", err)
	}
	parts := strings.SplitN(string(decoded), "|", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid ticket format")
	}
	// dbName := parts[0] // Unused
	query := parts[1]

	// Use the Arrow-native QueryArrow method
	arrowReader, schema, err := s.queryClient.QueryArrow(stream.Context(), query)
	if err != nil {
		return fmt.Errorf("arrow query failed: %w", err)
	}
	defer arrowReader.Release()

	writer := flight.NewRecordWriter(stream, ipc.WithSchema(schema))
	defer writer.Close()

	for arrowReader.Next() {
		rec := arrowReader.Record()
		if err := writer.Write(rec); err != nil {
			return err
		}
	}
	return nil
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
