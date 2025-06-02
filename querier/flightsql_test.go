package querier

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/apache/arrow/go/v14/arrow/flight/flightsql"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func startTestFlightSQLServer(qc *QueryClient, port int) (*grpc.Server, error) {
	s := grpc.NewServer()
	_ = NewFlightSQLServer(qc)
	ln, err := net.Listen("tcp", ":"+fmt.Sprint(port))
	if err != nil {
		return nil, err
	}
	go s.Serve(ln)
	// Wait a moment for server to start
	time.Sleep(200 * time.Millisecond)
	return s, nil
}

func TestFlightSQLServer_BasicConnection(t *testing.T) {
	qc := NewQueryClient("/tmp/testdata")
	_ = qc.Initialize()
	defer qc.Close()

	port := 32100
	s, err := startTestFlightSQLServer(qc, port)
	assert.NoError(t, err)
	defer s.Stop()

	client, err := flightsql.NewClient(
		fmt.Sprintf("localhost:%d", port),
		nil, nil,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	assert.NoError(t, err)
	if client != nil {
		assert.NoError(t, client.Close())
	}
} 