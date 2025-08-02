//go:generate go run build_ui.go
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi-querier/core"
	"github.com/gigapi/gigapi-querier/querier"
	"log"
	"net/http"
	"os"
)

func main() {
	config.InitConfig("")

	ctx := core.WithDefaultLogger(context.Background(), "main")
	// Add command line flags
	queryFlag := flag.String("query", "", "Execute a single query and exit")
	dbFlag := flag.String("db", "default", "Database name to query")
	flag.Parse()

	// Get configuration from environment variables
	port := config.Config.HTTP.Port

	flightsqlPort := config.Config.FlightSql.Port

	dataDir := querier.GetRootDir()

	// Create QueryClient
	client := querier.NewQueryClient(dataDir)
	err := client.Initialize()
	if err != nil {
		core.Errorf(ctx, "Failed to initialize query client: %v", err)
		os.Exit(1)
	}
	defer client.Close()

	// If query flag is provided, execute query and exit
	if *queryFlag != "" {
		results, err := client.Query(ctx, *queryFlag, *dbFlag)
		if err != nil {
			log.Fatalf("Query error: %v", err)
		}

		// Process and print results as JSON
		processedResults := querier.ProcessResultsForJSON(results)
		jsonData, err := json.MarshalIndent(processedResults, "", "  ")
		if err != nil {
			log.Fatalf("Failed to marshal results: %v", err)
		}
		fmt.Println(string(jsonData))
		return
	}

	// Create server for HTTP mode
	server, err := querier.NewServer(dataDir)
	if err != nil {
		core.Errorf(ctx, "Failed to initialize server: %v", err)
		os.Exit(1)
	}
	defer server.Close()

	// Create a new mux for routing
	mux := http.NewServeMux()

	// Set up routes
	mux.HandleFunc("/", server.HandleUI) // Serve UI at root path
	mux.HandleFunc("/health", server.HandleHealth)
	mux.HandleFunc("/query", server.HandleQuery)
	mux.HandleFunc("/api/v3/query_sql", server.HandleInfluxDB3Query) // InfluxDB3 compatibility endpoint

	// Start main server
	core.Infof(ctx, "GigAPI server running at http://localhost:%d", port)
	go func() {
		err = http.ListenAndServe(fmt.Sprintf(":%d", port), mux)
		if err != nil {
			core.Errorf(ctx, "Failed to start main server: %v", err)
			os.Exit(1)
		}
	}()

	core.Infof(ctx, "FlightSQL server running on port %s", flightsqlPort)
	err = querier.StartFlightSQLServer(flightsqlPort, client)
	if err != nil {
		core.Errorf(ctx, "Failed to start FlightSQL server: %v", err)
		os.Exit(1)
	}
}
