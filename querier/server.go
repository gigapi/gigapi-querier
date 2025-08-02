// server.go
package querier

import (
	"archive/zip"
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"github.com/gigapi/gigapi-config/config"
	"github.com/gigapi/gigapi-querier/core"
	"github.com/spf13/afero"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

//go:embed ui.zip
var ui []byte

// Server represents the API server
type Server struct {
	QueryClient *QueryClient
	DisableUI   bool
	UIFS        afero.Fs
}

func GetRootDir() string {
	dataDir := os.Getenv("DATA_DIR")
	if dataDir != "" {
		return dataDir
	}
	dataDir = config.Config.Gigapi.Root
	if dataDir != "" {
		return dataDir
	}
	return "./data"
}

// NewServer creates a new server instance
func NewServer(dataDir string) (*Server, error) {
	client := NewQueryClient(dataDir)
	err := client.Initialize()
	if err != nil {
		return nil, err
	}

	disableUI := !config.Config.Gigapi.UI

	memFS := afero.NewMemMapFs()

	// Unzip UI files into memory
	if err := unzipToMemFS(memFS, ui); err != nil {
		return nil, fmt.Errorf("failed to unzip UI: %w", err)
	}

	return &Server{
		QueryClient: client,
		DisableUI:   disableUI,
		UIFS:        memFS,
	}, nil
}

func unzipFileToMemFS(memFS afero.Fs, zipFile *zip.File, absPath string) error {
	rc, err := zipFile.Open()
	if err != nil {
		return err
	}
	defer rc.Close()

	file, err := memFS.Create(absPath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = io.Copy(file, rc)
	return err
}

func unzipToMemFS(memFS afero.Fs, zipData []byte) error {
	zipReader, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		return err
	}

	for _, zipFile := range zipReader.File {
		fpath := filepath.Clean("/" + zipFile.Name)

		root := "/"
		absPath := filepath.Join(root, fpath)
		if !strings.HasPrefix(absPath, filepath.Clean(root)) {
			log.Printf("Skipping file with invalid path: %s", zipFile.Name)
			continue
		}

		if zipFile.FileInfo().IsDir() {
			memFS.MkdirAll(absPath, zipFile.Mode())
			continue
		}
		err = unzipFileToMemFS(memFS, zipFile, absPath)
		if err != nil {
			return err
		}
	}

	return nil
}

// QueryRequest represents a query API request
type QueryRequest struct {
	Query string `json:"query"`
	DB    string `json:"db,omitempty"`
}

// QueryResponse represents a query API response
type QueryResponse struct {
	Results []map[string]interface{} `json:"results"`
}

// ErrorResponse represents an API error response
type ErrorResponse struct {
	Error string `json:"error"`
}

var reqId int32

// addCORSHeaders adds CORS headers to the response
func addCORSHeaders(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
}

// HandleQuery Handles the /query endpoint
func (s *Server) HandleQuery(w http.ResponseWriter, r *http.Request) {
	ctx := core.WithDefaultLogger(r.Context(), fmt.Sprintf("req-%d", atomic.AddInt32(&reqId, 1)))
	// Add CORS headers
	addCORSHeaders(w)

	// Handle preflight requests
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	// Only allow POST
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	format := r.URL.Query().Get("format")
	if format == "" {
		format = "json"
	}

	formatter, ok := formatters[format]
	if !ok {
		sendErrorResponse(w, fmt.Sprintf("Unsupported format: %s", format), http.StatusBadRequest)
		return
	}

	// Parse request body
	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		sendErrorResponse(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.Query == "" {
		sendErrorResponse(w, "Missing query parameter", http.StatusBadRequest)
		return
	}

	// Extract database name from query param or body
	dbName := r.URL.Query().Get("db")
	if dbName == "" {
		dbName = req.DB
	}
	if dbName == "" {
		dbName = "mydb" // Default
	}

	// Execute query
	results, err := s.QueryClient.Query(ctx, req.Query, dbName)
	if err != nil {
		sendErrorResponse(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = formatter(results, w)
	if err != nil {
		log.Printf("Error formatting results: %v", err)
		return
	}
}

// HandleInfluxDB3Query handles the /api/v3/query_sql endpoint for InfluxDB3 compatibility
func (s *Server) HandleInfluxDB3Query(w http.ResponseWriter, r *http.Request) {
	// Simply delegate to the existing HandleQuery method
	s.HandleQuery(w, r)
}

// Send an error response in JSON format
func sendErrorResponse(w http.ResponseWriter, message string, statusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(ErrorResponse{
		Error: message,
	})
}

// Health check endpoint
func (s *Server) HandleHealth(w http.ResponseWriter, r *http.Request) {
	// Add CORS headers
	addCORSHeaders(w)

	// Handle preflight requests
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "ok",
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

// HandleUI serves the main UI page
func (s *Server) HandleUI(w http.ResponseWriter, r *http.Request) {
	if s.DisableUI {
		http.NotFound(w, r)
		return
	}
	addCORSHeaders(w)
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Serve static files from dist
	distFS := afero.NewBasePathFs(s.UIFS, "/dist")
	httpFS := afero.NewHttpFs(distFS)
	fileServer := http.FileServer(httpFS)

	// Try to serve the requested file
	requestedPath := r.URL.Path
	if requestedPath == "/" || requestedPath == "" {
		content, err := distFS.Open("index.html")
		if err != nil {
			log.Printf("Error reading index.html: %v", err)
			http.Error(w, "Internal server error", http.StatusNotFound)
			return
		}
		w.WriteHeader(200)
		io.Copy(w, content)
		return
	}
	// Check if file exists in embedded FS
	_, err := distFS.Stat(path.Clean(requestedPath))
	if err != nil {
		http.Error(w, "Not found", http.StatusNotFound)
		return
	}
	fileServer.ServeHTTP(w, r)
}

// Close the server and release resources
func (s *Server) Close() error {
	return s.QueryClient.Close()
}
