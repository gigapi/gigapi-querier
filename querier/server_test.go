package querier

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/gigapi/gigapi-config/config"
)

func TestInfluxDB3Compatibility(t *testing.T) {
	config.InitConfig("")
	tempDir, err := os.MkdirTemp("", "gigapi-test-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	server, err := NewServer(tempDir)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer server.Close()

	testQuery := QueryRequest{
		Query: "SELECT 1 as test",
		DB:    "testdb",
	}

	body, _ := json.Marshal(testQuery)

	t.Run("/query endpoint", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/query", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		server.HandleQuery(w, req)
		if w.Code != 200 {
			t.Errorf("Expected 200, got %d", w.Code)
		}
	})

	t.Run("/api/v3/query_sql endpoint", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/api/v3/query_sql", bytes.NewBuffer(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		server.HandleInfluxDB3Query(w, req)
		if w.Code != 200 {
			t.Errorf("Expected 200, got %d", w.Code)
		}
	})

	t.Run("Both endpoints return same response", func(t *testing.T) {
		req1 := httptest.NewRequest("POST", "/query", bytes.NewBuffer(body))
		req1.Header.Set("Content-Type", "application/json")
		w1 := httptest.NewRecorder()
		server.HandleQuery(w1, req1)

		req2 := httptest.NewRequest("POST", "/api/v3/query_sql", bytes.NewBuffer(body))
		req2.Header.Set("Content-Type", "application/json")
		w2 := httptest.NewRecorder()
		server.HandleInfluxDB3Query(w2, req2)

		if w1.Code != w2.Code {
			t.Errorf("Status codes differ: /query=%d, /api/v3/query_sql=%d", w1.Code, w2.Code)
		}
		if w1.Body.String() != w2.Body.String() {
			t.Errorf("Response bodies differ:\n/query: %s\n/api/v3/query_sql: %s", w1.Body.String(), w2.Body.String())
		}
	})
} 