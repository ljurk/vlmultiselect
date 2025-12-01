package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Test parsing tenant and storageNode flags
func TestParseEndpointsFromFlags(t *testing.T) {
	tests := []struct {
		ids     string
		nodes   string
		wantErr bool
		wantLen int
	}{
		{"1:projA,2:projB", "node1.com,node2.com", false, 4},
		{"1:projA", "http://node1.com", false, 1},
		{"1projA", "node1.com", true, 0},
		{"", "", true, 0},
	}

	for _, tt := range tests {
		got, err := parseEndpointsFromFlags(tt.ids, tt.nodes)
		if (err != nil) != tt.wantErr {
			t.Errorf("parseEndpointsFromFlags() error = %v, wantErr %v", err, tt.wantErr)
			continue
		}
		if len(got) != tt.wantLen {
			t.Errorf("expected %d endpoints, got %d", tt.wantLen, len(got))
		}
	}
}

func TestForwardAndMerge_ndjson(t *testing.T) {
	// Fake backend that returns NDJSON
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := io.WriteString(w, `{"a":1}`+"\n"+`{"b":2}`+"\n")
		if err != nil {
			t.Fatalf("failed responding: %v", err)
		}
	}))
	defer server.Close()

	endpoints = []Endpoint{
		{AccountID: "1", ProjectID: "p1", URL: server.URL},
		{AccountID: "2", ProjectID: "p2", URL: server.URL},
	}

	req := httptest.NewRequest("POST", "/select/logsql/query?filter=ok", bytes.NewBuffer([]byte("test payload")))
	req.Header.Set("Content-Type", "application/json")

	got, err := forwardAndMerge(req, "/select/logsql/query", "ndjson")
	if err != nil {
		t.Fatalf("forwardAndMerge failed: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(got)), "\n")
	if len(lines) != 4 {
		t.Errorf("Expected 4 lines of NDJSON, got %d", len(lines))
	}
}

func TestMakeJSONHandler_NDJSON(t *testing.T) {
	endpoints = []Endpoint{{
		AccountID: "1", ProjectID: "p1",
		URL: httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, err := io.WriteString(w, `{"k":"v"}`+"\n")
			if err != nil {
				t.Fatalf("failed responding: %v", err)
			}
		})).URL,
	}}

	req := httptest.NewRequest("POST", "/select/logsql/query", bytes.NewBuffer([]byte("body")))
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	handler := makeJSONHandler("/select/logsql/query", "ndjson")
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v", rr.Code)
	}

	if !strings.Contains(rr.Body.String(), `"k":"v"`) {
		t.Errorf("expected NDJSON body, got %s", rr.Body.String())
	}
}
