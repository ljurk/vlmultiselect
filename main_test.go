package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// ensure deterministic order of values[]**
func normalize(m any) {
	obj, ok := m.(map[string]any)
	if !ok {
		return
	}
	rawArr, ok := obj["values"].([]any)
	if !ok {
		return
	}

	sort.Slice(rawArr, func(i, j int) bool {
		mi := rawArr[i].(map[string]any)
		mj := rawArr[j].(map[string]any)
		// Stable deterministic order by "value"
		return mi["value"].(string) < mj["value"].(string)
	})
}

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

func TestForwardAndMerge_json(t *testing.T) {
	tests := []struct {
		comment string
		path    string
		output1 string
		output2 string
		wantErr bool
		want    string
		strat   MergeStrategy
	}{
		{"one value, one to sum",
			"/select/logsql/field_names",
			`{"values":[{"hits":23,"value":"A"}]}`,
			`{"values":[{"hits":23,"value":"A"}]}`,
			false,
			`{"values":[{"hits":46,"value":"A"}]}`,
			Sum},
		{"two values, one to sum",
			"/select/logsql/field_names",
			`{"values":[{"hits":23,"value":"A"}]}`,
			`{"values":[{"hits":23,"value":"A"},{"hits":161,"value":"B"}]}`,
			false,
			`{"values":[{"hits":46,"value":"A"},{"hits":161,"value":"B"}]}`,
			Sum},
		{"two values, two to sum",
			"/select/logsql/field_names",
			`{"values":[{"hits":23,"value":"A"},{"hits":161,"value":"B"}]}`,
			`{"values":[{"hits":23,"value":"A"},{"hits":161,"value":"B"}]}`,
			false,
			`{"values":[{"hits":46,"value":"A"},{"hits":322,"value":"B"}]}`,
			Sum},
		{"invalid output server1",
			"/select/logsql/field_names",
			`foo`,
			`{"values":[{"hits":23,"value":"A"},{"hits":161,"value":"B"}]}`,
			true,
			"",
			Sum},
		{"invalid output server2",
			"/select/logsql/field_names",
			`{"values":[{"hits":23,"value":"A"},{"hits":161,"value":"B"}]}`,
			`foo`,
			true,
			"",
			Sum},
		{"invalid output both",
			"/select/logsql/field_names",
			"foo",
			"foo",
			true,
			"",
			Sum},
		{"other stucture",
			"/foo/bar",
			`{"foo": 2}`,
			`{"bar": 3}`,
			false,
			`{"foo": 2, "bar": 3}`,
			Merge},
	}

	for _, tt := range tests {
		fmt.Printf("Testing [%s]\n", tt.comment)
		server1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, err := io.WriteString(w, tt.output1)
			if err != nil {
				t.Fatalf("failed responding: %v", err)
			}
		}))
		defer server1.Close()

		server2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, err := io.WriteString(w, tt.output2)
			if err != nil {
				t.Fatalf("failed responding: %v", err)
			}
		}))
		defer server2.Close()

		endpoints := []Endpoint{
			{AccountID: "1", ProjectID: "p1", URL: server1.URL},
			{AccountID: "2", ProjectID: "p2", URL: server2.URL},
		}

		req := httptest.NewRequest("POST", fmt.Sprintf("%s?filter=ok", tt.path), bytes.NewBuffer([]byte("test payload")))
		req.Header.Set("Content-Type", "application/json")

		data, err := getEndpointData(req, tt.path, endpoints)

		if err != nil {
			t.Fatalf("getEndpointData() failed: %s", err)
			return
		}
		got, err := mergeData(data, JSON, tt.strat)
		if (err != nil) == tt.wantErr {
			continue
		}
		if err != nil {
			t.Fatalf("mergeData() failed: %s", err)
			return
		}

		var gotMap, wantMap any

		if err := json.Unmarshal(got, &gotMap); err != nil {
			t.Fatalf("json.Unmarshal(got) failed: %v\nraw: %s", err, got)
		}
		if err := json.Unmarshal([]byte(tt.want), &wantMap); err != nil {
			t.Fatalf("json.Unmarshal(want) failed: %v\nraw: %s", err, tt.want)
		}

		normalize(gotMap)
		normalize(wantMap)

		if !reflect.DeepEqual(gotMap, wantMap) {
			t.Errorf("merged JSON mismatch:\n  got:  %v\n  want: %v", gotMap, wantMap)
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

	endpoints := []Endpoint{
		{AccountID: "1", ProjectID: "p1", URL: server.URL},
		{AccountID: "2", ProjectID: "p2", URL: server.URL},
	}

	req := httptest.NewRequest("POST", "/select/logsql/query?filter=ok", bytes.NewBuffer([]byte("test payload")))
	req.Header.Set("Content-Type", "application/json")

	data, err := getEndpointData(req, "/select/logsql/query", endpoints)
	if err != nil {
		t.Fatalf("getEndpointData() failed: %s", err)
		return
	}
	got, err := mergeData(data, NDJSON, Merge)
	if err != nil {
		t.Fatalf("mergeData() failed: %s", err)
		return
	}

	lines := strings.Split(strings.TrimSpace(string(got)), "\n")
	if len(lines) != 4 {
		t.Errorf("Expected 4 lines of NDJSON, got %d", len(lines))
	}
}

func TestMakeJSONHandler_NDJSON(t *testing.T) {
	endpoints := []Endpoint{{
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
	handler := makeJSONHandler("/select/logsql/query", NDJSON, Merge, endpoints)
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v", rr.Code)
	}

	if !strings.Contains(rr.Body.String(), `"k":"v"`) {
		t.Errorf("expected NDJSON body, got %s", rr.Body.String())
	}
}
