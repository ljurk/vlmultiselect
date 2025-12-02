package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/qjebbs/go-jsons"
)

type MergeStrategy int

const (
	Merge MergeStrategy = iota
	Sum
)

type Format int

const (
	JSON Format = iota
	NDJSON
)

type Endpoint struct {
	AccountID string
	ProjectID string
	URL       string
}

type Route struct {
	Path          string
	Format        Format
	MergeStrategy MergeStrategy
}

var routes = []Route{
	{"/select/logsql/query", NDJSON, Merge},
	{"/select/logsql/hits", JSON, Merge},
	{"/select/logsql/field_names", JSON, Sum},
	{"/select/logsql/field_values", JSON, Sum},
	{"/select/logsql/facets", JSON, Merge},
	{"/select/logsql/stats_query", JSON, Merge},
	{"/select/logsql/stats_query_range", JSON, Merge},
	{"/select/logsql/stream_ids", JSON, Merge},
	{"/select/logsql/streams", JSON, Merge},
	{"/select/logsql/stream_field_names", JSON, Merge},
	{"/select/logsql/stream_field_values", JSON, Merge},
}

func mergeAndSumJSON(a, b []byte) ([]byte, error) {
	type Item struct {
		Hits  int    `json:"hits"`
		Value string `json:"value"`
	}
	type Payload struct {
		Values []Item `json:"values"`
	}

	var pa, pb Payload
	if err := json.Unmarshal(a, &pa); err != nil {
		return nil, fmt.Errorf("unmarshal a: %w", err)
	}
	if err := json.Unmarshal(b, &pb); err != nil {
		return nil, fmt.Errorf("unmarshal b: %w", err)
	}

	// Map by Value for easy sum
	mergedMap := make(map[string]int)
	for _, item := range pa.Values {
		mergedMap[item.Value] += item.Hits
	}
	for _, item := range pb.Values {
		mergedMap[item.Value] += item.Hits
	}

	// Build merged payload
	merged := Payload{Values: make([]Item, 0, len(mergedMap))}
	for value, hits := range mergedMap {
		merged.Values = append(merged.Values, Item{Hits: hits, Value: value})
	}

	return json.Marshal(merged)
}

func parseEndpointsFromFlags(ids string, nodes string) ([]Endpoint, error) {
	var endpoints []Endpoint
	for storageNode := range strings.SplitSeq(nodes, ",") {
		for id := range strings.SplitSeq(ids, ",") {
			if len(strings.Split(strings.TrimSpace(id), ":")) < 2 {
				return nil, fmt.Errorf("wrong tenant format, use <tenantID>:<projectID>")
			}

			if !strings.HasPrefix(storageNode, "http://") {
				storageNode = "http://" + storageNode
			}

			endpoints = append(endpoints, Endpoint{
				AccountID: strings.Split(strings.TrimSpace(id), ":")[0],
				ProjectID: strings.Split(strings.TrimSpace(id), ":")[1],
				URL:       storageNode,
			})
		}
	}
	return endpoints, nil
}

func main() {
	log.Println("Starting vlmultiselect")
	var idsFlag string
	var nodesFlag string
	flag.StringVar(&nodesFlag, "storageNode", "", "Comma-seperated list of storageNodes")
	flag.StringVar(&idsFlag, "tenants", "", "Comma-separated list of tenant IDs (e.g., 1,2,3)")
	flag.Parse()

	if nodesFlag == "" {
		log.Fatal("-storageNode not set")
	}
	if idsFlag == "" {
		log.Fatal("-tenants not set")
	}
	var err error
	var endpoints []Endpoint
	endpoints, err = parseEndpointsFromFlags(idsFlag, nodesFlag)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	log.Println("configured endpoints:")
	for _, i := range endpoints {
		log.Printf("URL: %s; AccountID: %s; ProjectID: %s\n", i.URL, i.AccountID, i.ProjectID)
	}

	health := func(w http.ResponseWriter, _ *http.Request) {
		_, err = io.WriteString(w, "OK")
		if err != nil {
			log.Fatalf("Error: %v", err)
		}
	}

	http.HandleFunc("/health", health)
	for _, r := range routes {
		route := r // create a new variable scoped to this iteration
		http.HandleFunc(route.Path, makeJSONHandler(route.Path, route.Format, route.MergeStrategy, endpoints))
	}

	log.Println("Listening on :8000")
	log.Fatal(http.ListenAndServe(":8000", nil))
}

func makeJSONHandler(path string, format Format, mergeStrategy MergeStrategy, endpoints []Endpoint) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logRequest(r)

		if format == JSON {
			w.Header().Set("Content-Type", "application/json")
		} else {
			w.Header().Set("Content-Type", "application/x-ndjson")
		}

		data, err := getEndpointData(r, path, endpoints)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		merged, err := mergeData(data, format, mergeStrategy)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if _, err := w.Write(merged); err != nil {
			log.Printf("failed to write response: %v", err)
		}
	}
}

func getEndpointData(r *http.Request, path string, endpoints []Endpoint) ([][]byte, error) {
	// check if request contains a body
	query := r.URL.RawQuery
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("error: failed to read request body: %w", err)
	}
	if err := r.Body.Close(); err != nil {
		log.Printf("warning: failed to close request body: %v", err)
	}
	if len(body) != 0 {
		log.Printf("[REQ] body: %s", body)
	}

	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		errs    = make([]error, len(endpoints))
		results = make([][]byte, len(endpoints))
	)

	for i, endpoint := range endpoints {
		wg.Add(1)
		go func(i int, ep Endpoint) {
			defer wg.Done()

			tempurl := ep.URL + path
			if query != "" {
				tempurl += "?" + query
			}

			req, err := http.NewRequest("POST", tempurl, bytes.NewReader(body))
			if err != nil {
				errs[i] = err
				return
			}
			req.Header.Set("AccountID", ep.AccountID)
			req.Header.Set("ProjectID", ep.ProjectID)
			if ct := r.Header.Get("Content-Type"); ct != "" {
				req.Header.Set("Content-Type", ct)
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				errs[i] = err
				return
			}
			defer func() {
				if err = resp.Body.Close(); err != nil {
					log.Printf("warning: failed to close response body: %v", err)
				}
			}()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				errs[i] = err
				return
			}

			if resp.StatusCode != http.StatusOK {
				errs[i] = fmt.Errorf("%s", body)
				return
			}

			mu.Lock()
			results[i] = body
			mu.Unlock()
		}(i, endpoint)
	}
	wg.Wait()

	for _, e := range errs {
		if e != nil {
			return nil, e
		}
	}
	return results, nil
}

func mergeData(data [][]byte, format Format, mergeStrategy MergeStrategy) ([]byte, error) {
	switch format {
	case JSON:
		merged := []byte(`{}`)
		for _, b := range data {
			var err error
			switch mergeStrategy {
			case Merge:
				merged, err = jsons.Merge(merged, b)
			case Sum:
				merged, err = mergeAndSumJSON(merged, b)
			default:
				log.Fatalf("unknown MergeStrategy: %d", mergeStrategy)
			}

			if err != nil {
				return nil, fmt.Errorf("json merge failed: %w", err)
			}
		}
		return merged, nil

	case NDJSON:
		var merged bytes.Buffer
		for _, b := range data {
			scanner := bufio.NewScanner(bytes.NewReader(b))
			for scanner.Scan() {
				merged.Write(scanner.Bytes())
				merged.WriteByte('\n')
			}
		}
		return merged.Bytes(), nil

	default:
		return nil, fmt.Errorf("unsupported format: %d", format)
	}
}

func logRequest(r *http.Request) {
	log.Printf("[REQ] %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
}
