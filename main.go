package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/qjebbs/go-jsons"
)

type Endpoint struct {
	AccountID string
	ProjectID string
	Url       string
}

var endpoints []Endpoint

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
				Url:       storageNode,
			})
		}
	}
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("no endpoints parsed")
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
	endpoints, err = parseEndpointsFromFlags(idsFlag, nodesFlag)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	log.Println("configured endpoints:")
	for _, i := range endpoints {
		log.Printf("Url: %s; AccountID: %s; ProjectID: %s\n", i.Url, i.AccountID, i.ProjectID)
	}

	health := func(w http.ResponseWriter, _ *http.Request) {
		_, err = io.WriteString(w, "OK")
		if err != nil {
			log.Fatalf("Error: %v", err)
		}
	}
	http.HandleFunc("/health", health)
	http.HandleFunc("/select/logsql/query", makeJSONHandler("/select/logsql/query", "ndjson"))
	http.HandleFunc("/select/logsql/hits", makeJSONHandler("/select/logsql/hits", "json"))
	http.HandleFunc("/select/logsql/field_names", makeJSONHandler("/select/logsql/field_names", "json"))
	http.HandleFunc("/select/logsql/field_values", makeJSONHandler("/select/logsql/field_values", "json"))
	http.HandleFunc("/select/logsql/facets", makeJSONHandler("/select/logsql/facets", "json"))
	http.HandleFunc("/select/logsql/stats_query", makeJSONHandler("/select/logsql/stats_query", "json"))
	http.HandleFunc("/select/logsql/stats_query_range", makeJSONHandler("/select/logsql/stats_query_range", "json"))
	http.HandleFunc("/select/logsql/stream_ids", makeJSONHandler("/select/logsql/stream_ids", "json"))
	http.HandleFunc("/select/logsql/streams", makeJSONHandler("/select/logsql/stream_ids", "json"))
	http.HandleFunc("/select/logsql/stream_field_names", makeJSONHandler("/select/logsql/stream_field_names", "json"))
	http.HandleFunc("/select/logsql/stream_field_values", makeJSONHandler("/select/logsql/stream_ids", "json"))

	log.Println("Listening on :8000")
	log.Fatal(http.ListenAndServe(":8000", nil))
}

func makeJSONHandler(path string, mode string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logRequest(r)
		merged, err := forwardAndMerge(r, path, mode)
		if err != nil {
			http.Error(w, "Error fetching NDJSON: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/x-ndjson")
		if _, err := w.Write(merged); err != nil {
			log.Printf("failed to write response: %v", err)
		}
	}
}

func forwardAndMerge(r *http.Request, path string, mode string) ([]byte, error) {
	query := r.URL.RawQuery
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
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

	logPrefix := ""
	logPrefix = strings.TrimPrefix(path, "/select/logsql/")

	for i, t := range endpoints {
		wg.Add(1)
		go func(i int, t struct {
			AccountID, ProjectID, Url string
		}) {
			defer wg.Done()

			tempurl := t.Url + path
			if query != "" {
				tempurl += "?" + query
			}

			req, err := http.NewRequest("POST", tempurl, bytes.NewReader(body))
			if err != nil {
				errs[i] = err
				return
			}
			req.Header.Set("AccountID", t.AccountID)
			req.Header.Set("ProjectID", t.ProjectID)
			if ct := r.Header.Get("Content-Type"); ct != "" {
				req.Header.Set("Content-Type", ct)
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				errs[i] = err
				return
			}
			defer func() {
				if err := resp.Body.Close(); err != nil {
					log.Printf("warning: failed to close response body: %v", err)
				}
			}()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				errs[i] = err
				return
			}

			if mode == "ndjson" && resp.StatusCode != http.StatusOK {
				errs[i] = fmt.Errorf("[%s] status %d", logPrefix, resp.StatusCode)
				return
			}

			mu.Lock()
			results[i] = body
			mu.Unlock()
		}(i, t)
	}
	wg.Wait()

	for _, e := range errs {
		if e != nil {
			return nil, e
		}
	}

	switch mode {
	case "json":
		merged := []byte(`{}`)
		for _, b := range results {
			var err error
			merged, err = jsons.Merge(merged, b)
			if err != nil {
				return nil, fmt.Errorf("json merge failed: %w", err)
			}
		}
		return merged, nil

	case "ndjson":
		var merged bytes.Buffer
		for _, b := range results {
			scanner := bufio.NewScanner(bytes.NewReader(b))
			for scanner.Scan() {
				merged.Write(scanner.Bytes())
				merged.WriteByte('\n')
			}
		}
		log.Printf("[%s] Merged NDJSON size: %d bytes", logPrefix, merged.Len())
		return merged.Bytes(), nil

	default:
		return nil, fmt.Errorf("unsupported mode: %s", mode)
	}
}

func logRequest(r *http.Request) {
	log.Printf("[REQ] %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
}
