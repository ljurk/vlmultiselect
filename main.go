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
	"sort"
	"strings"
	"sync"
)

var url string

type Tenant struct {
	TenantID  string
	ProjectID string
}

var tenants []Tenant

func parseTenantsFromFlags(ids string) ([]Tenant, error) {
	if ids == "" {
		return nil, fmt.Errorf("missing required flag: -tenant-ids")
	}

	idList := strings.Split(ids, ",")
	var tenants []Tenant
	for _, id := range idList {
		if len(strings.Split(strings.TrimSpace(id), ":")) < 2 {
			return nil, fmt.Errorf("wrong tenant format, use <tenantID>:<projectID>")
		}

		tenants = append(tenants, Tenant{
			TenantID:  strings.Split(strings.TrimSpace(id), ":")[0],
			ProjectID: strings.Split(strings.TrimSpace(id), ":")[1],
		})
	}
	if len(tenants) == 0 {
		return nil, fmt.Errorf("no valid tenant IDs found in -tenants")
	}
	return tenants, nil
}

func main() {
	var idsFlag string
	flag.StringVar(&url, "url", "", "base URL [default: http://localhost:9428]")
	flag.StringVar(&idsFlag, "tenants", "", "Comma-separated list of tenant IDs (e.g., 1,2,3)")
	flag.Parse()

	if url == "" {
		log.Fatal("-url not set")
	}
	if idsFlag == "" {
		log.Fatal("-tenants not set")
	}
	var err error
	tenants, err = parseTenantsFromFlags(idsFlag)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}

	fmt.Println(tenants)
	fmt.Println(url)
	http.HandleFunc("/select/logsql/query", makeNDJSONHandler("/select/logsql/query"))
	http.HandleFunc("/select/logsql/hits", makeNDJSONHandler("/select/logsql/hits"))
	http.HandleFunc("/select/logsql/field_names", makeJSONValuesHandler("/select/logsql/field_names"))
	http.HandleFunc("/select/logsql/field_values", makeJSONValuesHandler("/select/logsql/field_values"))

	log.Println("Listening on :8000")
	log.Fatal(http.ListenAndServe(":8000", nil))
}

func makeNDJSONHandler(path string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logRequest(r)
		merged, err := forwardAndMergeNDJSON(r, path)
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

func makeJSONValuesHandler(path string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		logRequest(r)
		merged, err := forwardAndMergeJSONArrays(r, path)
		if err != nil {
			http.Error(w, "Error fetching JSON: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if _, err := w.Write(merged); err != nil {
			log.Printf("failed to write response: %v", err)
		}
	}
}

func forwardAndMergeJSONArrays(r *http.Request, path string) ([]byte, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	defer func() {
		if err := r.Body.Close(); err != nil {
			log.Printf("warning: failed to close request body: %v", err)
		}
	}()

	type value struct {
		Value string `json:"value"`
		Hits  int    `json:"hits"`
	}

	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		merged = make(map[string]int)
		errs   = make([]error, len(tenants))
	)

	for i, t := range tenants {
		wg.Add(1)
		go func(i int, t struct {
			TenantID, ProjectID string
		}) {
			defer wg.Done()
			req, err := http.NewRequest("POST", url+path, bytes.NewReader(body))
			if err != nil {
				errs[i] = err
				return
			}
			req.Header.Set("AccountID", t.TenantID)
			req.Header.Set("ProjectID", t.ProjectID)
			req.Header.Set("Content-Type", r.Header.Get("Content-Type"))

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

			var result struct {
				Values []value `json:"values"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				errs[i] = err
				return
			}

			mu.Lock()
			for _, v := range result.Values {
				merged[v.Value] += v.Hits
			}
			mu.Unlock()
		}(i, t)
	}
	wg.Wait()

	for _, e := range errs {
		if e != nil {
			return nil, e
		}
	}

	var final []value
	for val, hits := range merged {
		final = append(final, value{val, hits})
	}
	sort.Slice(final, func(i, j int) bool {
		return final[i].Value < final[j].Value
	})

	return json.Marshal(map[string]any{"values": final})
}

func forwardAndMergeNDJSON(r *http.Request, path string) ([]byte, error) {
	query := r.URL.RawQuery
	logPrefix := strings.TrimPrefix(path, "/select/logsql/")

	var (
		wg   sync.WaitGroup
		bufs = make([][]byte, len(tenants))
		errs = make([]error, len(tenants))
	)

	for i, t := range tenants {
		wg.Add(1)
		go func(i int, t struct {
			TenantID, ProjectID string
		}) {
			defer wg.Done()
			req, err := http.NewRequest("POST", fmt.Sprintf("%s%s?%s", url, path, query), nil)
			if err != nil {
				errs[i] = err
				return
			}
			req.Header.Set("AccountID", t.TenantID)
			req.Header.Set("ProjectID", t.TenantID)
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

			if resp.StatusCode != http.StatusOK {
				errs[i] = fmt.Errorf("[%s] status %d", logPrefix, resp.StatusCode)
				return
			}

			bufs[i], err = io.ReadAll(resp.Body)
			if err != nil {
				errs[i] = err
			}
		}(i, t)
	}
	wg.Wait()

	for _, e := range errs {
		if e != nil {
			return nil, e
		}
	}

	var merged bytes.Buffer
	for _, b := range bufs {
		scanner := bufio.NewScanner(bytes.NewReader(b))
		for scanner.Scan() {
			merged.WriteString(scanner.Text())
			merged.WriteByte('\n')
		}
	}
	log.Printf("[%s] Merged NDJSON size: %d bytes", logPrefix, merged.Len())
	return merged.Bytes(), nil
}

func logRequest(r *http.Request) {
	log.Printf("[REQ] %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
}
