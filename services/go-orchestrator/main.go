package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
)

const maxBodyBytes = 1 << 20

var cLanguagePattern = regexp.MustCompile(`(?i)\bc\+\+|\bc#|\bc\b`)

type server struct {
	crawlerURLs  map[string]string
	crawlerURL   string
	discoveryURL string
	embeddingURL string
	client       *http.Client
}

type domainCandidate struct {
	Domain          string  `json:"domain"`
	Captures        int64   `json:"captures"`
	DiscoverySource string  `json:"discovery_source,omitempty"`
	Category        string  `json:"category,omitempty"`
	ExampleURL      string  `json:"example_url,omitempty"`
	Similarity      float32 `json:"similarity,omitempty"`
}

type candidateURL struct {
	URL             string `json:"url"`
	Domain          string `json:"domain"`
	MatchScore      int    `json:"match_score,omitempty"`
	DiscoverySource string `json:"discovery_source,omitempty"`
}

type commonCrawlDiscovery struct {
	CrawlID       string            `json:"crawl_id"`
	Domains       []domainCandidate `json:"domains"`
	CandidateURLs []candidateURL    `json:"candidate_urls"`
	Method        string            `json:"method"`
}

type indexedURLEmbedding struct {
	URL       string    `json:"url"`
	Embedding []float64 `json:"embedding"`
}

func main() {
	sharedCrawlerURL := strings.TrimRight(env("CRAWLER_SERVICE_URL", ""), "/")
	legacyCrawlerURL := env("CRAWLER_SERVICE_URL", "http://localhost:8081/api/v1/search")
	crawlerURLs := map[string]string{
		"technology": env("CRAWLER_TECHNOLOGY_URL", legacyCrawlerURL),
		"health":     env("CRAWLER_HEALTH_URL", "http://localhost:8082/api/v1/search"),
	}
	discoveryURL := env("COMMONCRAWL_DISCOVERY_URL", "http://localhost:8000/internal/v1/commoncrawl/discover")
	embeddingURL := env("COMMONCRAWL_EMBED_URLS", "http://localhost:8000/internal/v1/commoncrawl/embed-urls")
	s := &server{crawlerURLs: crawlerURLs, crawlerURL: sharedCrawlerURL, discoveryURL: discoveryURL, embeddingURL: embeddingURL, client: &http.Client{Timeout: 30 * time.Minute}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("POST /api/v1/search", s.search)
	mux.HandleFunc("GET /api/v1/crawl-jobs/{job_id}", s.crawlJobStatus)
	mux.HandleFunc("POST /api/v1/pages/extract", s.extractPage)
	// Render provides PORT; retain :8080 for local development.
	addr := env("LISTEN_ADDR", ":"+env("PORT", "8080"))
	log.Printf("Go orchestrator listening on %s; shared crawler=%s; technology crawler=%s; health crawler=%s; discovery=%s; embedding=%s", addr, sharedCrawlerURL, crawlerURLs["technology"], crawlerURLs["health"], discoveryURL, embeddingURL)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func (s *server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "healthy", "service": "go-orchestrator"})
}

// Return stored matches immediately. If there are no matches, expose candidate
// domains and enqueue a persistent Rust crawl job instead of waiting on CDX.
func (s *server) search(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	data, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil || len(data) == 0 || len(data) > maxBodyBytes || !json.Valid(data) {
		writeError(w, http.StatusBadRequest, "Request must be valid JSON no larger than 1 MiB")
		return
	}
	var analysis map[string]any
	if err := json.Unmarshal(data, &analysis); err != nil {
		writeError(w, http.StatusBadRequest, "Request must be a JSON object")
		return
	}
	if analysis == nil {
		writeError(w, http.StatusBadRequest, "Request must be a JSON object")
		return
	}

	// The Python embedding classifier selects the sector and database. Go only
	// forwards that decision to the shared Rust service.
	queryText := stringValue(analysis["normalized_query"])
	if strings.TrimSpace(queryText) == "" {
		queryText = stringValue(analysis["original_query"])
	}
	keywords := preserveLanguageKeywords(analysis["keywords"], queryText)
	analysis["keywords"] = keywords
	sectorGroup := strings.ToLower(strings.TrimSpace(stringValue(analysis["sector_group"])))
	if sectorGroup == "" {
		sectorGroup = strings.ToLower(strings.TrimSpace(stringValue(analysis["sector"])))
	}
	databaseTarget := "technology"
	if sectorGroup == "health" {
		databaseTarget = "health"
	}
	analysis["database_target"] = databaseTarget
	analysis["sector_group"] = databaseTarget
	analysis["metadata_only"] = false
	analysis["defer_crawl"] = true
	if plan, ok := analysis["search_plan"].(map[string]any); ok {
		plan["source"] = databaseTarget
		plan["strategy"] = "database_then_common_crawl_url_metadata"
	}
	routedBody, err := json.Marshal(analysis)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Could not prepare routed query")
		return
	}
	responseAnalysis := routedBody
	crawlerURL := s.crawlerURLs[databaseTarget]
	if s.crawlerURL != "" {
		crawlerURL = s.crawlerURL
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Minute)
	defer cancel()
	dbBody, status, err := s.postJSON(ctx, crawlerURL, routedBody)
	if err != nil {
		log.Printf("Rust database search failed: %v", err)
		writeError(w, http.StatusBadGateway, "Rust crawler/database service is unavailable")
		return
	}
	if status < 200 || status >= 300 {
		writeJSON(w, status, json.RawMessage(dbBody))
		return
	}
	var dbResult map[string]any
	if err := json.Unmarshal(dbBody, &dbResult); err != nil {
		writeError(w, http.StatusBadGateway, "Rust crawler returned invalid search JSON")
		return
	}
	for round := 0; round < 3 && boolValue(dbResult["needs_url_embeddings"]); round++ {
		urls := stringList(dbResult["url_embedding_candidates"])
		if len(urls) == 0 {
			writeError(w, http.StatusBadGateway, "Rust requested URL embeddings but returned no candidate URLs")
			return
		}
		payload, _ := json.Marshal(map[string]any{"urls": urls})
		embeddingBody, embeddingStatus, embeddingErr := s.postJSON(ctx, s.embeddingURL, payload)
		if embeddingErr != nil || embeddingStatus < 200 || embeddingStatus >= 300 {
			log.Printf("Candidate URL embedding failed: status=%d error=%v", embeddingStatus, embeddingErr)
			writeError(w, http.StatusBadGateway, "Python could not embed candidate URLs")
			return
		}
		var embedded struct {
			Embeddings []indexedURLEmbedding `json:"embeddings"`
		}
		if err := json.Unmarshal(embeddingBody, &embedded); err != nil || len(embedded.Embeddings) == 0 {
			writeError(w, http.StatusBadGateway, "Python returned invalid candidate URL embeddings")
			return
		}
		analysis["url_embeddings"] = embedded.Embeddings
		routedBody, err = json.Marshal(analysis)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "Could not prepare URL embedding request")
			return
		}
		dbBody, status, err = s.postJSON(ctx, crawlerURL, routedBody)
		if err != nil || status < 200 || status >= 300 {
			log.Printf("Rust URL-vector search failed: status=%d error=%v", status, err)
			writeError(w, http.StatusBadGateway, "Rust could not store or search candidate URL embeddings")
			return
		}
		if err := json.Unmarshal(dbBody, &dbResult); err != nil {
			writeError(w, http.StatusBadGateway, "Rust returned invalid search JSON after URL embedding")
			return
		}
	}
	if matchedCount(dbResult) > 0 {
		s.respond(w, responseAnalysis, dbBody, nil)
		return
	}
	if hasSemanticCandidates(dbResult) {
		candidates := parseDomainCandidates(dbResult["candidate_domains"])
		var candidateURLs []candidateURL
		discovery := map[string]any{
			"method":  "pgvector-domain-and-url-embeddings",
			"message": "Candidates were ranked from the persistent Common Crawl embedding index.",
		}
		// A broad domain embedding may favor a name such as "macecricket.com"
		// while missing a topic page on Wikipedia. For sports, supplement vector
		// candidates with known reference domains, then let Rust score page URLs.
		topic := strings.ToLower(strings.TrimSpace(stringValue(analysis["sector_id"])))
		if topic == "team_sports" || topic == "individual_sports" {
			body, code, discoveryErr := s.postJSON(ctx, s.discoveryURL, responseAnalysis)
			if discoveryErr == nil && code >= 200 && code < 300 {
				var result commonCrawlDiscovery
				if json.Unmarshal(body, &result) == nil {
					merged := make([]domainCandidate, 0, len(result.Domains)+len(candidates))
					seen := make(map[string]bool)
					for _, candidate := range append(result.Domains, candidates...) {
						key := strings.ToLower(strings.TrimSpace(candidate.Domain))
						if key != "" && !seen[key] {
							candidate.Domain = key
							merged = append(merged, candidate)
							seen[key] = true
						}
					}
					candidates = merged
					candidateURLs = result.CandidateURLs
					discovery = map[string]any{
						"method":         result.Method,
						"crawl_id":       result.CrawlID,
						"domains":        candidates,
						"candidate_urls": candidateURLs,
						"message":        "Topic reference domains and semantic candidates are queued for page-level matching.",
					}
				}
			}
		}
		s.enqueueCrawl(w, ctx, crawlerURL, analysis, responseAnalysis, dbResult, candidates, discovery)
		return
	}
	if registered, ok := dbResult["registered_domains"].([]any); ok && len(registered) > 0 {
		candidates := parseDomainCandidates(dbResult["candidate_domains"])
		if len(candidates) == 0 {
			for _, value := range registered {
				if domain, ok := value.(string); ok && strings.TrimSpace(domain) != "" {
					candidates = append(candidates, domainCandidate{Domain: domain, DiscoverySource: "postgres_registered_domain"})
				}
			}
		}
		discovery := map[string]any{"method": "postgres-registered-domains", "domains": candidates}
		body, code, discoveryErr := s.postJSON(ctx, s.discoveryURL, responseAnalysis)
		if discoveryErr == nil && code >= 200 && code < 300 {
			var result commonCrawlDiscovery
			if json.Unmarshal(body, &result) == nil {
				seen := make(map[string]bool)
				merged := make([]domainCandidate, 0, len(candidates)+len(result.Domains))
				for _, candidate := range append(candidates, result.Domains...) {
					key := strings.ToLower(strings.TrimSpace(candidate.Domain))
					if key != "" && !seen[key] {
						candidate.Domain = key
						merged = append(merged, candidate)
						seen[key] = true
					}
				}
				candidates = merged
				discovery = map[string]any{"method": result.Method, "crawl_id": result.CrawlID, "domains": candidates, "candidate_urls": result.CandidateURLs}
			}
		}
		s.enqueueCrawl(w, ctx, crawlerURL, analysis, responseAnalysis, dbResult, candidates, map[string]any{
			"method":         "postgres-registered-domains",
			"domains":        candidates,
			"message":        "Related registered domains were found. Common Crawl URL lookup is queued.",
			"candidate_urls": discovery["candidate_urls"],
		})
		return
	}

	discoveryBody, discoveryStatus, err := s.postJSON(ctx, s.discoveryURL, responseAnalysis)
	if err != nil || discoveryStatus < 200 || discoveryStatus >= 300 {
		log.Printf("Common Crawl domain discovery unavailable: status=%d error=%v", discoveryStatus, err)
		s.respond(w, responseAnalysis, dbBody, map[string]any{"status": "unavailable", "message": "Parquet URL-index domain discovery failed; returning database matches."})
		return
	}
	var discovery commonCrawlDiscovery
	if err := json.Unmarshal(discoveryBody, &discovery); err != nil {
		writeError(w, http.StatusBadGateway, "Python discovery service returned invalid JSON")
		return
	}
	candidates := make([]domainCandidate, 0, len(discovery.Domains))
	for _, candidate := range discovery.Domains {
		if d := strings.TrimSpace(candidate.Domain); d != "" {
			candidate.Domain = d
			candidates = append(candidates, candidate)
		}
	}
	if len(candidates) == 0 {
		s.respond(w, responseAnalysis, dbBody, map[string]any{"crawl_id": discovery.CrawlID, "domains": []string{}, "candidate_urls": discovery.CandidateURLs, "message": "No candidate domains matched the query in Common Crawl's URL index."})
		return
	}

	analysis["candidate_domains"] = candidates
	if discovery.CrawlID != "" {
		analysis["index_collection"] = discovery.CrawlID
	}
	s.enqueueCrawl(w, ctx, crawlerURL, analysis, responseAnalysis, dbResult, candidates,
		map[string]any{"method": discovery.Method, "crawl_id": discovery.CrawlID, "domains": candidates, "candidate_urls": discovery.CandidateURLs})
}

func parseDomainCandidates(value any) []domainCandidate {
	data, err := json.Marshal(value)
	if err != nil || string(data) == "null" {
		return nil
	}
	var candidates []domainCandidate
	if json.Unmarshal(data, &candidates) != nil {
		return nil
	}
	return candidates
}

func (s *server) enqueueCrawl(w http.ResponseWriter, ctx context.Context, crawlerURL string, analysis map[string]any, responseAnalysis []byte, searchResult map[string]any, candidates []domainCandidate, discovery any) {
	if len(candidates) == 0 {
		s.respond(w, responseAnalysis, mustJSON(searchResult), discovery)
		return
	}
	analysis["candidate_domains"] = candidates
	analysis["metadata_only"] = false
	analysis["defer_crawl"] = false
	jobPayload, err := json.Marshal(analysis)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Could not prepare Common Crawl job")
		return
	}
	jobURL := strings.TrimSuffix(crawlerURL, "/api/v1/search") + "/api/v1/crawl-jobs"
	jobBody, status, err := s.postJSON(ctx, jobURL, jobPayload)
	if err != nil || status < 200 || status >= 300 {
		log.Printf("Could not queue Common Crawl crawl job: status=%d error=%v", status, err)
		searchResult["crawl_status"] = "unavailable"
		searchResult["message"] = "Candidate domains were found, but the background crawl could not be queued."
		s.respond(w, responseAnalysis, mustJSON(searchResult), discovery)
		return
	}
	var job struct {
		JobID  int64  `json:"job_id"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(jobBody, &job); err != nil || job.JobID <= 0 {
		writeError(w, http.StatusBadGateway, "Rust returned an invalid crawl job")
		return
	}
	searchResult["candidate_domains"] = candidates
	searchResult["crawl_job_id"] = job.JobID
	searchResult["crawl_status"] = job.Status
	searchResult["message"] = "Candidate domains are shown now. Common Crawl is finding and storing matching page URLs in the background."
	s.respond(w, responseAnalysis, mustJSON(searchResult), discovery)
}

func mustJSON(value any) []byte {
	data, _ := json.Marshal(value)
	return data
}

func (s *server) crawlJobStatus(w http.ResponseWriter, r *http.Request) {
	jobID := r.PathValue("job_id")
	if jobID == "" || strings.Trim(jobID, "0123456789") != "" {
		writeError(w, http.StatusBadRequest, "job_id must be numeric")
		return
	}
	query := r.URL.Query()
	base := s.crawlerURL
	if base == "" {
		group := query.Get("sector_group")
		if group != "health" {
			group = "technology"
		}
		base = strings.TrimSuffix(s.crawlerURLs[group], "/api/v1/search")
	} else {
		base = strings.TrimSuffix(base, "/api/v1/search")
	}
	values := url.Values{}
	values.Set("database_name", query.Get("database_name"))
	values.Set("sector_group", query.Get("sector_group"))
	endpoint := base + "/api/v1/crawl-jobs/" + jobID + "?" + values.Encode()
	request, err := http.NewRequestWithContext(r.Context(), http.MethodGet, endpoint, nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Could not prepare crawl status request")
		return
	}
	response, err := s.client.Do(request)
	if err != nil {
		writeError(w, http.StatusBadGateway, "Rust crawl status service is unavailable")
		return
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil || len(body) >= 4<<20 || !json.Valid(body) {
		writeError(w, http.StatusBadGateway, "Rust returned invalid crawl status JSON")
		return
	}
	writeJSON(w, response.StatusCode, json.RawMessage(body))
}

func preserveLanguageKeywords(raw any, query string) []string {
	keywords := make([]string, 0, 24)
	if values, ok := raw.([]any); ok {
		for _, value := range values {
			if word, ok := value.(string); ok {
				word = strings.ToLower(strings.TrimSpace(word))
				if word != "" && !containsString(keywords, word) {
					keywords = append(keywords, word)
				}
			}
		}
	}
	for _, match := range cLanguagePattern.FindAllString(query, -1) {
		match = strings.ToLower(strings.TrimSpace(match))
		if match != "" && !containsString(keywords, match) {
			keywords = append(keywords, match)
		}
	}
	return keywords
}

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

// Page bodies are retrieved only when a caller explicitly requests extraction.
// The capture metadata must already exist in the selected sector database.
func (s *server) extractPage(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	data, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil || len(data) == 0 || len(data) > maxBodyBytes || !json.Valid(data) {
		writeError(w, http.StatusBadRequest, "Request must be valid JSON no larger than 1 MiB")
		return
	}
	var request map[string]any
	if err := json.Unmarshal(data, &request); err != nil {
		writeError(w, http.StatusBadRequest, "Request must be a JSON object")
		return
	}
	if request == nil || strings.TrimSpace(stringValue(request["url"])) == "" {
		writeError(w, http.StatusBadRequest, "Request must include a URL")
		return
	}
	target := "technology"
	if strings.EqualFold(strings.TrimSpace(stringValue(request["sector"])), "health") {
		target = "health"
	}
	crawlerURL := s.crawlerURLs[target]
	if s.crawlerURL != "" {
		crawlerURL = s.crawlerURL
	}
	extractURL := strings.TrimSuffix(crawlerURL, "/api/v1/search") + "/api/v1/pages/extract"
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Minute)
	defer cancel()
	body, status, err := s.postJSON(ctx, extractURL, data)
	if err != nil {
		log.Printf("Rust page extraction failed: %v", err)
		writeError(w, http.StatusBadGateway, "Rust page extraction service is unavailable")
		return
	}
	writeJSON(w, status, json.RawMessage(body))
}

func (s *server) postJSON(ctx context.Context, endpoint string, data []byte) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(data))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, resp.StatusCode, err
	}
	if len(body) >= 4<<20 || !json.Valid(body) {
		return nil, resp.StatusCode, io.ErrUnexpectedEOF
	}
	return body, resp.StatusCode, nil
}

func matchedCount(result map[string]any) int {
	value, ok := result["matched_count"].(float64)
	if !ok {
		return 0
	}
	return int(value)
}

func boolValue(value any) bool {
	flag, _ := value.(bool)
	return flag
}

func stringList(value any) []string {
	items, ok := value.([]any)
	if !ok {
		return nil
	}
	result := make([]string, 0, len(items))
	for _, item := range items {
		if text, ok := item.(string); ok && strings.TrimSpace(text) != "" {
			result = append(result, text)
		}
	}
	return result
}

func hasSemanticCandidates(result map[string]any) bool {
	items, ok := result["candidate_domains"].([]any)
	if !ok {
		return false
	}
	for _, item := range items {
		candidate, ok := item.(map[string]any)
		if ok && candidate["discovery_source"] == "pgvector_domain_embedding" {
			return true
		}
	}
	return false
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

func (s *server) respond(w http.ResponseWriter, analysis, search []byte, discovery any) {
	result := map[string]any{"status": "success", "analysis": json.RawMessage(analysis), "search": json.RawMessage(search)}
	if discovery != nil {
		result["discovery"] = discovery
	}
	writeJSON(w, http.StatusOK, result)
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
