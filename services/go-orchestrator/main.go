package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"
)

const maxBodyBytes = 1 << 20

var cLanguagePattern = regexp.MustCompile(`(?i)\bc\+\+|\bc#|\bc\b`)
var healthQueryTerms = []string{
	"health", "healthcare", "medical", "medicine", "heart", "heartattack",
	"cardiac", "cardiovascular", "stroke", "bloodpressure", "diabetes",
	"cancer", "symptom", "disease", "treatment", "diagnosis", "doctor",
	"hospital", "clinic", "vaccine", "infection", "fever", "pregnancy",
	"nutrition", "therapy", "patient", "wellness",
}

type server struct {
	crawlerURLs  map[string]string
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

type indexedURLEmbedding struct {
	URL       string    `json:"url"`
	Embedding []float64 `json:"embedding"`
}

func main() {
	legacyCrawlerURL := env("CRAWLER_SERVICE_URL", "http://localhost:8081/api/v1/search")
	crawlerURLs := map[string]string{
		"technology": env("CRAWLER_TECHNOLOGY_URL", legacyCrawlerURL),
		"health":     env("CRAWLER_HEALTH_URL", "http://localhost:8082/api/v1/search"),
	}
	discoveryURL := env("COMMONCRAWL_DISCOVERY_URL", "http://localhost:8000/internal/v1/commoncrawl/discover")
	embeddingURL := env("COMMONCRAWL_EMBED_URLS", "http://localhost:8000/internal/v1/commoncrawl/embed-urls")
	s := &server{crawlerURLs: crawlerURLs, discoveryURL: discoveryURL, embeddingURL: embeddingURL, client: &http.Client{Timeout: 30 * time.Minute}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.health)
	mux.HandleFunc("POST /api/v1/search", s.search)
	mux.HandleFunc("POST /api/v1/pages/extract", s.extractPage)
	// Render provides PORT; retain :8080 for local development.
	addr := env("LISTEN_ADDR", ":"+env("PORT", "8080"))
	log.Printf("Go orchestrator listening on %s; technology crawler=%s; health crawler=%s; discovery=%s; embedding=%s", addr, crawlerURLs["technology"], crawlerURLs["health"], discoveryURL, embeddingURL)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func (s *server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "healthy", "service": "go-orchestrator"})
}

// Return stored matches immediately. Rust checks matching registered domains
// and crawls them before asking Python to perform any Common Crawl discovery.
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

	// Route from the query text as well as Python's classification. This catches
	// clear health terms when a sentence embedding picks the wrong sector.
	queryText := stringValue(analysis["normalized_query"])
	if strings.TrimSpace(queryText) == "" {
		queryText = stringValue(analysis["original_query"])
	}
	keywords := preserveLanguageKeywords(analysis["keywords"], queryText)
	analysis["keywords"] = keywords
	sector := strings.ToLower(strings.TrimSpace(stringValue(analysis["sector"])))
	databaseTarget := "technology"
	healthOverride := hasHealthSignal(queryText, keywords) && sector != "health"
	if hasHealthSignal(queryText, keywords) || sector == "health" {
		databaseTarget = "health"
	}
	if healthOverride {
		analysis["sector_routing_reason"] = "health terms found in query; Go corrected the Python sector"
	}
	analysis["database_target"] = databaseTarget
	analysis["sector"] = databaseTarget
	analysis["metadata_only"] = true
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
		s.respond(w, responseAnalysis, dbBody, map[string]any{
			"method":  "pgvector-domain-and-url-embeddings",
			"message": "Candidates were ranked from the persistent Common Crawl embedding index.",
		})
		return
	}
	if registered, ok := dbResult["registered_domains"].([]any); ok && len(registered) > 0 {
		s.respond(w, responseAnalysis, dbBody, map[string]any{
			"method":  "postgres-registered-domains",
			"domains": registered,
			"message": "Related registered domains were crawled; returning the database result.",
		})
		return
	}

	discoveryBody, discoveryStatus, err := s.postJSON(ctx, s.discoveryURL, responseAnalysis)
	if err != nil || discoveryStatus < 200 || discoveryStatus >= 300 {
		log.Printf("Common Crawl domain discovery unavailable: status=%d error=%v", discoveryStatus, err)
		s.respond(w, responseAnalysis, dbBody, map[string]any{"status": "unavailable", "message": "Parquet URL-index domain discovery failed; returning database matches."})
		return
	}
	var discovery struct {
		CrawlID string            `json:"crawl_id"`
		Domains []domainCandidate `json:"domains"`
		Method  string            `json:"method"`
	}
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
		s.respond(w, responseAnalysis, dbBody, map[string]any{"crawl_id": discovery.CrawlID, "domains": []string{}, "message": "No candidate domains matched the query in Common Crawl's URL index."})
		return
	}

	analysis["candidate_domains"] = candidates
	if discovery.CrawlID != "" {
		analysis["index_collection"] = discovery.CrawlID
	}
	crawlRequest, err := json.Marshal(analysis)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "Could not prepare candidate crawl request")
		return
	}
	crawlBody, crawlStatus, err := s.postJSON(ctx, crawlerURL, crawlRequest)
	if err != nil {
		log.Printf("Rust candidate crawl failed: %v", err)
		writeError(w, http.StatusBadGateway, "Rust crawler failed to process candidate domains")
		return
	}
	if crawlStatus < 200 || crawlStatus >= 300 {
		writeJSON(w, crawlStatus, json.RawMessage(crawlBody))
		return
	}
	s.respond(w, responseAnalysis, crawlBody, map[string]any{"method": discovery.Method, "crawl_id": discovery.CrawlID, "domains": candidates})
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

func hasHealthSignal(query string, keywords []string) bool {
	combined := strings.ToLower(query + " " + strings.Join(keywords, " "))
	compact := regexp.MustCompile(`[^a-z0-9]+`).ReplaceAllString(combined, "")
	for _, term := range healthQueryTerms {
		if strings.Contains(compact, term) {
			return true
		}
	}
	return false
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
