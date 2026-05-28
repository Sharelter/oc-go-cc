package handlers

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"oc-go-cc/internal/client"
	"oc-go-cc/internal/config"
	"oc-go-cc/internal/metrics"
	"oc-go-cc/internal/router"
	"oc-go-cc/internal/token"
)

func TestStreamingAllModelsFailedReturns503(t *testing.T) {
	// When all upstream models fail during streaming and no headers were
	// written yet, the proxy returns HTTP 503 with Retry-After so the
	// SDK retries automatically.  No SSE error event is ever sent.
	fakeUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"simulated failure"}`))
	}))
	defer fakeUpstream.Close()

	cfg := &config.Config{
		Host:                          "127.0.0.1",
		Port:                          0,
		EnableStreamingScenarioRouting: false,
		Models: map[string]config.ModelConfig{
			"fast": {
				Provider: "opencode-go",
				ModelID:  "test-model",
			},
		},
		Fallbacks: map[string][]config.ModelConfig{
			"fast": {
				{Provider: "opencode-go", ModelID: "test-fallback"},
			},
		},
		OpenCodeGo: config.OpenCodeGoConfig{
			BaseURL:          fakeUpstream.URL,
			AnthropicBaseURL: fakeUpstream.URL,
			TimeoutMs:        5000,
		},
		Logging: config.LoggingConfig{
			Level:    "warn",
			Requests: false,
		},
	}

	atomicCfg := config.NewAtomicConfig(cfg, "")
	ocClient := client.NewOpenCodeClient(atomicCfg)
	modelRouter := router.NewModelRouter(atomicCfg)
	fallbackHandler := router.NewFallbackHandler(nil, 10, 30*time.Second)
	counter, err := token.NewCounter()
	if err != nil {
		t.Fatalf("NewCounter() error = %v", err)
	}
	m := metrics.New()
	handler := NewMessagesHandler(ocClient, modelRouter, fallbackHandler, counter, m)

	body := []byte(`{
		"model": "claude-opus-4-7",
		"messages": [{"role": "user", "content": "hello"}],
		"max_tokens": 100,
		"stream": true
	}`)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/messages?beta=true", bytes.NewReader(body))
	handler.HandleMessages(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d (503 Service Unavailable); body: %s",
			rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}

	if got := rec.Header().Get("Retry-After"); got != "5" {
		t.Fatalf("Retry-After = %q, want %q", got, "5")
	}

	// Verify the body is a valid Anthropic error response.
	var errResp struct {
		Type  string `json:"type"`
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("response is not valid JSON: %v; body: %s", err, rec.Body.String())
	}
	if errResp.Type != "error" {
		t.Fatalf("type = %q, want \"error\"", errResp.Type)
	}
	if errResp.Error.Type != "api_error" {
		t.Fatalf("error.type = %q, want \"api_error\"", errResp.Error.Type)
	}
}

func TestNonStreamingAllModelsFailedReturns503(t *testing.T) {
	// Non-streaming path: when all upstream models fail, the proxy must
	// return HTTP 503 (not 502) with Retry-After and an Anthropic error body.
	fakeUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"simulated failure"}`))
	}))
	defer fakeUpstream.Close()

	cfg := &config.Config{
		Host:                          "127.0.0.1",
		Port:                          0,
		EnableStreamingScenarioRouting: false,
		Models: map[string]config.ModelConfig{
			"default": {
				Provider: "opencode-go",
				ModelID:  "test-model",
			},
		},
		Fallbacks: map[string][]config.ModelConfig{
			"default": {
				{Provider: "opencode-go", ModelID: "test-fallback"},
			},
		},
		OpenCodeGo: config.OpenCodeGoConfig{
			BaseURL:          fakeUpstream.URL,
			AnthropicBaseURL: fakeUpstream.URL,
			TimeoutMs:        5000,
		},
		Logging: config.LoggingConfig{
			Level:    "warn",
			Requests: false,
		},
	}

	atomicCfg := config.NewAtomicConfig(cfg, "")
	ocClient := client.NewOpenCodeClient(atomicCfg)
	modelRouter := router.NewModelRouter(atomicCfg)
	fallbackHandler := router.NewFallbackHandler(nil, 10, 30*time.Second)
	counter, err := token.NewCounter()
	if err != nil {
		t.Fatalf("NewCounter() error = %v", err)
	}
	m := metrics.New()
	handler := NewMessagesHandler(ocClient, modelRouter, fallbackHandler, counter, m)

	body := []byte(`{
		"model": "claude-opus-4-7",
		"messages": [{"role": "user", "content": "hello"}],
		"max_tokens": 100,
		"stream": false
	}`)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/messages?beta=true", bytes.NewReader(body))

	handler.HandleMessages(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d; body: %s",
			rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}

	if got := rec.Header().Get("Retry-After"); got != "5" {
		t.Fatalf("Retry-After = %q, want %q", got, "5")
	}

	var errResp struct {
		Type  string `json:"type"`
		Error struct {
			Type    string `json:"type"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("response is not valid JSON: %v; body: %s", err, rec.Body.String())
	}
	if errResp.Type != "error" {
		t.Fatalf("type = %q, want \"error\"", errResp.Type)
	}
}
