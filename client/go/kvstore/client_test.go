package kvstore_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/saitejasrivilli/DistributedKVStore/client/kvstore"
)

func healthServer(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "healthy", "role": "leader",
			"wal_size": 3, "node_id": "test-node", "quorum_available": true,
		})
	})
	return httptest.NewServer(mux)
}

// Edge case: client must return an error (not hang) when the node is unreachable.
func TestHealth_unreachable_node_returns_error(t *testing.T) {
	_, err := kvstore.New("http://127.0.0.1:19999").Health(context.Background())
	if err == nil {
		t.Fatal("expected error for unreachable node, got nil")
	}
}

// Edge case: HealthAll probes all nodes concurrently — healthy nodes succeed,
// unreachable node returns an error, and all results are present (no dropped goroutine).
func TestHealthAll_concurrent_probes(t *testing.T) {
	srv1 := healthServer(t)
	srv2 := healthServer(t)
	defer srv1.Close()
	defer srv2.Close()

	results := kvstore.HealthAll(context.Background(), []string{
		srv1.URL, srv2.URL, "http://127.0.0.1:19999",
	})

	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}
	if results[0].Err != nil {
		t.Errorf("node 0 unexpected error: %v", results[0].Err)
	}
	if results[1].Err != nil {
		t.Errorf("node 1 unexpected error: %v", results[1].Err)
	}
	if results[2].Err == nil {
		t.Error("node 2 (unreachable) should have returned an error")
	}
}
