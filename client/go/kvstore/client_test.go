package kvstore_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/saitejasrivilli/DistributedKVStore/client/kvstore"
)

// fakeServer builds a minimal httptest.Server that mimics the KVStore API.
func fakeServer(t *testing.T, store map[string]string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("/kv/", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path[len("/kv/"):]
		switch r.Method {
		case http.MethodGet:
			v, ok := store[key]
			if !ok {
				http.Error(w, `{"detail":"not found"}`, http.StatusNotFound)
				return
			}
			json.NewEncoder(w).Encode(map[string]interface{}{
				"value": v, "version": 1, "consistency": "eventual",
			})

		case http.MethodPut:
			var body struct{ Value string }
			json.NewDecoder(r.Body).Decode(&body)
			store[key] = body.Value
			json.NewEncoder(w).Encode(map[string]interface{}{
				"ok": true, "version": 1, "acks": 1,
			})

		case http.MethodDelete:
			delete(store, key)
			json.NewEncoder(w).Encode(map[string]interface{}{"ok": true, "acks": 1})
		}
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status": "healthy", "role": "leader",
			"wal_size": 3, "node_id": "test-node", "quorum_available": true,
		})
	})

	return httptest.NewServer(mux)
}

func TestGet_existing_key(t *testing.T) {
	srv := fakeServer(t, map[string]string{"foo": "bar"})
	defer srv.Close()

	r, err := kvstore.New(srv.URL).Get(context.Background(), "foo", "eventual")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if r.Value != "bar" {
		t.Errorf("got value=%q, want %q", r.Value, "bar")
	}
	if r.Version != 1 {
		t.Errorf("got version=%d, want 1", r.Version)
	}
}

func TestGet_missing_key_returns_error(t *testing.T) {
	srv := fakeServer(t, map[string]string{})
	defer srv.Close()

	_, err := kvstore.New(srv.URL).Get(context.Background(), "missing", "eventual")
	if err == nil {
		t.Fatal("expected error for missing key, got nil")
	}
}

func TestPut_stores_and_returns_version(t *testing.T) {
	store := map[string]string{}
	srv := fakeServer(t, store)
	defer srv.Close()

	r, err := kvstore.New(srv.URL).Put(context.Background(), "k", "v")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !r.OK {
		t.Error("expected ok=true")
	}
	if store["k"] != "v" {
		t.Errorf("store has %q, want %q", store["k"], "v")
	}
}

func TestDelete_removes_key(t *testing.T) {
	store := map[string]string{"gone": "bye"}
	srv := fakeServer(t, store)
	defer srv.Close()

	if err := kvstore.New(srv.URL).Delete(context.Background(), "gone"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, exists := store["gone"]; exists {
		t.Error("key still present after delete")
	}
}

func TestHealth_healthy_node(t *testing.T) {
	srv := fakeServer(t, map[string]string{})
	defer srv.Close()

	h, err := kvstore.New(srv.URL).Health(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if h.Status != "healthy" {
		t.Errorf("got status=%q, want healthy", h.Status)
	}
	if !h.QuorumAvailable {
		t.Error("expected quorum_available=true")
	}
}

func TestHealth_unreachable_node_returns_error(t *testing.T) {
	// Port 19999 — nothing listening
	_, err := kvstore.New("http://127.0.0.1:19999").Health(context.Background())
	if err == nil {
		t.Fatal("expected error for unreachable node")
	}
}

func TestHealthAll_concurrent_probes(t *testing.T) {
	srv1 := fakeServer(t, map[string]string{})
	srv2 := fakeServer(t, map[string]string{})
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
