// Package kvstore provides an HTTP client for the DistributedKVStore cluster.
package kvstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client speaks to one node of the KVStore cluster over HTTP.
type Client struct {
	base string
	http *http.Client
}

// New returns a client pointed at addr (e.g. "http://localhost:8080").
func New(addr string) *Client {
	return &Client{
		base: addr,
		http: &http.Client{Timeout: 5 * time.Second},
	}
}

// GetResult is the decoded body of a successful GET /kv/{key}.
type GetResult struct {
	Value       string `json:"value"`
	Version     int64  `json:"version"`
	Consistency string `json:"consistency"`
}

// PutResult is the decoded body of a successful PUT /kv/{key}.
type PutResult struct {
	OK      bool  `json:"ok"`
	Version int64 `json:"version"`
	Acks    int   `json:"acks"`
}

// HealthResult is the decoded body of GET /health.
type HealthResult struct {
	Status          string `json:"status"`
	Role            string `json:"role"`
	WALSize         int    `json:"wal_size"`
	NodeID          string `json:"node_id"`
	QuorumAvailable bool   `json:"quorum_available"`
}

// NodeHealth bundles a health result with which node it came from and any error.
type NodeHealth struct {
	Addr   string
	Result *HealthResult
	Err    error
}

// Get fetches key with the given consistency ("eventual" or "strong").
func (c *Client) Get(ctx context.Context, key, consistency string) (*GetResult, error) {
	url := fmt.Sprintf("%s/kv/%s?consistency=%s", c.base, key, consistency)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("key %q not found", key)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("GET %s: status %d — %s", url, resp.StatusCode, body)
	}
	var out GetResult
	return &out, json.NewDecoder(resp.Body).Decode(&out)
}

// Put writes key=value and returns the assigned version and ack count.
func (c *Client) Put(ctx context.Context, key, value string) (*PutResult, error) {
	payload, _ := json.Marshal(map[string]string{"value": value})
	url := fmt.Sprintf("%s/kv/%s", c.base, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("PUT %s: status %d — %s", url, resp.StatusCode, body)
	}
	var out PutResult
	return &out, json.NewDecoder(resp.Body).Decode(&out)
}

// Delete removes key from the cluster.
func (c *Client) Delete(ctx context.Context, key string) error {
	url := fmt.Sprintf("%s/kv/%s", c.base, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("DELETE %s: status %d — %s", url, resp.StatusCode, body)
	}
	return nil
}

// Health fetches the health status of a single node.
func (c *Client) Health(ctx context.Context) (*HealthResult, error) {
	url := fmt.Sprintf("%s/health", c.base)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("node unreachable: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusServiceUnavailable {
		// Node is down — body still carries the health struct inside "detail"
		var wrapper struct {
			Detail *HealthResult `json:"detail"`
		}
		if err := json.Unmarshal(body, &wrapper); err == nil && wrapper.Detail != nil {
			return wrapper.Detail, fmt.Errorf("node is down")
		}
		return nil, fmt.Errorf("node is down (status 503)")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("health %s: status %d — %s", url, resp.StatusCode, body)
	}
	var out HealthResult
	return &out, json.Unmarshal(body, &out)
}

// HealthAll probes addrs concurrently and returns one result per address.
func HealthAll(ctx context.Context, addrs []string) []NodeHealth {
	results := make([]NodeHealth, len(addrs))
	done := make(chan struct{}, len(addrs))

	for i, addr := range addrs {
		i, addr := i, addr
		go func() {
			c := New(addr)
			r, err := c.Health(ctx)
			results[i] = NodeHealth{Addr: addr, Result: r, Err: err}
			done <- struct{}{}
		}()
	}
	for range addrs {
		<-done
	}
	return results
}
