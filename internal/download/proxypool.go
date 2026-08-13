package download

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// proxyFetcher pulls proxies from a remote API (e.g. proxy.scdn.io) with a
// short cache so multiple tasks don't hammer the API.
type proxyFetcher struct {
	mu      sync.Mutex
	proxies []string
	expiry  time.Time
	ttl     time.Duration
}

func newProxyFetcher() *proxyFetcher {
	return &proxyFetcher{ttl: 60 * time.Second}
}

// Get returns cached proxies or fetches fresh ones when the cache expired.
func (f *proxyFetcher) Get(ctx context.Context, apiURL string, count int) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if time.Now().Before(f.expiry) {
		return f.proxies
	}
	proxies, err := FetchProxies(ctx, apiURL, count)
	if err != nil {
		// Keep serving the stale cache if we have one.
		return f.proxies
	}
	f.proxies = proxies
	f.expiry = time.Now().Add(f.ttl)
	return proxies
}

// FetchProxies fetches proxies from a proxy-list API.
//
// Supported response formats:
//   - proxy.scdn.io JSON: {"code":200,"data":{"proxies":["ip:port",...]}}
//   - plain text: one "ip:port" per line (e.g. proxyscrape lists)
//
// Returned entries are normalized to "http://ip:port". Proxies that don't
// accept a TCP connection are dropped (validated concurrently, ~2s max).
func FetchProxies(ctx context.Context, apiURL string, count int) ([]string, error) {
	if count <= 0 {
		count = 5
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("proxy API HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}

	raw := parseProxyResponse(body)
	if len(raw) == 0 {
		return nil, fmt.Errorf("proxy API returned no proxies")
	}
	// Normalize + cap.
	normalized := make([]string, 0, len(raw))
	for _, p := range raw {
		p = normalizeProxy(p)
		if p != "" {
			normalized = append(normalized, p)
		}
		if len(normalized) >= count {
			break
		}
	}
	return validateProxies(ctx, normalized), nil
}

// parseProxyResponse extracts "ip:port" strings from JSON or plain text.
func parseProxyResponse(body []byte) []string {
	// proxy.scdn.io format
	var scdn struct {
		Data struct {
			Proxies []string `json:"proxies"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &scdn); err == nil && len(scdn.Data.Proxies) > 0 {
		return scdn.Data.Proxies
	}
	// generic JSON array of strings
	var arr []string
	if err := json.Unmarshal(body, &arr); err == nil && len(arr) > 0 {
		return arr
	}
	// plain text, one per line
	var out []string
	scanner := bufio.NewScanner(strings.NewReader(string(body)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" && !strings.HasPrefix(line, "#") {
			out = append(out, line)
		}
	}
	return out
}

// normalizeProxy ensures a "http://ip:port" style address.
func normalizeProxy(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		return ""
	}
	if !strings.Contains(p, "://") {
		p = "http://" + p
	}
	return p
}

// validateProxies keeps only proxies that accept a TCP connection.
func validateProxies(ctx context.Context, proxies []string) []string {
	type result struct {
		proxy string
		ok    bool
	}
	results := make(chan result, len(proxies))
	var wg sync.WaitGroup
	for _, p := range proxies {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			host := p
			if _, h, ok := strings.Cut(p, "://"); ok {
				host = h
			}
			conn, err := (&net.Dialer{Timeout: 2 * time.Second}).DialContext(ctx, "tcp", host)
			if err == nil {
				conn.Close()
				results <- result{p, true}
				return
			}
			results <- result{p, false}
		}(p)
	}
	wg.Wait()
	close(results)

	var out []string
	for r := range results {
		if r.ok {
			out = append(out, r.proxy)
		}
	}
	return out
}
