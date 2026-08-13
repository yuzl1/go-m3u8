package clash

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeAPI mimics the mihomo external-controller for tests.
type fakeAPI struct {
	t        *testing.T
	groups   map[string]Group
	configs  []string // captured uploaded payloads
	selected map[string]string
	mode     string
}

func newFakeAPI(t *testing.T) (*fakeAPI, *httptest.Server) {
	f := &fakeAPI{
		t:        t,
		selected: map[string]string{},
		mode:     "rule",
		groups: map[string]Group{
			"GLOBAL": {Name: "GLOBAL", Type: "Selector", Now: "DIRECT", All: []string{"DIRECT", "REJECT"}},
			"🚀 节点选择": {Name: "🚀 节点选择", Type: "Selector", Now: "node1", All: []string{"node1", "node2", "node3"}},
			"auto":   {Name: "auto", Type: "URLTest", Now: "node1", All: []string{"node1", "node2"}},
		},
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPut && r.URL.Path == "/configs":
			var body map[string]string
			json.NewDecoder(r.Body).Decode(&body)
			f.configs = append(f.configs, body["payload"])
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodPatch && r.URL.Path == "/configs":
			var body map[string]string
			json.NewDecoder(r.Body).Decode(&body)
			if m := body["mode"]; m != "" {
				f.mode = m
			}
			w.WriteHeader(http.StatusNoContent)
		case r.Method == http.MethodGet && r.URL.Path == "/proxies":
			json.NewEncoder(w).Encode(map[string]any{"proxies": f.groups})
		case r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/proxies/"):
			name := strings.TrimPrefix(r.URL.Path, "/proxies/")
			var body map[string]string
			json.NewDecoder(r.Body).Decode(&body)
			f.selected[name] = body["name"]
			w.WriteHeader(http.StatusNoContent)
		case strings.HasSuffix(r.URL.Path, "/delay"):
			// group delay test result: node1 fast, node2 dead
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]int64{"node1": 120, "node2": 0, "node3": 350})
		default:
			http.NotFound(w, r)
		}
	}))
	return f, srv
}

func TestUploadConfig(t *testing.T) {
	f, srv := newFakeAPI(t)
	defer srv.Close()

	c := New(srv.URL, "secret123")
	if err := c.UploadConfig("mixed-port: 7890"); err != nil {
		t.Fatal(err)
	}
	if len(f.configs) != 1 || f.configs[0] != "mixed-port: 7890" {
		t.Fatalf("payload not captured: %v", f.configs)
	}
}

func TestSelectorNodesPrefersGroup(t *testing.T) {
	_, srv := newFakeAPI(t)
	defer srv.Close()

	c := New(srv.URL, "")
	group, nodes, err := c.SelectorNodes("🚀 节点选择")
	if err != nil {
		t.Fatal(err)
	}
	if group != "🚀 节点选择" || len(nodes) != 3 || nodes[0] != "node1" {
		t.Fatalf("group=%q nodes=%v", group, nodes)
	}
}

func TestSelectorNodesAutoPicksFirstSelector(t *testing.T) {
	_, srv := newFakeAPI(t)
	defer srv.Close()

	c := New(srv.URL, "")
	group, nodes, err := c.SelectorNodes("")
	if err != nil {
		t.Fatal(err)
	}
	if group != "🚀 节点选择" {
		t.Fatalf("expected first selector group, got %q", group)
	}
	if len(nodes) != 3 {
		t.Fatalf("nodes = %v", nodes)
	}
}

func TestSelectNode(t *testing.T) {
	f, srv := newFakeAPI(t)
	defer srv.Close()

	c := New(srv.URL, "")
	if err := c.SelectNode("🚀 节点选择", "node2"); err != nil {
		t.Fatal(err)
	}
	if f.selected["🚀 节点选择"] != "node2" {
		t.Fatalf("selected = %v", f.selected)
	}
}

func TestGroupDelay(t *testing.T) {
	_, srv := newFakeAPI(t)
	defer srv.Close()

	c := New(srv.URL, "")
	delays, err := c.TestGroupDelay("🚀 节点选择", "", 5000)
	if err != nil {
		t.Fatal(err)
	}
	if delays["node1"] != 120 || delays["node2"] != 0 {
		t.Fatalf("delays = %v", delays)
	}
}

func TestExtractSecret(t *testing.T) {
	yaml := "mixed-port: 7890\nsecret: 'abc123'\nexternal-controller: 127.0.0.1:9090"
	if got := ExtractSecret(yaml); got != "abc123" {
		t.Fatalf("secret = %q", got)
	}
	if got := ExtractSecret("no secret here"); got != "" {
		t.Fatalf("expected empty secret, got %q", got)
	}
}

func TestSanitizePayload(t *testing.T) {
	out := SanitizePayload("proxies: []\nsecret: xyz")
	if !strings.Contains(out, "mixed-port: 7890") {
		t.Errorf("mixed-port missing: %s", out)
	}
	if !strings.Contains(out, "external-controller: 0.0.0.0:9090") {
		t.Errorf("external-controller missing: %s", out)
	}

	// Existing controller must be forced to 0.0.0.0.
	out = SanitizePayload("external-controller: 127.0.0.1:9090\nport: 7891")
	if strings.Contains(out, "127.0.0.1") {
		t.Errorf("external-controller not rewritten: %s", out)
	}
	if strings.Contains(out, "mixed-port:") {
		t.Errorf("mixed-port added despite existing port: %s", out)
	}
}
