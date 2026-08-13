package clash

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Client talks to a mihomo (Clash) external-controller API.
type Client struct {
	API    string // e.g. http://clash:9090
	Secret string
	client *http.Client
}

// New creates a Clash API client.
func New(api, secret string) *Client {
	return &Client{
		API:    strings.TrimRight(api, "/"),
		Secret: secret,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

// Group mirrors the mihomo /proxies entry for a proxy group.
type Group struct {
	Name string   `json:"name"`
	Type string   `json:"type"`
	Now  string   `json:"now"`
	All  []string `json:"all"`
}

func (c *Client) call(method, path string, body any, out any) error {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, c.API+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.Secret != "" {
		req.Header.Set("Authorization", "Bearer "+c.Secret)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("clash API %s: HTTP %d %s", path, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

// UploadConfig pushes a full YAML config payload to mihomo (in-place
// reload, no file needed).
func (c *Client) UploadConfig(yamlText string) error {
	return c.call(http.MethodPut, "/configs", map[string]string{
		"path":    "",
		"payload": yamlText,
	}, nil)
}

// SetMode switches the clash mode (rule/global/direct).
func (c *Client) SetMode(mode string) error {
	return c.call(http.MethodPatch, "/configs", map[string]string{"mode": mode}, nil)
}

// Groups returns all proxy groups (map keyed by name).
func (c *Client) Groups() (map[string]Group, error) {
	var resp struct {
		Proxies map[string]Group `json:"proxies"`
	}
	if err := c.call(http.MethodGet, "/proxies", nil, &resp); err != nil {
		return nil, err
	}
	return resp.Proxies, nil
}

// SelectNode switches a selector group to the given node.
func (c *Client) SelectNode(group, node string) error {
	return c.call(http.MethodPut, "/proxies/"+url.PathEscape(group), map[string]string{"name": node}, nil)
}

// SelectorNodes picks the rotation group: the preferred one if it exists,
// else the first Selector-type group, else GLOBAL (with global mode).
// Returns the group name and its node list.
func (c *Client) SelectorNodes(preferred string) (string, []string, error) {
	groups, err := c.Groups()
	if err != nil {
		return "", nil, err
	}

	pick := func(name string) (string, []string, bool) {
		g, ok := groups[name]
		if !ok {
			return "", nil, false
		}
		nodes := g.All
		if len(nodes) == 0 && g.Now != "" {
			nodes = []string{g.Now}
		}
		return name, nodes, len(nodes) > 0
	}

	if preferred != "" {
		if name, nodes, ok := pick(preferred); ok {
			return name, nodes, nil
		}
	}

	// First Selector-type group.
	names := make([]string, 0, len(groups))
	for name, g := range groups {
		if g.Type == "Selector" {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		if name == "GLOBAL" {
			continue
		}
		if gname, nodes, ok := pick(name); ok {
			return gname, nodes, nil
		}
	}

	// Fall back to GLOBAL (needs global mode).
	if name, nodes, ok := pick("GLOBAL"); ok {
		if err := c.SetMode("global"); err != nil {
			return "", nil, err
		}
		return name, nodes, nil
	}
	return "", nil, fmt.Errorf("no usable selector group found in clash config")
}

// CurrentNode returns the group's currently selected node.
func (c *Client) CurrentNode(group string) (string, error) {
	groups, err := c.Groups()
	if err != nil {
		return "", err
	}
	g, ok := groups[group]
	if !ok {
		return "", fmt.Errorf("group %q not found", group)
	}
	return g.Now, nil
}

// TestGroupDelay runs mihomo's built-in delay test for every node of a
// group. Nodes that fail are returned with 0 delay. The test URL must be
// reachable through the nodes (default http://www.gstatic.com/generate_204).
func (c *Client) TestGroupDelay(group, testURL string, timeoutMs int) (map[string]int64, error) {
	if testURL == "" {
		testURL = "http://www.gstatic.com/generate_204"
	}
	if timeoutMs <= 0 {
		timeoutMs = 5000
	}
	path := fmt.Sprintf("/group/%s/delay?url=%s&timeout=%d",
		url.PathEscape(group), url.QueryEscape(testURL), timeoutMs)
	var out map[string]int64
	if err := c.call(http.MethodGet, path, nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

var secretRe = regexp.MustCompile(`(?m)^\s*secret:\s*["']?([^"'\s#]+)`)

// ExtractSecret pulls the API secret out of a clash config yaml.
func ExtractSecret(yamlText string) string {
	if m := secretRe.FindStringSubmatch(yamlText); m != nil {
		return m[1]
	}
	return ""
}

// SanitizePayload ensures the pushed clash config has an inbound mixed
// port and an external-controller reachable from other containers. It
// also disables geo database auto-download and rewrites rules to avoid
// GEOIP lookups — the sidecar only runs delay tests, and downloading
// GeoIP.dat from GitHub fails on networks that cannot reach it (e.g.
// servers inside China), which breaks rule evaluation entirely.
func SanitizePayload(yamlText string) string {
	lines := strings.Split(yamlText, "\n")

	// Pass 1: find the first proxy-group name to use as the catch-all target.
	groupName := "DIRECT"
	inGroups := false
	for _, line := range lines {
		t := strings.TrimSpace(line)
		if t == "proxy-groups:" {
			inGroups = true
			continue
		}
		if inGroups {
			if strings.HasPrefix(t, "- name:") {
				groupName = strings.TrimSpace(strings.TrimPrefix(t, "- name:"))
				groupName = strings.Trim(groupName, `"'`)
				break
			}
			if t != "" && !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "\t") {
				inGroups = false // left the section
			}
		}
	}

	// Pass 2: rebuild the config.
	var out []string
	hasMixedPort := false
	hasPort := false
	controllerSeen := false
	geoSeen := false
	geodataSeen := false
	skippingRules := false

	strip := func(s string) string { return strings.TrimSpace(s) }

	for _, line := range lines {
		t := strip(line)
		// Top-level section change ends the rules-skip.
		if skippingRules && t != "" && !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "\t") {
			skippingRules = false
		}
		switch {
		case skippingRules:
			continue // drop GEOIP rules
		case strings.HasPrefix(t, "mixed-port:"):
			hasMixedPort = true
			out = append(out, line)
		case strings.HasPrefix(t, "port:"):
			hasPort = true
			out = append(out, line)
		case strings.HasPrefix(t, "external-controller:"):
			// Force 0.0.0.0 so the app container can reach the API.
			out = append(out, "external-controller: 0.0.0.0:9090")
			controllerSeen = true
		case strings.HasPrefix(t, "geo-auto-update:"):
			out = append(out, "geo-auto-update: false")
			geoSeen = true
		case strings.HasPrefix(t, "geodata-mode:"):
			out = append(out, "geodata-mode: false")
			geodataSeen = true
		case t == "rules:":
			// Replace all rules with a single catch-all through the
			// first proxy group — no GEOIP lookups, no MMDB needed.
			out = append(out, "rules:")
			out = append(out, "  - MATCH,"+groupName)
			skippingRules = true
		default:
			out = append(out, line)
		}
	}

	if !hasMixedPort && !hasPort {
		out = append([]string{"mixed-port: 7890"}, out...)
	}
	if !controllerSeen {
		out = append(out, "external-controller: 0.0.0.0:9090")
	}
	if !geoSeen {
		out = append(out, "geo-auto-update: false")
	}
	if !geodataSeen {
		out = append(out, "geodata-mode: false")
	}
	return strings.Join(out, "\n")
}
