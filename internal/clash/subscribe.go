package clash

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// FetchSubscription downloads a clash subscription and returns a clash
// YAML config. Handles:
//   - plain clash YAML
//   - base64-wrapped content (common for airport subscriptions)
//   - v2ray link lists (vmess:// ss:// trojan:// vless://), converted to YAML
func FetchSubscription(ctx context.Context, subURL string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, subURL, nil)
	if err != nil {
		return "", err
	}
	// Most panels return clash-format YAML when the client identifies as clash.
	req.Header.Set("User-Agent", "clash.meta")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("subscribe HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return "", err
	}
	text := strings.TrimSpace(string(body))

	// Base64-wrapped subscription?
	if decoded, ok := decodeMaybeBase64(text); ok {
		text = decoded
	}
	if strings.Contains(text, "proxies:") {
		return text, nil
	}
	// v2ray link list?
	if yaml, n := convertV2rayLinks(text); n > 0 {
		return yaml, nil
	}
	return "", fmt.Errorf("unrecognized subscription format (not yaml, not v2ray links)")
}

// decodeMaybeBase64 tries common base64 encodings; only accepts the
// result if it looks like a config or link list.
func decodeMaybeBase64(s string) (string, bool) {
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if d, err := enc.DecodeString(s); err == nil {
			t := strings.TrimSpace(string(d))
			if strings.Contains(t, "proxies:") || strings.Contains(t, "://") {
				return t, true
			}
		}
	}
	return s, false
}

// convertV2rayLinks converts vmess/ss/trojan/vless share links into a
// clash YAML config with a single selector group.
func convertV2rayLinks(text string) (string, int) {
	var blocks []string
	var names []string
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var block string
		var err error
		switch {
		case strings.HasPrefix(line, "vmess://"):
			block, err = convertVmess(line)
		case strings.HasPrefix(line, "ss://"):
			block, err = convertSS(line)
		case strings.HasPrefix(line, "trojan://"):
			block, err = convertTrojan(line)
		case strings.HasPrefix(line, "vless://"):
			block, err = convertVless(line)
		default:
			continue
		}
		if err != nil || block == "" {
			continue
		}
		blocks = append(blocks, block)
		if name := blockName(block); name != "" {
			names = append(names, name)
		}
	}
	if len(blocks) == 0 {
		return "", 0
	}
	var b strings.Builder
	b.WriteString("proxies:\n")
	for _, block := range blocks {
		for _, l := range strings.Split(strings.TrimRight(block, "\n"), "\n") {
			b.WriteString("  " + l + "\n")
		}
	}
	b.WriteString("proxy-groups:\n")
	b.WriteString("  - name: 🚀 节点选择\n")
	b.WriteString("    type: select\n")
	b.WriteString("    proxies:\n")
	for _, n := range names {
		b.WriteString("      - " + n + "\n")
	}
	b.WriteString("rules:\n")
	b.WriteString("  - MATCH,🚀 节点选择\n")
	return b.String(), len(blocks)
}

// blockName extracts "name: X" from a generated proxy block.
func blockName(block string) string {
	for _, l := range strings.Split(block, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimSpace(l), "name: "); ok {
			return v
		}
	}
	return ""
}

func yamlQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// ss://base64(method:password)@host:port#name  (SIP002)
// ss://base64(method:password@host:port)#name   (legacy)
func convertSS(link string) (string, error) {
	s := strings.TrimPrefix(link, "ss://")
	name := "ss-node"
	if i := strings.Index(s, "#"); i >= 0 {
		if n, err := url.QueryUnescape(s[i+1:]); err == nil {
			name = n
		}
		s = s[:i]
	}
	userinfo := s
	hostport := ""
	if i := strings.Index(s, "@"); i >= 0 {
		userinfo = s[:i]
		hostport = s[i+1:]
	} else {
		// legacy: the whole string is base64(method:password@host:port)
		if d, err := base64.RawURLEncoding.DecodeString(s); err == nil {
			userinfo = string(d)
		} else if d, err := base64.RawStdEncoding.DecodeString(s); err == nil {
			userinfo = string(d)
		}
		if i := strings.Index(userinfo, "@"); i >= 0 {
			hostport = userinfo[i+1:]
			userinfo = userinfo[:i]
		}
	}
	// userinfo itself is base64(method:password)
	if d, err := base64.RawStdEncoding.DecodeString(userinfo); err == nil {
		userinfo = string(d)
	} else if d, err := base64.RawURLEncoding.DecodeString(userinfo); err == nil {
		userinfo = string(d)
	} else if d, err := base64.StdEncoding.DecodeString(userinfo); err == nil {
		userinfo = string(d)
	}
	method, password, ok := strings.Cut(userinfo, ":")
	if !ok {
		return "", fmt.Errorf("invalid ss link")
	}
	host, port := splitHostPort(hostport)
	return fmt.Sprintf("name: %s\ntype: ss\nserver: %s\nport: %s\ncipher: %s\npassword: %s",
		yamlQuote(name), host, port, method, yamlQuote(password)), nil
}

// vmess://base64(json)
func convertVmess(link string) (string, error) {
	raw := strings.TrimPrefix(link, "vmess://")
	data, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		data, err = base64.RawStdEncoding.DecodeString(raw)
	}
	if err != nil {
		data, err = base64.StdEncoding.DecodeString(raw)
	}
	if err != nil {
		return "", fmt.Errorf("invalid vmess base64: %w", err)
	}
	var j struct {
		PS   string `json:"ps"`
		Add  string `json:"add"`
		Port any    `json:"port"`
		ID   string `json:"id"`
		Aid  any    `json:"aid"`
		Net  string `json:"net"`
		Type string `json:"type"`
		Host string `json:"host"`
		Path string `json:"path"`
		TLS  string `json:"tls"`
		SNI  string `json:"sni"`
	}
	if err := json.Unmarshal(data, &j); err != nil {
		return "", fmt.Errorf("invalid vmess json: %w", err)
	}
	name := j.PS
	if name == "" {
		name = j.Add
	}
	port := fmt.Sprintf("%v", j.Port)
	aid := fmt.Sprintf("%v", j.Aid)
	var b strings.Builder
	fmt.Fprintf(&b, "name: %s\ntype: vmess\nserver: %s\nport: %s\nuuid: %s\nalterId: %s\ncipher: auto\n",
		yamlQuote(name), j.Add, port, j.ID, aid)
	if j.Net != "" && j.Net != "tcp" {
		b.WriteString("network: " + j.Net + "\n")
		if j.Net == "ws" {
			b.WriteString("ws-opts:\n")
			b.WriteString("  path: " + yamlQuote(j.Path) + "\n")
			if j.Host != "" {
				b.WriteString("  headers:\n    Host: " + yamlQuote(j.Host) + "\n")
			}
		}
		if j.Net == "grpc" && j.Path != "" {
			b.WriteString("grpc-opts:\n  grpc-service-name: " + yamlQuote(j.Path) + "\n")
		}
	}
	if j.TLS == "tls" {
		b.WriteString("tls: true\n")
		if j.SNI != "" {
			b.WriteString("servername: " + yamlQuote(j.SNI) + "\n")
		}
	}
	return strings.TrimRight(b.String(), "\n"), nil
}

// trojan://password@host:port?query#name
func convertTrojan(link string) (string, error) {
	u, err := url.Parse(link)
	if err != nil {
		return "", err
	}
	password := ""
	if u.User != nil {
		password = u.User.Username()
	}
	name := u.Fragment
	if name == "" {
		name = u.Hostname()
	}
	q := u.Query()
	var b strings.Builder
	fmt.Fprintf(&b, "name: %s\ntype: trojan\nserver: %s\nport: %s\npassword: %s\n",
		yamlQuote(name), u.Hostname(), u.Port(), yamlQuote(password))
	if sni := q.Get("sni"); sni != "" {
		b.WriteString("sni: " + yamlQuote(sni) + "\n")
	} else if peer := q.Get("peer"); peer != "" {
		b.WriteString("sni: " + yamlQuote(peer) + "\n")
	}
	if q.Get("allowInsecure") == "1" {
		b.WriteString("skip-cert-verify: true\n")
	}
	return strings.TrimRight(b.String(), "\n"), nil
}

// vless://uuid@host:port?query#name
func convertVless(link string) (string, error) {
	u, err := url.Parse(link)
	if err != nil {
		return "", err
	}
	uuid := ""
	if u.User != nil {
		uuid = u.User.Username()
	}
	name := u.Fragment
	if name == "" {
		name = u.Hostname()
	}
	q := u.Query()
	netType := q.Get("type")
	if netType == "" {
		netType = "tcp"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "name: %s\ntype: vless\nserver: %s\nport: %s\nuuid: %s\n",
		yamlQuote(name), u.Hostname(), u.Port(), uuid)
	if netType != "tcp" {
		b.WriteString("network: " + netType + "\n")
		if netType == "ws" {
			b.WriteString("ws-opts:\n  path: " + yamlQuote(q.Get("path")) + "\n")
			if h := q.Get("host"); h != "" {
				b.WriteString("  headers:\n    Host: " + yamlQuote(h) + "\n")
			}
		}
	}
	if q.Get("security") == "tls" {
		b.WriteString("tls: true\n")
		if sni := q.Get("sni"); sni != "" {
			b.WriteString("servername: " + yamlQuote(sni) + "\n")
		}
	}
	if q.Get("allowInsecure") == "1" {
		b.WriteString("skip-cert-verify: true\n")
	}
	return strings.TrimRight(b.String(), "\n"), nil
}

// splitHostPort splits "host:port" (IPv6-aware).
func splitHostPort(s string) (string, string) {
	if i := strings.LastIndex(s, ":"); i > 0 {
		host := s[:i]
		port := s[i+1:]
		if _, err := strconv.Atoi(port); err == nil {
			return host, port
		}
	}
	return s, "443"
}
