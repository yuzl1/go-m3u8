package clash

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestFetchSubscriptionPlainYAML(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ua := r.Header.Get("User-Agent"); ua != "clash.meta" {
			t.Errorf("User-Agent = %q", ua)
		}
		w.Write([]byte("proxies:\n  - name: node1\n    type: ss\n    server: x\n    port: 1\n    cipher: aes-128-gcm\n    password: p\n"))
	}))
	defer srv.Close()

	got, err := FetchSubscription(context.Background(), srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "name: node1") {
		t.Fatalf("got %s", got)
	}
}

func TestFetchSubscriptionBase64(t *testing.T) {
	yaml := "proxies:\n  - name: b64-node\n    type: ss\n    server: x\n    port: 1\n    cipher: aes-128-gcm\n    password: p\n"
	body := base64.StdEncoding.EncodeToString([]byte(yaml))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(body))
	}))
	defer srv.Close()

	got, err := FetchSubscription(context.Background(), srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "name: b64-node") {
		t.Fatalf("got %s", got)
	}
}

func TestConvertSS(t *testing.T) {
	// SIP002: ss://base64(method:password)@host:port#name
	link := "ss://" + base64.RawURLEncoding.EncodeToString([]byte("aes-256-gcm:p@ss")) + "@hk1.example.com:8388#香港 01"
	block, err := convertSS(link)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"name: '香港 01'", "server: hk1.example.com", "port: 8388", "cipher: aes-256-gcm", "password: 'p@ss'"} {
		if !strings.Contains(block, want) {
			t.Errorf("block missing %q:\n%s", want, block)
		}
	}
}

func TestConvertVmess(t *testing.T) {
	jsonBody := `{"ps":"香港 VMess","add":"hk2.example.com","port":443,"id":"uuid-1234","aid":"0","net":"ws","path":"/ws","host":"cdn.example.com","tls":"tls","sni":"hk2.example.com"}`
	link := "vmess://" + base64.RawURLEncoding.EncodeToString([]byte(jsonBody))
	block, err := convertVmess(link)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"name: '香港 VMess'", "type: vmess", "server: hk2.example.com", "port: 443",
		"uuid: uuid-1234", "network: ws", "path: '/ws'", "Host: 'cdn.example.com'", "tls: true", "servername: 'hk2.example.com'",
	} {
		if !strings.Contains(block, want) {
			t.Errorf("block missing %q:\n%s", want, block)
		}
	}
}

func TestConvertTrojan(t *testing.T) {
	link := "trojan://password@hk3.example.com:443?security=tls&sni=hk3.example.com#香港 Trojan"
	block, err := convertTrojan(link)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"type: trojan", "server: hk3.example.com", "password: 'password'", "sni: 'hk3.example.com'"} {
		if !strings.Contains(block, want) {
			t.Errorf("block missing %q:\n%s", want, block)
		}
	}
}

func TestConvertV2rayLinksToYAML(t *testing.T) {
	links := "ss://" + base64.RawURLEncoding.EncodeToString([]byte("aes-128-gcm:pw")) + "@a.example.com:8388#节点A\n" +
		"trojan://pw@b.example.com:443#节点B\n"
	got, n := convertV2rayLinks(links)
	if n != 2 {
		t.Fatalf("converted %d links, want 2", n)
	}
	for _, want := range []string{"proxies:", "🚀 节点选择", "type: ss", "type: trojan", "- MATCH,🚀 节点选择"} {
		if !strings.Contains(got, want) {
			t.Errorf("yaml missing %q:\n%s", want, got)
		}
	}
}

func TestSplitHostPort(t *testing.T) {
	h, p := splitHostPort("1.2.3.4:8080")
	if h != "1.2.3.4" || p != "8080" {
		t.Fatalf("got %q %q", h, p)
	}
}
