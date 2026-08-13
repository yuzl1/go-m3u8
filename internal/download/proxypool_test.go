package download

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestParseProxyResponseSCDN(t *testing.T) {
	body := []byte(`{"code":200,"message":"success","data":{"proxies":["1.2.3.4:8080","5.6.7.8:3128"],"count":2}}`)
	got := parseProxyResponse(body)
	if len(got) != 2 || got[0] != "1.2.3.4:8080" {
		t.Fatalf("got %v", got)
	}
}

func TestParseProxyResponsePlainText(t *testing.T) {
	body := []byte("# comment\n1.2.3.4:8080\n\n5.6.7.8:3128\n")
	got := parseProxyResponse(body)
	if len(got) != 2 || got[1] != "5.6.7.8:3128" {
		t.Fatalf("got %v", got)
	}
}

func TestFetchProxiesFromAPI(t *testing.T) {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"code": 200,
			"data": map[string]any{"proxies": []string{"127.0.0.1:" + onlyPort(srv)}},
		})
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got, err := FetchProxies(ctx, srv.URL, 5)
	if err != nil {
		t.Fatal(err)
	}
	// The test server is reachable via TCP, so it should pass validation.
	if len(got) != 1 {
		t.Fatalf("expected 1 validated proxy, got %v", got)
	}
}

func TestFetchProxiesSkipsDead(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"code": 200,
			"data": map[string]any{"proxies": []string{"192.0.2.1:1"}}, // TEST-NET, unreachable
		})
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got, err := FetchProxies(ctx, srv.URL, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("expected dead proxy to be filtered, got %v", got)
	}
}

func onlyPort(srv *httptest.Server) string {
	u := srv.URL // http://127.0.0.1:port
	i := len(u) - 1
	for i >= 0 && u[i] != ':' {
		i--
	}
	return u[i+1:]
}
