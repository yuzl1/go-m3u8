package translate

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestParseGoogleResponse(t *testing.T) {
	// Real response shape from translate.googleapis.com
	body := []byte(`[[["你好","Hello",null,null,10],["世界"," world",null,null,10]],null,"en",null]`)
	got, err := parseGoogleResponse(body)
	if err != nil {
		t.Fatal(err)
	}
	if got != "你好世界" {
		t.Fatalf("got %q, want %q", got, "你好世界")
	}
}

func TestParseGoogleResponseEmpty(t *testing.T) {
	if _, err := parseGoogleResponse([]byte(`[[],null,"en"]`)); err == nil {
		t.Fatal("expected error for empty segments")
	}
}

func TestTranslateGoogle(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("q") == "" {
			t.Error("missing q param")
		}
		if got := r.URL.Query().Get("tl"); got != "zh-CN" {
			t.Errorf("tl = %q, want zh-CN", got)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[[["与空乘约会","Date with a Flight Attendant",null,null,10]],null,"en"]`))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := translateGoogle(ctx, "Date with a Flight Attendant", "zh-CN", srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	if got != "与空乘约会" {
		t.Fatalf("got %q", got)
	}
}

func TestTranslateCustom(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("q"); got != "hello world" {
			t.Errorf("q = %q", got)
		}
		json.NewEncoder(w).Encode(map[string]string{"translatedText": "你好世界"})
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := Translate(ctx, "hello world", "zh", Config{
		Provider: "custom",
		APIURL:   srv.URL + "/translate?q={text}&target={target}",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != "你好世界" {
		t.Fatalf("got %q", got)
	}
}

func TestTranslateCustomMissingPlaceholder(t *testing.T) {
	_, err := Translate(context.Background(), "x", "zh", Config{
		Provider: "custom",
		APIURL:   "http://host/translate",
	})
	if err == nil {
		t.Fatal("expected error for missing {text} placeholder")
	}
}

// TestTranslateBaidu verifies the Baidu sign generation and response
// parsing using the official docs example values.
func TestTranslateBaidu(t *testing.T) {
	const (
		appID  = "2015063000000001"
		appKey = "1234567890"
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		// Verify sign = md5(appid + q + salt + appkey), lowercase hex.
		want := fmt.Sprintf("%x", md5.Sum([]byte(appID+q.Get("q")+q.Get("salt")+appKey)))
		if got := q.Get("sign"); got != want {
			t.Errorf("sign mismatch: got %s want %s", got, want)
		}
		if got := q.Get("to"); got != "zh" {
			t.Errorf("to = %q, want zh", got)
		}
		if got := q.Get("from"); got != "auto" {
			t.Errorf("from = %q, want auto", got)
		}
		json.NewEncoder(w).Encode(map[string]any{
			"from": "en",
			"to":   "zh",
			"trans_result": []map[string]string{
				{"src": "apple", "dst": "苹果"},
			},
		})
	}))
	defer srv.Close()

	// Point the Baidu endpoint at the test server.
	old := baiduEndpoint
	baiduEndpoint = srv.URL
	defer func() { baiduEndpoint = old }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := Translate(ctx, "apple", "zh-CN", Config{
		Provider: "baidu",
		AppID:    appID,
		AppKey:   appKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != "苹果" {
		t.Fatalf("got %q", got)
	}
}

// TestParseBaiduError verifies error_code handling.
func TestParseBaiduError(t *testing.T) {
	_, err := parseBaiduResponse([]byte(`{"error_code":"54001","error_msg":"Invalid Sign"}`))
	if err == nil || !strings.Contains(err.Error(), "54001") {
		t.Fatalf("expected 54001 error, got %v", err)
	}
}
