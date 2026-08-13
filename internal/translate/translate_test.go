package translate

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
	got, err := Translate(ctx, "hello world", "zh", srv.URL+"/translate?q={text}&target={target}")
	if err != nil {
		t.Fatal(err)
	}
	if got != "你好世界" {
		t.Fatalf("got %q", got)
	}
}

func TestTranslateCustomMissingPlaceholder(t *testing.T) {
	_, err := Translate(context.Background(), "x", "zh", "http://host/translate")
	if err == nil {
		t.Fatal("expected error for missing {text} placeholder")
	}
}
