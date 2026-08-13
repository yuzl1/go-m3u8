package translate

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const defaultGoogleBase = "https://translate.googleapis.com"

// Translate translates text to the target language.
//
// When apiURL is empty, the free Google Translate endpoint is used
// (works without an API key from most overseas servers).
// When apiURL is set it is treated as a template: {text} and {target}
// placeholders are replaced (compatible with e.g. LibreTranslate:
// http://host:5000/translate?q={text}&source=en&target={target}).
func Translate(ctx context.Context, text, target, apiURL string) (string, error) {
	if apiURL != "" {
		return translateCustom(ctx, text, target, apiURL)
	}
	return translateGoogle(ctx, text, target, defaultGoogleBase)
}

func translateGoogle(ctx context.Context, text, target, base string) (string, error) {
	u := base + "/translate_a/single?client=gtx&sl=auto&tl=" +
		url.QueryEscape(target) + "&dt=t&q=" + url.QueryEscape(text)
	body, err := fetch(ctx, u)
	if err != nil {
		return "", err
	}
	return parseGoogleResponse(body)
}

// parseGoogleResponse extracts the translated text from the free Google
// endpoint JSON: [[["译文","原文",...],...],...]
func parseGoogleResponse(body []byte) (string, error) {
	var data []any
	if err := json.Unmarshal(body, &data); err != nil {
		return "", err
	}
	if len(data) == 0 {
		return "", fmt.Errorf("empty response")
	}
	segments, ok := data[0].([]any)
	if !ok || len(segments) == 0 {
		return "", fmt.Errorf("unexpected response format")
	}
	var out strings.Builder
	for _, seg := range segments {
		s, ok := seg.([]any)
		if !ok || len(s) == 0 {
			continue
		}
		t, ok := s[0].(string)
		if !ok {
			continue
		}
		out.WriteString(t)
	}
	if out.Len() == 0 {
		return "", fmt.Errorf("no translation in response")
	}
	return out.String(), nil
}

// translateCustom calls a user-supplied endpoint with {text}/{target}
// placeholders. Accepts JSON {"translatedText": "..."} or plain text.
func translateCustom(ctx context.Context, text, target, apiURL string) (string, error) {
	if !strings.Contains(apiURL, "{text}") {
		return "", fmt.Errorf("translate_api_url must contain a {text} placeholder")
	}
	u := strings.ReplaceAll(apiURL, "{text}", url.QueryEscape(text))
	u = strings.ReplaceAll(u, "{target}", url.QueryEscape(target))
	body, err := fetch(ctx, u)
	if err != nil {
		return "", err
	}
	var m struct {
		TranslatedText string `json:"translatedText"`
	}
	if err := json.Unmarshal(body, &m); err == nil && m.TranslatedText != "" {
		return m.TranslatedText, nil
	}
	return strings.TrimSpace(string(body)), nil
}

func fetch(ctx context.Context, u string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
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
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if len(body) == 0 {
		return nil, fmt.Errorf("empty response")
	}
	return body, nil
}
