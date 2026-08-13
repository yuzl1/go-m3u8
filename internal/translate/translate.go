package translate

import (
	"context"
	"crypto/md5"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// baiduEndpoint is a var so tests can point it at a local server.
var baiduEndpoint = "https://fanyi-api.baidu.com/api/trans/vip/translate"

const defaultGoogleBase = "https://translate.googleapis.com"

// Config selects the translation provider.
type Config struct {
	Provider string // "google" (default), "baidu", "custom"
	APIURL   string // custom endpoint template with {text}/{target}
	AppID    string // Baidu appid
	AppKey   string // Baidu appkey (secret)
}

// Translate translates text to the target language.
//
// Providers:
//   - google: free endpoint, no key needed
//   - baidu:  Baidu general translation API (sign = md5(appid+q+salt+appkey))
//   - custom: user endpoint template, e.g. LibreTranslate
//     http://host:5000/translate?q={text}&source=en&target={target}
func Translate(ctx context.Context, text, target string, cfg Config) (string, error) {
	switch cfg.Provider {
	case "baidu":
		return translateBaidu(ctx, text, target, cfg.AppID, cfg.AppKey)
	case "custom":
		return translateCustom(ctx, text, target, cfg.APIURL)
	default:
		return translateGoogle(ctx, text, target, defaultGoogleBase)
	}
}

// translateBaidu implements the Baidu general translation API.
// sign = md5(appid + q + salt + appkey), q NOT url-encoded in the sign.
func translateBaidu(ctx context.Context, text, target, appID, appKey string) (string, error) {
	if appID == "" || appKey == "" {
		return "", fmt.Errorf("baidu provider requires appid and appkey")
	}
	salt := randomSalt()
	signStr := appID + text + salt + appKey
	sign := fmt.Sprintf("%x", md5.Sum([]byte(signStr)))

	u := baiduEndpoint +
		"?q=" + url.QueryEscape(text) +
		"&from=auto&to=" + url.QueryEscape(mapBaiduLang(target)) +
		"&appid=" + url.QueryEscape(appID) +
		"&salt=" + url.QueryEscape(salt) +
		"&sign=" + sign

	body, err := fetch(ctx, u)
	if err != nil {
		return "", err
	}
	return parseBaiduResponse(body)
}

// parseBaiduResponse extracts the translation from a Baidu API response.
func parseBaiduResponse(body []byte) (string, error) {
	var resp struct {
		TransResult []struct {
			Src string `json:"src"`
			Dst string `json:"dst"`
		} `json:"trans_result"`
		ErrorCode string `json:"error_code"`
		ErrorMsg  string `json:"error_msg"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", err
	}
	if resp.ErrorCode != "" && resp.ErrorCode != "52000" {
		return "", fmt.Errorf("baidu error %s: %s", resp.ErrorCode, resp.ErrorMsg)
	}
	var out strings.Builder
	for _, tr := range resp.TransResult {
		out.WriteString(tr.Dst)
	}
	if out.Len() == 0 {
		return "", fmt.Errorf("no translation in response")
	}
	return out.String(), nil
}

// mapBaiduLang converts common target codes to Baidu language codes.
func mapBaiduLang(target string) string {
	switch strings.ToLower(target) {
	case "zh-cn", "zh":
		return "zh"
	case "zh-tw":
		return "cht"
	default:
		return target
	}
}

// randomSalt returns a random numeric salt (Baidu accepts letters/digits).
func randomSalt() string {
	b := make([]byte, 4)
	crand.Read(b)
	return fmt.Sprintf("%d", binary.BigEndian.Uint32(b))
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
