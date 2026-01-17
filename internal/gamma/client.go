package gamma

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Client struct {
	BaseURL string
	HTTP    *http.Client
}

type Market struct {
	Slug         string `json:"slug"`
	Question     string `json:"question"`
	ConditionID  string `json:"conditionId"`
	ClobTokenIds string `json:"clobTokenIds"` // JSON string: '["token1","token2"]'
}

func NewClient(baseURL string) *Client {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		baseURL = "https://gamma-api.polymarket.com"
	}
	return &Client{
		BaseURL: baseURL,
		HTTP: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

func (c *Client) GetMarketBySlug(ctx context.Context, slug string) (*Market, error) {
	slug = strings.TrimSpace(slug)
	if slug == "" {
		return nil, fmt.Errorf("gamma: slug is required")
	}

	u, err := url.Parse(c.BaseURL)
	if err != nil {
		return nil, fmt.Errorf("gamma: invalid base url: %w", err)
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/markets/slug/" + url.PathEscape(slug)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("gamma: new request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("gamma: request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("gamma: GET %s: status %d", u.String(), resp.StatusCode)
	}

	var m Market
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, fmt.Errorf("gamma: decode response: %w", err)
	}
	return &m, nil
}

func ParseClobTokenIDs(clobTokenIDs string) ([]string, error) {
	clobTokenIDs = strings.TrimSpace(clobTokenIDs)
	if clobTokenIDs == "" {
		return nil, fmt.Errorf("gamma: clobTokenIds is empty")
	}
	var tokenIDs []string
	if err := json.Unmarshal([]byte(clobTokenIDs), &tokenIDs); err != nil {
		return nil, fmt.Errorf("gamma: parse clobTokenIds: %w", err)
	}
	if len(tokenIDs) < 2 {
		return nil, fmt.Errorf("gamma: expected at least 2 token ids, got %d", len(tokenIDs))
	}
	return tokenIDs, nil
}
