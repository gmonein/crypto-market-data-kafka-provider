package clob

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	BaseURL       string
	ChainID       int64
	UseServerTime bool

	HTTP   *http.Client
	Signer *Signer
	Creds  *ApiKeyCreds
}

func NewClient(baseURL string, chainID int64, signer *Signer) *Client {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		baseURL = "https://clob.polymarket.com"
	}
	return &Client{
		BaseURL: baseURL,
		ChainID: chainID,
		HTTP: &http.Client{
			Timeout: 20 * time.Second,
		},
		Signer: signer,
	}
}

func (c *Client) SetCreds(creds ApiKeyCreds) {
	c.Creds = &creds
}

func (c *Client) serverTime(ctx context.Context) int64 {
	if !c.UseServerTime {
		return time.Now().Unix()
	}
	ts, err := c.GetServerTime(ctx)
	if err != nil {
		return time.Now().Unix()
	}
	return ts
}

func (c *Client) GetServerTime(ctx context.Context) (int64, error) {
	u := c.BaseURL + "/time"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var body any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, fmt.Errorf("decode /time: %w", err)
	}
	if m, ok := body.(map[string]any); ok {
		for _, k := range []string{"timestamp", "ts", "serverTime"} {
			if v, ok := m[k]; ok {
				switch t := v.(type) {
				case float64:
					return int64(t), nil
				case string:
					if n, err := strconv.ParseInt(t, 10, 64); err == nil {
						return n, nil
					}
				}
			}
		}
	}
	return 0, fmt.Errorf("unexpected /time payload: %T", body)
}

func (c *Client) CreateOrDeriveApiKey(ctx context.Context, nonce int64) (ApiKeyCreds, error) {
	if c.Signer == nil {
		return ApiKeyCreds{}, fmt.Errorf("clob: signer is required for L1 auth")
	}

	// Prefer derive (idempotent). If it fails, fall back to create.
	creds, err := c.DeriveApiKey(ctx, nonce)
	if err == nil && creds.Key != "" && creds.Secret != "" && creds.Passphrase != "" {
		return creds, nil
	}
	return c.CreateApiKey(ctx, nonce)
}

func (c *Client) CreateApiKey(ctx context.Context, nonce int64) (ApiKeyCreds, error) {
	ts := c.serverTime(ctx)
	sig, err := BuildL1ClobAuthSignature(c.Signer, c.ChainID, ts, nonce)
	if err != nil {
		return ApiKeyCreds{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/auth/api-key", nil)
	if err != nil {
		return ApiKeyCreds{}, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("POLY_ADDRESS", c.Signer.Address().Hex())
	req.Header.Set("POLY_SIGNATURE", sig)
	req.Header.Set("POLY_TIMESTAMP", fmt.Sprintf("%d", ts))
	req.Header.Set("POLY_NONCE", fmt.Sprintf("%d", nonce))

	return decodeApiKeyResp(c.HTTP.Do(req))
}

func (c *Client) DeriveApiKey(ctx context.Context, nonce int64) (ApiKeyCreds, error) {
	ts := c.serverTime(ctx)
	sig, err := BuildL1ClobAuthSignature(c.Signer, c.ChainID, ts, nonce)
	if err != nil {
		return ApiKeyCreds{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/auth/derive-api-key", nil)
	if err != nil {
		return ApiKeyCreds{}, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("POLY_ADDRESS", c.Signer.Address().Hex())
	req.Header.Set("POLY_SIGNATURE", sig)
	req.Header.Set("POLY_TIMESTAMP", fmt.Sprintf("%d", ts))
	req.Header.Set("POLY_NONCE", fmt.Sprintf("%d", nonce))

	return decodeApiKeyResp(c.HTTP.Do(req))
}

func decodeApiKeyResp(resp *http.Response, err error) (ApiKeyCreds, error) {
	if err != nil {
		return ApiKeyCreds{}, err
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return ApiKeyCreds{}, fmt.Errorf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(bodyBytes)))
	}

	var raw apiKeyRaw
	if err := json.Unmarshal(bodyBytes, &raw); err != nil {
		return ApiKeyCreds{}, fmt.Errorf("decode api key response: %w", err)
	}

	key := raw.Key
	if key == "" {
		key = raw.APIKey
	}
	if key == "" || raw.Secret == "" || raw.Passphrase == "" {
		return ApiKeyCreds{}, fmt.Errorf("invalid api key response: %s", strings.TrimSpace(string(bodyBytes)))
	}
	return ApiKeyCreds{Key: key, Secret: raw.Secret, Passphrase: raw.Passphrase}, nil
}

func (c *Client) GetTickSize(ctx context.Context, tokenID string) (string, error) {
	tokenID = strings.TrimSpace(tokenID)
	if tokenID == "" {
		return "", fmt.Errorf("token id is required")
	}

	u, _ := url.Parse(c.BaseURL + "/tick-size")
	q := u.Query()
	q.Set("token_id", tokenID)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("GET /tick-size: status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	var ts TickSizeResponse
	if err := json.NewDecoder(resp.Body).Decode(&ts); err != nil {
		return "", fmt.Errorf("decode tick size: %w", err)
	}
	// Preserve up to 4 decimals for known tick sizes; NormalizeTickSize will canonicalize.
	return strconv.FormatFloat(ts.MinimumTickSize, 'f', -1, 64), nil
}

func (c *Client) GetNegRisk(ctx context.Context, tokenID string) (bool, error) {
	tokenID = strings.TrimSpace(tokenID)
	if tokenID == "" {
		return false, fmt.Errorf("token id is required")
	}

	u, _ := url.Parse(c.BaseURL + "/neg-risk")
	q := u.Query()
	q.Set("token_id", tokenID)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("GET /neg-risk: status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	var nr NegRiskResponse
	if err := json.NewDecoder(resp.Body).Decode(&nr); err != nil {
		return false, fmt.Errorf("decode neg risk: %w", err)
	}
	return nr.NegRisk, nil
}

func (c *Client) GetFeeRateBps(ctx context.Context, tokenID string) (int64, error) {
	tokenID = strings.TrimSpace(tokenID)
	if tokenID == "" {
		return 0, fmt.Errorf("token id is required")
	}

	u, _ := url.Parse(c.BaseURL + "/fee-rate")
	q := u.Query()
	q.Set("token_id", tokenID)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("GET /fee-rate: status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	var fr FeeRateResponse
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	if err := dec.Decode(&fr); err != nil {
		return 0, fmt.Errorf("decode fee rate: %w", err)
	}
	baseStr := strings.TrimSpace(fr.BaseFee.String())
	if baseStr == "" {
		return 0, fmt.Errorf("fee rate missing base_fee")
	}
	if v, err := fr.BaseFee.Int64(); err == nil {
		return v, nil
	}
	if v, err := fr.BaseFee.Float64(); err == nil {
		return int64(math.Round(v)), nil
	}
	if v, err := strconv.ParseInt(baseStr, 10, 64); err == nil {
		return v, nil
	}
	return 0, fmt.Errorf("invalid base_fee %q", baseStr)
}

func (c *Client) GetOrderBook(ctx context.Context, tokenID string) (*OrderBookSummary, error) {
	tokenID = strings.TrimSpace(tokenID)
	if tokenID == "" {
		return nil, fmt.Errorf("token id is required")
	}

	u, _ := url.Parse(c.BaseURL + "/book")
	q := u.Query()
	q.Set("token_id", tokenID)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("GET /book: status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}

	var book OrderBookSummary
	if err := json.NewDecoder(resp.Body).Decode(&book); err != nil {
		return nil, fmt.Errorf("decode order book: %w", err)
	}
	return &book, nil
}

func (c *Client) PostOrder(ctx context.Context, reqBody *NewOrderRequest) (*OrderResponse, error) {
	if c.Signer == nil || c.Creds == nil {
		return nil, fmt.Errorf("clob: signer and creds are required for L2 auth")
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	path := "/order"
	ts := c.serverTime(ctx)
	sig, err := BuildPolyHmacSignature(c.Creds.Secret, ts, http.MethodPost, path, string(body))
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("POLY_ADDRESS", c.Signer.Address().Hex())
	req.Header.Set("POLY_SIGNATURE", sig)
	req.Header.Set("POLY_TIMESTAMP", fmt.Sprintf("%d", ts))
	req.Header.Set("POLY_API_KEY", c.Creds.Key)
	req.Header.Set("POLY_PASSPHRASE", c.Creds.Passphrase)

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &HTTPError{
			Method:     http.MethodPost,
			Path:       path,
			StatusCode: resp.StatusCode,
			Body:       strings.TrimSpace(string(respBytes)),
			RetryAfter: retryAfterFromHeader(resp.Header),
		}
	}

	var or OrderResponse
	if err := json.Unmarshal(respBytes, &or); err != nil {
		return nil, fmt.Errorf("decode order response: %w", err)
	}
	if !or.Success && or.OrderID == "" {
		msg := firstNonEmpty(or.ErrorMsg, or.Error, or.Message)
		if msg == "" {
			msg = strings.TrimSpace(string(respBytes))
		}
		return nil, fmt.Errorf("POST /order failed: %s", msg)
	}
	return &or, nil
}

func (c *Client) PostOrders(ctx context.Context, reqBodies []*NewOrderRequest) ([]OrderResponse, error) {
	if c.Signer == nil || c.Creds == nil {
		return nil, fmt.Errorf("clob: signer and creds are required for L2 auth")
	}
	if len(reqBodies) == 0 {
		return nil, fmt.Errorf("orders payload is empty")
	}

	body, err := json.Marshal(reqBodies)
	if err != nil {
		return nil, err
	}

	path := "/orders"
	ts := c.serverTime(ctx)
	sig, err := BuildPolyHmacSignature(c.Creds.Secret, ts, http.MethodPost, path, string(body))
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("POLY_ADDRESS", c.Signer.Address().Hex())
	req.Header.Set("POLY_SIGNATURE", sig)
	req.Header.Set("POLY_TIMESTAMP", fmt.Sprintf("%d", ts))
	req.Header.Set("POLY_API_KEY", c.Creds.Key)
	req.Header.Set("POLY_PASSPHRASE", c.Creds.Passphrase)

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, &HTTPError{
			Method:     http.MethodPost,
			Path:       path,
			StatusCode: resp.StatusCode,
			Body:       strings.TrimSpace(string(respBytes)),
			RetryAfter: retryAfterFromHeader(resp.Header),
		}
	}

	var ors []OrderResponse
	if err := json.Unmarshal(respBytes, &ors); err != nil {
		return nil, fmt.Errorf("decode orders response: %w", err)
	}
	return ors, nil
}

func (c *Client) CancelOrder(ctx context.Context, orderID string) error {
	if c.Signer == nil || c.Creds == nil {
		return fmt.Errorf("clob: signer and creds are required for L2 auth")
	}
	orderID = strings.TrimSpace(orderID)
	if orderID == "" {
		return fmt.Errorf("order id is required")
	}

	payload := CancelOrderPayload{OrderID: orderID}
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	path := "/order"
	ts := c.serverTime(ctx)
	sig, err := BuildPolyHmacSignature(c.Creds.Secret, ts, http.MethodDelete, path, string(body))
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("POLY_ADDRESS", c.Signer.Address().Hex())
	req.Header.Set("POLY_SIGNATURE", sig)
	req.Header.Set("POLY_TIMESTAMP", fmt.Sprintf("%d", ts))
	req.Header.Set("POLY_API_KEY", c.Creds.Key)
	req.Header.Set("POLY_PASSPHRASE", c.Creds.Passphrase)

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &HTTPError{
			Method:     http.MethodDelete,
			Path:       path,
			StatusCode: resp.StatusCode,
			Body:       strings.TrimSpace(string(respBytes)),
			RetryAfter: retryAfterFromHeader(resp.Header),
		}
	}
	return nil
}

func (c *Client) CancelOrders(ctx context.Context, orderIDs []string) error {
	if c.Signer == nil || c.Creds == nil {
		return fmt.Errorf("clob: signer and creds are required for L2 auth")
	}
	if len(orderIDs) == 0 {
		return nil
	}

	body, err := json.Marshal(orderIDs)
	if err != nil {
		return err
	}

	path := "/orders"
	ts := c.serverTime(ctx)
	sig, err := BuildPolyHmacSignature(c.Creds.Secret, ts, http.MethodDelete, path, string(body))
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("POLY_ADDRESS", c.Signer.Address().Hex())
	req.Header.Set("POLY_SIGNATURE", sig)
	req.Header.Set("POLY_TIMESTAMP", fmt.Sprintf("%d", ts))
	req.Header.Set("POLY_API_KEY", c.Creds.Key)
	req.Header.Set("POLY_PASSPHRASE", c.Creds.Passphrase)

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &HTTPError{
			Method:     http.MethodDelete,
			Path:       path,
			StatusCode: resp.StatusCode,
			Body:       strings.TrimSpace(string(respBytes)),
			RetryAfter: retryAfterFromHeader(resp.Header),
		}
	}
	return nil
}

func (c *Client) CancelAll(ctx context.Context) error {
	if c.Signer == nil || c.Creds == nil {
		return fmt.Errorf("clob: signer and creds are required for L2 auth")
	}

	path := "/cancel-all"
	ts := c.serverTime(ctx)
	sig, err := BuildPolyHmacSignature(c.Creds.Secret, ts, http.MethodDelete, path, "")
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.BaseURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("POLY_ADDRESS", c.Signer.Address().Hex())
	req.Header.Set("POLY_SIGNATURE", sig)
	req.Header.Set("POLY_TIMESTAMP", fmt.Sprintf("%d", ts))
	req.Header.Set("POLY_API_KEY", c.Creds.Key)
	req.Header.Set("POLY_PASSPHRASE", c.Creds.Passphrase)

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return &HTTPError{
			Method:     http.MethodDelete,
			Path:       path,
			StatusCode: resp.StatusCode,
			Body:       strings.TrimSpace(string(respBytes)),
			RetryAfter: retryAfterFromHeader(resp.Header),
		}
	}
	return nil
}

func (c *Client) GetOrder(ctx context.Context, orderID string) (*OpenOrder, error) {
	if c.Signer == nil || c.Creds == nil {
		return nil, fmt.Errorf("clob: signer and creds are required for L2 auth")
	}
	orderID = strings.TrimSpace(orderID)
	if orderID == "" {
		return nil, fmt.Errorf("order id is required")
	}

	path := "/data/order/" + url.PathEscape(orderID)
	ts := c.serverTime(ctx)
	sig, err := BuildPolyHmacSignature(c.Creds.Secret, ts, http.MethodGet, path, "")
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("POLY_ADDRESS", c.Signer.Address().Hex())
	req.Header.Set("POLY_SIGNATURE", sig)
	req.Header.Set("POLY_TIMESTAMP", fmt.Sprintf("%d", ts))
	req.Header.Set("POLY_API_KEY", c.Creds.Key)
	req.Header.Set("POLY_PASSPHRASE", c.Creds.Passphrase)

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GET %s: status %d: %s", path, resp.StatusCode, strings.TrimSpace(string(respBytes)))
	}

	var oo OpenOrder
	if err := json.Unmarshal(respBytes, &oo); err != nil {
		return nil, fmt.Errorf("decode get order response: %w", err)
	}
	return &oo, nil
}

func (c *Client) GetOpenOrders(ctx context.Context, assetID string) ([]OpenOrder, error) {
	if c.Signer == nil || c.Creds == nil {
		return nil, fmt.Errorf("clob: signer and creds are required for L2 auth")
	}
	assetID = strings.TrimSpace(assetID)
	path := "/data/orders"

	u, _ := url.Parse(c.BaseURL + path)
	if assetID != "" {
		q := u.Query()
		q.Set("asset_id", assetID)
		u.RawQuery = q.Encode()
	}

	ts := c.serverTime(ctx)
	sig, err := BuildPolyHmacSignature(c.Creds.Secret, ts, http.MethodGet, path, "")
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("POLY_ADDRESS", c.Signer.Address().Hex())
	req.Header.Set("POLY_SIGNATURE", sig)
	req.Header.Set("POLY_TIMESTAMP", fmt.Sprintf("%d", ts))
	req.Header.Set("POLY_API_KEY", c.Creds.Key)
	req.Header.Set("POLY_PASSPHRASE", c.Creds.Passphrase)

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GET %s: status %d: %s", path, resp.StatusCode, strings.TrimSpace(string(respBytes)))
	}

	var orders []OpenOrder
	if err := json.Unmarshal(respBytes, &orders); err != nil {
		return nil, fmt.Errorf("decode open orders response: %w", err)
	}
	return orders, nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v != "" {
			return v
		}
	}
	return ""
}
