package clob

import "encoding/json"

type ApiKeyCreds struct {
	Key        string `json:"key"`
	Secret     string `json:"secret"`
	Passphrase string `json:"passphrase"`
}

type apiKeyRaw struct {
	APIKey     string `json:"apiKey"`
	Key        string `json:"key"`
	Secret     string `json:"secret"`
	Passphrase string `json:"passphrase"`
}

type TickSizeResponse struct {
	MinimumTickSize float64 `json:"minimum_tick_size"`
}

type NegRiskResponse struct {
	NegRisk bool `json:"neg_risk"`
}

type FeeRateResponse struct {
	BaseFee json.Number `json:"base_fee"`
}

type OrderResponse struct {
	Success    bool   `json:"success"`
	ErrorMsg   string `json:"errorMsg"`
	OrderID    string `json:"orderID"`
	Status     string `json:"status"`
	MakingAmt  string `json:"makingAmount"`
	TakingAmt  string `json:"takingAmount"`
	Error      string `json:"error"`
	Message    string `json:"message"`
	StatusText string `json:"statusText"`
}

type CancelOrderPayload struct {
	OrderID string `json:"orderID"`
}

type OpenOrder struct {
	ID           string `json:"id"`
	Status       string `json:"status"`
	Owner        string `json:"owner"`
	MakerAddress string `json:"maker_address"`
	Market       string `json:"market"`
	AssetID      string `json:"asset_id"`
	Side         string `json:"side"`
	OriginalSize string `json:"original_size"`
	SizeMatched  string `json:"size_matched"`
	Price        string `json:"price"`
	Outcome      string `json:"outcome"`
	CreatedAt    int64  `json:"created_at"`
	Expiration   string `json:"expiration"`
	OrderType    string `json:"order_type"`
}

type OrderBookLevel struct {
	Price string `json:"price"`
	Size  string `json:"size"`
}

type OrderBookSummary struct {
	AssetID   string           `json:"asset_id"`
	Bids      []OrderBookLevel `json:"bids"`
	Asks      []OrderBookLevel `json:"asks"`
	Timestamp string           `json:"timestamp"`
	Hash      string           `json:"hash"`
}
