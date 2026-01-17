package models

import "encoding/json"

type BinanceTrade struct {
	EventType string `json:"e"`
	EventTime int64  `json:"E"`
	Symbol    string `json:"s"`
	Price     string `json:"p"`
	Quantity  string `json:"q"`
	TradeTime int64  `json:"T"`
	IsMaker   bool   `json:"m"`
}

type BinanceKline struct {
	EventType string      `json:"e"`
	EventTime int64       `json:"E"`
	Symbol    string      `json:"s"`
	Kline     KlineDetail `json:"k"`
}

type KlineDetail struct {
	StartTime    int64       `json:"t"`
	CloseTime    int64       `json:"T"`
	Symbol       string      `json:"s"`
	Interval     string      `json:"i"`
	Open         json.Number `json:"o"`
	Close        json.Number `json:"c"`
	High         json.Number `json:"h"`
	Low          json.Number `json:"l"`
	Volume       json.Number `json:"v"`
	QuoteVolume  json.Number `json:"q"`
	TradeCount   int64       `json:"n"`
	TakerBuyVol  json.Number `json:"V"`
	TakerBuyQVol json.Number `json:"Q"`
}

type ChainlinkPrice struct {
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp"`
	Symbol    string  `json:"symbol"`
}
