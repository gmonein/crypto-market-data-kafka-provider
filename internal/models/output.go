package models

type PriceOutput struct {
	Timestamp       int64  `json:"T"`
	Price           string `json:"p"`
	BestBid         string `json:"bb"`
	BestAsk         string `json:"ba"`
	BestBidQuantity string `json:"bq"`
	BestAskQuantity string `json:"aq"`
	Symbol          string `json:"s"`
}

type VolumeOutput struct {
	Timestamp int64  `json:"T"`
	Volume    string `json:"v"`
	Symbol    string `json:"s"`
}

type OrderbookOutput struct {
	Timestamp       int64      `json:"T"`
	Symbol          string     `json:"s"`
	BestBid         string     `json:"bb"`
	BestAsk         string     `json:"ba"`
	BestBidQuantity string     `json:"bq"`
	BestAskQuantity string     `json:"aq"`
	Bids            [][]string `json:"bids"`
	Asks            [][]string `json:"asks"`
	Snapshot        bool       `json:"snapshot"`
}

type OrderbookFeaturesOutput struct {
	Timestamp       int64   `json:"T"`
	Symbol          string  `json:"s"`
	BestBid         string  `json:"bb"`
	BestAsk         string  `json:"ba"`
	BestBidQuantity string  `json:"bq"`
	BestAskQuantity string  `json:"aq"`
	Mid             float64 `json:"mid"`
	Spread          float64 `json:"spr"`
	SpreadBps       float64 `json:"sb"`
	MicroPrice      float64 `json:"mic"`
	Imbalance1      float64 `json:"i1"`
	Imbalance5      float64 `json:"i5"`
	Imbalance10     float64 `json:"i10"`
	BidDepth5       float64 `json:"bd5"`
	AskDepth5       float64 `json:"ad5"`
	BidDepth10      float64 `json:"bd10"`
	AskDepth10      float64 `json:"ad10"`
}
