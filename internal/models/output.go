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
