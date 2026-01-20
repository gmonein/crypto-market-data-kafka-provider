package features

import (
	"strconv"

	"market_follower/internal/orderbook"
)

type OrderbookFeatures struct {
	Mid         float64
	Spread      float64
	SpreadBps   float64
	MicroPrice  float64
	Imbalance1  float64
	Imbalance5  float64
	Imbalance10 float64
	BidDepth5   float64
	AskDepth5   float64
	BidDepth10  float64
	AskDepth10  float64
}

func ComputeOrderbookFeatures(snap orderbook.Snapshot) (OrderbookFeatures, bool) {
	bestBid, ok := parseFloat(snap.BestBid)
	if !ok {
		return OrderbookFeatures{}, false
	}
	bestAsk, ok := parseFloat(snap.BestAsk)
	if !ok {
		return OrderbookFeatures{}, false
	}
	bestBidQty, ok := parseFloat(snap.BestBidQty)
	if !ok {
		return OrderbookFeatures{}, false
	}
	bestAskQty, ok := parseFloat(snap.BestAskQty)
	if !ok {
		return OrderbookFeatures{}, false
	}

	mid := (bestBid + bestAsk) / 2.0
	spread := bestAsk - bestBid
	spreadBps := 0.0
	if mid > 0 {
		spreadBps = (spread / mid) * 10000.0
	}

	micro := mid
	if bestBidQty+bestAskQty > 0 {
		micro = (bestBid*bestAskQty + bestAsk*bestBidQty) / (bestBidQty + bestAskQty)
	}

	bidQtys := extractQtys(snap.Bids, 10)
	askQtys := extractQtys(snap.Asks, 10)
	bidDepth5 := sumTop(bidQtys, 5)
	askDepth5 := sumTop(askQtys, 5)
	bidDepth10 := sumTop(bidQtys, 10)
	askDepth10 := sumTop(askQtys, 10)

	return OrderbookFeatures{
		Mid:         mid,
		Spread:      spread,
		SpreadBps:   spreadBps,
		MicroPrice:  micro,
		Imbalance1:  imbalance(bestBidQty, bestAskQty),
		Imbalance5:  imbalance(bidDepth5, askDepth5),
		Imbalance10: imbalance(bidDepth10, askDepth10),
		BidDepth5:   bidDepth5,
		AskDepth5:   askDepth5,
		BidDepth10:  bidDepth10,
		AskDepth10:  askDepth10,
	}, true
}

func extractQtys(levels [][]string, max int) []float64 {
	if len(levels) == 0 {
		return nil
	}
	limit := len(levels)
	if max > 0 && limit > max {
		limit = max
	}
	qtys := make([]float64, 0, limit)
	for i := 0; i < limit; i++ {
		if len(levels[i]) < 2 {
			continue
		}
		qty, ok := parseFloat(levels[i][1])
		if !ok {
			continue
		}
		qtys = append(qtys, qty)
	}
	return qtys
}

func sumTop(values []float64, max int) float64 {
	if len(values) == 0 {
		return 0
	}
	limit := len(values)
	if max > 0 && limit > max {
		limit = max
	}
	sum := 0.0
	for i := 0; i < limit; i++ {
		sum += values[i]
	}
	return sum
}

func imbalance(bidQty, askQty float64) float64 {
	if bidQty+askQty == 0 {
		return 0
	}
	return (bidQty - askQty) / (bidQty + askQty)
}

func parseFloat(raw string) (float64, bool) {
	if raw == "" {
		return 0, false
	}
	val, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, false
	}
	return val, true
}
