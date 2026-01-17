package orderbook

import (
	"sort"
	"strconv"
	"sync"
)

type Book struct {
	mu   sync.Mutex
	bids map[string]level
	asks map[string]level
}

type level struct {
	price    float64
	priceStr string
	qtyStr   string
}

type Snapshot struct {
	Bids       [][]string
	Asks       [][]string
	BestBid    string
	BestBidQty string
	BestAsk    string
	BestAskQty string
}

func New() *Book {
	return &Book{
		bids: make(map[string]level),
		asks: make(map[string]level),
	}
}

func (b *Book) ApplySnapshot(bids, asks [][]string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.bids = make(map[string]level, len(bids))
	b.asks = make(map[string]level, len(asks))
	applySnapshotLevels(b.bids, bids)
	applySnapshotLevels(b.asks, asks)
}

func (b *Book) ApplyDelta(bids, asks [][]string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	applyDeltaLevels(b.bids, bids)
	applyDeltaLevels(b.asks, asks)
}

func (b *Book) Snapshot(depth int) Snapshot {
	b.mu.Lock()
	defer b.mu.Unlock()

	bids := snapshotLevels(b.bids, true, depth)
	asks := snapshotLevels(b.asks, false, depth)

	snap := Snapshot{
		Bids: bids,
		Asks: asks,
	}
	if len(bids) > 0 {
		snap.BestBid = bids[0][0]
		snap.BestBidQty = bids[0][1]
	}
	if len(asks) > 0 {
		snap.BestAsk = asks[0][0]
		snap.BestAskQty = asks[0][1]
	}

	return snap
}

func applySnapshotLevels(dst map[string]level, levels [][]string) {
	for _, lvl := range levels {
		if len(lvl) < 2 {
			continue
		}
		priceStr := lvl[0]
		qtyStr := lvl[1]
		price, qty, ok := parseLevel(priceStr, qtyStr)
		if !ok || qty <= 0 {
			continue
		}
		dst[priceStr] = level{
			price:    price,
			priceStr: priceStr,
			qtyStr:   qtyStr,
		}
	}
}

func applyDeltaLevels(dst map[string]level, updates [][]string) {
	for _, lvl := range updates {
		if len(lvl) < 2 {
			continue
		}
		priceStr := lvl[0]
		qtyStr := lvl[1]
		price, qty, ok := parseLevel(priceStr, qtyStr)
		if !ok {
			continue
		}
		if qty <= 0 {
			delete(dst, priceStr)
			continue
		}
		dst[priceStr] = level{
			price:    price,
			priceStr: priceStr,
			qtyStr:   qtyStr,
		}
	}
}

func parseLevel(priceStr, qtyStr string) (float64, float64, bool) {
	price, err := strconv.ParseFloat(priceStr, 64)
	if err != nil {
		return 0, 0, false
	}
	qty, err := strconv.ParseFloat(qtyStr, 64)
	if err != nil {
		return 0, 0, false
	}
	return price, qty, true
}

func snapshotLevels(levels map[string]level, desc bool, depth int) [][]string {
	if len(levels) == 0 {
		return nil
	}

	items := make([]level, 0, len(levels))
	for _, lvl := range levels {
		items = append(items, lvl)
	}

	sort.Slice(items, func(i, j int) bool {
		if desc {
			return items[i].price > items[j].price
		}
		return items[i].price < items[j].price
	})

	if depth > 0 && len(items) > depth {
		items = items[:depth]
	}

	out := make([][]string, 0, len(items))
	for _, lvl := range items {
		out = append(out, []string{lvl.priceStr, lvl.qtyStr})
	}
	return out
}
