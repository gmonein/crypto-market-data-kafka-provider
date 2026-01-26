package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"market_follower/internal/features"
	"market_follower/internal/models"
	"market_follower/internal/nats"
	"market_follower/internal/orderbook"
	"market_follower/internal/symbols"

	"github.com/gorilla/websocket"
)

var (
	kafkaBrokers = flag.String("brokers", "nats://localhost:4222", "NATS URLs")
	topicFlag    = flag.String("topic", "", "NATS subject")
	wsURL        = "wss://ws.kraken.com"
	pairFlag     = flag.String("pair", "", "Kraken pair (e.g. XBT/USD)")
	symbolFlag   = flag.String("symbol", symbols.FromEnv("BTCUSDT"), "Kraken symbol (e.g. BTCUSDT)")
	depthFlag    = flag.Int("depth", 100, "Kraken book depth (e.g. 10, 25, 100, 500, 1000)")
)

type SystemEvent struct {
	Event string `json:"event"`
}

func parseInt64Any(raw json.RawMessage) (int64, bool) {
	if len(raw) == 0 {
		return 0, false
	}
	var asInt int64
	if err := json.Unmarshal(raw, &asInt); err == nil {
		return asInt, true
	}
	var asFloat float64
	if err := json.Unmarshal(raw, &asFloat); err == nil {
		return int64(asFloat), true
	}
	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		val, err := strconv.ParseInt(asString, 10, 64)
		if err == nil {
			return val, true
		}
	}
	return 0, false
}

func bestPriceFromLevels(levels [][]string, wantMax bool) (float64, bool) {
	found := false
	best := 0.0
	for _, lvl := range levels {
		if len(lvl) < 1 {
			continue
		}
		val, err := strconv.ParseFloat(lvl[0], 64)
		if err != nil {
			continue
		}
		if !found {
			best = val
			found = true
			continue
		}
		if wantMax {
			if val > best {
				best = val
			}
		} else if val < best {
			best = val
		}
	}
	return best, found
}

func checksumMatches(remote int64, local uint32) bool {
	if remote == int64(local) {
		return true
	}
	if remote == int64(int32(local)) {
		return true
	}
	return false
}

func normalizeLevels(levels [][]any) [][]string {
	if len(levels) == 0 {
		return nil
	}
	out := make([][]string, 0, len(levels))
	for _, lvl := range levels {
		if len(lvl) < 2 {
			continue
		}
		price, ok := anyToString(lvl[0])
		if !ok {
			continue
		}
		qty, ok := anyToString(lvl[1])
		if !ok {
			continue
		}
		out = append(out, []string{price, qty})
	}
	return out
}

func anyToString(val any) (string, bool) {
	switch v := val.(type) {
	case string:
		return v, true
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), true
	case json.Number:
		return v.String(), true
	default:
		return "", false
	}
}

func main() {
	flag.Parse()
	brokers := strings.Split(*kafkaBrokers, ",")
	symbol := *symbolFlag
	if symbol == "" && *pairFlag == "" {
		log.Fatal("symbol or pair is required")
	}
	symbolNorm := symbols.Normalize(symbol)
	pair := *pairFlag
	if pair == "" {
		pair = symbols.KrakenPair(symbolNorm)
	}
	if pair == "" {
		log.Fatal("unable to derive pair from symbol")
	}
	topic := *topicFlag
	if topic == "" {
		topic = "kraken_" + symbols.Lower(symbolNorm) + "_orderbook"
	}

	producer := nats.NewProducer(brokers, topic)
	defer producer.Close()

	depth := *depthFlag
	if depth <= 0 {
		depth = 100
	}
	outputMode := orderbook.OutputMode()
	mode := strings.ToLower(strings.TrimSpace(os.Getenv("ORDERBOOK_MODE")))
	if mode == "" {
		mode = "ws"
	}
	if mode != "ws" && mode != "rest" {
		mode = "ws"
	}
	restInterval := time.Second
	if raw := strings.TrimSpace(os.Getenv("ORDERBOOK_REST_INTERVAL_MS")); raw != "" {
		if ms, err := strconv.ParseInt(raw, 10, 64); err == nil && ms > 0 {
			restInterval = time.Duration(ms) * time.Millisecond
		}
	}
	log.Printf("Starting Kraken Orderbook Follower. Brokers: %v, Topic: %s, Pair: %s, Depth: %d, Output: %s, Mode: %s, RestInterval: %s", brokers, topic, pair, depth, outputMode, mode, restInterval)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	stop := make(chan struct{})
	var stopOnce sync.Once
	closeStop := func() {
		stopOnce.Do(func() { close(stop) })
	}

	book := orderbook.New()
	var lastWSAt atomic.Int64
	var resyncMu sync.Mutex
	lastResyncAt := time.Time{}
	restClient := &http.Client{Timeout: 5 * time.Second}
	restDepth := depth
	if restDepth <= 0 {
		restDepth = 100
	}

	emitSnapshot := func(ts int64, snapshot bool) {
		snap := book.Snapshot(depth)
		if len(snap.Bids) == 0 && len(snap.Asks) == 0 {
			return
		}
		var out any
		if outputMode == orderbook.OutputFeatures {
			metrics, ok := features.ComputeOrderbookFeatures(snap)
			if !ok {
				return
			}
			out = models.OrderbookFeaturesOutput{
				Timestamp:       ts,
				Symbol:          symbolNorm,
				BestBid:         snap.BestBid,
				BestAsk:         snap.BestAsk,
				BestBidQuantity: snap.BestBidQty,
				BestAskQuantity: snap.BestAskQty,
				Mid:             metrics.Mid,
				Spread:          metrics.Spread,
				SpreadBps:       metrics.SpreadBps,
				MicroPrice:      metrics.MicroPrice,
				Imbalance1:      metrics.Imbalance1,
				Imbalance5:      metrics.Imbalance5,
				Imbalance10:     metrics.Imbalance10,
				BidDepth5:       metrics.BidDepth5,
				AskDepth5:       metrics.AskDepth5,
				BidDepth10:      metrics.BidDepth10,
				AskDepth10:      metrics.AskDepth10,
			}
		} else {
			out = models.OrderbookOutput{
				Timestamp:       ts,
				Symbol:          symbolNorm,
				BestBid:         snap.BestBid,
				BestAsk:         snap.BestAsk,
				BestBidQuantity: snap.BestBidQty,
				BestAskQuantity: snap.BestAskQty,
				Bids:            snap.Bids,
				Asks:            snap.Asks,
				Snapshot:        snapshot,
			}
		}
		b, err := json.Marshal(out)
		if err != nil {
			return
		}
		if err := producer.WriteMessage(nil, b); err != nil {
			log.Printf("NATS publish error: %v", err)
		}
	}

	fetchSnapshot := func(ctx context.Context) ([][]string, [][]string, int64, error) {
		pairParam := strings.ReplaceAll(pair, "/", "")
		url := "https://api.kraken.com/0/public/Depth?pair=" + pairParam + "&count=" + strconv.Itoa(restDepth)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, nil, 0, err
		}
		resp, err := restClient.Do(req)
		if err != nil {
			return nil, nil, 0, err
		}
		defer resp.Body.Close()

		var payload struct {
			Result map[string]struct {
				Bids [][]any `json:"bids"`
				Asks [][]any `json:"asks"`
			} `json:"result"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			return nil, nil, 0, err
		}
		for _, data := range payload.Result {
			return normalizeLevels(data.Bids), normalizeLevels(data.Asks), time.Now().UnixMilli(), nil
		}
		return nil, nil, 0, nil
	}

	pollSnapshot := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		bids, asks, ts, err := fetchSnapshot(ctx)
		cancel()
		if err != nil {
			log.Printf("REST snapshot error: %v", err)
			return
		}
		if len(bids) == 0 && len(asks) == 0 {
			return
		}
		book.ApplySnapshot(bids, asks)
		emitSnapshot(ts, true)
	}

	resync := func(reason string) {
		resyncMu.Lock()
		defer resyncMu.Unlock()
		if !lastResyncAt.IsZero() && time.Since(lastResyncAt) < time.Second {
			return
		}
		lastResyncAt = time.Now()

		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		bids, asks, ts, err := fetchSnapshot(ctx)
		cancel()
		if err != nil {
			log.Printf("Kraken resync error (%s): %v", reason, err)
			return
		}
		if len(bids) == 0 && len(asks) == 0 {
			return
		}
		book.ApplySnapshot(bids, asks)
		emitSnapshot(ts, true)
		log.Printf("Kraken resync applied (%s)", reason)
	}

	go func() {
		ticker := time.NewTicker(restInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if mode != "rest" {
					last := lastWSAt.Load()
					if last != 0 && time.Since(time.Unix(0, last)) < 2*time.Second {
						continue
					}
				}
				pollSnapshot()
			}
		}
	}()

	if mode == "rest" {
		<-interrupt
		log.Println("Shutting down...")
		closeStop()
		return
	}

	for {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("Dial error: %v. Retrying...", err)
			select {
			case <-interrupt:
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}

		sub := map[string]interface{}{
			"event": "subscribe",
			"pair":  []string{pair},
			"subscription": map[string]interface{}{
				"name":  "book",
				"depth": depth,
			},
		}
		if err := conn.WriteJSON(sub); err != nil {
			log.Printf("Subscribe error: %v", err)
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}

		done := make(chan struct{})
		swapSides := false

		go func() {
			defer close(done)
			defer conn.Close()

			for {
				_, message, err := conn.ReadMessage()
				if err != nil {
					log.Printf("Read error: %v", err)
					return
				}

				if len(message) > 0 && message[0] == '{' {
					var sys SystemEvent
					if err := json.Unmarshal(message, &sys); err == nil && sys.Event == "heartbeat" {
						continue
					}
					continue
				}

				var dataArr []json.RawMessage
				if err := json.Unmarshal(message, &dataArr); err != nil {
					continue
				}
				if len(dataArr) < 4 {
					continue
				}

				var payload map[string]json.RawMessage
				if err := json.Unmarshal(dataArr[1], &payload); err != nil {
					continue
				}

				var snapshotBids [][]string
				var snapshotAsks [][]string
				var deltaBids [][]string
				var deltaAsks [][]string
				snapshot := false
				if raw, ok := payload["bs"]; ok {
					if err := json.Unmarshal(raw, &snapshotBids); err == nil {
						snapshot = true
					}
				}
				if raw, ok := payload["as"]; ok {
					if err := json.Unmarshal(raw, &snapshotAsks); err == nil {
						snapshot = true
					}
				}
				if raw, ok := payload["b"]; ok {
					_ = json.Unmarshal(raw, &deltaBids)
				}
				if raw, ok := payload["a"]; ok {
					_ = json.Unmarshal(raw, &deltaAsks)
				}

				if len(snapshotBids) == 0 && len(snapshotAsks) == 0 && len(deltaBids) == 0 && len(deltaAsks) == 0 {
					continue
				}

				if snapshot && !swapSides {
					rawBid, hasRawBid := bestPriceFromLevels(snapshotBids, true)
					rawAsk, hasRawAsk := bestPriceFromLevels(snapshotAsks, false)
					if hasRawBid && hasRawAsk && rawBid > rawAsk {
						swapSides = true
						log.Printf("Kraken snapshot appears crossed, swapping sides raw_bid=%.8f raw_ask=%.8f", rawBid, rawAsk)
					}
				}
				if swapSides {
					snapshotBids, snapshotAsks = snapshotAsks, snapshotBids
					deltaBids, deltaAsks = deltaAsks, deltaBids
				}

				if snapshot {
					book.ApplySnapshot(snapshotBids, snapshotAsks)
				}
				if len(deltaBids) > 0 || len(deltaAsks) > 0 {
					book.ApplyDelta(deltaBids, deltaAsks)
				}

				if raw, ok := payload["c"]; ok {
					if checksum, ok := parseInt64Any(raw); ok {
						snap := book.Snapshot(10)
						local := orderbook.ChecksumKraken(snap.Bids, snap.Asks, 10)
						if !checksumMatches(checksum, local) {
							log.Printf("Kraken checksum mismatch remote=%d local=%d", checksum, int64(int32(local)))
							resync("checksum_mismatch")
							conn.Close()
							return
						}
					}
				}
				if book.IsCrossed() {
					bestBid, bestAsk, _ := book.BestBidAsk()
					log.Printf("Kraken book crossed (bb=%.8f ba=%.8f), forcing resync", bestBid, bestAsk)
					resync("crossed")
					conn.Close()
					return
				}

				emitSnapshot(time.Now().UnixMilli(), snapshot)
				lastWSAt.Store(time.Now().UnixNano())
			}
		}()

		select {
		case <-interrupt:
			log.Println("Shutting down...")
			closeStop()
			conn.Close()
			return
		case <-done:
			conn.Close()
			time.Sleep(1 * time.Second)
		}
	}
}
