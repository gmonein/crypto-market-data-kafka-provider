package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
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
	kafkaBrokers  = flag.String("brokers", "nats://localhost:4222", "NATS URLs")
	topicFlag     = flag.String("topic", "", "NATS subject")
	wsURL         = "wss://ws-feed.exchange.coinbase.com"
	productIDFlag = flag.String("product-id", "", "Coinbase product id (e.g. BTC-USD or BTC-USDT)")
	symbolFlag    = flag.String("symbol", symbols.FromEnv("BTCUSDT"), "Coinbase symbol (e.g. BTCUSDT)")
	depthFlag     = flag.Int("depth", 50, "Coinbase orderbook depth (levels to emit)")
)

type coinbaseMessage struct {
	Type      string    `json:"type"`
	ProductID string    `json:"product_id"`
	Time      string    `json:"time"`
	Sequence  int64     `json:"sequence"`
	Bids      rawLevels `json:"bids"`
	Asks      rawLevels `json:"asks"`
	Changes   rawLevels `json:"changes"`
	Message   string    `json:"message"`
	Reason    string    `json:"reason"`
}

type pendingUpdate struct {
	sequence int64
	bids     [][]string
	asks     [][]string
	ts       int64
}

type rawLevels [][]json.RawMessage

func coinbaseWSSignature(secret, timestamp string) (string, error) {
	key, err := base64.StdEncoding.DecodeString(secret)
	if err != nil {
		return "", err
	}
	payload := timestamp + "GET" + "/users/self/verify"
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(payload))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil)), nil
}

func main() {
	flag.Parse()
	brokers := strings.Split(*kafkaBrokers, ",")
	symbol := *symbolFlag
	if symbol == "" && *productIDFlag == "" {
		log.Fatal("symbol or product-id is required")
	}
	symbolNorm := symbols.Normalize(symbol)
	productID := strings.TrimSpace(os.Getenv("COINBASE_PRODUCT_ID"))
	if *productIDFlag != "" {
		productID = *productIDFlag
	}
	if productID == "" {
		productID = symbols.CoinbaseProductID(symbolNorm)
	}
	if productID == "" {
		log.Fatal("unable to derive product-id from symbol")
	}
	topic := *topicFlag
	if topic == "" {
		topic = "coinbase_" + symbols.Lower(symbolNorm) + "_orderbook"
	}
	depth := *depthFlag
	if depth <= 0 {
		depth = 50
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

	producer := nats.NewProducer(brokers, topic)
	defer producer.Close()

	log.Printf("Starting Coinbase Orderbook Follower. Brokers: %v, Topic: %s, Product: %s, Output: %s, Mode: %s, RestInterval: %s", brokers, topic, productID, outputMode, mode, restInterval)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	stop := make(chan struct{})
	var stopOnce sync.Once
	closeStop := func() {
		stopOnce.Do(func() { close(stop) })
	}

	book := orderbook.New()
	var lastWSAt atomic.Int64
	restClient := &http.Client{Timeout: 5 * time.Second}
	var stateMu sync.Mutex
	var bookReady bool
	var lastSequence int64
	var pending []pendingUpdate
	apiKey := strings.TrimSpace(os.Getenv("COINBASE_API_KEY"))
	apiSecret := strings.TrimSpace(os.Getenv("COINBASE_API_SECRET"))
	apiPassphrase := strings.TrimSpace(os.Getenv("COINBASE_API_PASSPHRASE"))
	authEnabled := apiKey != "" && apiSecret != "" && apiPassphrase != ""

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

	parseRawValue := func(raw json.RawMessage) (string, bool) {
		if len(raw) == 0 {
			return "", false
		}
		var s string
		if err := json.Unmarshal(raw, &s); err == nil {
			return s, true
		}
		var num json.Number
		if err := json.Unmarshal(raw, &num); err == nil {
			return num.String(), true
		}
		return "", false
	}

	parseRawLevels := func(levels rawLevels) [][]string {
		if len(levels) == 0 {
			return nil
		}
		out := make([][]string, 0, len(levels))
		for _, lvl := range levels {
			if len(lvl) < 2 {
				continue
			}
			price, ok := parseRawValue(lvl[0])
			if !ok {
				continue
			}
			size, ok := parseRawValue(lvl[1])
			if !ok {
				continue
			}
			out = append(out, []string{price, size})
		}
		return out
	}

	splitChanges := func(changes rawLevels) ([][]string, [][]string) {
		var bids [][]string
		var asks [][]string
		for _, change := range changes {
			if len(change) < 3 {
				continue
			}
			side, ok := parseRawValue(change[0])
			if !ok {
				continue
			}
			price, ok := parseRawValue(change[1])
			if !ok {
				continue
			}
			size, ok := parseRawValue(change[2])
			if !ok {
				continue
			}
			switch strings.ToLower(side) {
			case "buy", "bid":
				bids = append(bids, []string{price, size})
			case "sell", "ask", "offer":
				asks = append(asks, []string{price, size})
			}
		}
		return bids, asks
	}

	applySnapshot := func(bids, asks [][]string, seq, ts int64, snapshot bool) bool {
		if len(bids) == 0 && len(asks) == 0 {
			return false
		}
		emitTs := ts

		stateMu.Lock()
		book.ApplySnapshot(bids, asks)
		if seq > 0 {
			lastSequence = seq
		} else {
			lastSequence = 0
		}
		bookReady = true

		if len(pending) > 0 {
			for i, upd := range pending {
				if upd.sequence > 0 && lastSequence > 0 {
					if upd.sequence <= lastSequence {
						continue
					}
					if upd.sequence > lastSequence+1 {
						log.Printf("Sequence gap during resync (last=%d, next=%d); waiting for fresh snapshot", lastSequence, upd.sequence)
						bookReady = false
						lastSequence = 0
						pending = pending[i:]
						stateMu.Unlock()
						return false
					}
					lastSequence = upd.sequence
				} else if upd.sequence > 0 && lastSequence == 0 {
					lastSequence = upd.sequence
				}

				book.ApplyDelta(upd.bids, upd.asks)
				if upd.ts > emitTs {
					emitTs = upd.ts
				}
			}
			pending = nil
		}
		stateMu.Unlock()

		emitSnapshot(emitTs, snapshot)
		return true
	}

	applyUpdate := func(bids, asks [][]string, seq, ts int64) bool {
		if len(bids) == 0 && len(asks) == 0 {
			return false
		}

		stateMu.Lock()
		if !bookReady {
			pending = append(pending, pendingUpdate{
				sequence: seq,
				bids:     bids,
				asks:     asks,
				ts:       ts,
			})
			stateMu.Unlock()
			return false
		}

		if seq > 0 && lastSequence > 0 {
			if seq <= lastSequence {
				stateMu.Unlock()
				return false
			}
			if seq > lastSequence+1 {
				prevSeq := lastSequence
				bookReady = false
				lastSequence = 0
				pending = pending[:0]
				pending = append(pending, pendingUpdate{
					sequence: seq,
					bids:     bids,
					asks:     asks,
					ts:       ts,
				})
				stateMu.Unlock()
				log.Printf("Sequence gap (last=%d, next=%d); resyncing", prevSeq, seq)
				return false
			}
			lastSequence = seq
		} else if seq > 0 && lastSequence == 0 {
			lastSequence = seq
		}

		book.ApplyDelta(bids, asks)
		stateMu.Unlock()

		emitSnapshot(ts, false)
		return true
	}

	fetchSnapshot := func(ctx context.Context) ([][]string, [][]string, int64, int64, error) {
		url := "https://api.exchange.coinbase.com/products/" + productID + "/book?level=2"
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, nil, 0, 0, err
		}
		resp, err := restClient.Do(req)
		if err != nil {
			return nil, nil, 0, 0, err
		}
		defer resp.Body.Close()

		var payload struct {
			Sequence int64     `json:"sequence"`
			Bids     rawLevels `json:"bids"`
			Asks     rawLevels `json:"asks"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			return nil, nil, 0, 0, err
		}
		ts := time.Now().UnixMilli()
		return parseRawLevels(payload.Bids), parseRawLevels(payload.Asks), payload.Sequence, ts, nil
	}

	pollSnapshot := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		bids, asks, seq, ts, err := fetchSnapshot(ctx)
		cancel()
		if err != nil {
			log.Printf("REST snapshot error: %v", err)
			return
		}
		if len(bids) == 0 && len(asks) == 0 {
			return
		}
		applySnapshot(bids, asks, seq, ts, true)
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
					stateMu.Lock()
					ready := bookReady
					stateMu.Unlock()
					if ready {
						last := lastWSAt.Load()
						if last != 0 && time.Since(time.Unix(0, last)) < 2*time.Second {
							continue
						}
					}
				}
				pollSnapshot()
			}
		}
	}()

	if mode == "rest" {
		<-interrupt
		log.Println("Interrupt received, shutting down...")
		closeStop()
		return
	}

	for {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("Dial error: %v. Retrying in 5s...", err)
			select {
			case <-interrupt:
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}

		sub := map[string]any{
			"type":        "subscribe",
			"product_ids": []string{productID},
			"channels":    []string{"level2"},
		}
		if authEnabled {
			ts := strconv.FormatInt(time.Now().Unix(), 10)
			sig, err := coinbaseWSSignature(apiSecret, ts)
			if err != nil {
				log.Printf("Coinbase auth error: %v", err)
			} else {
				sub["signature"] = sig
				sub["key"] = apiKey
				sub["passphrase"] = apiPassphrase
				sub["timestamp"] = ts
			}
		}
		if err := conn.WriteJSON(sub); err != nil {
			log.Printf("Subscribe error: %v", err)
			conn.Close()
			continue
		}

		done := make(chan struct{})

		go func() {
			defer close(done)
			defer conn.Close()

			for {
				_, message, err := conn.ReadMessage()
				if err != nil {
					log.Printf("Read error: %v", err)
					return
				}

				var msg coinbaseMessage
				if err := json.Unmarshal(message, &msg); err != nil {
					continue
				}

				if msg.Type == "" {
					continue
				}
				if msg.Type == "error" {
					if msg.Reason != "" {
						log.Printf("Coinbase error: %s (%s)", msg.Message, msg.Reason)
					} else {
						log.Printf("Coinbase error: %s", msg.Message)
					}
					continue
				}

				ts := time.Now().UnixMilli()
				if msg.Time != "" {
					if parsed, err := time.Parse(time.RFC3339Nano, msg.Time); err == nil {
						ts = parsed.UnixMilli()
					}
				}

				switch msg.Type {
				case "snapshot":
					if applySnapshot(parseRawLevels(msg.Bids), parseRawLevels(msg.Asks), msg.Sequence, ts, true) {
						lastWSAt.Store(time.Now().UnixNano())
					}
				case "l2update", "update":
					bids, asks := splitChanges(msg.Changes)
					if applyUpdate(bids, asks, msg.Sequence, ts) {
						lastWSAt.Store(time.Now().UnixNano())
					}
				default:
					continue
				}
			}
		}()

		select {
		case <-interrupt:
			log.Println("Interrupt received, shutting down...")
			closeStop()
			conn.Close()
			return
		case <-done:
			conn.Close()
			time.Sleep(1 * time.Second)
		}
	}
}
