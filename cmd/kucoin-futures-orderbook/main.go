package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
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
	"market_follower/internal/kucoin"
	"market_follower/internal/models"
	"market_follower/internal/nats"
	"market_follower/internal/orderbook"
	"market_follower/internal/symbols"

	"github.com/gorilla/websocket"
)

var (
	kafkaBrokers = flag.String("brokers", "nats://localhost:4222", "NATS URLs")
	topicFlag    = flag.String("topic", "", "NATS subject")
	restURL      = flag.String("rest-url", "https://api-futures.kucoin.com", "KuCoin Futures REST base URL")
	symbolFlag   = flag.String("symbol", symbols.FromEnv("BTCUSDT"), "KuCoin Futures symbol (e.g. BTCUSDT)")
	contractFlag = flag.String("contract", "", "KuCoin Futures contract symbol (e.g. XBTUSDTM)")
	depthFlag    = flag.Int("depth", 50, "KuCoin Futures orderbook depth (5 or 50)")
)

type kucoinEnvelope struct {
	Type    string          `json:"type"`
	Topic   string          `json:"topic"`
	Subject string          `json:"subject"`
	Data    json.RawMessage `json:"data"`
}

type kucoinDepthData struct {
	Asks [][]json.RawMessage `json:"asks"`
	Bids [][]json.RawMessage `json:"bids"`
	Ts   json.RawMessage     `json:"ts"`
}

func rawToString(raw json.RawMessage) (string, bool) {
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
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil {
		return strconv.FormatFloat(f, 'f', -1, 64), true
	}
	return "", false
}

func rawToInt64(raw json.RawMessage) (int64, bool) {
	if len(raw) == 0 {
		return 0, false
	}
	var i int64
	if err := json.Unmarshal(raw, &i); err == nil {
		return i, true
	}
	var f float64
	if err := json.Unmarshal(raw, &f); err == nil {
		return int64(f), true
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		if v, err := strconv.ParseInt(s, 10, 64); err == nil {
			return v, true
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return int64(f), true
		}
	}
	return 0, false
}

func normalizeMillis(v int64) int64 {
	switch {
	case v > 1e15:
		return v / 1e6
	case v > 1e12:
		return v
	case v > 1e9:
		return v * 1000
	default:
		return v
	}
}

func convertLevels(levels [][]json.RawMessage) [][]string {
	out := make([][]string, 0, len(levels))
	for _, lvl := range levels {
		if len(lvl) < 2 {
			continue
		}
		price, ok1 := rawToString(lvl[0])
		qty, ok2 := rawToString(lvl[1])
		if !ok1 || !ok2 {
			continue
		}
		out = append(out, []string{price, qty})
	}
	return out
}

func main() {
	flag.Parse()
	brokers := strings.Split(*kafkaBrokers, ",")
	symbol := *symbolFlag
	if symbol == "" {
		log.Fatal("symbol is required")
	}
	symbolNorm := symbols.Normalize(symbol)
	symbolLower := symbols.Lower(symbolNorm)
	contract := strings.TrimSpace(*contractFlag)
	if contract == "" {
		contract = strings.TrimSpace(os.Getenv("KUCOIN_FUTURES_SYMBOL"))
	}
	if contract == "" {
		contract = symbols.KucoinFuturesSymbol(symbolNorm)
	}
	if contract == "" {
		log.Fatal("unable to derive KuCoin Futures contract symbol")
	}

	topic := *topicFlag
	if topic == "" {
		topic = "kucoinfutures_" + symbolLower + "_orderbook"
	}

	depth := *depthFlag
	switch depth {
	case 5, 50:
	default:
		log.Printf("Unsupported depth %d, defaulting to 50", depth)
		depth = 50
	}
	outputMode := orderbook.OutputMode()

	producer := nats.NewProducer(brokers, topic)
	defer producer.Close()

	log.Printf("Starting KuCoin Futures Orderbook Follower. Brokers: %v, Topic: %s, Symbol: %s, Contract: %s, Depth: %d, Output: %s", brokers, topic, symbolNorm, contract, depth, outputMode)

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
		url := fmt.Sprintf("%s/api/v1/level2/snapshot?symbol=%s", *restURL, contract)
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
			Code string `json:"code"`
			Data struct {
				Asks [][]json.RawMessage `json:"asks"`
				Bids [][]json.RawMessage `json:"bids"`
				Ts   json.RawMessage     `json:"ts"`
			} `json:"data"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			return nil, nil, 0, err
		}
		if payload.Code != "200000" {
			return nil, nil, 0, fmt.Errorf("unexpected response code: %s", payload.Code)
		}

		bids := convertLevels(payload.Data.Bids)
		asks := convertLevels(payload.Data.Asks)
		tsRaw, _ := rawToInt64(payload.Data.Ts)
		ts := normalizeMillis(tsRaw)
		if ts == 0 {
			ts = time.Now().UnixMilli()
		}
		return bids, asks, ts, nil
	}

	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				last := lastWSAt.Load()
				if last != 0 && time.Since(time.Unix(0, last)) < 2*time.Second {
					continue
				}
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
				bids, asks, ts, err := fetchSnapshot(ctx)
				cancel()
				if err != nil {
					log.Printf("REST snapshot error: %v", err)
					continue
				}
				if len(bids) == 0 && len(asks) == 0 {
					continue
				}
				book.ApplySnapshot(bids, asks)
				emitSnapshot(ts, true)
			}
		}
	}()

	subscribe := func(conn *websocket.Conn) error {
		channel := "/contractMarket/level2Depth50:"
		if depth == 5 {
			channel = "/contractMarket/level2Depth5:"
		}
		msg := map[string]any{
			"id":       time.Now().UnixNano(),
			"type":     "subscribe",
			"topic":    channel + contract,
			"response": true,
		}
		return conn.WriteJSON(msg)
	}

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		cfg, err := kucoin.FetchPublicConfig(ctx, *restURL)
		cancel()
		if err != nil {
			log.Printf("KuCoin futures token error: %v. Retrying in 5s...", err)
			select {
			case <-interrupt:
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}

		conn, _, err := websocket.DefaultDialer.Dial(kucoin.WSURL(cfg), nil)
		if err != nil {
			log.Printf("Error connecting to WebSocket: %v. Retrying in 5s...", err)
			select {
			case <-interrupt:
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}

		if err := subscribe(conn); err != nil {
			log.Printf("Subscription error: %v", err)
			conn.Close()
			time.Sleep(1 * time.Second)
			continue
		}

		done := make(chan struct{})

		go func() {
			interval := cfg.PingInterval
			if interval <= 0 {
				interval = 20 * time.Second
			}
			pingTicker := time.NewTicker(interval)
			defer pingTicker.Stop()
			for {
				select {
				case <-done:
					return
				case <-pingTicker.C:
					if err := conn.WriteJSON(map[string]any{"id": time.Now().UnixNano(), "type": "ping"}); err != nil {
						conn.Close()
						return
					}
				}
			}
		}()

		go func() {
			defer close(done)
			defer conn.Close()

			for {
				_, message, err := conn.ReadMessage()
				if err != nil {
					log.Printf("Read error: %v", err)
					return
				}

				var env kucoinEnvelope
				if err := json.Unmarshal(message, &env); err != nil {
					continue
				}
				if env.Type != "message" || env.Subject != "level2" {
					continue
				}

				var data kucoinDepthData
				if err := json.Unmarshal(env.Data, &data); err != nil {
					continue
				}
				bids := convertLevels(data.Bids)
				asks := convertLevels(data.Asks)
				if len(bids) == 0 && len(asks) == 0 {
					continue
				}
				tsRaw, _ := rawToInt64(data.Ts)
				ts := normalizeMillis(tsRaw)
				if ts == 0 {
					ts = time.Now().UnixMilli()
				}

				book.ApplySnapshot(bids, asks)
				emitSnapshot(ts, true)
				lastWSAt.Store(time.Now().UnixNano())
			}
		}()

		select {
		case <-interrupt:
			log.Println("Interrupt received, shutting down...")
			closeStop()
			conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			select {
			case <-done:
			case <-time.After(time.Second):
			}
			return
		case <-done:
			log.Println("WebSocket closed, reconnecting...")
			time.Sleep(1 * time.Second)
		}
	}
}
