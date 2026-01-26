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
	"market_follower/internal/models"
	"market_follower/internal/nats"
	"market_follower/internal/orderbook"
	"market_follower/internal/symbols"

	"github.com/gorilla/websocket"
)

var (
	kafkaBrokers = flag.String("brokers", "nats://localhost:4222", "NATS URLs")
	topicFlag    = flag.String("topic", "", "NATS subject")
	wsURL        = flag.String("ws-url", "wss://api.gateio.ws/ws/v4/", "Gate.io WebSocket URL")
	symbolFlag   = flag.String("symbol", symbols.FromEnv("BTCUSDT"), "Gate.io symbol (e.g. BTCUSDT)")
	depthFlag    = flag.Int("depth", 20, "Gate.io orderbook depth")
)

type gateOrderBookEnvelope struct {
	Time    int64           `json:"time"`
	TimeMS  int64           `json:"time_ms"`
	Channel string          `json:"channel"`
	Event   string          `json:"event"`
	Result  json.RawMessage `json:"result"`
}

type gateOrderBook struct {
	Bids [][]string      `json:"bids"`
	Asks [][]string      `json:"asks"`
	T    json.RawMessage `json:"t"`
	ID   json.RawMessage `json:"id"`
}

func parseGateOrderBook(raw json.RawMessage) (gateOrderBook, bool) {
	if len(raw) == 0 {
		return gateOrderBook{}, false
	}
	var ob gateOrderBook
	if err := json.Unmarshal(raw, &ob); err != nil {
		return gateOrderBook{}, false
	}
	return ob, true
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

func main() {
	flag.Parse()
	brokers := strings.Split(*kafkaBrokers, ",")
	symbol := *symbolFlag
	if symbol == "" {
		log.Fatal("symbol is required")
	}
	symbolNorm := symbols.Normalize(symbol)
	symbolLower := symbols.Lower(symbolNorm)
	subSymbol := symbols.GateSpotSymbol(symbolNorm)
	if subSymbol == "" {
		log.Fatal("unable to derive Gate.io symbol")
	}
	topic := *topicFlag
	if topic == "" {
		topic = "gate_" + symbolLower + "_orderbook"
	}
	depth := *depthFlag
	if depth <= 0 {
		depth = 20
	}
	outputMode := orderbook.OutputMode()

	producer := nats.NewProducer(brokers, topic)
	defer producer.Close()

	log.Printf("Starting Gate.io Orderbook Follower. Brokers: %v, Topic: %s, Symbol: %s, Depth: %d, Output: %s", brokers, topic, symbolNorm, depth, outputMode)

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
		url := fmt.Sprintf("https://api.gateio.ws/api/v4/spot/order_book?currency_pair=%s&limit=%d", subSymbol, depth)
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
			Current json.RawMessage `json:"current"`
			Update  json.RawMessage `json:"update"`
			Bids    [][]string      `json:"bids"`
			Asks    [][]string      `json:"asks"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			return nil, nil, 0, err
		}
		ts := time.Now().UnixMilli()
		if v, ok := rawToInt64(payload.Update); ok {
			ts = normalizeMillis(v)
		} else if v, ok := rawToInt64(payload.Current); ok {
			ts = normalizeMillis(v)
		}
		return payload.Bids, payload.Asks, ts, nil
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
		msg := map[string]any{
			"time":    time.Now().Unix(),
			"channel": "spot.order_book",
			"event":   "subscribe",
			"payload": []string{subSymbol, strconv.Itoa(depth), "100ms"},
		}
		return conn.WriteJSON(msg)
	}

	for {
		conn, _, err := websocket.DefaultDialer.Dial(*wsURL, nil)
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
			defer close(done)
			defer conn.Close()

			for {
				_, message, err := conn.ReadMessage()
				if err != nil {
					log.Printf("Read error: %v", err)
					return
				}

				var env gateOrderBookEnvelope
				if err := json.Unmarshal(message, &env); err != nil {
					continue
				}
				if env.Channel != "spot.order_book" || env.Event != "update" {
					continue
				}

				ob, ok := parseGateOrderBook(env.Result)
				if !ok {
					continue
				}
				if len(ob.Bids) == 0 && len(ob.Asks) == 0 {
					continue
				}

				ts := env.TimeMS
				if ts == 0 && env.Time > 0 {
					ts = env.Time * 1000
				}
				if v, ok := rawToInt64(ob.T); ok {
					ts = normalizeMillis(v)
				}
				if ts == 0 {
					ts = time.Now().UnixMilli()
				}

				book.ApplySnapshot(ob.Bids, ob.Asks)
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
