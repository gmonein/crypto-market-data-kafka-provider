package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
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
	wsURL        = flag.String("ws-url", "wss://contract.mexc.com/edge", "MEXC contract WebSocket URL")
	symbolFlag   = flag.String("symbol", symbols.FromEnv("BTCUSDT"), "MEXC symbol (e.g. BTCUSDT)")
	depthFlag    = flag.Int("depth", 20, "MEXC orderbook depth (5, 10, 20)")
)

type mexcDepthEnvelope struct {
	Channel string `json:"channel"`
	Data    struct {
		Asks [][]json.RawMessage `json:"asks"`
		Bids [][]json.RawMessage `json:"bids"`
	} `json:"data"`
	Ts json.RawMessage `json:"ts"`
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

func convertLevels(levels [][]json.RawMessage) [][]string {
	out := make([][]string, 0, len(levels))
	for _, lvl := range levels {
		if len(lvl) < 2 {
			continue
		}
		price, ok1 := rawToString(lvl[0])
		qtyRaw := lvl[1]
		if len(lvl) >= 3 {
			qtyRaw = lvl[2]
		}
		qty, ok2 := rawToString(qtyRaw)
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
	subSymbol := symbols.MexcFuturesSymbol(symbolNorm)
	if subSymbol == "" {
		log.Fatal("unable to derive MEXC symbol")
	}
	topic := *topicFlag
	if topic == "" {
		topic = "mexc_" + symbolLower + "_orderbook"
	}
	depth := *depthFlag
	switch depth {
	case 5, 10, 20:
	default:
		log.Printf("Unsupported depth %d, defaulting to 20", depth)
		depth = 20
	}
	outputMode := orderbook.OutputMode()

	producer := nats.NewProducer(brokers, topic)
	defer producer.Close()

	log.Printf("Starting MEXC Orderbook Follower. Brokers: %v, Topic: %s, Symbol: %s, Depth: %d, Output: %s", brokers, topic, symbolNorm, depth, outputMode)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	stop := make(chan struct{})
	var stopOnce sync.Once
	closeStop := func() {
		stopOnce.Do(func() { close(stop) })
	}

	book := orderbook.New()

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

	subscribe := func(conn *websocket.Conn) error {
		msg := map[string]any{
			"method": "sub.depth.full",
			"param": map[string]any{
				"symbol": subSymbol,
				"limit":  depth,
			},
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
			pingTicker := time.NewTicker(20 * time.Second)
			defer pingTicker.Stop()
			for {
				select {
				case <-done:
					return
				case <-pingTicker.C:
					if err := conn.WriteJSON(map[string]string{"method": "ping"}); err != nil {
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

				var env mexcDepthEnvelope
				if err := json.Unmarshal(message, &env); err != nil {
					continue
				}
				if !strings.HasPrefix(env.Channel, "push.depth") {
					continue
				}

				bids := convertLevels(env.Data.Bids)
				asks := convertLevels(env.Data.Asks)
				if len(bids) == 0 && len(asks) == 0 {
					continue
				}

				ts, _ := rawToInt64(env.Ts)
				if ts == 0 {
					ts = time.Now().UnixMilli()
				}

				book.ApplySnapshot(bids, asks)
				emitSnapshot(ts, true)
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
