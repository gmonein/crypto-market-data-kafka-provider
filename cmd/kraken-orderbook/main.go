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

	"market_follower/internal/kafka"
	"market_follower/internal/models"
	"market_follower/internal/orderbook"
	"market_follower/internal/symbols"

	"github.com/gorilla/websocket"
)

var (
	kafkaBrokers = flag.String("brokers", "localhost:9092", "Kafka brokers")
	topicFlag    = flag.String("topic", "", "Kafka topic")
	wsURL        = "wss://ws.kraken.com"
	pairFlag     = flag.String("pair", "", "Kraken pair (e.g. XBT/USD)")
	symbolFlag   = flag.String("symbol", symbols.FromEnv("BTCUSDT"), "Kraken symbol (e.g. BTCUSDT)")
	depthFlag    = flag.Int("depth", 100, "Kraken book depth (e.g. 10, 25, 100, 500, 1000)")
)

type SystemEvent struct {
	Event string `json:"event"`
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
		if symbolNorm == "BTCUSDT" || symbolNorm == "BTCUSD" {
			topic = "kraken_orderbook"
		} else {
			topic = "kraken_" + symbols.Lower(symbolNorm) + "_orderbook"
		}
	}

	producer := kafka.NewProducer(brokers, topic)
	defer producer.Close()

	depth := *depthFlag
	if depth <= 0 {
		depth = 100
	}
	log.Printf("Starting Kraken Orderbook Follower. Brokers: %v, Topic: %s, Pair: %s, Depth: %d", brokers, topic, pair, depth)

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
	restDepth := depth
	if restDepth <= 0 {
		restDepth = 100
	}

	emitSnapshot := func(ts int64, snapshot bool) {
		snap := book.Snapshot(depth)
		if len(snap.Bids) == 0 && len(snap.Asks) == 0 {
			return
		}
		out := models.OrderbookOutput{
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
		b, err := json.Marshal(out)
		if err != nil {
			return
		}
		if err := producer.WriteMessage(nil, b); err != nil {
			log.Printf("Kafka write error: %v", err)
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
				Bids [][]string `json:"bids"`
				Asks [][]string `json:"asks"`
			} `json:"result"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			return nil, nil, 0, err
		}
		for _, data := range payload.Result {
			return data.Bids, data.Asks, time.Now().UnixMilli(), nil
		}
		return nil, nil, 0, nil
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

				if snapshot {
					book.ApplySnapshot(snapshotBids, snapshotAsks)
				}
				if len(deltaBids) > 0 || len(deltaAsks) > 0 {
					book.ApplyDelta(deltaBids, deltaAsks)
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
