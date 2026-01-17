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
	wsURL        = flag.String("ws-url", "wss://ws.okx.com:8443/ws/v5/public", "OKX WebSocket URL")
	instIDFlag   = flag.String("inst-id", "", "OKX instrument id (e.g. BTC-USDT)")
	symbolFlag   = flag.String("symbol", symbols.FromEnv("BTCUSDT"), "OKX symbol (e.g. BTCUSDT)")
	depthFlag    = flag.Int("depth", 50, "OKX orderbook depth (5, 50, or 0 for full)")
)

type okxOrderBookEnvelope struct {
	Action string `json:"action"`
	Arg    struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
	Data []struct {
		Bids [][]string `json:"bids"`
		Asks [][]string `json:"asks"`
		Ts   string     `json:"ts"`
	} `json:"data"`
}

func main() {
	flag.Parse()
	brokers := strings.Split(*kafkaBrokers, ",")
	symbol := *symbolFlag
	if symbol == "" && *instIDFlag == "" {
		log.Fatal("symbol or inst-id is required")
	}
	symbolNorm := symbols.Normalize(symbol)
	instID := *instIDFlag
	if instID == "" {
		instID = symbols.OkxInstID(symbolNorm)
	}
	if instID == "" {
		log.Fatal("unable to derive inst-id from symbol")
	}
	topic := *topicFlag
	if topic == "" {
		if symbolNorm == "BTCUSDT" || symbolNorm == "BTCUSD" {
			topic = "okx_orderbook"
		} else {
			topic = "okx_" + symbols.Lower(symbolNorm) + "_orderbook"
		}
	}
	channel := "books"
	depth := *depthFlag
	switch depth {
	case 0:
		channel = "books"
	case 5, 50:
		channel = "books" + strconv.Itoa(depth)
	default:
		log.Printf("Unsupported depth %d, defaulting to full book", depth)
		channel = "books"
	}

	producer := kafka.NewProducer(brokers, topic)
	defer producer.Close()

	log.Printf("Starting OKX Orderbook Follower. Brokers: %v, Topic: %s, InstID: %s, Channel: %s", brokers, topic, instID, channel)

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
		restDepth = 50
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
		url := "https://www.okx.com/api/v5/market/books?instId=" + instID + "&sz=" + strconv.Itoa(restDepth)
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
			Data []struct {
				Bids [][]string `json:"bids"`
				Asks [][]string `json:"asks"`
				Ts   string     `json:"ts"`
			} `json:"data"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
			return nil, nil, 0, err
		}
		if len(payload.Data) == 0 {
			return nil, nil, 0, nil
		}
		ts := time.Now().UnixMilli()
		if payload.Data[0].Ts != "" {
			if parsed, err := strconv.ParseInt(payload.Data[0].Ts, 10, 64); err == nil {
				ts = parsed
			}
		}
		return payload.Data[0].Bids, payload.Data[0].Asks, ts, nil
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
			"op": "subscribe",
			"args": []map[string]string{
				{"channel": channel, "instId": instID},
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
					if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
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

				if string(message) == "pong" {
					continue
				}

				var env okxOrderBookEnvelope
				if err := json.Unmarshal(message, &env); err != nil {
					continue
				}
				if env.Arg.Channel != channel || len(env.Data) == 0 {
					continue
				}

				d := env.Data[0]
				ts := time.Now().UnixMilli()
				if d.Ts != "" {
					if parsed, err := strconv.ParseInt(d.Ts, 10, 64); err == nil {
						ts = parsed
					}
				}

				snapshot := strings.EqualFold(env.Action, "snapshot")
				if snapshot {
					book.ApplySnapshot(d.Bids, d.Asks)
				} else {
					book.ApplyDelta(d.Bids, d.Asks)
				}
				emitSnapshot(ts, snapshot)
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
