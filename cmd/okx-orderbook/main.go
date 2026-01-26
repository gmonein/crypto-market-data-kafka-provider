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
	"market_follower/internal/nats"
	"market_follower/internal/models"
	"market_follower/internal/orderbook"
	"market_follower/internal/symbols"

	"github.com/gorilla/websocket"
)

var (
	kafkaBrokers = flag.String("brokers", "nats://localhost:4222", "NATS URLs")
	topicFlag    = flag.String("topic", "", "NATS subject")
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
		Bids      [][]string    `json:"bids"`
		Asks      [][]string    `json:"asks"`
		Ts        string        `json:"ts"`
		Checksum  json.RawMessage `json:"checksum"`
		SeqID     json.RawMessage `json:"seqId"`
		PrevSeqID json.RawMessage `json:"prevSeqId"`
	} `json:"data"`
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
		topic = "okx_" + symbols.Lower(symbolNorm) + "_orderbook"
	}
	channel := "books"
	depth := *depthFlag
	snapshotOnlyChannel := false
	switch depth {
	case 0:
		channel = "books"
	case 5, 50:
		channel = "books" + strconv.Itoa(depth)
		snapshotOnlyChannel = true
	default:
		log.Printf("Unsupported depth %d, defaulting to full book", depth)
		channel = "books"
	}
	outputMode := orderbook.OutputMode()

	producer := nats.NewProducer(brokers, topic)
	defer producer.Close()

	log.Printf("Starting OKX Orderbook Follower. Brokers: %v, Topic: %s, InstID: %s, Channel: %s, Output: %s", brokers, topic, instID, channel, outputMode)

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	stop := make(chan struct{})
	var stopOnce sync.Once
	closeStop := func() {
		stopOnce.Do(func() { close(stop) })
	}

	book := orderbook.New()
	var lastWSAt atomic.Int64
	var lastSeq int64
	var resyncMu sync.Mutex
	lastResyncAt := time.Time{}
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
			log.Printf("OKX resync error (%s): %v", reason, err)
			return
		}
		if len(bids) == 0 && len(asks) == 0 {
			return
		}
		book.ApplySnapshot(bids, asks)
		lastSeq = 0
		emitSnapshot(ts, true)
		log.Printf("OKX resync applied (%s)", reason)
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
		swapSides := false

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

				snapshot := snapshotOnlyChannel || strings.EqualFold(env.Action, "snapshot") || env.Action == ""
				seqID, hasSeq := parseInt64Any(d.SeqID)
				prevSeq, hasPrev := parseInt64Any(d.PrevSeqID)
				checksum, hasChecksum := parseInt64Any(d.Checksum)
				bids := d.Bids
				asks := d.Asks
				rawBid, hasRawBid := bestPriceFromLevels(bids, true)
				rawAsk, hasRawAsk := bestPriceFromLevels(asks, false)
				if snapshot && !swapSides && hasRawBid && hasRawAsk && rawBid > rawAsk {
					swapSides = true
					log.Printf("OKX snapshot appears crossed, swapping sides raw_bid=%.8f raw_ask=%.8f", rawBid, rawAsk)
				}
				if swapSides {
					bids, asks = asks, bids
				}
				if snapshot {
					book.ApplySnapshot(bids, asks)
					if hasSeq {
						lastSeq = seqID
					}
				} else {
					if hasPrev && lastSeq != 0 && prevSeq != lastSeq {
						log.Printf("OKX seq mismatch prev=%d last=%d", prevSeq, lastSeq)
						resync("seq_mismatch")
						conn.Close()
						return
					}
					book.ApplyDelta(bids, asks)
					if hasSeq {
						lastSeq = seqID
					}
				}
				if hasChecksum {
					snap := book.Snapshot(depth)
					local := orderbook.ChecksumOKX(snap.Bids, snap.Asks, depth)
					if !checksumMatches(checksum, local) {
						log.Printf("OKX checksum mismatch remote=%d local=%d", checksum, int64(int32(local)))
						resync("checksum_mismatch")
						conn.Close()
						return
					}
				}
				if book.IsCrossed() {
					bestBid, bestAsk, _ := book.BestBidAsk()
					log.Printf("OKX book crossed (bb=%.8f ba=%.8f raw_bb=%.8f raw_ba=%.8f), forcing resync", bestBid, bestAsk, rawBid, rawAsk)
					resync("crossed")
					conn.Close()
					return
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
