FROM golang:1.24-alpine AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ cmd/
COPY internal/ internal/

RUN go build -o /bin/binance-price ./cmd/binance-price
RUN go build -o /bin/binance-volume ./cmd/binance-volume
RUN go build -o /bin/coinbase-price ./cmd/coinbase-price
RUN go build -o /bin/coinbase-volume ./cmd/coinbase-volume
RUN go build -o /bin/kraken-price ./cmd/kraken-price
RUN go build -o /bin/kraken-volume ./cmd/kraken-volume
RUN go build -o /bin/okx-price ./cmd/okx-price
RUN go build -o /bin/okx-volume ./cmd/okx-volume
RUN go build -o /bin/bybit-price ./cmd/bybit-price
RUN go build -o /bin/bybit-volume ./cmd/bybit-volume
RUN go build -o /bin/bitget-price ./cmd/bitget-price
RUN go build -o /bin/bitget-volume ./cmd/bitget-volume
RUN go build -o /bin/binance-orderbook ./cmd/binance-orderbook
RUN go build -o /bin/bybit-orderbook ./cmd/bybit-orderbook
RUN go build -o /bin/bitget-orderbook ./cmd/bitget-orderbook
RUN go build -o /bin/okx-orderbook ./cmd/okx-orderbook
RUN go build -o /bin/coinbase-orderbook ./cmd/coinbase-orderbook
RUN go build -o /bin/kraken-orderbook ./cmd/kraken-orderbook
RUN go build -o /bin/chainlink-price ./cmd/chainlink-price

FROM alpine:latest
WORKDIR /app
RUN apk --no-cache add ca-certificates

COPY --from=builder /bin/binance-price .
COPY --from=builder /bin/binance-volume .
COPY --from=builder /bin/coinbase-price .
COPY --from=builder /bin/coinbase-volume .
COPY --from=builder /bin/kraken-price .
COPY --from=builder /bin/kraken-volume .
COPY --from=builder /bin/okx-price .
COPY --from=builder /bin/okx-volume .
COPY --from=builder /bin/bybit-price .
COPY --from=builder /bin/bybit-volume .
COPY --from=builder /bin/bitget-price .
COPY --from=builder /bin/bitget-volume .
COPY --from=builder /bin/binance-orderbook .
COPY --from=builder /bin/bybit-orderbook .
COPY --from=builder /bin/bitget-orderbook .
COPY --from=builder /bin/okx-orderbook .
COPY --from=builder /bin/coinbase-orderbook .
COPY --from=builder /bin/kraken-orderbook .
COPY --from=builder /bin/chainlink-price .

COPY entrypoint.sh .
ENTRYPOINT ["./entrypoint.sh"]
