package main

import (
	"testing"
	"time"
)

func TestBuildPriceOutputMetadataGuards(t *testing.T) {
	eventTsMs := int64(2_000)
	recvTsMs := int64(1_500)
	out := buildPriceOutput("BTCUSDT", "100.0", eventTsMs, recvTsMs, 1, "evt-1", time.UnixMilli(1_000))

	if out.IngestLagMs < 0 {
		t.Fatalf("ingest_lag_ms must be >= 0, got %d", out.IngestLagMs)
	}
	if out.PublishTsMs < out.RecvTsMs {
		t.Fatalf("publish_ts_ms must be >= recv_ts_ms, got publish=%d recv=%d", out.PublishTsMs, out.RecvTsMs)
	}
}
