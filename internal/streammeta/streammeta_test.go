package streammeta

import (
	"sync"
	"testing"
	"time"
)

func TestIngestLagMsClampsNegative(t *testing.T) {
	if got := IngestLagMs(2000, 1500); got != 0 {
		t.Fatalf("expected negative lag to clamp to 0, got %d", got)
	}
	if got := IngestLagMs(1000, 1250); got != 250 {
		t.Fatalf("unexpected lag value: got %d", got)
	}
}

func TestBuildClampsPublishTimestamp(t *testing.T) {
	recv := int64(2000)
	stamp := Build(1500, recv, 7, "evt-1", time.UnixMilli(1500))

	if stamp.PublishTsMs != recv {
		t.Fatalf("publish_ts_ms must be >= recv_ts_ms: got publish=%d recv=%d", stamp.PublishTsMs, recv)
	}
	if stamp.IngestLagMs < 0 {
		t.Fatalf("ingest_lag_ms must be >= 0, got %d", stamp.IngestLagMs)
	}
}

func TestBuildBackfillsMissingTimes(t *testing.T) {
	stamp := Build(0, 0, 1, "", time.UnixMilli(5555))
	if stamp.EventTsMs != 5555 {
		t.Fatalf("expected event_ts_ms fallback to publish time, got %d", stamp.EventTsMs)
	}
	if stamp.RecvTsMs != 5555 {
		t.Fatalf("expected recv_ts_ms fallback to publish time, got %d", stamp.RecvTsMs)
	}
	if stamp.PublishTsMs != 5555 {
		t.Fatalf("expected publish_ts_ms to match publish time, got %d", stamp.PublishTsMs)
	}
}

func TestSequencerMonotonic(t *testing.T) {
	seq := NewSequencer()
	const n = 1000
	values := make([]uint64, 0, n)
	valuesMu := sync.Mutex{}
	wg := sync.WaitGroup{}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < n/10; j++ {
				v := seq.Next()
				valuesMu.Lock()
				values = append(values, v)
				valuesMu.Unlock()
			}
		}()
	}
	wg.Wait()

	seen := make(map[uint64]struct{}, n)
	for _, v := range values {
		if v == 0 {
			t.Fatal("source_seq should start at 1")
		}
		if _, ok := seen[v]; ok {
			t.Fatalf("duplicate sequence value: %d", v)
		}
		seen[v] = struct{}{}
	}
	if len(seen) != n {
		t.Fatalf("expected %d unique sequence values, got %d", n, len(seen))
	}
}
