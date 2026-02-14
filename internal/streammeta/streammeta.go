package streammeta

import (
	"sync/atomic"
	"time"

	"market_follower/internal/models"
)

type Sequencer struct {
	value atomic.Uint64
}

func NewSequencer() *Sequencer {
	return &Sequencer{}
}

func (s *Sequencer) Next() uint64 {
	if s == nil {
		return 0
	}
	return s.value.Add(1)
}

func CaptureRecvTsMs() int64 {
	return time.Now().UnixMilli()
}

func IngestLagMs(eventTsMs int64, recvTsMs int64) int64 {
	if eventTsMs <= 0 || recvTsMs <= 0 {
		return 0
	}
	lag := recvTsMs - eventTsMs
	if lag < 0 {
		return 0
	}
	return lag
}

func ClampPublishTsMs(recvTsMs int64, publishTsMs int64) int64 {
	if publishTsMs < recvTsMs {
		return recvTsMs
	}
	return publishTsMs
}

func Build(eventTsMs int64, recvTsMs int64, sourceSeq uint64, sourceEventID string, publishAt time.Time) models.StreamMeta {
	publishTsMs := publishAt.UnixMilli()
	if recvTsMs <= 0 {
		recvTsMs = publishTsMs
	}
	if eventTsMs <= 0 {
		eventTsMs = recvTsMs
	}
	publishTsMs = ClampPublishTsMs(recvTsMs, publishTsMs)
	return models.StreamMeta{
		EventTsMs:     eventTsMs,
		RecvTsMs:      recvTsMs,
		PublishTsMs:   publishTsMs,
		IngestLagMs:   IngestLagMs(eventTsMs, recvTsMs),
		SourceSeq:     sourceSeq,
		SourceEventID: sourceEventID,
	}
}

func BuildNow(eventTsMs int64, recvTsMs int64, sourceSeq uint64, sourceEventID string) models.StreamMeta {
	return Build(eventTsMs, recvTsMs, sourceSeq, sourceEventID, time.Now())
}
