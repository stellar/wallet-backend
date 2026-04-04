package services

import (
	"fmt"
	"time"
)

// backfillWorkerStats accumulates timing for a single process worker.
// Each worker owns its instance — no mutex needed.
type backfillWorkerStats struct {
	fetchCount   int
	fetchTotal   time.Duration
	processCount int
	processTotal time.Duration
	channelWait  map[string]time.Duration // key: "channel:direction"
}

func (s *backfillWorkerStats) addFetch(d time.Duration) {
	s.fetchCount++
	s.fetchTotal += d
}

func (s *backfillWorkerStats) addProcess(d time.Duration) {
	s.processCount++
	s.processTotal += d
}

func (s *backfillWorkerStats) addChannelWait(channel, direction string, d time.Duration) {
	if s.channelWait == nil {
		s.channelWait = make(map[string]time.Duration)
	}
	s.channelWait[fmt.Sprintf("%s:%s", channel, direction)] += d
}

// backfillFlushWorkerStats accumulates timing for a single flush worker.
type backfillFlushWorkerStats struct {
	flushCount  int
	flushTotal  time.Duration
	channelWait map[string]time.Duration
}

func (s *backfillFlushWorkerStats) addFlush(d time.Duration) {
	s.flushCount++
	s.flushTotal += d
}

func (s *backfillFlushWorkerStats) addChannelWait(channel, direction string, d time.Duration) {
	if s.channelWait == nil {
		s.channelWait = make(map[string]time.Duration)
	}
	s.channelWait[fmt.Sprintf("%s:%s", channel, direction)] += d
}

// backfillGapStats aggregates stats from all workers for a single gap.
// Only accessed from processGap after workers complete — no mutex needed.
type backfillGapStats struct {
	fetchCount   int
	fetchTotal   time.Duration
	processCount int
	processTotal time.Duration
	flushCount   int
	flushTotal   time.Duration
	channelWait  map[string]time.Duration
}

func newBackfillGapStats() *backfillGapStats {
	return &backfillGapStats{
		channelWait: make(map[string]time.Duration),
	}
}

func (gs *backfillGapStats) mergeWorker(w *backfillWorkerStats) {
	gs.fetchCount += w.fetchCount
	gs.fetchTotal += w.fetchTotal
	gs.processCount += w.processCount
	gs.processTotal += w.processTotal
	for k, v := range w.channelWait {
		gs.channelWait[k] += v
	}
}

func (gs *backfillGapStats) mergeFlushWorker(w *backfillFlushWorkerStats) {
	gs.flushCount += w.flushCount
	gs.flushTotal += w.flushTotal
	for k, v := range w.channelWait {
		gs.channelWait[k] += v
	}
}

// avgOrZero returns average duration, or zero if count is 0.
func avgOrZero(total time.Duration, count int) time.Duration {
	if count == 0 {
		return 0
	}
	return total / time.Duration(count)
}
