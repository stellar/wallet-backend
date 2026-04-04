package services

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBackfillWorkerStats_Add(t *testing.T) {
	s := &backfillWorkerStats{}

	s.addFetch(100 * time.Millisecond)
	s.addFetch(200 * time.Millisecond)
	s.addProcess(50 * time.Millisecond)
	s.addChannelWait("ledger", "send", 10*time.Millisecond)

	assert.Equal(t, 2, s.fetchCount)
	assert.Equal(t, 300*time.Millisecond, s.fetchTotal)
	assert.Equal(t, 1, s.processCount)
	assert.Equal(t, 50*time.Millisecond, s.processTotal)
	assert.Equal(t, 10*time.Millisecond, s.channelWait["ledger:send"])
}

func TestBackfillGapStats_Merge(t *testing.T) {
	gs := newBackfillGapStats()

	w1 := &backfillWorkerStats{}
	w1.addFetch(100 * time.Millisecond)
	w1.addFetch(200 * time.Millisecond)
	w1.addProcess(50 * time.Millisecond)
	w1.addChannelWait("ledger", "send", 10*time.Millisecond)

	w2 := &backfillWorkerStats{}
	w2.addFetch(200 * time.Millisecond)
	w2.addProcess(75 * time.Millisecond)
	w2.addChannelWait("ledger", "send", 20*time.Millisecond)

	gs.mergeWorker(w1)
	gs.mergeWorker(w2)

	assert.Equal(t, 3, gs.fetchCount)
	assert.Equal(t, 500*time.Millisecond, gs.fetchTotal)
	assert.Equal(t, 2, gs.processCount)
	assert.Equal(t, 125*time.Millisecond, gs.processTotal)
	assert.Equal(t, 30*time.Millisecond, gs.channelWait["ledger:send"])
}

func TestBackfillGapStats_MergeFlush(t *testing.T) {
	gs := newBackfillGapStats()

	w := &backfillFlushWorkerStats{}
	w.addFlush(500 * time.Millisecond)
	w.addFlush(300 * time.Millisecond)
	w.addChannelWait("flush", "receive", 100*time.Millisecond)

	gs.mergeFlushWorker(w)

	assert.Equal(t, 2, gs.flushCount)
	assert.Equal(t, 800*time.Millisecond, gs.flushTotal)
	assert.Equal(t, 100*time.Millisecond, gs.channelWait["flush:receive"])
}

func TestAvgOrZero(t *testing.T) {
	assert.Equal(t, time.Duration(0), avgOrZero(100*time.Millisecond, 0))
	assert.Equal(t, 50*time.Millisecond, avgOrZero(100*time.Millisecond, 2))
}
