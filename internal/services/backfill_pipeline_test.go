package services

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func seqs(run []processedLedger) []uint32 {
	out := make([]uint32, len(run))
	for i, p := range run {
		out[i] = p.seq
	}
	return out
}

func Test_orderedBuffer_yieldsContiguousAscendingRuns(t *testing.T) {
	ct := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	ob := newOrderedBuffer(100)

	assert.Empty(t, seqs(ob.add(processedLedger{seq: 102, closeTime: ct})))
	assert.Empty(t, seqs(ob.add(processedLedger{seq: 101, closeTime: ct})))

	run := ob.add(processedLedger{seq: 100, closeTime: ct})
	require.Equal(t, []uint32{100, 101, 102}, seqs(run))

	assert.Empty(t, seqs(ob.add(processedLedger{seq: 104, closeTime: ct})))

	run = ob.add(processedLedger{seq: 103, closeTime: ct})
	require.Equal(t, []uint32{103, 104}, seqs(run))
}
