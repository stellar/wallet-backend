package services

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_drainLatest_coalescesToMax(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	ch := make(chan time.Time, 8)
	ch <- base.Add(2 * time.Hour)
	ch <- base.Add(1 * time.Hour)
	ch <- base.Add(3 * time.Hour)

	got := drainLatest(ch, base)

	assert.Equal(t, base.Add(3*time.Hour), got) // max of seed + queued
	assert.Empty(t, ch)                         // channel fully drained
}

func Test_drainLatest_returnsSeedWhenEmpty(t *testing.T) {
	base := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	ch := make(chan time.Time, 1)
	assert.Equal(t, base, drainLatest(ch, base))
}
