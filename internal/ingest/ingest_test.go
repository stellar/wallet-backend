package ingest

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConfigsStreamingLoadtestFields(t *testing.T) {
	cfg := Configs{
		MetaPipePath:        "/tmp/fake.pipe",
		LedgerCloseDuration: 2 * time.Second,
	}
	assert.Equal(t, "/tmp/fake.pipe", cfg.MetaPipePath)
	assert.Equal(t, 2*time.Second, cfg.LedgerCloseDuration)
}
