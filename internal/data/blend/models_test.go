// Unit tests for the Blend v2 Models aggregate.
package blend_test

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"

	"github.com/stellar/wallet-backend/internal/data/blend"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func TestNewModels(t *testing.T) {
	m := blend.NewModels(nil, metrics.NewMetrics(prometheus.NewRegistry()).DB)

	assert.NotNil(t, m.Pools)
	assert.NotNil(t, m.Positions)
	assert.NotNil(t, m.Reserves)
	assert.NotNil(t, m.BackstopPositions)
	assert.NotNil(t, m.BackstopPools)
	assert.NotNil(t, m.Emissions)
	assert.NotNil(t, m.ReserveEmissions)
}
