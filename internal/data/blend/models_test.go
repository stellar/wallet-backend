// Unit tests for the Blend v2 Models aggregate.
package blend_test

import (
	"reflect"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"

	"github.com/stellar/wallet-backend/internal/data/blend"
	"github.com/stellar/wallet-backend/internal/metrics"
)

func TestNewModels(t *testing.T) {
	m := blend.NewModels(nil, metrics.NewMetrics(prometheus.NewRegistry()).DB)

	// Walk every field reflectively so a model added to the struct but not to
	// NewModels fails here instead of nil-panicking at runtime.
	v := reflect.ValueOf(m)
	for i := range v.NumField() {
		assert.False(t, v.Field(i).IsNil(), "Models.%s is not constructed by NewModels", v.Type().Field(i).Name)
	}
}
