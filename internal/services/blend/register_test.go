package blend

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/services"
)

// TestRegister verifies that importing this package (which runs init())
// registers both a validator and a processor factory for ProtocolID under
// the framework's global registries.
func TestRegister(t *testing.T) {
	validatorFactory, ok := services.GetValidator(ProtocolID)
	require.True(t, ok)
	require.NotNil(t, validatorFactory)

	processorFactory, ok := services.GetProcessor(ProtocolID)
	require.True(t, ok)
	require.NotNil(t, processorFactory)

	v := validatorFactory(services.ProtocolDeps{})
	require.NotNil(t, v)
	assert.Equal(t, ProtocolID, v.ProtocolID())

	p := processorFactory(services.ProtocolDeps{})
	require.NotNil(t, p)
	assert.Equal(t, ProtocolID, p.ProtocolID())
}
