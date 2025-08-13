package serve

import (
	"testing"
	"time"

	"github.com/stellar/go/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewHTTPClientProvider(t *testing.T) {
	provider := NewHTTPClientProvider()
	require.NotNil(t, provider)

	client := provider.GetClient()
	require.NotNil(t, client)
	assert.Equal(t, 30*time.Second, client.Timeout)
}

func TestNewAuthProvider(t *testing.T) {
	t.Run("with valid public keys", func(t *testing.T) {
		kp := keypair.MustRandom()
		publicKeys := []string{kp.Address()}

		provider, err := NewAuthProvider(publicKeys)
		require.NoError(t, err)
		assert.NotNil(t, provider)

		verifier := provider.GetRequestVerifier()
		assert.NotNil(t, verifier)
	})

	t.Run("with invalid public key", func(t *testing.T) {
		invalidKeys := []string{"invalid_key"}

		provider, err := NewAuthProvider(invalidKeys)
		assert.Error(t, err)
		assert.Nil(t, provider)
		assert.Contains(t, err.Error(), "creating JWT token parser")
	})
}
