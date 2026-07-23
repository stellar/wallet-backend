package serve

import (
	"context"
	"testing"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildRequestAuthVerifier(t *testing.T) {
	baseCfg := Configs{
		ClientAuthMaxTimeoutSeconds: 15,
		ClientAuthMaxBodySizeBytes:  102_400,
	}

	t.Run("auth disabled when no public keys are provided", func(t *testing.T) {
		cfg := baseCfg
		cfg.ClientAuthPublicKeys = nil

		verifier, err := buildRequestAuthVerifier(context.Background(), cfg)
		require.NoError(t, err)
		assert.Nil(t, verifier)
	})

	t.Run("auth enabled when public keys are provided", func(t *testing.T) {
		cfg := baseCfg
		cfg.ClientAuthPublicKeys = []string{keypair.MustRandom().Address()}

		verifier, err := buildRequestAuthVerifier(context.Background(), cfg)
		require.NoError(t, err)
		assert.NotNil(t, verifier)
	})

	t.Run("invalid public key returns error", func(t *testing.T) {
		cfg := baseCfg
		cfg.ClientAuthPublicKeys = []string{"not-a-stellar-public-key"}

		verifier, err := buildRequestAuthVerifier(context.Background(), cfg)
		require.ErrorContains(t, err, "instantiating multi JWT token parser")
		assert.Nil(t, verifier)
	})
}
