package entities

import (
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateSignersWeights(t *testing.T) {
	t.Run("no_full_signers", func(t *testing.T) {
		signers := []Signer{}
		fw, err := ValidateSignersWeights(signers)
		assert.Empty(t, fw)
		assert.EqualError(t, err, "no full signers provided")
	})

	t.Run("full_signers_with_not_same_weight", func(t *testing.T) {
		signers := []Signer{
			{
				Address: keypair.MustRandom().Address(),
				Weight:  20,
				Type:    FullSignerType,
			},
			{
				Address: keypair.MustRandom().Address(),
				Weight:  19,
				Type:    FullSignerType,
			},
		}
		fw, err := ValidateSignersWeights(signers)
		assert.Empty(t, fw)
		assert.EqualError(t, err, "all full signers must have the same weight")
	})

	t.Run("partial_signer_should_be_less_than_full_signers", func(t *testing.T) {
		signers := []Signer{
			{
				Address: keypair.MustRandom().Address(),
				Weight:  20,
				Type:    FullSignerType,
			},
			{
				Address: keypair.MustRandom().Address(),
				Weight:  20,
				Type:    PartialSignerType,
			},
		}
		fw, err := ValidateSignersWeights(signers)
		assert.EqualError(t, err, "all partial signers' weights must be less than the weight of full signers")
		assert.Empty(t, fw)
	})

	t.Run("full_signers_same_weight", func(t *testing.T) {
		signers := []Signer{
			{
				Address: keypair.MustRandom().Address(),
				Weight:  20,
				Type:    FullSignerType,
			},
			{
				Address: keypair.MustRandom().Address(),
				Weight:  20,
				Type:    FullSignerType,
			},
		}
		fw, err := ValidateSignersWeights(signers)
		require.NoError(t, err)
		assert.Equal(t, 20, fw)
	})

	t.Run("full_signers_and_partial_signers", func(t *testing.T) {
		signers := []Signer{
			{
				Address: keypair.MustRandom().Address(),
				Weight:  20,
				Type:    FullSignerType,
			},
			{
				Address: keypair.MustRandom().Address(),
				Weight:  10,
				Type:    PartialSignerType,
			},
		}
		fw, err := ValidateSignersWeights(signers)
		require.NoError(t, err)
		assert.Equal(t, 20, fw)
	})
}
