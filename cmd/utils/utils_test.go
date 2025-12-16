package utils

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/keypair"
	"github.com/stellar/go-stellar-sdk/network"
	"github.com/stretchr/testify/assert"

	"github.com/stellar/wallet-backend/internal/signing"
)

func TestSignatureClientResolver(t *testing.T) {
	t.Run("invalid_signature_client_type", func(t *testing.T) {
		sc, err := SignatureClientResolver(&SignatureClientOptions{})
		assert.ErrorIs(t, err, signing.ErrInvalidSignatureClientType)
		assert.Nil(t, sc)

		sc, err = SignatureClientResolver(&SignatureClientOptions{Type: "invalid"})
		assert.ErrorIs(t, err, signing.ErrInvalidSignatureClientType)
		assert.Nil(t, sc)
	})

	t.Run("env_signature_client_opts", func(t *testing.T) {
		sc, err := SignatureClientResolver(&SignatureClientOptions{
			Type:                         signing.EnvSignatureClientType,
			DistributionAccountSecretKey: "invalid",
		})
		assert.EqualError(t, err, "resolving signature client: parsing distribution account private key: base32 decode failed: illegal base32 data at input byte 7")
		assert.Nil(t, sc)

		sc, err = SignatureClientResolver(&SignatureClientOptions{
			Type:                         signing.EnvSignatureClientType,
			DistributionAccountSecretKey: keypair.MustRandom().Seed(),
			NetworkPassphrase:            network.PublicNetworkPassphrase,
		})
		assert.NoError(t, err)
		assert.NotNil(t, sc)
	})

	t.Run("kms_signature_client_opts", func(t *testing.T) {
		sc, err := SignatureClientResolver(&SignatureClientOptions{
			Type:                         signing.KMSSignatureClientType,
			DistributionAccountPublicKey: keypair.MustRandom().Address(),
		})
		assert.EqualError(t, err, "resolving signature client: instantiating kms client: aws region cannot be empty")
		assert.Nil(t, sc)

		sc, err = SignatureClientResolver(&SignatureClientOptions{
			Type:                         signing.KMSSignatureClientType,
			DistributionAccountPublicKey: "invalid",
			AWSRegion:                    "us-east-2",
		})
		assert.ErrorIs(t, err, signing.ErrInvalidPublicKeyProvided)
		assert.Nil(t, sc)

		sc, err = SignatureClientResolver(&SignatureClientOptions{
			Type:                         signing.KMSSignatureClientType,
			DistributionAccountPublicKey: keypair.MustRandom().Address(),
			NetworkPassphrase:            network.PublicNetworkPassphrase,
			AWSRegion:                    "us-east-2",
		})
		assert.EqualError(t, err, "resolving signature client: aws key arn cannot be empty")
		assert.Nil(t, sc)

		sc, err = SignatureClientResolver(&SignatureClientOptions{
			Type:                         signing.KMSSignatureClientType,
			DistributionAccountPublicKey: keypair.MustRandom().Address(),
			NetworkPassphrase:            network.PublicNetworkPassphrase,
			AWSRegion:                    "us-east-2",
			KMSKeyARN:                    "kms-key-arn",
		})
		assert.NoError(t, err)
		assert.NotNil(t, sc)
	})
}
