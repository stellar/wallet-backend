package signing

import (
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stretchr/testify/assert"
)

func TestNewSignatureClient(t *testing.T) {
	t.Run("invalid_signature_client_type", func(t *testing.T) {
		sc, err := NewSignatureClient(&SignatureClientOptions{})
		assert.ErrorIs(t, err, ErrInvalidSignatureClientType)
		assert.Nil(t, sc)

		sc, err = NewSignatureClient(&SignatureClientOptions{Type: "invalid"})
		assert.ErrorIs(t, err, ErrInvalidSignatureClientType)
		assert.Nil(t, sc)
	})

	t.Run("env_signature_client_opts", func(t *testing.T) {
		sc, err := NewSignatureClient(&SignatureClientOptions{
			Type:                         EnvSignatureClientType,
			DistributionAccountSecretKey: "invalid",
		})
		assert.EqualError(t, err, "parsing distribution account private key: base32 decode failed: illegal base32 data at input byte 7")
		assert.Nil(t, sc)

		sc, err = NewSignatureClient(&SignatureClientOptions{
			Type:                         EnvSignatureClientType,
			DistributionAccountSecretKey: keypair.MustRandom().Seed(),
		})
		assert.EqualError(t, err, "invalid network passphrase provided: ")
		assert.Nil(t, sc)

		sc, err = NewSignatureClient(&SignatureClientOptions{
			Type:                         EnvSignatureClientType,
			DistributionAccountSecretKey: keypair.MustRandom().Seed(),
			NetworkPassphrase:            network.PublicNetworkPassphrase,
		})
		assert.NoError(t, err)
		assert.NotNil(t, sc)
	})

	t.Run("kms_signature_client_opts", func(t *testing.T) {
		sc, err := NewSignatureClient(&SignatureClientOptions{
			Type:                         KMSSignatureClientType,
			DistributionAccountPublicKey: keypair.MustRandom().Address(),
		})
		assert.EqualError(t, err, "instantiating kms client: aws region cannot be empty")
		assert.Nil(t, sc)

		sc, err = NewSignatureClient(&SignatureClientOptions{
			Type:                         KMSSignatureClientType,
			DistributionAccountPublicKey: "invalid",
			AWSRegion:                    "us-east-2",
		})
		assert.ErrorIs(t, err, ErrInvalidPublicKeyProvided)
		assert.Nil(t, sc)

		sc, err = NewSignatureClient(&SignatureClientOptions{
			Type:                         KMSSignatureClientType,
			DistributionAccountPublicKey: keypair.MustRandom().Address(),
			AWSRegion:                    "us-east-2",
		})
		assert.EqualError(t, err, "invalid network passphrase provided: ")
		assert.Nil(t, sc)

		sc, err = NewSignatureClient(&SignatureClientOptions{
			Type:                         KMSSignatureClientType,
			DistributionAccountPublicKey: keypair.MustRandom().Address(),
			NetworkPassphrase:            network.PublicNetworkPassphrase,
			AWSRegion:                    "us-east-2",
		})
		assert.EqualError(t, err, "aws key arn cannot be empty")
		assert.Nil(t, sc)
	})
}
