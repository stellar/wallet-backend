package wbclient

import (
	"context"
	"testing"
	"time"

	"github.com/stellar/go/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/serve/auth"
)

func Test_SignatureCreator_CreateSignature(t *testing.T) {
	hostname := "https://example.com"
	wrongHostname := "https://wrong.example.com"
	signingKey := keypair.MustParseFull("SBFZ723B5G7VKMV5J5J3DEDGQOLTF7YQ4EXEGZI3MZYTNWYE5FGCNGCK")
	wrongSigningKey := keypair.MustRandom()

	now := time.Now()
	nowUnix := now.Unix()
	expiredUnix := (now.Add(-time.Second * 10)).Unix()

	ctx := context.Background()
	signatureVerifier, err := auth.NewStellarSignatureVerifier(hostname, signingKey.Address())
	require.NoError(t, err)

	testCases := []struct {
		name            string
		timestampUnix   int64
		hostname        string
		requestBody     []byte
		bodyToBeSigned  []byte
		signer          *keypair.Full
		wantErrContains []string
	}{
		{
			name:            "🔴expired",
			timestampUnix:   expiredUnix,
			hostname:        hostname,
			requestBody:     []byte(`{"value": "expected value"}`),
			bodyToBeSigned:  []byte(`{"value": "expected value"}`),
			signer:          signingKey,
			wantErrContains: []string{"signature timestamp has expired", auth.ErrStellarSignatureNotVerified.Error()},
		},
		{
			name:            "🔴wrong_hostname",
			timestampUnix:   nowUnix,
			hostname:        wrongHostname,
			requestBody:     []byte(`{"value": "expected value"}`),
			bodyToBeSigned:  []byte(`{"value": "expected value"}`),
			signer:          signingKey,
			wantErrContains: []string{"unable to verify the signature", auth.ErrStellarSignatureNotVerified.Error()},
		},
		{
			name:            "🔴wrong_body",
			timestampUnix:   nowUnix,
			hostname:        hostname,
			requestBody:     []byte(`{"value": "expected value"}`),
			bodyToBeSigned:  []byte(`{"value": "wrong value"}`),
			signer:          signingKey,
			wantErrContains: []string{"unable to verify the signature", auth.ErrStellarSignatureNotVerified.Error()},
		},
		{
			name:            "🔴wrong_signer",
			timestampUnix:   nowUnix,
			hostname:        hostname,
			requestBody:     []byte(`{"value": "expected value"}`),
			bodyToBeSigned:  []byte(`{"value": "expected value"}`),
			signer:          wrongSigningKey,
			wantErrContains: []string{"unable to verify the signature", auth.ErrStellarSignatureNotVerified.Error()},
		},
		{
			name:            "🟢empty_body",
			timestampUnix:   nowUnix,
			hostname:        hostname,
			requestBody:     nil,
			bodyToBeSigned:  nil,
			signer:          signingKey,
			wantErrContains: nil,
		},
		{
			name:            "🟢existing_body",
			timestampUnix:   nowUnix,
			hostname:        hostname,
			requestBody:     []byte(`{"value": "expected value"}`),
			bodyToBeSigned:  []byte(`{"value": "expected value"}`),
			signer:          signingKey,
			wantErrContains: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			signatureCreator := SignatureCreator{Signer: tc.signer}
			signatureHeader, err := signatureCreator.CreateSignatureHeader(tc.hostname, tc.timestampUnix, tc.bodyToBeSigned)
			require.NoError(t, err)

			err = signatureVerifier.VerifySignature(ctx, signatureHeader, tc.requestBody)
			if len(tc.wantErrContains) == 0 {
				assert.NoError(t, err)
			} else {
				for _, wantErrContains := range tc.wantErrContains {
					assert.ErrorContains(t, err, wantErrContains)
				}
			}
		})
	}
}
