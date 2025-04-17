package wbclient

import (
	"context"
	"fmt"
	"net/url"
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

	expectedRequestBody := []byte(`{"value": "expected value"}`)
	wrongRequestBody := []byte(`{"value": "wrong value"}`)

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
			requestBody:     expectedRequestBody,
			bodyToBeSigned:  expectedRequestBody,
			signer:          signingKey,
			wantErrContains: []string{"signature timestamp has expired"},
		},
		{
			name:            "🔴wrong_hostname",
			timestampUnix:   nowUnix,
			hostname:        wrongHostname,
			requestBody:     expectedRequestBody,
			bodyToBeSigned:  expectedRequestBody,
			signer:          signingKey,
			wantErrContains: []string{"unable to verify the signature"},
		},
		{
			name:            "🔴wrong_body",
			timestampUnix:   nowUnix,
			hostname:        hostname,
			requestBody:     expectedRequestBody,
			bodyToBeSigned:  wrongRequestBody,
			signer:          signingKey,
			wantErrContains: []string{"unable to verify the signature"},
		},
		{
			name:            "🔴wrong_signer",
			timestampUnix:   nowUnix,
			hostname:        hostname,
			requestBody:     expectedRequestBody,
			bodyToBeSigned:  expectedRequestBody,
			signer:          wrongSigningKey,
			wantErrContains: []string{"unable to verify the signature"},
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
			requestBody:     expectedRequestBody,
			bodyToBeSigned:  expectedRequestBody,
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

	t.Run("🟢manually_compare_format", func(t *testing.T) {
		host, err := url.ParseRequestURI(hostname)
		require.NoError(t, err)

		reqBody := []byte(`{"value": "expected value"}`)
		sig := fmt.Sprintf("%d.%s.%s", nowUnix, host.Hostname(), string(reqBody))
		sig, err = signingKey.SignBase64([]byte(sig))
		require.NoError(t, err)
		wantSignatureHeader := fmt.Sprintf("t=%d, s=%s", nowUnix, sig)

		signatureCreator := SignatureCreator{Signer: signingKey}
		signatureHeader, err := signatureCreator.CreateSignatureHeader(hostname, nowUnix, reqBody)
		require.NoError(t, err)

		assert.Equal(t, wantSignatureHeader, signatureHeader)
	})
}
