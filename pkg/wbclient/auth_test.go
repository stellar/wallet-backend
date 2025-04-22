package wbclient

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stellar/go/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/internal/serve/auth"
)

func Test_RequestSigner(t *testing.T) {
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

	t.Run("build_signature_header", func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				sigCreator := RequestSigner{Signer: tc.signer}
				signatureHeader, err := sigCreator.BuildSignatureHeader(tc.hostname, tc.timestampUnix, tc.bodyToBeSigned)
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
	})

	t.Run("sign_http_request", func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				request, err := http.NewRequest(http.MethodPost, tc.hostname, bytes.NewBuffer(tc.bodyToBeSigned))
				require.NoError(t, err)

				sigCreator := RequestSigner{Signer: tc.signer}
				err = sigCreator.SignHTTPRequest(request, tc.timestampUnix)
				require.NoError(t, err)

				signatureHeader := request.Header.Get("Signature")
				require.NotEmpty(t, signatureHeader)

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
	})

	t.Run("🟢manually_compare_signature_format", func(t *testing.T) {
		host, err := url.ParseRequestURI(hostname)
		require.NoError(t, err)

		reqBody := []byte(`{"value": "expected value"}`)
		sig := fmt.Sprintf("%d.%s.%s", nowUnix, host.Hostname(), string(reqBody))
		sig, err = signingKey.SignBase64([]byte(sig))
		require.NoError(t, err)
		wantSignatureHeader := fmt.Sprintf("t=%d, s=%s", nowUnix, sig)

		signatureCreator := RequestSigner{Signer: signingKey}
		signatureHeader, err := signatureCreator.BuildSignatureHeader(hostname, nowUnix, reqBody)
		require.NoError(t, err)

		assert.Equal(t, wantSignatureHeader, signatureHeader)
	})
}
