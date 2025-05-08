package wbclient

import (
	"bytes"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stellar/go/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/stellar/wallet-backend/pkg/wbclient/auth"
)

func Test_RequestSigner(t *testing.T) {
	signingKey := keypair.MustParseFull("SBFZ723B5G7VKMV5J5J3DEDGQOLTF7YQ4EXEGZI3MZYTNWYE5FGCNGCK")
	wrongSigningKey := keypair.MustRandom()

	validTimeout := time.Second * 10
	expiredTimeout := -time.Second * 10

	expectedRequestBody := []byte(`{"value": "expected value"}`)
	wrongRequestBody := []byte(`{"value": "wrong value"}`)

	testCases := []struct {
		name            string
		timeout         time.Duration
		hostname        string
		requestBody     []byte
		bodyToBeSigned  []byte
		signer          *keypair.Full
		wantErrContains string
	}{
		{
			name:            "🔴expired",
			timeout:         expiredTimeout,
			requestBody:     expectedRequestBody,
			bodyToBeSigned:  expectedRequestBody,
			signer:          signingKey,
			wantErrContains: "parsing JWT token with claims: token is expired by",
		},
		{
			name:            "🔴wrong_body",
			timeout:         validTimeout,
			requestBody:     expectedRequestBody,
			bodyToBeSigned:  wrongRequestBody,
			signer:          signingKey,
			wantErrContains: "the claims' hashed body does not match the request body's hash",
		},
		{
			name:            "🔴wrong_signer",
			timeout:         validTimeout,
			requestBody:     expectedRequestBody,
			bodyToBeSigned:  expectedRequestBody,
			signer:          wrongSigningKey,
			wantErrContains: "parsing JWT token with claims: ed25519: verification error",
		},
		{
			name:           "🟢empty_body",
			timeout:        validTimeout,
			requestBody:    nil,
			bodyToBeSigned: nil,
			signer:         signingKey,
		},
		{
			name:           "🟢existing_body",
			timeout:        validTimeout,
			requestBody:    expectedRequestBody,
			bodyToBeSigned: expectedRequestBody,
			signer:         signingKey,
		},
	}

	t.Run("sign_http_request", func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				request, err := http.NewRequest(http.MethodPost, "https://test.com", bytes.NewBuffer(tc.bodyToBeSigned))
				require.NoError(t, err)

				jwtTokenGenerator, err := auth.NewJWTTokenGenerator(tc.signer.Seed())
				require.NoError(t, err)
				sigCreator := RequestSigner{JWTTokenGenerator: jwtTokenGenerator}
				err = sigCreator.SignHTTPRequest(request, tc.timeout)
				require.NoError(t, err)

				authHeader := request.Header.Get("Authorization")
				require.NotEmpty(t, authHeader)
				tokenStr := strings.TrimPrefix(authHeader, "Bearer ")

				jwtTokenParser, err := auth.NewJWTTokenParser(auth.DefaultMaxTimeout, signingKey.Address())
				require.NoError(t, err)
				jwtToken, claims, err := jwtTokenParser.ParseJWT(tokenStr, "test.com", tc.requestBody)
				if len(tc.wantErrContains) == 0 {
					assert.NoError(t, err)
					assert.True(t, jwtToken.Valid)
					assert.Equal(t, auth.HashBody(tc.bodyToBeSigned), claims.HashedBody)
				} else {
					assert.ErrorContains(t, err, tc.wantErrContains)
					assert.Nil(t, jwtToken)
					assert.Nil(t, claims)
				}
			})
		}
	})
}
