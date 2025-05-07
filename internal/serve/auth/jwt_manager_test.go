package auth

import (
	"testing"
	"time"

	jwtgo "github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type ecdsaKeypair struct {
	privateKeyStr string
	publicKeyStr  string
}

var testECDSAKeypair1 = ecdsaKeypair{
	publicKeyStr: `-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAER88h7AiQyVDysRTxKvBB6CaiO/kS
cvGyimApUE/12gFhNTRf37SE19CSCllKxstnVFOpLLWB7Qu5OJ0Wvcz3hg==
-----END PUBLIC KEY-----`,
	privateKeyStr: `-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgIqI1MzMZIw2pQDLx
Jn0+FcNT/hNjwtn2TW43710JKZqhRANCAARHzyHsCJDJUPKxFPEq8EHoJqI7+RJy
8bKKYClQT/XaAWE1NF/ftITX0JIKWUrGy2dUU6kstYHtC7k4nRa9zPeG
-----END PRIVATE KEY-----`,
}

var testECDSAKeypair2 = ecdsaKeypair{
	publicKeyStr: `-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAERJtGEWVxHTOghAFU9XyANbF10aXK
zT3U72jUfBk38fceemINJERxdLbBs2O1foeFd8HyJ6Zn7tLvZWGNvVN+cA==
-----END PUBLIC KEY-----`,
	privateKeyStr: `-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgw8lMqTKWEdxusLOW
J16L7THmguSKZq1PPS1SRravKpOhRANCAAREm0YRZXEdM6CEAVT1fIA1sXXRpcrN
PdTvaNR8GTfx9x56Yg0kRHF0tsGzY7V+h4V3wfInpmfu0u9lYY29U35w
-----END PRIVATE KEY-----`,
}

func Test_JWTManager_GenerateAndParseToken(t *testing.T) {
	expiredTimestamp := time.Now().Add(-time.Nanosecond)
	validTimestamp := time.Now().Add(time.Second * 2)
	tooLongTimestamp := time.Now().Add(DefaultMaxTimeout * 2)

	testCases := []struct {
		name            string
		JWTManager      JWTManager
		jwtBody         []byte
		requestBody     []byte
		expiresAt       time.Time
		wantErrContains string
	}{
		{
			name: "🟢valid_KP_1_no_body",
			JWTManager: JWTManager{
				PrivateKey: testECDSAKeypair1.privateKeyStr,
				PublicKey:  testECDSAKeypair1.publicKeyStr,
			},
			jwtBody:         nil,
			requestBody:     nil,
			expiresAt:       validTimestamp,
			wantErrContains: "",
		},
		{
			name: "🟢valid_KP_1_with_body",
			JWTManager: JWTManager{
				PrivateKey: testECDSAKeypair1.privateKeyStr,
				PublicKey:  testECDSAKeypair1.publicKeyStr,
			},
			jwtBody:         []byte(`{"foo": "bar"}`),
			requestBody:     []byte(`{"foo": "bar"}`),
			expiresAt:       validTimestamp,
			wantErrContains: "",
		},
		{
			name: "🟢valid_KP_2_no_body",
			JWTManager: JWTManager{
				PrivateKey: testECDSAKeypair2.privateKeyStr,
				PublicKey:  testECDSAKeypair2.publicKeyStr,
			},
			jwtBody:         nil,
			requestBody:     nil,
			expiresAt:       validTimestamp,
			wantErrContains: "",
		},
		{
			name: "🟢valid_KP_2_with_body",
			JWTManager: JWTManager{
				PrivateKey: testECDSAKeypair2.privateKeyStr,
				PublicKey:  testECDSAKeypair2.publicKeyStr,
			},
			jwtBody:         []byte(`{"foo": "bar"}`),
			requestBody:     []byte(`{"foo": "bar"}`),
			expiresAt:       validTimestamp,
			wantErrContains: "",
		},
		{
			name: "🔴expired",
			JWTManager: JWTManager{
				PrivateKey: testECDSAKeypair1.privateKeyStr,
				PublicKey:  testECDSAKeypair1.publicKeyStr,
			},
			expiresAt:       expiredTimestamp,
			wantErrContains: "parsing JWT token with claims: token is expired by",
		},
		{
			name: "🔴expiration_is_too_long",
			JWTManager: JWTManager{
				PrivateKey: testECDSAKeypair1.privateKeyStr,
				PublicKey:  testECDSAKeypair1.publicKeyStr,
				MaxTimeout: DefaultMaxTimeout,
			},
			expiresAt:       tooLongTimestamp,
			wantErrContains: "the token expiration is too long, max timeout is 15s",
		},
		{
			name: "🔴mismatch_KP",
			JWTManager: JWTManager{
				PrivateKey: testECDSAKeypair1.privateKeyStr,
				PublicKey:  testECDSAKeypair2.publicKeyStr,
			},
			expiresAt:       validTimestamp,
			wantErrContains: "parsing JWT token with claims: crypto/ecdsa: verification error",
		},
		{
			name: "🔴mismatch_body",
			JWTManager: JWTManager{
				PrivateKey: testECDSAKeypair1.privateKeyStr,
				PublicKey:  testECDSAKeypair1.publicKeyStr,
			},
			jwtBody:         []byte(`{"foo": "bar"}`),
			requestBody:     []byte(`{"x": "y"}`),
			expiresAt:       validTimestamp,
			wantErrContains: "the claims' hashed body does not match the request body's hash",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			token, err := tc.JWTManager.GenerateToken(tc.jwtBody, tc.expiresAt)
			require.NoError(t, err)

			parsedToken, parsedClaims, err := tc.JWTManager.ParseToken(token, tc.requestBody)
			if tc.wantErrContains != "" {
				assert.ErrorContains(t, err, tc.wantErrContains)
				assert.Nil(t, parsedToken, "parsed token should be nil")
				assert.Nil(t, parsedClaims, "parsed claims should be nil")
				return
			} else {
				assert.NoError(t, err)
				assert.True(t, parsedToken.Valid, "parsed token should be valid")
				require.Equal(t, hashBody(tc.jwtBody), parsedClaims.HashedBody)
				require.Equal(t, hashBody(tc.requestBody), parsedClaims.HashedBody)
				require.Equal(t, jwtgo.NewNumericDate(tc.expiresAt), parsedClaims.ExpiresAt)
			}
		})
	}

	t.Run("🔴invalid_Public_Key", func(t *testing.T) {
		jwtManager := &JWTManager{PublicKey: "invalid-public-key"}
		parsedToken, parsedClaims, err := jwtManager.ParseToken("eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJoYXNoZWRfYm9keSI6IjQyNmZjMDRmMDRiZjhmZGI1ODMxZGMzN2JiYjZkY2Y3MGY2M2EzN2UwNWE2OGM2ZWE1ZjYzZTg1YWU1NzkzNzYiLCJleHAiOjE3NDY2NDU5MjB9.5IyKtf8NWljLlD8TFyseDXi7ufxI92xXwvBJ56BNuqM1Z1ofg5umhpILtlXnMZb-7NpykQmTSKHOhOjA1XMjuQ", []byte("some-body"))
		assert.ErrorContains(t, err, "parsing EC Public Key")
		assert.Nil(t, parsedToken, "parsed token should be nil")
		assert.Nil(t, parsedClaims, "parsed claims should be nil")
	})

	t.Run("🔴invalid_Private_Key", func(t *testing.T) {
		jwtManager := &JWTManager{PrivateKey: "invalid-private-key"}
		token, err := jwtManager.GenerateToken([]byte("some-body"), time.Now().Add(time.Hour))
		assert.ErrorContains(t, err, "parsing EC Private Key")
		assert.Empty(t, token, "token should be empty")
	})
}
