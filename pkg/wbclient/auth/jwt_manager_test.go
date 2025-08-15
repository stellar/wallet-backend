package auth

import (
	"testing"
	"time"

	jwtgo "github.com/golang-jwt/jwt/v5"
	"github.com/stellar/go/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// GATTZ2V2VLTJ7VLYK5XI6FQTUYYB5W6H7HUQ5JJ7HCKJVX32RYXALO3S
var testKP1 = keypair.MustParseFull("SAAT4JQDSVHVLTFHXNYOMQ6FAXBDUK4W4KQA6CC5GMHXU2ZQYJAZ5R4E")

// GBQFTE2O5A7RIXVFQFRVWSSMQRPF5PKSYRZCQV6DX5XH4KGUG6HEWN2M
var testKP2 = keypair.MustParseFull("SBE6BQJ2UFS2PEJRXXEEAZ7EOPQF2GHBGQBKK2JZNXL34OCZJLY35PXG")

func Test_JWTManager_GenerateAndParseToken(t *testing.T) {
	expiredTimestamp := time.Now().Add(-time.Nanosecond)
	validTimestamp := time.Now().Add(time.Second * 2)
	tooLongTimestamp := time.Now().Add(DefaultMaxTimeout * 2)

	validMethodAndPath := "GET /valid-route"
	invalidMethodAndPath := "GET /invalid-route"

	testCases := []struct {
		name            string
		JWTManager      func(t *testing.T) *JWTManager
		jwtBody         []byte
		requestBody     []byte
		uri             string
		expiresAt       time.Time
		wantErrContains string
	}{
		{
			name: "🟢valid_KP_1_no_body",
			JWTManager: func(t *testing.T) *JWTManager {
				jwtManager, err := NewJWTManager(testKP1.Seed(), testKP1.Address(), 0)
				require.NoError(t, err)
				return jwtManager
			},
			jwtBody:         nil,
			requestBody:     nil,
			uri:             validMethodAndPath,
			expiresAt:       validTimestamp,
			wantErrContains: "",
		},
		{
			name: "🟢valid_KP_1_with_body",
			JWTManager: func(t *testing.T) *JWTManager {
				jwtManager, err := NewJWTManager(testKP1.Seed(), testKP1.Address(), 0)
				require.NoError(t, err)
				return jwtManager
			},
			jwtBody:         []byte(`{"foo": "bar"}`),
			requestBody:     []byte(`{"foo": "bar"}`),
			uri:             validMethodAndPath,
			expiresAt:       validTimestamp,
			wantErrContains: "",
		},
		{
			name: "🟢valid_KP_2_no_body",
			JWTManager: func(t *testing.T) *JWTManager {
				jwtManager, err := NewJWTManager(testKP2.Seed(), testKP2.Address(), 0)
				require.NoError(t, err)
				return jwtManager
			},
			jwtBody:         nil,
			requestBody:     nil,
			uri:             validMethodAndPath,
			expiresAt:       validTimestamp,
			wantErrContains: "",
		},
		{
			name: "🟢valid_KP_2_with_body",
			JWTManager: func(t *testing.T) *JWTManager {
				jwtManager, err := NewJWTManager(testKP2.Seed(), testKP2.Address(), 0)
				require.NoError(t, err)
				return jwtManager
			},
			jwtBody:         []byte(`{"foo": "bar"}`),
			requestBody:     []byte(`{"foo": "bar"}`),
			uri:             validMethodAndPath,
			expiresAt:       validTimestamp,
			wantErrContains: "",
		},
		{
			name: "🔴expired",
			JWTManager: func(t *testing.T) *JWTManager {
				jwtManager, err := NewJWTManager(testKP1.Seed(), testKP1.Address(), 0)
				require.NoError(t, err)
				return jwtManager
			},
			uri:             validMethodAndPath,
			expiresAt:       expiredTimestamp,
			wantErrContains: "the JWT token has expired by",
		},
		{
			name: "🔴expiration_is_too_long",
			JWTManager: func(t *testing.T) *JWTManager {
				jwtManager, err := NewJWTManager(testKP1.Seed(), testKP1.Address(), DefaultMaxTimeout)
				require.NoError(t, err)
				return jwtManager
			},
			uri:             validMethodAndPath,
			expiresAt:       tooLongTimestamp,
			wantErrContains: "pre-validating JWT token claims: the JWT expiration is too long, max timeout is 15s",
		},
		{
			name: "🔴mismatch_KP",
			JWTManager: func(t *testing.T) *JWTManager {
				jwtManager, err := NewJWTManager(testKP1.Seed(), testKP2.Address(), 0)
				require.NoError(t, err)
				return jwtManager
			},
			uri:             validMethodAndPath,
			expiresAt:       validTimestamp,
			wantErrContains: "parsing JWT token with claims: token signature is invalid: ed25519: verification error",
		},
		{
			name: "🔴mismatch_body",
			JWTManager: func(t *testing.T) *JWTManager {
				jwtManager, err := NewJWTManager(testKP1.Seed(), testKP1.Address(), 0)
				require.NoError(t, err)
				return jwtManager
			},
			jwtBody:         []byte(`{"foo": "bar"}`),
			requestBody:     []byte(`{"x": "y"}`),
			uri:             validMethodAndPath,
			expiresAt:       validTimestamp,
			wantErrContains: "pre-validating JWT token claims: the JWT hashed body does not match the expected value",
		},
		{
			name: "🔴invalid_method_and_path",
			JWTManager: func(t *testing.T) *JWTManager {
				jwtManager, err := NewJWTManager(testKP1.Seed(), testKP1.Address(), 0)
				require.NoError(t, err)
				return jwtManager
			},
			uri:             invalidMethodAndPath,
			expiresAt:       validTimestamp,
			wantErrContains: `pre-validating JWT token claims: the JWT method-and-path "GET /invalid-route" does not match the expected method-and-path "GET /valid-route"`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			jwtManager := tc.JWTManager(t)
			token, err := jwtManager.GenerateJWT(tc.uri, tc.jwtBody, tc.expiresAt)
			require.NoError(t, err)

			parsedToken, parsedClaims, err := jwtManager.ParseJWT(token, validMethodAndPath, tc.requestBody)
			if tc.wantErrContains != "" {
				assert.ErrorContains(t, err, tc.wantErrContains)
				assert.Nil(t, parsedToken, "parsed token should be nil")
				assert.Nil(t, parsedClaims, "parsed claims should be nil")
			} else {
				assert.NoError(t, err)
				assert.True(t, parsedToken.Valid, "parsed token should be valid")
				require.Equal(t, HashBody(tc.jwtBody), parsedClaims.BodyHash)
				require.Equal(t, HashBody(tc.requestBody), parsedClaims.BodyHash)
				require.Equal(t, jwtgo.NewNumericDate(tc.expiresAt), parsedClaims.ExpiresAt)
			}
		})
	}

	jwtManager, err := NewJWTManager(testKP1.Seed(), testKP1.Address(), 0)
	require.NoError(t, err)
	validToken, err := jwtManager.GenerateJWT(validMethodAndPath, nil, validTimestamp)
	require.NoError(t, err)

	t.Run("🔴invalid_Public_Key", func(t *testing.T) {
		jwtManager := &JWTManager{PublicKey: "invalid-public-key"}
		parsedToken, parsedClaims, err := jwtManager.ParseJWT(validToken, validMethodAndPath, nil)
		assert.ErrorContains(t, err, "the JWT token is not signed by the expected Stellar public key")
		assert.Nil(t, parsedToken, "parsed token should be nil")
		assert.Nil(t, parsedClaims, "parsed claims should be nil")
	})

	t.Run("🔴invalid_Private_Key", func(t *testing.T) {
		jwtManager := &JWTManager{PrivateKey: "invalid-private-key"}
		token, err := jwtManager.GenerateJWT(validMethodAndPath, nil, validTimestamp)
		assert.ErrorContains(t, err, "decoding Stellar private key")
		assert.Empty(t, token, "token should be empty")
	})
}

func Test_NewJWTManager(t *testing.T) {
	testCases := []struct {
		name              string
		stellarPrivateKey string
		stellarPublicKey  string
		maxTimeout        time.Duration
		wantErrContains   string
		wantJWTManager    *JWTManager
	}{
		{
			name:              "🔴invalid_Public_Key",
			stellarPrivateKey: testKP1.Seed(),
			stellarPublicKey:  "invalid-public-key",
			wantErrContains:   "invalid Stellar public key",
			wantJWTManager:    nil,
		},
		{
			name:              "🔴invalid_Private_Key",
			stellarPrivateKey: "invalid-private-key",
			stellarPublicKey:  testKP1.Address(),
			wantErrContains:   "invalid Stellar private key",
			wantJWTManager:    nil,
		},
		{
			name:              "🟢valid_KP_1",
			stellarPrivateKey: testKP1.Seed(),
			stellarPublicKey:  testKP1.Address(),
			wantErrContains:   "",
			wantJWTManager: &JWTManager{
				PrivateKey: testKP1.Seed(),
				PublicKey:  testKP1.Address(),
				MaxTimeout: DefaultMaxTimeout,
			},
		},
		{
			name:              "🟢valid_KP_2",
			stellarPrivateKey: testKP2.Seed(),
			stellarPublicKey:  testKP2.Address(),
			wantErrContains:   "",
			wantJWTManager: &JWTManager{
				PrivateKey: testKP2.Seed(),
				PublicKey:  testKP2.Address(),
				MaxTimeout: DefaultMaxTimeout,
			},
		},
		{
			name:              "🟢valid_with_custom_max_timeout",
			stellarPrivateKey: testKP1.Seed(),
			stellarPublicKey:  testKP1.Address(),
			maxTimeout:        10 * time.Second,
			wantErrContains:   "",
			wantJWTManager: &JWTManager{
				PrivateKey: testKP1.Seed(),
				PublicKey:  testKP1.Address(),
				MaxTimeout: 10 * time.Second,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			jwtManager, err := NewJWTManager(tc.stellarPrivateKey, tc.stellarPublicKey, tc.maxTimeout)
			if tc.wantErrContains != "" {
				assert.ErrorContains(t, err, tc.wantErrContains)
				assert.Nil(t, jwtManager, "jwt manager should be nil")
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.wantJWTManager, jwtManager)
			}
		})
	}
}

func Test_NewJWTTokenParser(t *testing.T) {
	testCases := []struct {
		name            string
		publicKey       string
		reqBody         []byte
		wantErrContains string
	}{
		{
			name:            "🔴invalid_Public_Key",
			publicKey:       "invalid-public-key",
			wantErrContains: "invalid Stellar public key",
		},
		{
			name:      "🟢valid_publicKey_with_body",
			publicKey: testKP1.Address(),
			reqBody:   []byte(`{"foo": "bar"}`),
		},
		{
			name:      "🟢valid_publicKey_no_body",
			publicKey: testKP1.Address(),
			reqBody:   nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			jwtTokenParser, err := NewJWTTokenParser(0, tc.publicKey)
			if tc.wantErrContains != "" {
				assert.ErrorContains(t, err, tc.wantErrContains)
				assert.Nil(t, jwtTokenParser, "jwt token parser should be nil")
			} else {
				require.NoError(t, err)

				// Generate a JWT token
				jwtManager, err := NewJWTManager(testKP1.Seed(), testKP1.Address(), 0)
				require.NoError(t, err)
				token, err := jwtManager.GenerateJWT("", tc.reqBody, time.Now().Add(time.Second*2))
				require.NoError(t, err)

				// Parse the JWT token
				parsedToken, parsedClaims, err := jwtTokenParser.ParseJWT(token, "", tc.reqBody)
				require.NoError(t, err)
				assert.True(t, parsedToken.Valid, "parsed token should be valid")
				require.Equal(t, HashBody(tc.reqBody), parsedClaims.BodyHash)
			}
		})
	}
}

func Test_NewMultiJWTTokenParser(t *testing.T) {
	testCases := []struct {
		name            string
		publicKeys      []string
		reqBody         []byte
		wantErrContains string
	}{
		{
			name:            "🔴invalid_Public_Key",
			publicKeys:      []string{"invalid-public-key"},
			wantErrContains: "invalid Stellar public key",
		},
		{
			name:       "🟢valid_publicKey1_with_body",
			publicKeys: []string{testKP1.Address()},
			reqBody:    []byte(`{"foo": "bar"}`),
		},
		{
			name:       "🟢valid_publicKey1_no_body",
			publicKeys: []string{testKP1.Address()},
			reqBody:    nil,
		},
		{
			name:       "🟢valid_publicKey2_with_body",
			publicKeys: []string{testKP2.Address(), testKP1.Address()},
			reqBody:    []byte(`{"foo": "bar"}`),
		},
		{
			name:       "🟢valid_publicKey2_no_body",
			publicKeys: []string{testKP1.Address(), testKP2.Address()},
			reqBody:    nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			jwtTokenParser, err := NewMultiJWTTokenParser(0, tc.publicKeys...)
			if tc.wantErrContains != "" {
				assert.ErrorContains(t, err, tc.wantErrContains)
				assert.Nil(t, jwtTokenParser, "jwt token parser should be nil")
			} else {
				require.NoError(t, err)

				// Generate a JWT token
				jwtManager, err := NewJWTManager(testKP1.Seed(), testKP1.Address(), 0)
				require.NoError(t, err)
				token, err := jwtManager.GenerateJWT("", tc.reqBody, time.Now().Add(time.Second*2))
				require.NoError(t, err)

				// Parse the JWT token
				parsedToken, parsedClaims, err := jwtTokenParser.ParseJWT(token, "", tc.reqBody)
				require.NoError(t, err)
				assert.True(t, parsedToken.Valid, "parsed token should be valid")
				require.Equal(t, HashBody(tc.reqBody), parsedClaims.BodyHash)
			}
		})
	}
}

func Test_NewJWTTokenGenerator(t *testing.T) {
	testCases := []struct {
		name            string
		privateKey      string
		reqBody         []byte
		wantErrContains string
	}{
		{
			name:            "🔴invalid_Private_Key",
			privateKey:      "invalid-private-key",
			wantErrContains: "invalid Stellar private key",
		},
		{
			name:       "🟢valid_Private_Key_with_body",
			privateKey: testKP1.Seed(),
			reqBody:    []byte(`{"foo": "bar"}`),
		},
		{
			name:       "🟢valid_Private_Key_no_body",
			privateKey: testKP1.Seed(),
			reqBody:    nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			jwtTokenGenerator, err := NewJWTTokenGenerator(tc.privateKey)
			if tc.wantErrContains != "" {
				assert.ErrorContains(t, err, tc.wantErrContains)
				assert.Nil(t, jwtTokenGenerator, "jwt token generator should be nil")
			} else {
				require.NoError(t, err)
				assert.NotNil(t, jwtTokenGenerator, "jwt token generator should not be nil")

				// Generate a JWT token
				token, err := jwtTokenGenerator.GenerateJWT("", tc.reqBody, time.Now().Add(time.Second*2))
				require.NoError(t, err)
				assert.NotEmpty(t, token, "token should not be empty")
			}
		})
	}
}
