package auth

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"strings"
	"time"

	jwtgo "github.com/golang-jwt/jwt/v5"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/strkey"
)

const DefaultMaxTimeout = 15 * time.Second

type JWTManager struct {
	PrivateKey string
	PublicKey  string
	MaxTimeout time.Duration
}

type JWTTokenParser interface {
	// ParseJWT parses a JWT token and returns it with the claims.
	ParseJWT(tokenString, audience, uri string, body []byte) (*jwtgo.Token, *customClaims, error)
}

type JWTTokenGenerator interface {
	// GenerateJWT generates a JWT token with the given body and expiration time.
	GenerateJWT(audience, uri string, body []byte, expiresAt time.Time) (string, error)
}

// ParseJWT parses a JWT token and returns it with the claims. It also checks if the token expiration is within [now,
// now+MaxTimeout], and if the claims' hashed_body matches the requestBody's hash.
func (m *JWTManager) ParseJWT(tokenString, audience, uri string, body []byte) (*jwtgo.Token, *customClaims, error) {
	claims := &customClaims{}
	err := claims.DecodeTokenString(tokenString)
	if err != nil {
		return nil, nil, fmt.Errorf("decoding JWT token: %w", err)
	}

	err = claims.Validate(audience, uri, body, m.MaxTimeout)
	if err != nil {
		return nil, nil, fmt.Errorf("pre-validating JWT token claims: %w", err)
	}

	if claims.Subject != m.PublicKey {
		return nil, nil, fmt.Errorf("the JWT token is not signed by the expected Stellar public key")
	}

	token, err := jwtgo.ParseWithClaims(tokenString, claims, func(t *jwtgo.Token) (interface{}, error) {
		pubKeyBytes, innerErr := strkey.Decode(strkey.VersionByteAccountID, m.PublicKey)
		if innerErr != nil {
			return nil, fmt.Errorf("decoding Stellar public key: %w", innerErr)
		}

		return ed25519.PublicKey(pubKeyBytes), nil
	})
	if err != nil {
		if errors.Is(err, jwtgo.ErrTokenExpired) {
			return nil, nil, &ExpiredTokenError{ExpiredBy: time.Since(claims.ExpiresAt.Time), InnerErr: err}
		}
		return nil, nil, fmt.Errorf("parsing JWT token with claims: %w", err)
	}

	return token, claims, nil
}

// GenerateJWT generates a JWT token with the given body and expiration time.
func (m *JWTManager) GenerateJWT(audience, uri string, body []byte, expiresAt time.Time) (string, error) {
	claims := &customClaims{
		BodyHash: HashBody(body),
		URI:      strings.TrimSpace(uri),
		RegisteredClaims: jwtgo.RegisteredClaims{
			IssuedAt:  jwtgo.NewNumericDate(time.Now()),
			ExpiresAt: jwtgo.NewNumericDate(expiresAt),
			Audience:  jwtgo.ClaimStrings{audience},
			Subject:   m.PublicKey,
		},
	}
	token := jwtgo.NewWithClaims(jwtgo.SigningMethodEdDSA, claims)

	privateKeyBytes, err := strkey.Decode(strkey.VersionByteSeed, m.PrivateKey)
	if err != nil {
		return "", fmt.Errorf("decoding Stellar private key: %w", err)
	}

	tokenString, err := token.SignedString(ed25519.NewKeyFromSeed(privateKeyBytes))
	if err != nil {
		return "", fmt.Errorf("signing JWT token with claims: %w", err)
	}

	return tokenString, nil
}

// NewJWTManager creates a new JWT token manager that can generate and parse JWT tokens.
func NewJWTManager(stellarPrivateKey string, stellarPublicKey string, maxTimeout time.Duration) (*JWTManager, error) {
	if !strkey.IsValidEd25519PublicKey(stellarPublicKey) {
		return nil, fmt.Errorf("invalid Stellar public key")
	}

	if !strkey.IsValidEd25519SecretSeed(stellarPrivateKey) {
		return nil, fmt.Errorf("invalid Stellar private key")
	}

	if maxTimeout <= 0 {
		maxTimeout = DefaultMaxTimeout
	}

	return &JWTManager{
		PrivateKey: stellarPrivateKey,
		PublicKey:  stellarPublicKey,
		MaxTimeout: maxTimeout,
	}, nil
}

// NewJWTTokenParser creates a new JWT token parser that can parse a JWT token as long as it has been signed by the provided Stellar public key.
func NewJWTTokenParser(maxTimeout time.Duration, stellarPublicKey string) (JWTTokenParser, error) {
	if !strkey.IsValidEd25519PublicKey(stellarPublicKey) {
		return nil, fmt.Errorf("invalid Stellar public key")
	}

	if maxTimeout <= 0 {
		maxTimeout = DefaultMaxTimeout
	}

	return &JWTManager{PublicKey: stellarPublicKey, MaxTimeout: maxTimeout}, nil
}

// NewMultiJWTTokenParser creates a new JWT token parser that can parse a JWT token as long as it has been signed by an least one of the provided Stellar public keys.
func NewMultiJWTTokenParser(maxTimeout time.Duration, stellarPublicKeys ...string) (JWTTokenParser, error) {
	if len(stellarPublicKeys) == 0 {
		return nil, fmt.Errorf("no Stellar public keys provided")
	}

	jwtParsers := map[string]JWTTokenParser{}
	for _, pubKey := range stellarPublicKeys {
		jwtParser, err := NewJWTTokenParser(maxTimeout, pubKey)
		if err != nil {
			return nil, fmt.Errorf("creating JWT parser: %w", err)
		}
		jwtParsers[pubKey] = jwtParser
	}

	return &MultiJWTTokenParser{jwtParsers}, nil
}

type MultiJWTTokenParser struct {
	// jwtParsers is a map of Stellar public keys to JWT token parsers.
	jwtParsers map[string]JWTTokenParser
}

func (m MultiJWTTokenParser) ParseJWT(tokenString, audience, uri string, body []byte) (*jwtgo.Token, *customClaims, error) {
	claims := &customClaims{}
	err := claims.DecodeTokenString(tokenString)
	if err != nil {
		return nil, nil, fmt.Errorf("decoding JWT token: %w", err)
	}

	jwtParser, ok := m.jwtParsers[claims.Subject]
	if !ok {
		return nil, nil, fmt.Errorf("the JWT token subject %s is not one of the expected Stellar public keys", claims.Subject)
	}

	//nolint:wrapcheck // we're ok not wrapping the error here because we're not adding any additional context
	return jwtParser.ParseJWT(tokenString, audience, uri, body)
}

func NewJWTTokenGenerator(stellarPrivateKey string) (JWTTokenGenerator, error) {
	kp, err := keypair.ParseFull(stellarPrivateKey)
	if err != nil {
		return nil, fmt.Errorf("invalid Stellar private key: %w", err)
	}

	return &JWTManager{PrivateKey: kp.Seed(), PublicKey: kp.Address()}, nil
}

var (
	_ JWTTokenParser    = (*JWTManager)(nil)
	_ JWTTokenParser    = (*MultiJWTTokenParser)(nil)
	_ JWTTokenGenerator = (*JWTManager)(nil)
)
