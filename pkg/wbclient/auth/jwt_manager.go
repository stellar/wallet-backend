package auth

import (
	"bytes"
	"crypto/ed25519"
	"fmt"
	"time"

	jwtgo "github.com/golang-jwt/jwt/v4"
	"github.com/stellar/go/strkey"
)

const DefaultMaxTimeout = 15 * time.Second

type customClaims struct {
	HashedBody string `json:"hashed_body"`
	jwtgo.RegisteredClaims
}

type JWTManager struct {
	PrivateKey string
	PublicKey  string
	MaxTimeout time.Duration
}

type JWTTokenParser interface {
	// ParseJWT parses a JWT token and returns it with the claims.
	ParseJWT(tokenString string, body []byte) (*jwtgo.Token, *customClaims, error)
}

type JWTTokenGenerator interface {
	// GenerateJWT generates a JWT token with the given body and expiration time.
	GenerateJWT(body []byte, expiresAt time.Time) (string, error)
}

// ParseJWT parses a JWT token and returns it with the claims. It also checks if the token expiration is within [now,
// now+MaxTimeout], and if the claims' hashed_body matches the requestBody's hash.
func (m *JWTManager) ParseJWT(tokenString string, body []byte) (*jwtgo.Token, *customClaims, error) {
	claims := &customClaims{}
	token, err := jwtgo.ParseWithClaims(tokenString, claims, func(t *jwtgo.Token) (interface{}, error) {
		pubKeyBytes, err := strkey.Decode(strkey.VersionByteAccountID, m.PublicKey)
		if err != nil {
			return nil, fmt.Errorf("decoding Stellar public key: %w", err)
		}

		return ed25519.PublicKey(pubKeyBytes), nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("parsing JWT token with claims: %w", err)
	}

	if claims.HashedBody != HashBody(body) {
		return nil, nil, fmt.Errorf("the claims' hashed body does not match the request body's hash")
	}

	maxTimeout := m.MaxTimeout
	if maxTimeout == 0 {
		maxTimeout = DefaultMaxTimeout
	}
	if claims.ExpiresAt.After(time.Now().Add(maxTimeout)) {
		return nil, nil, fmt.Errorf("the token expiration is too long, max timeout is %s", maxTimeout)
	}

	return token, claims, nil
}

// GenerateJWT generates a JWT token with the given body and expiration time.
func (m *JWTManager) GenerateJWT(body []byte, expiresAt time.Time) (string, error) {
	privateKeyBytes, err := strkey.Decode(strkey.VersionByteSeed, m.PrivateKey)
	if err != nil {
		return "", fmt.Errorf("decoding Stellar private key: %w", err)
	}

	_, ed25519PrivateKey, err := ed25519.GenerateKey(bytes.NewReader(privateKeyBytes))
	if err != nil {
		return "", fmt.Errorf("generating ed25519 key pair: %w", err)
	}

	claims := &customClaims{
		HashedBody: HashBody(body),
		RegisteredClaims: jwtgo.RegisteredClaims{
			ExpiresAt: jwtgo.NewNumericDate(expiresAt),
		},
	}

	token := jwtgo.NewWithClaims(jwtgo.SigningMethodEdDSA, claims)
	tokenString, err := token.SignedString(ed25519PrivateKey)
	if err != nil {
		return "", fmt.Errorf("signing JWT token with claims: %w", err)
	}

	return tokenString, nil
}

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

func NewJWTTokenParser(stellarPublicKey string, maxTimeout time.Duration) (JWTTokenParser, error) {
	if !strkey.IsValidEd25519PublicKey(stellarPublicKey) {
		return nil, fmt.Errorf("invalid Stellar public key")
	}

	if maxTimeout <= 0 {
		maxTimeout = DefaultMaxTimeout
	}

	return &JWTManager{PublicKey: stellarPublicKey, MaxTimeout: maxTimeout}, nil
}

func NewJWTTokenGenerator(stellarPrivateKey string) (JWTTokenGenerator, error) {
	if !strkey.IsValidEd25519SecretSeed(stellarPrivateKey) {
		return nil, fmt.Errorf("invalid Stellar private key")
	}

	return &JWTManager{PrivateKey: stellarPrivateKey}, nil
}

var (
	_ JWTTokenParser    = (*JWTManager)(nil)
	_ JWTTokenGenerator = (*JWTManager)(nil)
)
