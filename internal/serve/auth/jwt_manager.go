package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	jwtgo "github.com/golang-jwt/jwt/v4"
)

const DefaultMaxTimeout = 15 * time.Second

type customClaims struct {
	HashedBody string `json:"hashed_body"`
	jwtgo.RegisteredClaims
}

// JWTManager
type JWTManager struct {
	PrivateKey string
	PublicKey  string
	MaxTimeout time.Duration
}

// ParseToken parses a JWT token and returns the claims. It also checks if the token expiration is within [now,
// now+MaxTimeout], and if the claims' hashed_body matches the requestBody's hash.
func (m *JWTManager) ParseToken(tokenString string, body []byte) (*jwtgo.Token, *customClaims, error) {
	claims := &customClaims{}
	token, err := jwtgo.ParseWithClaims(tokenString, claims, func(t *jwtgo.Token) (interface{}, error) {
		esPublicKey, err := jwtgo.ParseECPublicKeyFromPEM([]byte(m.PublicKey))
		if err != nil {
			return nil, fmt.Errorf("parsing EC Public Key: %w", err)
		}

		return esPublicKey, nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("parsing JWT token with claims: %w", err)
	}

	if claims.HashedBody != hashBody(body) {
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

// GenerateToken generates a JWT token with the given body and expiration time.
func (m *JWTManager) GenerateToken(body []byte, expiresAt time.Time) (string, error) {
	esPrivateKey, err := jwtgo.ParseECPrivateKeyFromPEM([]byte(m.PrivateKey))
	if err != nil {
		return "", fmt.Errorf("parsing EC Private Key: %w", err)
	}

	claims := &customClaims{
		HashedBody: hashBody(body),
		RegisteredClaims: jwtgo.RegisteredClaims{
			ExpiresAt: jwtgo.NewNumericDate(expiresAt),
		},
	}

	token := jwtgo.NewWithClaims(jwtgo.SigningMethodES256, claims)
	tokenString, err := token.SignedString(esPrivateKey)
	if err != nil {
		return "", fmt.Errorf("signing JWT token with claims: %w", err)
	}

	return tokenString, nil
}

// hashBody returns the SHA-256 hash of the body.
func hashBody(body []byte) string {
	hashedBodyBytes := sha256.Sum256(body)
	return hex.EncodeToString(hashedBodyBytes[:])
}
