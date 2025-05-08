package auth

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	jwtgo "github.com/golang-jwt/jwt/v5"
	"github.com/stellar/go/strkey"
)

type customClaims struct {
	BodyHash      string `json:"bodyHash"`
	MethodAndPath string `json:"methodAndPath"`
	jwtgo.RegisteredClaims
}

func (c *customClaims) Validate(audience, methodAndPath string, body []byte, maxTimeout time.Duration) error {
	if maxTimeout == 0 {
		maxTimeout = DefaultMaxTimeout
	}

	if c.ExpiresAt == nil {
		return errors.New("the JWT expiration is not set")
	}
	if c.IssuedAt == nil {
		return errors.New("the JWT issuance time is not set")
	}

	if c.ExpiresAt.After(time.Now().Add(maxTimeout)) {
		return fmt.Errorf("the JWT expiration is too long, max timeout is %s", maxTimeout)
	}
	if c.ExpiresAt.Sub(c.IssuedAt.Time) > maxTimeout {
		return fmt.Errorf("the difference between JWT expiration and issuance time is too long, max is %s", maxTimeout)
	}

	if !strkey.IsValidEd25519PublicKey(c.Subject) {
		return errors.New("the JWT subject is not a valid Stellar public key")
	}

	if audience != "" && !slices.Contains(c.Audience, audience) {
		return fmt.Errorf("the JWT audience %s does not match the expected audience [%s]", c.Audience, audience)
	}

	if c.MethodAndPath != strings.TrimSpace(methodAndPath) {
		return fmt.Errorf("the JWT method-and-path %q does not match the expected method-and-path %q", c.MethodAndPath, methodAndPath)
	}

	if c.BodyHash != HashBody(body) {
		return errors.New("the JWT hashed body does not match the expected value")
	}

	return nil
}

// DecodeTokenString parses the token string and populates the customClaims object.
func (c *customClaims) DecodeTokenString(tokenString string) error {
	// Parse the token without verifying the signature
	token, _, err := new(jwtgo.Parser).ParseUnverified(tokenString, c)
	if err != nil {
		return fmt.Errorf("parsing token: %w", err)
	}

	if _, ok := token.Claims.(*customClaims); !ok {
		return fmt.Errorf("invalid claims type")
	}

	return nil
}
